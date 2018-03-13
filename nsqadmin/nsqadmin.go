package nsqadmin

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/youzan/nsq/internal/http_api"
	"github.com/youzan/nsq/internal/util"
	"github.com/youzan/nsq/internal/version"
)

type NSQAdmin struct {
	sync.RWMutex
	opts                *Options
	httpListener        net.Listener
	waitGroup           util.WaitGroupWrapper
	notifications       chan *AdminAction
	graphiteURL         *url.URL
	httpClientTLSConfig *tls.Config
	accessTokens map[string]bool
}

func New(opts *Options) *NSQAdmin {
	adminLog.Logger = opts.Logger
	n := &NSQAdmin{
		opts:          opts,
		notifications: make(chan *AdminAction),
	}
	if opts.AuthUrl != "" {
		if opts.AuthSecret == "" {
			n.logf("FATAL: authentication secret could not be empty")
			os.Exit(1)
		}
		if opts.AuthUrl == "" {
			n.logf("FATAL: authentication url could not be empty")
			os.Exit(1)
		} else {
			authUrl, err := url.Parse(opts.AuthUrl)
			v, err := url.ParseQuery(authUrl.RawQuery)
			if err != nil {
				n.logf("FATAL: failed to resolve cas queries (%s) - %s", authUrl.RawQuery, err)
				os.Exit(1)
			}
			v.Add("name", opts.AppName)
			authUrl.RawQuery = v.Encode()
			opts.AuthUrl = authUrl.String()
		}
		if opts.LogoutUrl == "" {
			n.logf("FATAL: failed to resolve cas address (%s)", opts.LogoutUrl)
			os.Exit(1)
		} else {
			logoutUrl, err := url.Parse(opts.LogoutUrl)
			if err != nil {
				n.logf("FATAL: failed to resolve cas address (%s) - %s", opts.LogoutUrl, err)
				os.Exit(1)
			}
			v, err := url.ParseQuery(logoutUrl.RawQuery)
			if err != nil {
				n.logf("FATAL: failed to resolve cas queries (%s) - %s", logoutUrl.RawQuery, err)
				os.Exit(1)
			}
			v.Add("redirect", opts.RedirectUrl)
			logoutUrl.RawQuery = v.Encode()
			opts.LogoutUrl = logoutUrl.String()
		}

		if len(opts.AccessTokens) > 0 {
			n.accessTokens = make(map[string]bool)
			for _, k := range opts.AccessTokens {
				n.accessTokens[k] = true
			}
		}
	}

	if len(opts.NSQDHTTPAddresses) == 0 && len(opts.NSQLookupdHTTPAddresses) == 0 {
		n.logf("--nsqd-http-address or --lookupd-http-address required.")
		os.Exit(1)
	}

	if len(opts.NSQDHTTPAddresses) != 0 && len(opts.NSQLookupdHTTPAddresses) != 0 {
		n.logf("use --nsqd-http-address or --lookupd-http-address not both")
		os.Exit(1)
	}

	// verify that the supplied address is valid
	verifyAddress := func(arg string, address string) *net.TCPAddr {
		addr, err := net.ResolveTCPAddr("tcp", address)
		if err != nil {
			n.logf("FATAL: failed to resolve %s address (%s) - %s", arg, address, err)
			os.Exit(1)
		}
		return addr
	}

	if opts.HTTPClientTLSCert != "" && opts.HTTPClientTLSKey == "" {
		n.logf("FATAL: --http-client-tls-key must be specified with --http-client-tls-cert")
		os.Exit(1)
	}

	if opts.HTTPClientTLSKey != "" && opts.HTTPClientTLSCert == "" {
		n.logf("FATAL: --http-client-tls-cert must be specified with --http-client-tls-key")
		os.Exit(1)
	}

	n.httpClientTLSConfig = &tls.Config{
		InsecureSkipVerify: opts.HTTPClientTLSInsecureSkipVerify,
	}
	if opts.HTTPClientTLSCert != "" && opts.HTTPClientTLSKey != "" {
		cert, err := tls.LoadX509KeyPair(opts.HTTPClientTLSCert, opts.HTTPClientTLSKey)
		if err != nil {
			n.logf("FATAL: failed to LoadX509KeyPair %s, %s - %s",
				opts.HTTPClientTLSCert, opts.HTTPClientTLSKey, err)
			os.Exit(1)
		}
		n.httpClientTLSConfig.Certificates = []tls.Certificate{cert}
	}
	if opts.HTTPClientTLSRootCAFile != "" {
		tlsCertPool := x509.NewCertPool()
		caCertFile, err := ioutil.ReadFile(opts.HTTPClientTLSRootCAFile)
		if err != nil {
			n.logf("FATAL: failed to read TLS root CA file %s - %s",
				opts.HTTPClientTLSRootCAFile, err)
			os.Exit(1)
		}
		if !tlsCertPool.AppendCertsFromPEM(caCertFile) {
			n.logf("FATAL: failed to AppendCertsFromPEM %s", opts.HTTPClientTLSRootCAFile)
			os.Exit(1)
		}
		n.httpClientTLSConfig.ClientCAs = tlsCertPool
	}

	// require that both the hostname and port be specified
	for _, address := range opts.NSQLookupdHTTPAddresses {
		verifyAddress("--lookupd-http-address", address)
	}

	for _, address := range opts.NSQDHTTPAddresses {
		verifyAddress("--nsqd-http-address", address)
	}

	if opts.ProxyGraphite {
		url, err := url.Parse(opts.GraphiteURL)
		if err != nil {
			n.logf("FATAL: failed to parse --graphite-url='%s' - %s", opts.GraphiteURL, err)
			os.Exit(1)
		}
		n.graphiteURL = url
	}

	n.logf(version.String("nsqadmin"))

	return n
}

func (n *NSQAdmin) logf(f string, args ...interface{}) {
	if n.opts.Logger == nil {
		return
	}
	n.opts.Logger.Output(2, fmt.Sprintf(f, args...))
}

func (n *NSQAdmin) RealHTTPAddr() *net.TCPAddr {
	n.RLock()
	defer n.RUnlock()
	return n.httpListener.Addr().(*net.TCPAddr)
}

func (n *NSQAdmin) handleAdminActions() {
	for action := range n.notifications {
		content, err := json.Marshal(action)
		if err != nil {
			n.logf("ERROR: failed to serialize admin action - %s", err)
		}
		httpclient := &http.Client{Transport: http_api.NewDeadlineTransport(10 * time.Second)}
		n.logf("POSTing notification to %s", n.opts.NotificationHTTPEndpoint)
		resp, err := httpclient.Post(n.opts.NotificationHTTPEndpoint,
			"application/json", bytes.NewBuffer(content))
		if err != nil {
			n.logf("ERROR: failed to POST notification - %s", err)
		}
		resp.Body.Close()
	}
}

func (n *NSQAdmin) IsAuthEnabled() bool {
	return n.opts.AuthUrl != ""
}

func (n *NSQAdmin) Main() {
	httpListener, err := net.Listen("tcp", n.opts.HTTPAddress)
	if err != nil {
		n.logf("FATAL: listen (%s) failed - %s", n.opts.HTTPAddress, err)
		os.Exit(1)
	}
	n.Lock()
	n.httpListener = httpListener
	n.Unlock()
	httpServer := NewHTTPServer(&Context{n})
	n.waitGroup.Wrap(func() {
		http_api.Serve(n.httpListener, http_api.CompressHandler(httpServer), "HTTP", n.opts.Logger)
	})
	n.waitGroup.Wrap(func() { n.handleAdminActions() })
}

func (n *NSQAdmin) Exit() {
	n.httpListener.Close()
	close(n.notifications)
	n.waitGroup.Wait()
}
