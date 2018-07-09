package nsqdserver

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io/ioutil"
	"net"
	"os"
	"strconv"
	"sync/atomic"

	"github.com/youzan/nsq/consistence"
	"github.com/youzan/nsq/nsqd"

	"github.com/youzan/nsq/internal/http_api"
	"github.com/youzan/nsq/internal/protocol"
	"github.com/youzan/nsq/internal/util"
	"github.com/youzan/nsq/internal/version"
)

type NsqdServer struct {
	ctx           *context
	lookupPeers   atomic.Value
	waitGroup     util.WaitGroupWrapper
	tcpListener   net.Listener
	httpListener  net.Listener
	httpsListener net.Listener
	exitChan      chan int
}

const (
	TLSNotRequired = iota
	TLSRequiredExceptHTTP
	TLSRequired
)

func buildTLSConfig(opts *nsqd.Options) (*tls.Config, error) {
	var tlsConfig *tls.Config

	if opts.TLSCert == "" && opts.TLSKey == "" {
		return nil, nil
	}

	tlsClientAuthPolicy := tls.VerifyClientCertIfGiven

	cert, err := tls.LoadX509KeyPair(opts.TLSCert, opts.TLSKey)
	if err != nil {
		return nil, err
	}
	switch opts.TLSClientAuthPolicy {
	case "require":
		tlsClientAuthPolicy = tls.RequireAnyClientCert
	case "require-verify":
		tlsClientAuthPolicy = tls.RequireAndVerifyClientCert
	default:
		tlsClientAuthPolicy = tls.NoClientCert
	}

	tlsConfig = &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tlsClientAuthPolicy,
		MinVersion:   opts.TLSMinVersion,
		MaxVersion:   tls.VersionTLS12, // enable TLS_FALLBACK_SCSV prior to Go 1.5: https://go-review.googlesource.com/#/c/1776/
	}

	if opts.TLSRootCAFile != "" {
		tlsCertPool := x509.NewCertPool()
		caCertFile, err := ioutil.ReadFile(opts.TLSRootCAFile)
		if err != nil {
			return nil, err
		}
		if !tlsCertPool.AppendCertsFromPEM(caCertFile) {
			return nil, errors.New("failed to append certificate to pool")
		}
		tlsConfig.ClientCAs = tlsCertPool
	}

	tlsConfig.BuildNameToCertificate()

	return tlsConfig, nil
}

func NewNsqdServer(opts *nsqd.Options) (*nsqd.NSQD, *NsqdServer) {
	ip := opts.DecideBroadcast()
	if opts.StartAsFixMode {
		consistence.ForceFixLeaderData = true
		nsqd.NsqLogger().LogWarningf("starting in data fix mode...")
	}

	nsqdInstance := nsqd.New(opts)

	s := &NsqdServer{}
	ctx := &context{}
	ctx.nsqd = nsqdInstance
	_, tcpPort, _ := net.SplitHostPort(opts.TCPAddress)
	_, httpPort, _ := net.SplitHostPort(opts.HTTPAddress)
	rpcport := opts.RPCPort
	if rpcport != "" {
		ip = opts.BroadcastAddress
		consistence.SetCoordLogger(opts.Logger, opts.LogLevel)
		if opts.RetentionSizePerDay > 0 {
			consistence.MaxTopicRetentionSizePerDay = opts.RetentionSizePerDay
		}
		coord := consistence.NewNsqdCoordinator(opts.ClusterID, ip, tcpPort, rpcport, httpPort,
			strconv.FormatInt(opts.ID, 10), opts.DataPath, nsqdInstance)
		l, err := consistence.NewNsqdEtcdMgr(opts.ClusterLeadershipAddresses)
		if err != nil {
			nsqd.NsqLogger().LogErrorf("FATAL: failed to init etcd leadership - %s", err)
			os.Exit(1)
		}
		coord.SetLeadershipMgr(l)
		ctx.nsqdCoord = coord
	} else {
		nsqd.NsqLogger().LogWarningf("Start without nsqd coordinator enabled")
		ctx.nsqdCoord = nil
	}

	s.ctx = ctx

	s.exitChan = make(chan int)

	tlsConfig, err := buildTLSConfig(opts)
	if err != nil {
		nsqd.NsqLogger().LogErrorf("FATAL: failed to build TLS config - %s", err)
		os.Exit(1)
	}
	if tlsConfig == nil && opts.TLSRequired != TLSNotRequired {
		nsqd.NsqLogger().LogErrorf("FATAL: cannot require TLS client connections without TLS key and cert")
		os.Exit(1)
	}
	s.ctx.tlsConfig = tlsConfig
	s.ctx.nsqd.SetPubLoop(s.ctx.internalPubLoop)
	s.ctx.nsqd.SetReqToEndCB(s.ctx.internalRequeueToEnd)

	nsqd.NsqLogger().Logf(version.String("nsqd"))
	nsqd.NsqLogger().Logf("ID: %d", opts.ID)

	return nsqdInstance, s
}

func (s *NsqdServer) GetNsqdInstance() *nsqd.NSQD {
	return s.ctx.nsqd
}

func (s *NsqdServer) Exit() {
	nsqd.NsqLogger().Logf("nsqd server stopping.")
	if s.tcpListener != nil {
		s.tcpListener.Close()
	}
	if s.ctx.nsqdCoord != nil {
		s.ctx.nsqdCoord.Stop()
	}

	if s.httpListener != nil {
		s.httpListener.Close()
	}
	if s.httpsListener != nil {
		s.httpsListener.Close()
	}

	if s.ctx.nsqd != nil {
		s.ctx.nsqd.Exit()
	}

	close(s.exitChan)
	s.waitGroup.Wait()
	nsqd.NsqLogger().Logf("nsqd server stopped.")
}

func (s *NsqdServer) Main() {
	var httpListener net.Listener
	var httpsListener net.Listener

	if s.ctx.nsqdCoord != nil {
		err := s.ctx.nsqdCoord.Start()
		if err != nil {
			nsqd.NsqLogger().LogErrorf("FATAL: start coordinator failed - %v", err)
			os.Exit(1)
		}
	}

	opts := s.ctx.getOpts()
	tcpListener, err := net.Listen("tcp", opts.TCPAddress)
	if err != nil {
		nsqd.NsqLogger().LogErrorf("FATAL: listen (%s) failed - %s", opts.TCPAddress, err)
		os.Exit(1)
	}
	s.tcpListener = tcpListener
	s.ctx.tcpAddr = tcpListener.Addr().(*net.TCPAddr)
	nsqd.NsqLogger().Logf("TCP: listening on %s", tcpListener.Addr())

	tcpServer := &tcpServer{ctx: s.ctx}
	s.waitGroup.Wrap(func() {
		protocol.TCPServer(s.tcpListener, tcpServer)
		nsqd.NsqLogger().Logf("TCP: closing %s", s.tcpListener.Addr())
	})

	if s.ctx.GetTlsConfig() != nil && opts.HTTPSAddress != "" {
		httpsListener, err = tls.Listen("tcp", opts.HTTPSAddress, s.ctx.GetTlsConfig())
		if err != nil {
			nsqd.NsqLogger().LogErrorf("FATAL: listen (%s) failed - %s", opts.HTTPSAddress, err)
			os.Exit(1)
		}
		s.httpsListener = httpsListener
		httpsServer := newHTTPServer(s.ctx, true, true)
		s.waitGroup.Wrap(func() {
			http_api.Serve(s.httpsListener, httpsServer, "HTTPS", opts.Logger)
		})
	}
	httpListener, err = net.Listen("tcp", opts.HTTPAddress)
	if err != nil {
		nsqd.NsqLogger().LogErrorf("FATAL: listen (%s) failed - %s", opts.HTTPAddress, err)
		os.Exit(1)
	}
	s.httpListener = httpListener
	s.ctx.httpAddr = httpListener.Addr().(*net.TCPAddr)
	s.ctx.reverseProxyPort = opts.ReverseProxyPort

	httpServer := newHTTPServer(s.ctx, false, opts.TLSRequired == TLSRequired)
	s.waitGroup.Wrap(func() {
		http_api.Serve(s.httpListener, httpServer, "HTTP", opts.Logger)
	})

	s.ctx.nsqd.Start()

	s.waitGroup.Wrap(func() {
		s.lookupLoop(opts.LookupPingInterval, s.ctx.nsqd.MetaNotifyChan, s.ctx.nsqd.OptsNotificationChan, s.exitChan)
	})

	if opts.StatsdAddress != "" {
		s.waitGroup.Wrap(s.statsdLoop)
	}
}
