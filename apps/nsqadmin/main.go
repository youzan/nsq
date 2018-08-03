package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/absolute8511/glog"
	"github.com/mreiferson/go-options"
	"github.com/youzan/nsq/internal/app"
	"github.com/youzan/nsq/internal/version"
	"github.com/youzan/nsq/nsqadmin"
)

var (
	flagSet = flag.NewFlagSet("nsqadmin", flag.ExitOnError)

	config      = flagSet.String("config", "", "path to config file")
	showVersion = flagSet.Bool("version", false, "print version string")

	httpAddress = flagSet.String("http-address", "0.0.0.0:4171", "<addr>:<port> to listen on for HTTP clients")
	templateDir = flagSet.String("template-dir", "", "path to templates directory")
	logDir      = flagSet.String("log-dir", "", "directory for logs")

	graphiteURL   = flagSet.String("graphite-url", "", "graphite HTTP address")
	proxyGraphite = flagSet.Bool("proxy-graphite", false, "proxy HTTP requests to graphite")

	useStatsdPrefixes   = flagSet.Bool("use-statsd-prefixes", true, "(Deprecated - Use --statsd-counter-format and --statsd-gauge-format) Expect statsd prefixed keys in graphite (ie: 'stats.counters.' and 'stats.gauges.')")
	statsdCounterFormat = flagSet.String("statsd-counter-format", "stats.counters.%s.count", "The counter stats key formatting applied by the implementation of statsd. If no formatting is desired, set this to an empty string.")
	statsdGaugeFormat   = flagSet.String("statsd-gauge-format", "stats.gauges.%s", "The gauge stats key formatting applied by the implementation of statsd. If no formatting is desired, set this to an empty string.")
	statsdPrefix        = flagSet.String("statsd-prefix", "nsq.%s", "prefix used for keys sent to statsd (%s for host replacement, must match nsqd)")
	statsdInterval      = flagSet.Duration("statsd-interval", 60*time.Second, "time interval nsqd is configured to push to statsd (must match nsqd)")

	notificationHTTPEndpoint = flagSet.String("notification-http-endpoint", "", "HTTP endpoint (fully qualified) to which POST notifications of admin actions will be sent")

	httpClientTLSInsecureSkipVerify = flagSet.Bool("http-client-tls-insecure-skip-verify", false, "configure the HTTP client to skip verification of TLS certificates")
	httpClientTLSRootCAFile         = flagSet.String("http-client-tls-root-ca-file", "", "path to CA file for the HTTP client")
	httpClientTLSCert               = flagSet.String("http-client-tls-cert", "", "path to certificate file for the HTTP client")
	httpClientTLSKey                = flagSet.String("http-client-tls-key", "", "path to key file for the HTTP client")

	traceQueryURL     = flagSet.String("trace-query-url", "", "trace service url")
	traceAppID        = flagSet.String("trace-app-id", "", "trace service app")
	traceAppName      = flagSet.String("trace-app-name", "", "trace service app")
	traceLogIndexID   = flagSet.String("trace-log-index-id", "", "trace service log index")
	traceLogIndexName = flagSet.String("trace-log-index-name", "", "trace service log index")
	traceLogPageCount = flagSet.Int("trace-log-page-count", 60, "trace service log page count")

	channelCreateRetry           = flagSet.Int("channel-create-retry", 3, "max retry for creating channel in topic creation")
	channelCreateBackoffInterval = flagSet.Int("channel-create-backoff-interval", 1000, "backoff interval when default channel fail to create in topic creation")

	AuthUrl = flagSet.String("auth-url", "", "authentication service url")
	AuthSecret = flagSet.String("auth-secret", "", "authentication secret")
	LogoutUrl = flagSet.String("logout-url", "", "logout url")

	AppName = flagSet.String("app-name", "", "current application name in authentication service")
	RedirectUrl = flagSet.String("redirect-url", "", "refirect url")

	nsqlookupdHTTPAddresses = app.StringArray{}
	nsqdHTTPAddresses       = app.StringArray{}
	dcNsqlookupdHTTPAddresses = app.StringArray{}

	accessTokens = app.StringArray{}
)

func init() {
	flagSet.Var(&nsqlookupdHTTPAddresses, "lookupd-http-address", "lookupd HTTP address (may be given multiple times)")
	flagSet.Var(&nsqdHTTPAddresses, "nsqd-http-address", "nsqd HTTP address (may be given multiple times)")
	flagSet.Var(&accessTokens, "access-tokens", "access token for api access")
	flagSet.Var(&dcNsqlookupdHTTPAddresses, "dc-lookupd-http-address", "lookupd HTTP address (may be given multiple times) with data center info")
}

func main() {
	glog.InitWithFlag(flagSet)
	flagSet.Parse(os.Args[1:])

	if *showVersion {
		fmt.Println(version.String("nsqadmin"))
		return
	}
	defer glog.Flush()

	if *templateDir != "" {
		log.Printf("WARNING: --template-dir is deprecated and will be removed in the next release (templates are now compiled into the binary)")
	}

	exitChan := make(chan int)
	signalChan := make(chan os.Signal, 1)
	go func() {
		<-signalChan
		exitChan <- 1
	}()
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	var cfg map[string]interface{}
	if *config != "" {
		_, err := toml.DecodeFile(*config, &cfg)
		if err != nil {
			log.Fatalf("ERROR: failed to load config file %s - %s", *config, err)
		}
	}
	opts := nsqadmin.NewOptions()
	options.Resolve(opts, flagSet, cfg)
	if opts.LogDir != "" {
		glog.SetGLogDir(opts.LogDir)
	}
	glog.StartWorker(time.Second * 2)

	nsqadmin := nsqadmin.New(opts)
	nsqadmin.Main()
	<-exitChan
	nsqadmin.Exit()
}
