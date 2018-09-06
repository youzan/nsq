package nsqadmin

import (
	"time"

	"github.com/youzan/nsq/internal/levellogger"
	"github.com/youzan/nsq/internal/clusterinfo"
)

type Options struct {
	HTTPAddress string `flag:"http-address"`

	GraphiteURL   string `flag:"graphite-url"`
	ProxyGraphite bool   `flag:"proxy-graphite"`

	UseStatsdPrefixes   bool   `flag:"use-statsd-prefixes"`
	StatsdPrefix        string `flag:"statsd-prefix"`
	StatsdCounterFormat string `flag:"statsd-counter-format"`
	StatsdGaugeFormat   string `flag:"statsd-gauge-format"`

	StatsdInterval time.Duration `flag:"statsd-interval"`

	NSQLookupdHTTPAddresses []string `flag:"lookupd-http-address" cfg:"nsqlookupd_http_addresses"`
	NSQDHTTPAddresses       []string `flag:"nsqd-http-address" cfg:"nsqd_http_addresses"`

	DCNSQLookupdHTTPAddresses []string `flag:"dc-lookupd-http-address" cfg:"dc_lookupd_http_addresses"`

	NSQLookupdHTTPAddressesDC []clusterinfo.LookupdAddressDC

	HTTPClientTLSInsecureSkipVerify bool   `flag:"http-client-tls-insecure-skip-verify"`
	HTTPClientTLSRootCAFile         string `flag:"http-client-tls-root-ca-file"`
	HTTPClientTLSCert               string `flag:"http-client-tls-cert"`
	HTTPClientTLSKey                string `flag:"http-client-tls-key"`

	NotificationHTTPEndpoint string `flag:"notification-http-endpoint"`
	TraceQueryURL            string `flag:"trace-query-url"`
	TraceAppID               string `flag:"trace-app-id"`
	TraceAppName             string `flag:"trace-app-name"`
	TraceLogIndexID          string `flag:"trace-log-index-id"`
	TraceLogIndexName        string `flag:"trace-log-index-name"`
	TraceLogPageCount        int    `flag:"trace-log-page-count"`

	ChannelCreationRetry           int `flag:"channel-create-retry"`
	ChannelCreationBackoffInterval int `flag:"channel-create-backoff-interval"`

	AuthUrl			string `flag:"auth-url" cfg:"auth_url"`
	AuthSecret		string `flag:"auth-secret" cfg:"auth_secret"`
	LogoutUrl		string `flag:"logout-url" cfg:"logout_url"`
	AppName              string `flag:"app-name" cfg:"app_name"`
	RedirectUrl          string `flag:"redirect-url" cfg:"redirect_url"`
	LogDir string `flag:"log-dir" cfg:"log_dir"`
	Logger levellogger.Logger
	AccessTokens		[]string `flag:"access-tokens" cfg:"access_tokens"`

	AccessControlFile	string `flag:"access-control-file"`
	EnableZanTestSkip    bool `flag:"enable-zan-test-skip"`
}

func NewOptions() *Options {
	return &Options{
		HTTPAddress:                    "0.0.0.0:4171",
		UseStatsdPrefixes:              true,
		StatsdPrefix:                   "nsq.%s",
		StatsdCounterFormat:            "stats.counters.%s.count",
		StatsdGaugeFormat:              "stats.gauges.%s",
		StatsdInterval:                 60 * time.Second,
		ChannelCreationRetry:           3,
		ChannelCreationBackoffInterval: 1000,
		Logger:            &levellogger.GLogger{},
		TraceLogPageCount: 60,
	}
}
