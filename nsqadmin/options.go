package nsqadmin

import (
	"time"

	"github.com/youzan/nsq/internal/levellogger"
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

	CASUrl			string `flag:"cas-url" cfg:"cas_url"`
	CASAuthSecret		string `flag:"cas-auth-secret" cfg:"cas_auth_secret"`
	CASLogoutUrl		string `flag:"cas-logout-url" cfg:"cas_logout_url"`
	CASAuthUrl		string `flag:"cas-auth-url" cfg:"cas_auth_url"`
	CASAppName              string `flag:"cas-app-name" cfg:"cas_app_name"`
	CASRedirectUrl          string `flag:"cas-redirect-url" cfg:"cas_redirect_url"`
	LogDir string `flag:"log-dir" cfg:"log_dir"`
	Logger levellogger.Logger
	AccessTokens		[]string `flag:"access-tokens" cfg:"access_tokens"`
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
