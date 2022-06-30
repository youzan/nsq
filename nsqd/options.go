package nsqd

import (
	"crypto/md5"
	"crypto/tls"
	"hash/crc32"
	"io"
	"log"
	"net"
	"os"
	"time"

	"github.com/youzan/nsq/internal/levellogger"
)

const (
	MAX_NODE_ID = 1024 * 1024
)

type Options struct {
	// basic options
	ID                         int64         `flag:"worker-id" cfg:"id"`
	Verbose                    bool          `flag:"verbose"`
	ClusterID                  string        `flag:"cluster-id"`
	ClusterLeadershipAddresses string        `flag:"cluster-leadership-addresses" cfg:"cluster_leadership_addresses"`
	ClusterLeadershipUsername  string        `flag:"cluster-leadership-username" cfg:"cluster_leadership_username"`
	ClusterLeadershipPassword  string        `flag:"cluster-leadership-password" cfg:"cluster_leadership_password"`
	ClusterLeadershipRootDir   string        `flag:"cluster-leadership-root-dir" cfg:"cluster_leadership_root_dir"`
	TCPAddress                 string        `flag:"tcp-address"`
	RPCPort                    string        `flag:"rpc-port"`
	ReverseProxyPort           string        `flag:"reverse-proxy-port"`
	HTTPAddress                string        `flag:"http-address"`
	MetricAddress              string        `flag:"metric-address"`
	HTTPSAddress               string        `flag:"https-address"`
	BroadcastAddress           string        `flag:"broadcast-address"`
	BroadcastInterface         string        `flag:"broadcast-interface"`
	NSQLookupdTCPAddresses     []string      `flag:"lookupd-tcp-address" cfg:"nsqlookupd_tcp_addresses"`
	AuthHTTPAddresses          []string      `flag:"auth-http-address" cfg:"auth_http_addresses"`
	LookupPingInterval         time.Duration `flag:"lookup-ping-interval" arg:"5s"`

	// diskqueue options
	DataPath        string        `flag:"data-path"`
	MemQueueSize    int64         `flag:"mem-queue-size"`
	MaxBytesPerFile int64         `flag:"max-bytes-per-file"`
	SyncEvery       int64         `flag:"sync-every"`
	SyncTimeout     time.Duration `flag:"sync-timeout"`

	QueueScanInterval          time.Duration `flag:"queue-scan-interval"`
	QueueScanRefreshInterval   time.Duration `flag:"queue-scan-refresh-interval"`
	QueueScanSelectionCount    int           `flag:"queue-scan-selection-count"`
	QueueScanWorkerPoolMax     int           `flag:"queue-scan-worker-pool-max"`
	QueueTopicJobWorkerPoolMax int           `flag:"queue-topic-job-worker-pool-max"`
	QueueScanDirtyPercent      float64       `flag:"queue-scan-dirty-percent"`

	// msg and command options
	MsgTimeout    time.Duration `flag:"msg-timeout" arg:"60s"`
	MaxMsgTimeout time.Duration `flag:"max-msg-timeout"`
	MaxMsgSize    int64         `flag:"max-msg-size" deprecated:"max-message-size" cfg:"max_msg_size"`
	MaxBodySize   int64         `flag:"max-body-size"`
	// max for each topic partition
	MaxPubWaitingSize     int64         `flag:"max-pub-waiting-size"`
	MaxReqTimeout         time.Duration `flag:"max-req-timeout"`
	AckOldThanTime        time.Duration `flag:"ack-old-than-time"`
	AckRetryCnt           int           `flag:"ack-retry-cnt"`
	MaxConfirmWin         int64         `flag:"max-confirm-win"`
	MaxChannelDelayedQNum int64         `flag:"max-channel-delayed-qnum"`
	ClientTimeout         time.Duration
	ReqToEndThreshold     time.Duration `flag:"req-to-end-threshold"`

	// client overridable configuration options
	MaxHeartbeatInterval   time.Duration `flag:"max-heartbeat-interval"`
	MaxRdyCount            int64         `flag:"max-rdy-count"`
	MaxOutputBufferSize    int64         `flag:"max-output-buffer-size"`
	MaxOutputBufferTimeout time.Duration `flag:"max-output-buffer-timeout"`
	ChannelRateLimitKB     int64         `flag:"channel-ratelimit-kb"`

	// statsd integration
	StatsdAddress  string        `flag:"statsd-address"`
	StatsdPrefix   string        `flag:"statsd-prefix"`
	StatsdProtocol string        `flag:"statsd-protocol"`
	StatsdInterval time.Duration `flag:"statsd-interval" arg:"60s"`
	StatsdMemStats bool          `flag:"statsd-mem-stats"`

	// e2e message latency
	E2EProcessingLatencyWindowTime  time.Duration `flag:"e2e-processing-latency-window-time"`
	E2EProcessingLatencyPercentiles []float64     `flag:"e2e-processing-latency-percentile" cfg:"e2e_processing_latency_percentiles"`

	// TLS config
	TLSCert             string `flag:"tls-cert"`
	TLSKey              string `flag:"tls-key"`
	TLSClientAuthPolicy string `flag:"tls-client-auth-policy"`
	TLSRootCAFile       string `flag:"tls-root-ca-file"`
	TLSRequired         int    `flag:"tls-required"`
	TLSMinVersion       uint16 `flag:"tls-min-version"`

	// compression
	DeflateEnabled  bool `flag:"deflate"`
	MaxDeflateLevel int  `flag:"max-deflate-level"`
	SnappyEnabled   bool `flag:"snappy"`

	LogLevel     int32  `flag:"log-level" cfg:"log_level"`
	LogDir       string `flag:"log-dir" cfg:"log_dir"`
	Logger       levellogger.Logger
	RemoteTracer string `flag:"remote-tracer"`

	RetentionDays             int32 `flag:"retention-days" cfg:"retention_days"`
	RetentionSizePerDay       int64 `flag:"retention-size-per-day" cfg:"retention_size_per_day"`
	StartAsFixMode            bool  `flag:"start-as-fix-mode"`
	AllowExtCompatible        bool  `flag:"allow-ext-compatible" cfg:"allow_ext_compatible"`
	AllowSubExtCompatible     bool  `flag:"allow-sub-ext-compatible" cfg:"allow_sub_ext_compatible"`
	AllowZanTestSkip          bool  `flag:"allow-zan-test-skip"`
	DefaultCommitBuf          int32 `flag:"default-commit-buf" cfg:"default_commit_buf"`
	MaxCommitBuf              int32 `flag:"max-commit-buf" cfg:"max_commit_buf"`
	UseFsync                  bool  `flag:"use-fsync"`
	MaxConnForClient          int64 `flag:"max-conn-for-client" cfg:"max_conn_for_client"`
	QueueReadBufferSize       int   `flag:"queue-read-buffer-size" cfg:"queue_read_buffer_size"`
	QueueWriteBufferSize      int   `flag:"queue-write-buffer-size" cfg:"queue_write_buffer_size"`
	PubQueueSize              int   `flag:"pub-queue-size" cfg:"pub_queue_size"`
	SleepMsBetweenLogSyncPull int   `flag:"sleepms-between-log-sync-pull" cfg:"sleepms_between_log_sync_pull"`

	// options for kv engine
	KVEnabled              bool  `flag:"kv-enabled" cfg:"kv_enabled"`
	KVBlockCache           int64 `flag:"kv-block-cache" cfg:"kv_block_cache"`
	KVWriteBufferSize      int64 `flag:"kv-write-buffer-size" cfg:"kv_write_buffer_size"`
	KVMaxWriteBufferNumber int64 `flag:"kv-max-write-buffer-number" cfg:"kv_max_write_buffer_number"`
}

func NewOptions() *Options {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

	h := md5.New()
	io.WriteString(h, hostname)
	defaultID := int64(crc32.ChecksumIEEE(h.Sum(nil)) % MAX_NODE_ID)

	opts := &Options{
		ID: defaultID,

		ClusterID:                  "nsq-clusterid-test-only",
		ClusterLeadershipAddresses: "",
		TCPAddress:                 "0.0.0.0:4150",
		HTTPAddress:                "0.0.0.0:4151",
		HTTPSAddress:               "0.0.0.0:4152",
		BroadcastAddress:           hostname,
		BroadcastInterface:         "eth0",

		NSQLookupdTCPAddresses: make([]string, 0),
		AuthHTTPAddresses:      make([]string, 0),
		LookupPingInterval:     5 * time.Second,

		MemQueueSize:    10000,
		MaxBytesPerFile: 100 * 1024 * 1024,
		SyncEvery:       2500,
		SyncTimeout:     2 * time.Second,

		QueueScanInterval:          500 * time.Millisecond,
		QueueScanRefreshInterval:   5 * time.Second,
		QueueScanSelectionCount:    20,
		QueueScanWorkerPoolMax:     16,
		QueueTopicJobWorkerPoolMax: 100,
		QueueScanDirtyPercent:      0.25,

		MsgTimeout:        60 * time.Second,
		MaxMsgTimeout:     15 * time.Minute,
		MaxMsgSize:        1024 * 1024,
		MaxBodySize:       5 * 1024 * 1024,
		MaxPubWaitingSize: 200 * 1024 * 1024,
		MaxReqTimeout:     3 * 24 * time.Hour,
		ClientTimeout:     60 * time.Second,
		ReqToEndThreshold: 15 * time.Minute,

		MaxHeartbeatInterval:   60 * time.Second,
		MaxRdyCount:            2500,
		MaxOutputBufferSize:    64 * 1024,
		MaxOutputBufferTimeout: 1 * time.Second,
		ChannelRateLimitKB:     100 * 1024,
		MaxConfirmWin:          500,
		MaxChannelDelayedQNum:  DefaultMaxChDelayedQNum,

		StatsdPrefix:   "nsq.%s",
		StatsdProtocol: "udp",
		StatsdInterval: 60 * time.Second,
		StatsdMemStats: true,

		E2EProcessingLatencyWindowTime: time.Duration(10 * time.Minute),

		DeflateEnabled:  true,
		MaxDeflateLevel: 6,
		SnappyEnabled:   true,

		TLSMinVersion: tls.VersionTLS10,

		LogLevel: levellogger.LOG_INFO,
		LogDir:   "",
		Logger:   &levellogger.GLogger{},

		RetentionDays:    int32(DEFAULT_RETENTION_DAYS),
		MaxConnForClient: 500000,
		KVEnabled:        false,
	}

	return opts
}

func getIPv4ForInterfaceName(ifname string) string {
	interfaces, _ := net.Interfaces()
	for _, inter := range interfaces {
		if inter.Name == ifname {
			if addrs, err := inter.Addrs(); err == nil {
				for _, addr := range addrs {
					switch ip := addr.(type) {
					case *net.IPNet:
						if ip.IP.DefaultMask() != nil {
							return ip.IP.String()
						}
					}
				}
			}
		}
	}
	return ""
}

func (opts *Options) DecideBroadcast() string {
	ip := ""
	if opts.BroadcastInterface != "" {
		ip = getIPv4ForInterfaceName(opts.BroadcastInterface)
	}
	if ip == "" {
		ip = opts.BroadcastAddress
	} else {
		opts.BroadcastAddress = ip
	}
	if ip == "0.0.0.0" || ip == "" {
		log.Fatalf("can not decide the broadcast ip: %v", ip)
		os.Exit(1)
	}
	return ip
}
