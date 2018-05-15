package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/absolute8511/glog"
	"github.com/judwhite/go-svc/svc"
	"github.com/mreiferson/go-options"
	"github.com/youzan/nsq/internal/app"
	"github.com/youzan/nsq/internal/version"
	"github.com/youzan/nsq/nsqlookupd"
	"github.com/coreos/etcd/embed"
	"github.com/coreos/pkg/capnslog"
	"github.com/coreos/etcd/pkg/flags"
)

var (
	flagSet = flag.NewFlagSet("nsqlookupd", flag.ExitOnError)

	config      = flagSet.String("config", "", "path to config file")
	showVersion = flagSet.Bool("version", false, "print version string")
	verbose     = flagSet.Bool("verbose", false, "enable verbose logging")

	tcpAddress         = flagSet.String("tcp-address", "0.0.0.0:4160", "<addr>:<port> to listen on for TCP clients")
	httpAddress        = flagSet.String("http-address", "0.0.0.0:4161", "<addr>:<port> to listen on for HTTP clients")
	rpcPort            = flagSet.String("rpc-port", "", "<port> to listen on for Rpc call")
	broadcastAddress   = flagSet.String("broadcast-address", "", "address of this lookupd node, (default to the OS hostname)")
	broadcastInterface = flagSet.String("broadcast-interface", "", "address of this lookupd node, (default to the OS hostname)")
	reverseProxyPort   = flagSet.String("reverse-proxy-port", "", "<port> for reverse proxy")

	clusterLeadershipAddresses = flagSet.String("cluster-leadership-addresses", "", " the cluster leadership server list")
	clusterID                  = flagSet.String("cluster-id", "nsq-test-cluster", "the cluster id used for separating different nsq cluster.")

	inactiveProducerTimeout  = flagSet.Duration("inactive-producer-timeout", 60*time.Second, "duration of time a producer will remain in the active list since its last ping")
	nsqdPingTimeout          = flagSet.Duration("nsqd-ping-timeout", 15*time.Second, "duration of nsqd ping timeout, should be at least twice as the nsqd ping interval")
	tombstoneLifetime        = flagSet.Duration("tombstone-lifetime", 45*time.Second, "duration of time a producer will remain tombstoned if registration remains")
	logLevel                 = flagSet.Int("log-level", 1, "log verbose level")
	logDir                   = flagSet.String("log-dir", "", "directory for log file")
	allowWriteWithNoChannels = flagSet.Bool("allow-write-with-nochannels", false, "allow write to topic with no channels")
	balanceInterval          = app.StringArray{}

	// etcd embed config, refers to github.com/coreos/etcd/etcdmain/config.go
        ec                       = embed.NewConfig()
)

func init() {
	flagSet.Var(&balanceInterval, "balance-interval", "the balance time interval")

	// etcd member flags:
	flagSet.StringVar(&ec.Name, "etcd.name", ec.Name, "Human-readable name for this member.")
	flagSet.StringVar(&ec.Dir, "etcd.data-dir", ec.Dir, "Path to the data directory.")
	flagSet.StringVar(&ec.WalDir, "etcd.wal-dir", ec.WalDir, "Path to the dedicated wal directory.")
	flagSet.Var(flags.NewURLsValue(embed.DefaultListenPeerURLs), "etcd.listen-peer-urls", "List of URLs to listen on for peer traffic.")
	flagSet.Var(flags.NewURLsValue(embed.DefaultListenClientURLs), "etcd.listen-client-urls", "List of URLs to listen on for client traffic.")
	flagSet.UintVar(&ec.MaxSnapFiles, "etcd.max-snapshots", ec.MaxSnapFiles, "Maximum number of snapshot files to retain (0 is unlimited).")
	flagSet.UintVar(&ec.MaxWalFiles, "etcd.max-wals", ec.MaxWalFiles, "Maximum number of wal files to retain (0 is unlimited).")

	flagSet.Uint64Var(&ec.SnapCount, "etcd.snapshot-count", ec.SnapCount, "Number of committed transactions to trigger a snapshot to disk.")
	flagSet.UintVar(&ec.TickMs, "etcd.heartbeat-interval", ec.TickMs, "Time (in milliseconds) of a heartbeat interval.")
	flagSet.UintVar(&ec.ElectionMs, "etcd.election-timeout", ec.ElectionMs, "Time (in milliseconds) for an election to timeout.")
	flagSet.BoolVar(&ec.InitialElectionTickAdvance, "etcd.initial-election-tick-advance", ec.InitialElectionTickAdvance, "Whether to fast-forward initial election ticks on boot for faster election.")
	flagSet.Int64Var(&ec.QuotaBackendBytes, "etcd.quota-backend-bytes", ec.QuotaBackendBytes, "Raise alarms when backend size exceeds the given quota. 0 means use the default quota.")
	flagSet.UintVar(&ec.MaxTxnOps, "etcd.max-txn-ops", ec.MaxTxnOps, "Maximum number of operations permitted in a transaction.")
	flagSet.UintVar(&ec.MaxRequestBytes, "etcd.max-request-bytes", ec.MaxRequestBytes, "Maximum client request size in bytes the server will accept.")
	flagSet.DurationVar(&ec.GRPCKeepAliveMinTime, "etcd.grpc-keepalive-min-time", ec.GRPCKeepAliveMinTime, "Minimum interval duration that a client should wait before pinging server.")
	flagSet.DurationVar(&ec.GRPCKeepAliveInterval, "etcd.grpc-keepalive-interval", ec.GRPCKeepAliveInterval, "Frequency duration of server-to-client ping to check if a connection is alive (0 to disable).")
	flagSet.DurationVar(&ec.GRPCKeepAliveTimeout, "etcd.grpc-keepalive-timeout", ec.GRPCKeepAliveTimeout, "Additional duration of wait before closing a non-responsive connection (0 to disable).")
	
	// etcd clustering flags:
	flagSet.Var(flags.NewURLsValue(embed.DefaultInitialAdvertisePeerURLs), "etcd.initial-advertise-peer-urls", "List of this member's peer URLs to advertise to the rest of the cluster.")
	flagSet.Var(flags.NewURLsValue(embed.DefaultAdvertiseClientURLs), "etcd.advertise-client-urls", "List of this member's client URLs to advertise to the public.")
	flagSet.StringVar(&ec.Durl, "etcd.discovery", ec.Durl, "Discovery URL used to bootstrap the cluster.")
	flagSet.StringVar(&ec.Dproxy, "etcd.discovery-proxy", ec.Dproxy, "HTTP proxy to use for traffic to discovery service.")
	flagSet.StringVar(&ec.DNSCluster, "etcd.discovery-srv", ec.DNSCluster, "DNS domain used to bootstrap initial cluster.")
	flagSet.StringVar(&ec.InitialCluster, "etcd.initial-cluster", ec.InitialCluster, "Initial cluster configuration for bootstrapping.")
	flagSet.StringVar(&ec.InitialClusterToken, "etcd.initial-cluster-token", ec.InitialClusterToken, "Initial cluster token for the etcd cluster during bootstrap.")
	flagSet.StringVar(&ec.ClusterState, "etcd.initial-cluster-state", embed.ClusterStateFlagNew, "Initial cluster state ('new' or 'existing').")

	flagSet.BoolVar(&ec.StrictReconfigCheck, "etcd.strict-reconfig-check", ec.StrictReconfigCheck, "Reject reconfiguration requests that would cause quorum loss.")
	flagSet.BoolVar(&ec.EnableV2, "etcd.enable-v2", ec.EnableV2, "Accept etcd V2 client requests.")
	flagSet.StringVar(&ec.ExperimentalEnableV2V3, "etcd.experimental-enable-v2v3", ec.ExperimentalEnableV2V3, "v3 prefix for serving emulated v2 state.")
	flagSet.StringVar(&ec.AutoCompactionRetention, "etcd.auto-compaction-retention", "0", "Auto compaction retention for mvcc key value store. 0 means disable auto compaction.")
	flagSet.StringVar(&ec.AutoCompactionMode, "etcd.auto-compaction-mode", "periodic", "interpret 'auto-compaction-retention' one of: periodic|revision. 'periodic' for duration based retention, defaulting to hours if no time unit is provided (e.g. '5m'). 'revision' for revision number based retention.")

	// etcd security flags:
	flagSet.StringVar(&ec.ClientTLSInfo.CertFile, "etcd.cert-file", "", "Path to the client server TLS cert file.")
	flagSet.StringVar(&ec.ClientTLSInfo.KeyFile, "etcd.key-file", "", "Path to the client server TLS key file.")
	flagSet.BoolVar(&ec.ClientTLSInfo.ClientCertAuth, "etcd.client-cert-auth", false, "Enable client cert authentication.")
	flagSet.StringVar(&ec.ClientTLSInfo.CRLFile, "etcd.client-crl-file", "", "Path to the client certificate revocation list file.")
	flagSet.StringVar(&ec.ClientTLSInfo.TrustedCAFile, "etcd.trusted-ca-file", "", "Path to the client server TLS trusted CA cert file.")
	flagSet.BoolVar(&ec.ClientAutoTLS, "etcd.auto-tls", false, "Client TLS using generated certificates")
	flagSet.StringVar(&ec.PeerTLSInfo.CertFile, "etcd.peer-cert-file", "", "Path to the peer server TLS cert file.")
	flagSet.StringVar(&ec.PeerTLSInfo.KeyFile, "etcd.peer-key-file", "", "Path to the peer server TLS key file.")
	flagSet.BoolVar(&ec.PeerTLSInfo.ClientCertAuth, "etcd.peer-client-cert-auth", false, "Enable peer client cert authentication.")
	flagSet.StringVar(&ec.PeerTLSInfo.TrustedCAFile, "etcd.peer-trusted-ca-file", "", "Path to the peer server TLS trusted CA file.")
	flagSet.BoolVar(&ec.PeerAutoTLS, "etcd.peer-auto-tls", false, "Peer TLS using generated certificates")
	flagSet.StringVar(&ec.PeerTLSInfo.CRLFile, "etcd.peer-crl-file", "", "Path to the peer certificate revocation list file.")
	flagSet.StringVar(&ec.PeerTLSInfo.AllowedCN, "etcd.peer-cert-allowed-cn", "", "Allowed CN for inter peer authentication.")

	// etcd logging flags:
	flagSet.BoolVar(&ec.Debug, "etcd.debug", false, "Enable debug-level logging for etcd.")
	flagSet.StringVar(&ec.LogPkgLevels, "etcd.log-package-levels", "", "Specify a particular log level for each etcd package (eg: 'etcdmain=CRITICAL,etcdserver=DEBUG').")
	flagSet.StringVar(&ec.LogOutput, "etcd.log-output", embed.DefaultLogOutput, "Specify 'stdout' or 'stderr' to skip journald logging even when running under systemd.")

	// etcd profiling and monitoring flags:
	flagSet.BoolVar(&ec.EnablePprof, "etcd.enable-pprof", false, "Enable runtime profiling data via HTTP server. Address is at client URL + \"/debug/pprof/\"")
	flagSet.StringVar(&ec.Metrics, "etcd.metrics", ec.Metrics, "Set level of detail for exported metrics, specify 'extensive' to include histogram metrics")

	// etcd auth flags:
	flagSet.StringVar(&ec.AuthToken, "etcd.auth-token", ec.AuthToken, "Specify auth token specific options.")
}

type program struct {
	nsqlookupd *nsqlookupd.NSQLookupd
}

func main() {
	defer glog.Flush()
	prg := &program{}
	if err := svc.Run(prg, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGINT); err != nil {
		log.Fatal(err)
	}
}

func (p *program) Init(env svc.Environment) error {
	if env.IsWindowsService() {
		dir := filepath.Dir(os.Args[0])
		return os.Chdir(dir)
	}
	return nil
}

func (p *program) Start() error {
	glog.InitWithFlag(flagSet)

	flagSet.Parse(os.Args[1:])

	if *showVersion {
		fmt.Println(version.String("nsqlookupd"))
		os.Exit(0)
	}

	var cfg map[string]interface{}
	if *config != "" {
		_, err := toml.DecodeFile(*config, &cfg)
		if err != nil {
			log.Fatalf("ERROR: failed to load config file %s - %s", *config, err.Error())
		}
	}

	opts := nsqlookupd.NewOptions()
	options.Resolve(opts, flagSet, cfg)
	if opts.LogDir != "" {
		glog.SetGLogDir(opts.LogDir)
	}
	nsqlookupd.SetLogger(opts.Logger, opts.LogLevel)
	glog.StartWorker(time.Second * 2)

	if ec.Name != "" {
	    // use embed etcd cluster
	    // special cases of etcd flags handling:
	    ec.LPUrls = flags.URLsFromFlag(flagSet, "etcd.listen-peer-urls")
	    ec.APUrls = flags.URLsFromFlag(flagSet, "etcd.initial-advertise-peer-urls")
	    ec.LCUrls = flags.URLsFromFlag(flagSet, "etcd.listen-client-urls")
	    ec.ACUrls = flags.URLsFromFlag(flagSet, "etcd.advertise-client-urls")

	    // disable default initial-cluster if discovery is set
	    if (ec.Durl != "" || ec.DNSCluster != "") && !flags.IsSet(flagSet, "initial-cluster") {
		    ec.InitialCluster = ""
	    }

	    // redirect etcd stdout and stderr to an file
	    var etcdLogFile string
	    etcdLogFile = filepath.Join(ec.Dir, fmt.Sprintf("%s-etcd.log", ec.Name))
	    f, err := os.OpenFile(etcdLogFile, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	    if err != nil {
		    log.Fatal(err)
	    }
	    defer f.Close()
	    capnslog.SetFormatter(capnslog.NewPrettyFormatter(f, ec.Debug))

	    opts.ClusterLeadershipAddresses = flagSet.Lookup("etcd.listen-client-urls").Value.(*flags.URLsValue).String()
	}
	opts.EtcdConf = ec

	daemon := nsqlookupd.New(opts)

	daemon.Main()
	p.nsqlookupd = daemon
	return nil
}

func (p *program) Stop() error {
	if p.nsqlookupd != nil {
		p.nsqlookupd.Exit()
	}
	return nil
}
