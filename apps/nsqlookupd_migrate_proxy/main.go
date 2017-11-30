package main

import (
	"flag"
	"net/http"
	migrate "github.com/youzan/nsq/nsqlookupd_migrate"

	"log"
	"github.com/BurntSushi/toml"
	"github.com/mreiferson/go-options"
	"os"
	"github.com/absolute8511/glog"
	"time"
)

var (
	flagSet  = flag.NewFlagSet("nsqlookup_migrate", flag.ExitOnError)
	port        = flagSet.String("http-address", "0.0.0.0:4161", "<hostname>:<port> to listen on for HTTP clients")
	testPort        = flagSet.String("http-address-test", "0.0.0.0:4161", "<hostname>:<port> to listen on for HTTP tester")
	originalLookupdHttpAddr = flagSet.String("origin-lookupd-http", "http://0.0.0.0:4161", "<hostname>:<port> original lookupd to access to fetch lookup info.")
	targetLookupdHttpAddr = flagSet.String("target-lookupd-http", "http://0.0.0.0:4161", "<hostname>:<port> target lookupd to access to fetch lookup info.")
	dccRemoteUrl = flagSet.String("dcc-url", "0.0.0.0:8089", "http://<hostname>:<port> dcc remote url for migrate control in topic base.")
	dccBackupFile = flagSet.String("dcc-backup-file", "dccbackup.bak", "backup file for dcc.")
	env = flagSet.String("env", "", "env")
	log_level = flagSet.Int64("log-level", 2, "log level")
	config = flagSet.String("config", "", "path to config file")
	migrate_dcc_key  = flagSet.String("migrate-dcc-key", "", "dcc key for migrate switches, it not specified, migrate proxy try parse DCC key from origin&target lookupd addresses.")
	log_dir = flagSet.String("log_dir", "", "dir for log files")
	test = flagSet.Bool("test", false, "performance test flag for nsqlookup_migrate")
	testClientNum = flagSet.Int64("test_client_num", 100, "access client number")
	mcTest = flagSet.Bool("mc_test", false, "migrate config test")
)

func main() {
	glog.InitWithFlag(flagSet)
	flagSet.Parse(os.Args[1:])
	var cfg map[string]interface{}
	if *config != "" {
		_, err := toml.DecodeFile(*config, &cfg)
		if err != nil {
			log.Fatalf("ERROR: failed to load config file %s - %s", *config, err.Error())
		}
	}

	context := &migrate.Context{}
	options.Resolve(context, flagSet, cfg)
	log.Printf("origin lookup http address: %v", context.LookupAddrOri)
	log.Printf("target lookup http address: %v", context.LookupAddrTar)
	if context.LogDir != "" {
		glog.SetGLogDir(context.LogDir)
		log.Printf("log dir: %v", context.LogDir)
	}
	glog.StartWorker(time.Second * 2)
	if context.Test {
		log.Printf("proxy starts as tester")
		httpTester, err := migrate.NewProxyTester(context)
		if err != nil {
			panic(err)
		}
		migrate.SetupLogger(context)
		log.Printf("http server listen on %v", context.ProxyHttpAddr)
		log.Fatal(http.ListenAndServe(context.ProxyHttpAddr, httpTester.Router))
		return
	}

	httpServer, err := migrate.NewHTTPServer(context)
	if err != nil {
		panic(err)
	}
	log.Printf("http server listen on %v", context.ProxyHttpAddr)
	log.Fatal(http.ListenAndServe(context.ProxyHttpAddr, httpServer.Router))
}