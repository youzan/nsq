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
	env = flagSet.String("env", "", "env")
	log_level = flagSet.Int64("log-level", 2, "log level")
	config = flagSet.String("config", "", "path to config file")
	migrate_key  = flagSet.String("migrate-key", "", "key for migrate switches, it not specified, migrate proxy try parse key from origin&target lookupd addresses.")
	log_dir = flagSet.String("log_dir", "", "dir for log files")
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

	httpServer, err := migrate.NewHTTPServer(context)
	if err != nil {
		panic(err)
	}
	log.Printf("http server listen on %v", context.ProxyHttpAddr)
	log.Fatal(http.ListenAndServe(context.ProxyHttpAddr, httpServer.Router))
}