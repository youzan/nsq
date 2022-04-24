// This is an NSQ client that reads the specified topic/channel
// and re-publishes the messages to destination nsqd via TCP

package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/bitly/go-simplejson"
	"github.com/spaolacci/murmur3"
	"github.com/youzan/go-nsq"
	"github.com/youzan/nsq/internal/app"
	"github.com/youzan/nsq/internal/protocol"
	"github.com/youzan/nsq/internal/version"
)

const (
	ModeRoundRobin = iota
	ModeHostPool
)

var (
	showVersion = flag.Bool("version", false, "print version string")

	topic        = flag.String("topic", "", "nsq topic")
	channel      = flag.String("channel", "nsq_to_nsq", "nsq channel")
	destTopic    = flag.String("destination-topic", "", "destination nsq topic")
	maxInFlight  = flag.Int("max-in-flight", 200, "max number of messages to allow in flight")
	parallNum    = flag.Int("parall-num", 100, "number of parallel run")
	orderJsonKey = flag.String("order-json-key", "testkey", "json key to ordered sharding")

	statusEvery = flag.Int("status-every", 250, "the # of requests between logging status (per destination), 0 disables")

	lookupdHTTPAddrs     = app.StringArray{}
	destLookupdHTTPAddrs = app.StringArray{}

	// TODO: remove, deprecated
	maxBackoffDuration = flag.Duration("max-backoff-duration", 120*time.Second, "(deprecated) use --consumer-opt=max_backoff_duration,X")
)

func init() {
	flag.Var(&destLookupdHTTPAddrs, "destination-lookupd-address", "destination address (may be given multiple times)")
	flag.Var(&lookupdHTTPAddrs, "lookupd-http-address", "lookupd HTTP address (may be given multiple times)")
}

type PublishHandler struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	counter uint64

	producers *nsq.TopicProducerMgr
	mode      int

	requireJSONValueParsed   bool
	requireJSONValueIsNumber bool
	requireJSONNumber        float64
}

func getMsgKeyFromBody(body []byte, parentKey string) ([]byte, error) {
	jsonMsg, err := simplejson.NewJson(body)
	if err != nil {
		log.Printf("ERROR: Unable to decode json: %s", body)
		return nil, err
	}

	jsonV, ok := jsonMsg.CheckGet(parentKey)
	if !ok {
		log.Printf("ERROR: Unable to get json: %s", body)
		return nil, err
	}
	jsonV, ok = jsonV.CheckGet("key2")
	if !ok {
		log.Printf("ERROR: Unable to get json: %s", body)
		return nil, errors.New("unable to get json key")
	}
	msgKey, err := jsonV.Bytes()
	if err != nil {
		log.Printf("WARN: json key value is not string: %s, %v", body, jsonV)
		intV, err := jsonV.Int()
		if err != nil {
			return nil, err
		}
		return []byte(strconv.Itoa(intV)), nil
	}
	return msgKey, nil
}

func (ph *PublishHandler) HandleMessage(m *nsq.Message) error {
	var err error
	msgBody := m.Body
	msgKey, err := getMsgKeyFromBody(msgBody, *orderJsonKey)
	if err != nil {
		return err
	}

	var ext *nsq.MsgExt
	if m.ExtVer > 0 {
		var err2 error
		ext, err2 = m.GetJsonExt()
		if err2 != nil {
			log.Printf("failed to get ext, ignore it: %v, header: %s", err2, m.ExtBytes)
		}
	}
	if ext != nil {
		_, _, _, err = ph.producers.PublishOrderedWithJsonExt(*destTopic, msgKey, msgBody, ext)
		if err != nil {
			return err
		}
	} else {
		_, _, _, err = ph.producers.PublishOrdered(*destTopic, msgKey, msgBody)
		if err != nil {
			return err
		}
	}
	return nil
}

func hasArg(s string) bool {
	argExist := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == s {
			argExist = true
		}
	})
	return argExist
}

func main() {
	cCfg := nsq.NewConfig()
	pCfg := nsq.NewConfig()

	flag.Var(&nsq.ConfigFlag{cCfg}, "consumer-opt", "option to passthrough to nsq.Consumer (may be given multiple times, see http://godoc.org/github.com/youzan/go-nsq#Config)")
	flag.Var(&nsq.ConfigFlag{pCfg}, "producer-opt", "option to passthrough to nsq.Producer (may be given multiple times, see http://godoc.org/github.com/youzan/go-nsq#Config)")

	flag.Parse()

	if *showVersion {
		fmt.Printf("nsq_to_nsq v%s\n", version.Binary)
		return
	}

	if *topic == "" || *channel == "" {
		log.Fatal("--topic and --channel are required")
	}

	if *destTopic == "" {
		*destTopic = *topic
	}

	if !protocol.IsValidTopicName(*topic) {
		log.Fatal("--topic is invalid")
	}

	if !protocol.IsValidTopicName(*destTopic) {
		log.Fatal("--destination-topic is invalid")
	}

	if !protocol.IsValidChannelName(*channel) {
		log.Fatal("--channel is invalid")
	}

	if len(lookupdHTTPAddrs) == 0 {
		log.Fatal("--lookupd-http-address required")
	}

	if len(destLookupdHTTPAddrs) == 0 {
		destLookupdHTTPAddrs = lookupdHTTPAddrs
	}

	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)

	defaultUA := fmt.Sprintf("nsq_to_nsq/%s go-nsq/%s", version.Binary, nsq.VERSION)

	cCfg.UserAgent = defaultUA
	cCfg.MaxInFlight = *maxInFlight

	// TODO: remove, deprecated
	if hasArg("max-backoff-duration") {
		log.Printf("WARNING: --max-backoff-duration is deprecated in favor of --consumer-opt=max_backoff_duration,X")
		cCfg.MaxBackoffDuration = *maxBackoffDuration
	}

	cCfg.EnableOrdered = true
	consumer, err := nsq.NewConsumer(*topic, *channel, cCfg)
	if err != nil {
		log.Fatal(err)
	}

	pCfg.UserAgent = defaultUA
	pCfg.LookupdSeeds = destLookupdHTTPAddrs
	pCfg.ProducerPoolSize = *parallNum
	pCfg.EnableOrdered = true
	pCfg.Hasher = murmur3.New32()
	producerMgr, err := nsq.NewTopicProducerMgr([]string{*destTopic}, pCfg)

	handler := &PublishHandler{
		producers: producerMgr,
	}
	consumer.AddConcurrentHandlers(handler, *parallNum)

	err = consumer.ConnectToNSQLookupds(lookupdHTTPAddrs)
	if err != nil {
		log.Fatal(err)
	}

	for {
		select {
		case <-consumer.StopChan:
			return
		case <-termChan:
			consumer.Stop()
		}
	}
}
