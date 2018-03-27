package nsq_sync

import (
	"log"
	"github.com/bitly/timer_metrics"
	"fmt"
	"github.com/bitly/go-hostpool"
	"github.com/youzan/go-nsq"
	"github.com/youzan/nsq/internal/app"
	"sync/atomic"
	"time"
	"errors"
)

const (
	ModeRoundRobin = iota
	ModeHostPool
)

type TopicSync struct {
	topic	 string
	channel string
	destTopic string
	maxInFlight int

	lookupdHTTPAddrs    []string
	destNsqdTCPAddrs    []string

	statusEvery int
	mode string

	userAgent string
	stopChan chan int
}

func NewTopicSync(topic string, channel string, destTopic string, lookupdHttpAddrs []string, destNsqdTCPAddrs []string) *TopicSync {
	return &TopicSync{
		topic: topic,
		channel:  channel,
		destTopic: destTopic,
		lookupdHTTPAddrs: lookupdHttpAddrs,
		destNsqdTCPAddrs:destNsqdTCPAddrs,
		statusEvery: 250,
		mode: "hostpool",
		stopChan: make(chan int, 1),
	}
}

func (c *TopicSync) Connect() error {
	cCfg := nsq.NewConfig()
	pCfg := nsq.NewConfig()

	var selectedMode int
	switch *c.mode {
	case "round-robin":
		selectedMode = ModeRoundRobin
	case "hostpool", "epsilon-greedy":
		selectedMode = ModeHostPool
	}

	consumer, err := nsq.NewConsumer(c.topic, c.channel, cCfg)
	if err != nil {
		log.Fatal(err)
	}

	producers := make(map[string]*nsq.Producer)
	for _, addr := range c.destNsqdTCPAddrs {
		producer, err := nsq.NewProducer(addr, pCfg)
		if err != nil {
			log.Fatalf("failed creating producer %s", err)
		}
		producers[addr] = producer
	}

	perAddressStatus := make(map[string]*timer_metrics.TimerMetrics)

	if len(c.destNsqdTCPAddrs) == 1 {
		// disable since there is only one address
		perAddressStatus[c.destNsqdTCPAddrs[0]] = timer_metrics.NewTimerMetrics(0, "")
	} else {
		for _, a := range c.destNsqdTCPAddrs {
			perAddressStatus[a] = timer_metrics.NewTimerMetrics(c.statusEvery,
				fmt.Sprintf("[%s]:", a))
		}
	}

	hostPool := hostpool.New(c.destNsqdTCPAddrs)
	if c.mode == "epsilon-greedy" {
		hostPool = hostpool.NewEpsilonGreedy(c.destNsqdTCPAddrs, 0, &hostpool.LinearEpsilonValueCalculator{})
	}

	handler := &PublishHandler{
		addresses:        c.destNsqdTCPAddrs,
		producers:        producers,
		mode:             selectedMode,
		hostPool:         hostPool,
		respChan:         make(chan *nsq.ProducerTransaction, len(c.destNsqdTCPAddrs)),
		perAddressStatus: perAddressStatus,
		timermetrics:     timer_metrics.NewTimerMetrics(*c.statusEvery, "[aggregate]:"),
		destTopic: &c.destTopic,
	}

	consumer.AddConcurrentHandlers(handler, len(c.destNsqdTCPAddrs))

	for i := 0; i < len(c.destNsqdTCPAddrs); i++ {
		go handler.responder()
	}

	err = consumer.ConnectToNSQLookupds(c.lookupdHTTPAddrs)
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case <-consumer.StopChan:
				goto exit
			case <-c.stopChan:
				consumer.Stop()
			}
		}
exit:
		nsqSyncLog.Infof("topic sync process exit. topic: %v, channel: %v to %v", c.topic, c.channel, c.destTopic)
	}()
	return nil
}

type PublishHandler struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	counter uint64

	addresses app.StringArray
	producers map[string]*nsq.Producer

	mode      int
	hostPool  hostpool.HostPool
	respChan  chan *nsq.ProducerTransaction

	requireJSONValueParsed   bool
	requireJSONValueIsNumber bool
	requireJSONNumber        float64

	perAddressStatus map[string]*timer_metrics.TimerMetrics
	timermetrics     *timer_metrics.TimerMetrics

	destTopic *string
}

func (ph *PublishHandler) responder() {
	var msg *nsq.Message
	var startTime time.Time
	var address string
	var hostPoolResponse hostpool.HostPoolResponse

	for t := range ph.respChan {
		switch ph.mode {
		case ModeRoundRobin:
			msg = t.Args[0].(*nsq.Message)
			startTime = t.Args[1].(time.Time)
			hostPoolResponse = nil
			address = t.Args[2].(string)
		case ModeHostPool:
			msg = t.Args[0].(*nsq.Message)
			startTime = t.Args[1].(time.Time)
			hostPoolResponse = t.Args[2].(hostpool.HostPoolResponse)
			address = hostPoolResponse.Host()
		}

		success := t.Error == nil

		if hostPoolResponse != nil {
			if !success {
				hostPoolResponse.Mark(errors.New("failed"))
			} else {
				hostPoolResponse.Mark(nil)
			}
		}

		if success {
			msg.Finish()
		} else {
			msg.Requeue(-1)
		}

		ph.perAddressStatus[address].Status(startTime)
		ph.timermetrics.Status(startTime)
	}
}


func (ph *PublishHandler) HandleMessage(m *nsq.Message) error {
	var err error
	msgBody := m.Body

	startTime := time.Now()
	switch ph.mode {
	case ModeRoundRobin:
		counter := atomic.AddUint64(&ph.counter, 1)
		idx := counter % uint64(len(ph.addresses))
		addr := ph.addresses[idx]
		p := ph.producers[addr]
		err = p.PublishAsync(*ph.destTopic, msgBody, ph.respChan, m, startTime, addr)
	case ModeHostPool:
		hostPoolResponse := ph.hostPool.Get()
		p := ph.producers[hostPoolResponse.Host()]
		err = p.PublishAsync(*ph.destTopic, msgBody, ph.respChan, m, startTime, hostPoolResponse)
		if err != nil {
			hostPoolResponse.Mark(err)
		}
	}


	if err != nil {
		return err
	}
	m.DisableAutoResponse()
	return nil
}