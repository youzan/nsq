package nsqlookupd_migrate

import (
	"net/http"
	"time"
	"github.com/DoraALin/docker/docker/pkg/random"
	"sync/atomic"
	"encoding/json"
	"math/rand"
	"fmt"
	"sync"
	"github.com/julienschmidt/httprouter"
	"github.com/twinj/uuid"
	"strings"
	dcc "gitlab.qima-inc.com/wangjian/go-dcc-sdk"
)

var LIST_LOOKUP_PATH = "/listlookup"
var LOOKUP_PATH = "/lookup?topic=%v&access=%v&metainfo=%v"
var LIST_TOPICS_PATH = "/topics"
var testerLog *MigrateLogger

type MCTester struct {
	mc *MigrateConfig
	client	*http.Client
	lookupdAddr string
	topiclist []string
	topicsConfig map[string]int
	sync.RWMutex
	updateTopicsClsChan chan int
	updateMigrateConfigClsChan chan int
	dccClient	*dcc.DccClient
}

func NewMCTester(cxt *Context) (*MCTester, error) {
	mc, err := NewMigrateConfig(cxt)
	if err != nil {
		testerLog.Error("fail to creat migrate config %v", err)
		return nil, err
	}
	mcTester := &MCTester{
		mc:	mc,
		dccClient : dcc.NewDccClient(cxt.DccUrl, cxt.DccBackupFile, "nsq_migrate_test"),
		client:	&http.Client{},
		lookupdAddr: cxt.ProxyHttpAddrTest,
		updateTopicsClsChan: make(chan int),
		updateMigrateConfigClsChan: make(chan int),
		topicsConfig: make(map[string]int),
	}

	return mcTester, nil
}

func (m *MCTester) updateTopicList(closeChan chan int) {
	for {
		topics, err := getTopics(m.client, m.lookupdAddr)
		if err != nil {
			testerLog.Error("migrate config tester fails to get topics", err)
			return
		}

		m.Lock()
		m.topiclist = topics.Data.Topics
		m.Unlock()

		select {
		case <- time.After(30 * time.Minute):
		case <- closeChan:
			goto exit
		}
	}
exit:
	testerLog.Info("update topic list process exits.")
}

func (m *MCTester) updateMigrateConfig(closeChan chan int) {
	for {
		topicNum := rand.Intn(5)
		var topics []string
		m.RLock()
		topicsTotal := len(m.topiclist)
		start := rand.Intn(topicsTotal)
		for i := 0; i < topicNum; i ++ {
			topics = append(topics, m.topiclist[(start + i)%topicsTotal])
		}
		m.RUnlock()
		var kvReqs []dcc.KVPair
		for _, topic := range topics {
			s := fmt.Sprintf("%v", (m.topicsConfig[topic] + 1)%4)
			kvReqs = append(kvReqs, dcc.KVPair{
				Key: topic,
				Value:	s,
			})
			testerLog.Info("topic: %v, switch: %v", topic, s)
		}
		publishReq := []*dcc.PublishRequest{
			{
				App:m.mc.App,
				Key:m.mc.Key,
				Type:m.mc.ReqType,
				CombVal:kvReqs,
			},
		}
		err := m.dccClient.Publish(publishReq)
		if err != nil {
			testerLog.Error("fail to publish migrate config %v. err %v", publishReq, err)
		}

		select {
		case <- time.After(10 * time.Minute):
		case <- closeChan:
			goto exit
		}
	}
exit:
	testerLog.Info("update migrate config process exits.")
}

func (m *MCTester) Start() {
	go m.updateTopicList(m.updateTopicsClsChan)
	<-time.After(1 * time.Second)
	go m.updateMigrateConfig(m.updateMigrateConfigClsChan)
	testerLog.Info("update topics process starts")
}

func (m *MCTester) Close() {
	close(m.updateMigrateConfigClsChan)
	close(m.updateTopicsClsChan)
	testerLog.Info("mc tester exits.")
}

type Tester struct {
	ProxyAddr string
	ClientPool []*AccessClient
	TestClientNum int64
	lastTotalCnt  int64
	lastFailedCnt int64
	lastSuccessCnt int64
	qpsLock        sync.RWMutex
	qps	       float32
	closeChan chan int
	start uint32
	startMCTest   bool
	mcTester	*MCTester
}

func (t *Tester) Start() {
	var i int64
	if !atomic.CompareAndSwapUint32(&t.start, 0, 1) {
		return
	}
	if int64(len(t.ClientPool)) != t.TestClientNum {
		t.ClientPool = t.ClientPool[:0]
		for i = 0; i < t.TestClientNum; i++ {
			client := &AccessClient{
				ProxyAddr:t.ProxyAddr,
				CloseChan: make(chan int),
				client:&http.Client{},
			}
			t.ClientPool = append(t.ClientPool, client)
		}
	}

	for _, client := range t.ClientPool {
		client.start()
	}

	//start migrate modification
	go t.calculateQps()

	if t.startMCTest {
		t.mcTester.Start()
	}
}

func (t *Tester) startMigrateGuardTest() {

}

func (t *Tester) Close() {
	if !atomic.CompareAndSwapUint32(&t.start, 1, 0) {
		return
	}
	for _, client := range t.ClientPool {
		client.close()
	}
	t.closeChan <- 1
	testerLog.Info("tester closes")

	//close mc tester
	if t.startMCTest {
		t.mcTester.Close()
	}
}

func (t *Tester) calculateQps() {
	for {
		select {
		case <-time.After(5 * time.Second):
		case <- t.closeChan:
			testerLog.Info("calculate qps quit")
			return
		}
		var totalCnt, failedCnt, successCnt int64
		for _, client := range t.ClientPool {
			totalCnt += client.getTotalCnt()
			failedCnt += client.getFailedCnt()
			successCnt += client.getSuccessCnt()
		}
		qps := float32(totalCnt - t.lastTotalCnt)/5
		t.lastTotalCnt = totalCnt
		t.lastFailedCnt = failedCnt
		t.lastSuccessCnt = successCnt

		t.qpsLock.Lock()
		t.qps = qps
		t.qpsLock.Unlock()
	}
}

func (t *Tester) GetQps() float32 {
	t.qpsLock.RLock()
	defer t.qpsLock.RUnlock()
	return t.qps
}

func NewTester(context *Context) (*Tester, error) {
	if testerLog == nil {
		testerLog = NewMigrateLogger(context.LogLevel)
	}
	test := &Tester{
		ProxyAddr:	context.ProxyHttpAddrTest,
		TestClientNum:  context.TestClientNum,
		closeChan: make(chan int, 1),
		startMCTest:   context.MCTest,
	}
	var err error
	if test.startMCTest {
		test.mcTester, err = NewMCTester(context)
		if err != nil {
			return nil, err
		}
	}
	return test, nil
}

type AccessClient struct {
	ProxyAddr string
	CloseChan chan int
	client *http.Client

	success	int64
	fail 	int64
	total   int64
}

func (c *AccessClient) start() {
	go c.process()
	testerLog.Info("access client starts")
}

func (c *AccessClient) close() {
	c.CloseChan <- 1
}

func (c *AccessClient) getFailedCnt() int64 {
	return atomic.LoadInt64(&c.fail)
}

func (c *AccessClient) getSuccessCnt() int64 {
	return atomic.LoadInt64(&c.success)
}

func (c *AccessClient) getTotalCnt() int64 {
	return atomic.LoadInt64(&c.total)
}

func (c *AccessClient) incFailed() {
	atomic.AddInt64(&c.fail, 1)
	c.incTotal()
}

func (c *AccessClient) incSuccess() {
	atomic.AddInt64(&c.success, 1)
	c.incTotal()
}

func (c *AccessClient) incTotal() {
	atomic.AddInt64(&c.total, 1)
}

func getTopics(client *http.Client, lookupdAddr string) (*Topics_old, error) {
	//process topics
	resp, err := client.Get(lookupdAddr + LIST_TOPICS_PATH)
	if err != nil {
		return nil, fmt.Errorf("fail to list topics. err %v", err)
	}

	var topics Topics_old
	err = json.NewDecoder(resp.Body).Decode(&topics)
	resp.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("fail to parse topics, err %v", err)
	}

	return &topics, nil
}

func (c *AccessClient) process() {
	for {
		sleep := random.Rand.Intn(5) + 2
		select {
		case <-time.After(time.Duration(sleep * 100) * time.Millisecond):
		case <- c.CloseChan:
			goto exit
		}


		topics, err := getTopics(c.client, c.ProxyAddr)
		if err != nil {
			testerLog.Error("fail to list topics. err %v", err)
			c.incFailed()
			continue
		}

		//pick one topic to lookup
		topicIdx := rand.Intn(len(topics.Data.Topics))
		topic := topics.Data.Topics[topicIdx]

		//process listlookup
		resp, err := c.client.Get(c.ProxyAddr + LIST_LOOKUP_PATH)
		if err != nil {
			testerLog.Error("fail to listlookup. err %v", err)
			c.incFailed()
			continue
		}
		resp.Body.Close()

		access := "w"
		if rand.Intn(2) == 1 {
			access = "r"
		}

		metainfo := true
		if rand.Intn(2) == 1 {
			metainfo = false
		}

		lookupUrl := fmt.Sprintf(c.ProxyAddr + LOOKUP_PATH, topic, access, metainfo)
		//give up listlookup response, as migrate always returns 400
		resp, err = c.client.Get(lookupUrl)
		if (err != nil) {
			testerLog.Error("fail to listlookup. err %v", err)
			c.incFailed()
			continue
		}

		if resp.StatusCode != 200 {
			testerLog.Error("lookup response with code: %v, lookupUrl: %v", resp.StatusCode, lookupUrl)
			c.incFailed()
			continue
		}

		var lookupInfo Lookupinfo_old
		err = json.NewDecoder(resp.Body).Decode(&lookupInfo)
		resp.Body.Close()
		if err != nil {
			testerLog.Error("fail to parse lookup response.", err)
			c.incFailed()
			continue
		}

		c.incSuccess()
	}

exit:
	testerLog.Info("access client exit.")
}

type httpTester struct {
	Router http.Handler
	context *Context
	Tester *Tester
}

func NewProxyTester(context *Context) (*httpTester, error) {
	testerLog = NewMigrateLogger(context.LogLevel)
	context.Logger = testerLog
	router := httprouter.New()
	router.HandleMethodNotAllowed = true
	//router.PanicHandler = LogPanicHandler()
	router.NotFound = LogNotFoundHandler()
	router.MethodNotAllowed = LogMethodNotAllowedHandler()
	tester, err := NewTester(context)
	if err != nil {
		return nil, err
	}

	s := &httpTester{
		Router: router,
		context: context,
		Tester:  tester,
	}
	router.Handle("GET", "/start", Decorate(s.testerHandler, TagMigrate))
	router.Handle("GET", "/stop", Decorate(s.testerHandler, TagMigrate))
	router.Handle("GET", "/qps", Decorate(s.qpsHandler, TagMigrate))

	testerLog.Info("HTTP proxy tester initialized")
	return s, nil
}

func (s *httpTester) qpsHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, *QueryHost, error) {
	qh := &QueryHost{
		QueryId: uuid.NewV4().String(),
		RemoteAddr:req.RemoteAddr,
		QueryURI:req.RequestURI,
		TimeArrives:time.Now(),
	}
	qps := fmt.Sprintf("%v", s.Tester.GetQps())
	return qps, qh, nil
}

func (s *httpTester) testerHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, *QueryHost, error) {
	qh := &QueryHost{
		QueryId: uuid.NewV4().String(),
		RemoteAddr:req.RemoteAddr,
		QueryURI:req.RequestURI,
		TimeArrives:time.Now(),
	}
	if strings.Contains(req.RequestURI, "/start") {
		s.Tester.Start()
		return "start", qh, nil
	} else {
		s.Tester.Close()
		return "close", qh, nil
	}
}
