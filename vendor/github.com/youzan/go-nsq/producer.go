package nsq

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	MAX_PARTITION_NUM = 1024
	MIN_RETRY_SLEEP   = time.Millisecond * 8
)

var (
	ErrTopicNotSet        = errors.New("topic is not set as producer")
	ErrNoProducer         = errors.New("topic producer not found")
	errMissingShardingKey = errors.New("missing sharding key for ordered publish")
	removingKeepTime      = time.Minute * 10
)

type producerConn interface {
	String() string
	SetLogger(logger, LogLevel, string)
	Connect() (*IdentifyResponse, error)
	Close() error
	WriteCommand(*Command) error
}

// Producer is a high-level type to publish to NSQ.
//
// A Producer instance is 1:1 with a destination `nsqd`
// and will lazily connect to that instance (and re-connect)
// when Publish commands are executed.
type Producer struct {
	id     int64
	addr   string
	conn   producerConn
	config Config

	logger   logger
	logLvl   LogLevel
	logGuard sync.RWMutex

	responseChan chan []byte
	errorChan    chan []byte
	closeChan    chan int

	transactionChan chan *ProducerTransaction
	transactions    []*ProducerTransaction
	state           int32

	concurrentProducers int32
	stopFlag            int32
	exitChan            chan int
	wg                  sync.WaitGroup
	guard               sync.Mutex
	failedCnt           int32
}

// ProducerTransaction is returned by the async publish methods
// to retrieve metadata about the command after the
// response is received.
type ProducerTransaction struct {
	cmd          *Command
	doneChan     chan *ProducerTransaction
	Error        error         // the error (or nil) of the publish command
	Args         []interface{} // the slice of variadic arguments passed to PublishAsync or MultiPublishAsync
	ResponseData []byte
}

func (t *ProducerTransaction) finish() {
	if t.doneChan != nil {
		t.doneChan <- t
	}
}

// NewProducer returns an instance of Producer for the specified address
//
// The only valid way to create a Config is via NewConfig, using a struct literal will panic.
// After Config is passed into NewProducer the values are no longer mutable (they are copied).
func NewProducer(addr string, config *Config) (*Producer, error) {
	config.assertInitialized()
	err := config.Validate()
	if err != nil {
		return nil, err
	}

	p := &Producer{
		id: atomic.AddInt64(&instCount, 1),

		addr:   addr,
		config: *config,

		logger: log.New(os.Stderr, "", log.Flags()),
		logLvl: LogLevelInfo,

		transactionChan: make(chan *ProducerTransaction),
		exitChan:        make(chan int),
		responseChan:    make(chan []byte),
		errorChan:       make(chan []byte),
	}
	return p, nil
}

// Ping causes the Producer to connect to it's configured nsqd (if not already
// connected) and send a `Nop` command, returning any error that might occur.
//
// This method can be used to verify that a newly-created Producer instance is
// configured correctly, rather than relying on the lazy "connect on Publish"
// behavior of a Producer.
func (w *Producer) Ping() error {
	if atomic.LoadInt32(&w.state) != StateConnected {
		err := w.connect()
		if err != nil {
			return err
		}
	}

	return w.conn.WriteCommand(Nop())
}

// SetLogger assigns the logger to use as well as a level
//
// The logger parameter is an interface that requires the following
// method to be implemented (such as the the stdlib log.Logger):
//
//    Output(calldepth int, s string)
//
func (w *Producer) SetLogger(l logger, lvl LogLevel) {
	w.logGuard.Lock()
	defer w.logGuard.Unlock()

	w.logger = l
	w.logLvl = lvl
}

func (w *Producer) getLogger() (logger, LogLevel) {
	w.logGuard.RLock()
	defer w.logGuard.RUnlock()

	return w.logger, w.logLvl
}

// String returns the address of the Producer
func (w *Producer) String() string {
	return w.addr
}

// Stop initiates a graceful stop of the Producer (permanent)
//
// NOTE: this blocks until completion
func (w *Producer) Stop() {
	w.guard.Lock()
	if !atomic.CompareAndSwapInt32(&w.stopFlag, 0, 1) {
		w.guard.Unlock()
		return
	}
	w.log(LogLevelInfo, "stopping")
	close(w.exitChan)
	w.close()
	w.guard.Unlock()
	w.wg.Wait()
}

// PublishAsync publishes a message body to the specified topic
// but does not wait for the response from `nsqd`.
//
// When the Producer eventually receives the response from `nsqd`,
// the supplied `doneChan` (if specified)
// will receive a `ProducerTransaction` instance with the supplied variadic arguments
// and the response error if present
func (w *Producer) PublishAsync(topic string, body []byte, doneChan chan *ProducerTransaction,
	args ...interface{}) error {
	return w.sendCommandAsync(Publish(topic, body), doneChan, args)
}

// MultiPublishAsync publishes a slice of message bodies to the specified topic
// but does not wait for the response from `nsqd`.
//
// When the Producer eventually receives the response from `nsqd`,
// the supplied `doneChan` (if specified)
// will receive a `ProducerTransaction` instance with the supplied variadic arguments
// and the response error if present
func (w *Producer) MultiPublishAsync(topic string, body [][]byte, doneChan chan *ProducerTransaction,
	args ...interface{}) error {
	cmd, err := MultiPublish(topic, body)
	if err != nil {
		return err
	}
	return w.sendCommandAsync(cmd, doneChan, args)
}

// Publish synchronously publishes a message body to the specified topic, returning
// an error if publish failed
func (w *Producer) Publish(topic string, body []byte) error {
	_, err := w.sendCommand(Publish(topic, body))
	return err
}

// MultiPublish synchronously publishes a slice of message bodies to the specified topic, returning
// an error if publish failed
func (w *Producer) MultiPublish(topic string, body [][]byte) error {
	cmd, err := MultiPublish(topic, body)
	if err != nil {
		return err
	}
	_, err = w.sendCommand(cmd)
	return err
}

func (w *Producer) sendCommand(cmd *Command) ([]byte, error) {
	doneChan := make(chan *ProducerTransaction)
	err := w.sendCommandAsync(cmd, doneChan, nil)
	if err != nil {
		close(doneChan)
		return nil, err
	}
	t := <-doneChan
	return t.ResponseData, t.Error
}

func (w *Producer) sendCommandAsync(cmd *Command, doneChan chan *ProducerTransaction,
	args []interface{}) error {
	// keep track of how many outstanding producers we're dealing with
	// in order to later ensure that we clean them all up...
	atomic.AddInt32(&w.concurrentProducers, 1)
	defer atomic.AddInt32(&w.concurrentProducers, -1)

	if atomic.LoadInt32(&w.state) != StateConnected {
		err := w.connect()
		if err != nil {
			return err
		}
	}

	t := &ProducerTransaction{
		cmd:      cmd,
		doneChan: doneChan,
		Args:     args,
	}

	select {
	case w.transactionChan <- t:
	case <-w.exitChan:
		return ErrStopped
	}

	return nil
}

func (w *Producer) connect() error {
	w.guard.Lock()
	defer w.guard.Unlock()

	if atomic.LoadInt32(&w.stopFlag) == 1 {
		return ErrStopped
	}

	switch state := atomic.LoadInt32(&w.state); state {
	case StateInit:
	case StateConnected:
		return nil
	default:
		return ErrNotConnected
	}

	w.log(LogLevelInfo, "(%s) connecting to nsqd", w.addr)

	logger, logLvl := w.getLogger()

	w.conn = NewConn(w.addr, &w.config, &producerConnDelegate{w})
	w.conn.SetLogger(logger, logLvl, fmt.Sprintf("%3d (%%s)", w.id))

	_, err := w.conn.Connect()
	if err != nil {
		w.conn.Close()
		w.log(LogLevelError, "(%s) error connecting to nsqd - %s", w.addr, err)
		atomic.AddInt32(&w.failedCnt, 1)
		return err
	}
	atomic.StoreInt32(&w.state, StateConnected)
	atomic.StoreInt32(&w.failedCnt, 0)
	w.closeChan = make(chan int)
	w.wg.Add(1)
	go w.router()

	return nil
}

func (w *Producer) close() {
	if !atomic.CompareAndSwapInt32(&w.state, StateConnected, StateDisconnected) {
		return
	}
	w.conn.Close()
	go func() {
		// we need to handle this in a goroutine so we don't
		// block the caller from making progress
		w.wg.Wait()
		atomic.StoreInt32(&w.state, StateInit)
	}()
}

func (w *Producer) router() {
	for {
		select {
		case t := <-w.transactionChan:
			w.transactions = append(w.transactions, t)
			err := w.conn.WriteCommand(t.cmd)
			if err != nil {
				w.log(LogLevelError, "(%s) sending command - %s", w.conn.String(), err)
				w.close()
			}
		case data := <-w.responseChan:
			w.popTransaction(FrameTypeResponse, data)
		case data := <-w.errorChan:
			w.popTransaction(FrameTypeError, data)
		case <-w.closeChan:
			goto exit
		case <-w.exitChan:
			goto exit
		}
	}

exit:
	w.transactionCleanup()
	w.wg.Done()
	w.log(LogLevelInfo, "exiting router")
}

func (w *Producer) popTransaction(frameType int32, data []byte) {
	t := w.transactions[0]
	w.transactions = w.transactions[1:]
	if frameType == FrameTypeError {
		t.Error = ErrProtocol{string(data)}
		if IsFailedOnNotLeader(t.Error) || IsTopicNotExist(t.Error) || IsFailedOnNotWritable(t.Error) {
			// TODO: notify to reload topic-producer relation.
		}
	} else {
		t.ResponseData = data
	}
	t.finish()
}

func (w *Producer) transactionCleanup() {
	// clean up transactions we can easily account for
	for _, t := range w.transactions {
		t.Error = ErrNotConnected
		t.finish()
	}
	w.transactions = w.transactions[:0]

	// spin and free up any writes that might have raced
	// with the cleanup process (blocked on writing
	// to transactionChan)
	for {
		select {
		case t := <-w.transactionChan:
			t.Error = ErrNotConnected
			t.finish()
		default:
			// keep spinning until there are 0 concurrent producers
			if atomic.LoadInt32(&w.concurrentProducers) == 0 {
				return
			}
			// give the runtime a chance to schedule other racing goroutines
			time.Sleep(5 * time.Millisecond)
		}
	}
}

func (w *Producer) log(lvl LogLevel, line string, args ...interface{}) {
	logger, logLvl := w.getLogger()

	if logger == nil {
		return
	}

	if logLvl > lvl {
		return
	}

	logger.Output(2, fmt.Sprintf("%-4s %3d %s", lvl, w.id, fmt.Sprintf(line, args...)))
}

func (w *Producer) onConnResponse(c *Conn, data []byte) { w.responseChan <- data }
func (w *Producer) onConnError(c *Conn, data []byte)    { w.errorChan <- data }
func (w *Producer) onConnHeartbeat(c *Conn)             {}
func (w *Producer) onConnIOError(c *Conn, err error)    { w.close() }
func (w *Producer) onConnClose(c *Conn) {
	w.guard.Lock()
	defer w.guard.Unlock()
	close(w.closeChan)
}

// the strategy how the message publish on different partitions
type PubStrategyType int

const (
	PubRR PubStrategyType = iota
	// not yet supported
	PubDynamicLoad
)

type AddrPartInfo struct {
	addr string
	pid  int
}

type TopicPartProducerInfo struct {
	currentIndex  uint32
	allPartitions []AddrPartInfo
	meta          metaInfo
	isMetaValid   bool
}

func NewTopicPartProducerInfo(meta metaInfo, isMetaValid bool) *TopicPartProducerInfo {
	return &TopicPartProducerInfo{
		currentIndex:  0,
		allPartitions: make([]AddrPartInfo, 0, meta.PartitionNum),
		meta:          meta,
		isMetaValid:   isMetaValid,
	}
}

func (self *TopicPartProducerInfo) updatePartitionInfo(index uint32, addrInfo AddrPartInfo) {
	for len(self.allPartitions) <= int(index) {
		self.allPartitions = append(self.allPartitions, AddrPartInfo{})
	}
	self.allPartitions[index] = addrInfo
}

func (self *TopicPartProducerInfo) removePartitionInfo(index uint32) AddrPartInfo {
	if len(self.allPartitions) <= int(index) {
		return AddrPartInfo{}
	}
	removed := self.allPartitions[index]
	self.allPartitions[index] = AddrPartInfo{}
	return removed
}

func (self *TopicPartProducerInfo) getPartitionInfo(index uint32) AddrPartInfo {
	if len(self.allPartitions) <= int(index) {
		return AddrPartInfo{}
	}
	addr := self.allPartitions[index]
	return addr
}

func (self *TopicPartProducerInfo) getSpecificPartitionInfo(pid int) AddrPartInfo {
	if pid < 0 {
		return AddrPartInfo{}
	}
	if pid < len(self.allPartitions) {
		addrInfo := self.allPartitions[pid]
		if addrInfo.pid == pid {
			return addrInfo
		}
	}
	for _, addrInfo := range self.allPartitions {
		if addrInfo.pid == pid {
			return addrInfo
		}
	}
	return AddrPartInfo{}
}

func FindString(src []string, f string) int {
	for i, v := range src {
		if f == v {
			return i
		}
	}
	return -1
}

type RemoveProducerInfo struct {
	producer *Producer
	ts       time.Time
}

type TopicProducerMgr struct {
	pubStrategy        PubStrategyType
	producerMtx        sync.RWMutex
	topicMtx           sync.RWMutex
	hashMtx            sync.Mutex
	topics             map[string]*TopicPartProducerInfo
	producers          map[string]*Producer
	removingProducers  map[string]*RemoveProducerInfo
	config             Config
	etcdServers        []string
	etcdClusterID      string
	etcdLookupPath     string
	lookupdHTTPAddrs   []string
	logger             logger
	logLvl             LogLevel
	logGuard           sync.RWMutex
	mtx                sync.RWMutex
	lookupdQueryIndex  int
	exitChan           chan int
	wg                 sync.WaitGroup
	lookupdRecheckChan chan int
	newTopicChan       chan string
	newTopicRspChan    chan int
}

// use part=-1 to handle all partitions of topic
func NewTopicProducerMgr(topics []string, conf *Config) (*TopicProducerMgr, error) {
	conf.assertInitialized()
	err := conf.Validate()
	if err != nil {
		return nil, err
	}

	mgr := &TopicProducerMgr{
		topics:             make(map[string]*TopicPartProducerInfo, len(topics)),
		pubStrategy:        conf.PubStrategy,
		producers:          make(map[string]*Producer),
		removingProducers:  make(map[string]*RemoveProducerInfo),
		config:             *conf,
		lookupdRecheckChan: make(chan int),
		exitChan:           make(chan int),
		newTopicChan:       make(chan string),
		newTopicRspChan:    make(chan int),
	}
	for _, t := range topics {
		mgr.topics[t] = NewTopicPartProducerInfo(metaInfo{}, false)
	}
	return mgr, nil
}

func (self *TopicProducerMgr) ConnectToSeeds() error {
	for _, lookup := range self.config.LookupdSeeds {
		err := self.ConnectToNSQLookupd(lookup)
		if err != nil {
			return err
		}
	}
	return nil
}

func (self *TopicProducerMgr) Stop() {
	close(self.exitChan)
	self.producerMtx.RLock()
	for _, p := range self.producers {
		p.Stop()
	}
	for _, p := range self.removingProducers {
		p.producer.Stop()
	}
	self.producerMtx.RUnlock()
	self.wg.Wait()
}

func (self *TopicProducerMgr) SetEtcdConf(servers []string, cluster string, lookupPath string) {
	self.etcdServers = servers
	self.etcdClusterID = cluster
	self.etcdLookupPath = lookupPath
}

func (self *TopicProducerMgr) AddLookupdNodes(addresses []string) {
	for _, addr := range addresses {
		self.ConnectToNSQLookupd(addr)
	}
}

func (self *TopicProducerMgr) ConnectToNSQLookupd(addr string) error {
	if err := validatedLookupAddr(addr); err != nil {
		return err
	}

	self.mtx.Lock()
	for _, x := range self.lookupdHTTPAddrs {
		if x == addr {
			self.mtx.Unlock()
			return nil
		}
	}
	self.lookupdHTTPAddrs = append(self.lookupdHTTPAddrs, addr)
	numLookupd := len(self.lookupdHTTPAddrs)
	self.mtx.Unlock()

	self.log(LogLevelInfo, "new lookupd address added: %s", addr)
	// if this is the first one, kick off the go loop
	if numLookupd == 1 {
		self.queryLookupd("")
		self.wg.Add(1)
		go self.lookupLoop()
	}
	return nil
}

// for async operation, the async error should be check by the application if async operation has error.
func (self *TopicProducerMgr) TriggerCheckForError(err error, delay time.Duration) {
	if err == nil {
		return
	}
	if IsFailedOnNotLeader(err) || IsTopicNotExist(err) || IsFailedOnNotWritable(err) {
		time.Sleep(delay)
		select {
		case self.lookupdRecheckChan <- 1:
		default:
		}
	}
}

// TODO: may be we can move the lookup query to some other manager class and all producer share the
// lookup manager
func (self *TopicProducerMgr) nextLookupdEndpoint(newTopic string) (string, map[string]string, string) {
	self.mtx.RLock()
	if self.lookupdQueryIndex >= len(self.lookupdHTTPAddrs) {
		self.lookupdQueryIndex = 0
	}
	addr := self.lookupdHTTPAddrs[self.lookupdQueryIndex]
	num := len(self.lookupdHTTPAddrs)
	self.mtx.RUnlock()
	self.lookupdQueryIndex = (self.lookupdQueryIndex + 1) % num

	urlString := addr
	if !strings.Contains(urlString, "://") {
		urlString = "http://" + addr
	}

	u, err := url.Parse(urlString)
	if err != nil {
		panic(err)
	}
	listUrl := *u
	if u.Path == "/" || u.Path == "" {
		u.Path = "/lookup"
	}
	listUrl.Path = "/listlookup"

	urlList := make(map[string]string, 0)
	if newTopic != "" {
		tmpUrl := *u
		v, _ := url.ParseQuery(tmpUrl.RawQuery)
		v.Add("topic", newTopic)
		v.Add("metainfo", "true")
		v.Add("access", "w")
		tmpUrl.RawQuery = v.Encode()
		urlList[newTopic] = tmpUrl.String()
	} else {
		for t, _ := range self.topics {
			tmpUrl := *u
			v, _ := url.ParseQuery(tmpUrl.RawQuery)
			v.Add("topic", t)
			v.Add("metainfo", "true")
			v.Add("access", "w")
			tmpUrl.RawQuery = v.Encode()
			urlList[t] = tmpUrl.String()
		}
	}
	return addr, urlList, listUrl.String()
}

func (self *TopicProducerMgr) queryLookupd(newTopic string) {
	if newTopic != "" {
		self.log(LogLevelInfo, "new topic %v added", newTopic)
	}
	addr, topicQueryList, discoveryUrl := self.nextLookupdEndpoint(newTopic)
	// discovery other lookupd nodes from current lookupd or from etcd
	self.log(LogLevelDebug, "discovery nsqlookupd %s", discoveryUrl)
	var lookupdList lookupListResp
	err := apiRequestNegotiateV1("GET", discoveryUrl, nil, &lookupdList)
	if err != nil {
		self.log(LogLevelError, "error discovery nsqlookupd (%s) - %s", discoveryUrl, err)
		if strings.Contains(strings.ToLower(err.Error()), "connection refused") &&
			FindString(self.config.LookupdSeeds, addr) == -1 {
			self.mtx.Lock()
			// remove failed
			self.log(LogLevelInfo, "removing failed lookup : %v", addr)
			newLookupList := make([]string, 0)
			for _, v := range self.lookupdHTTPAddrs {
				if v == addr {
					continue
				} else {
					newLookupList = append(newLookupList, v)
				}
			}
			if len(newLookupList) > 0 {
				self.lookupdHTTPAddrs = newLookupList
			}
			self.mtx.Unlock()
			select {
			case self.lookupdRecheckChan <- 1:
				self.log(LogLevelInfo, "trigger tend for err: %v", err)
			default:
			}
			return
		}
	} else {
		for _, node := range lookupdList.LookupdNodes {
			addr := net.JoinHostPort(node.NodeIp, node.HttpPort)
			self.ConnectToNSQLookupd(addr)
		}
	}

	allTopicParts := make([]AddrPartInfo, 0)
	cleanProducers := make(map[string]*Producer)
	for topicName, topicUrl := range topicQueryList {
		if newTopic != "" {
			if topicName != newTopic {
				continue
			}
		}
		self.log(LogLevelDebug, "querying nsqlookupd for topic %s", topicUrl)
		self.topicMtx.Lock()
		partProducerInfo, ok := self.topics[topicName]
		if newTopic != "" {
			partProducerInfo = NewTopicPartProducerInfo(metaInfo{}, false)
			self.topics[topicName] = partProducerInfo
			ok = true
		}
		self.topicMtx.Unlock()
		if !ok {
			self.log(LogLevelError, "topic %v is not set.", topicName)
			continue
		}
		var data lookupResp
		err = apiRequestNegotiateV1("GET", topicUrl, nil, &data)
		if err != nil {
			// TODO: handle removing the failed nsqlookup and try get the new list.
			self.log(LogLevelError, "error querying nsqlookupd (%s) - %s", topicUrl, err)
			continue
		}

		isMetaValid := true
		if data.Meta.PartitionNum <= 0 {
			isMetaValid = false
			data.Meta.PartitionNum = len(data.Partitions)
		}
		newProducerInfo := NewTopicPartProducerInfo(data.Meta, isMetaValid)
		changed := false
		for partStr, producer := range data.Partitions {
			partID, err := strconv.Atoi(partStr)
			if err != nil {
				self.log(LogLevelError, "got partition producer with invalid partition string: %v", partStr)
				continue
			}
			if partID > MAX_PARTITION_NUM || partID < 0 {
				self.log(LogLevelError, "got partition producer invalid partition : %v", partID)
				continue
			}
			broadcastAddress := producer.BroadcastAddress
			port := producer.TCPPort
			addr := net.JoinHostPort(broadcastAddress, strconv.Itoa(port))
			// for the new lookup response, we use the partition id as the index of slice.
			newProducerInfo.updatePartitionInfo(uint32(partID), AddrPartInfo{addr, partID})
			oldAddr := partProducerInfo.getPartitionInfo(uint32(partID))
			if oldAddr.addr != addr {
				self.log(LogLevelInfo, "topic %v partition [%v] producer changed from [%v] to [%v]", topicName, partID, oldAddr, addr)
				changed = true
			}
		}
		// the old lookup did not return the partition info.
		if len(data.Partitions) == 0 {
			self.log(LogLevelWarning, "no partitions info , use the default producer node info.")
			for index, producer := range data.Producers {
				if index > MAX_PARTITION_NUM {
					self.log(LogLevelError, "got too much producer partition : %v", index)
					continue
				}
				broadcastAddress := producer.BroadcastAddress
				port := producer.TCPPort
				addr := net.JoinHostPort(broadcastAddress, strconv.Itoa(port))
				// for the old lookup response, we do not know the partition id so we just set to -1.
				newProducerInfo.updatePartitionInfo(uint32(index), AddrPartInfo{addr, -1})
				oldAddr := partProducerInfo.getPartitionInfo(uint32(index))
				if oldAddr.addr != addr {
					self.log(LogLevelInfo, "topic %v partition at index [%v] producer changed from [%v] to [%v]", topicName, index, oldAddr, addr)
					changed = true
				}
			}
		}

		self.producerMtx.Lock()
		if len(newProducerInfo.allPartitions) > 0 {
			allTopicParts = append(allTopicParts, newProducerInfo.allPartitions...)
		}
		if changed {
			for partID, addrInfo := range newProducerInfo.allPartitions {
				addr := addrInfo.addr
				_, ok := self.producers[addr]
				if !ok {
					if addr == "" {
						continue
					}
					self.log(LogLevelInfo, "init new producer %v for topic %v-%v", addr, topicName, partID)
					newProd, err := NewProducer(addr, &self.config)
					if err != nil {
						self.log(LogLevelError, "producer %v init failed: %v", addr, err)
					} else {
						newProd.SetLogger(self.getLogger())
						self.producers[addr] = newProd
					}
				}
				delete(self.removingProducers, addr)
			}
		}
		self.producerMtx.Unlock()

		self.topicMtx.Lock()
		self.topics[topicName] = newProducerInfo
		self.topicMtx.Unlock()
	}
	self.producerMtx.Lock()
	if len(allTopicParts) > 0 {
		for k, p := range self.producers {
			found := false
			for _, addrInfo := range allTopicParts {
				if p.addr == addrInfo.addr {
					found = true
					break
				}
			}
			if !found {
				self.log(LogLevelInfo, "mark producer %v removing since not in lookup for all topics", p.addr)
				delete(self.producers, k)
				if _, ok := self.removingProducers[k]; ok {
					continue
				}
				self.removingProducers[k] = &RemoveProducerInfo{producer: p, ts: time.Now()}
			} else {
				delete(self.removingProducers, k)
			}
		}
	}
	for k, p := range self.removingProducers {
		if time.Since(p.ts) > removingKeepTime {
			self.log(LogLevelInfo, "removing producer %v finally stopped", p.producer.addr)
			delete(self.removingProducers, k)
			cleanProducers[k] = p.producer
		}
	}
	self.producerMtx.Unlock()
	for _, p := range cleanProducers {
		p.Stop()
	}
}

func (self *TopicProducerMgr) lookupLoop() {
	defer self.wg.Done()
	ticker := time.NewTicker(self.config.LookupdPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			self.queryLookupd("")
		case <-self.lookupdRecheckChan:
			self.queryLookupd("")
			time.Sleep(time.Millisecond * 100)
		case topic := <-self.newTopicChan:
			self.topicMtx.RLock()
			_, ok := self.topics[topic]
			self.topicMtx.RUnlock()
			if !ok {
				self.queryLookupd(topic)
			}
			self.newTopicRspChan <- 1
		case <-self.exitChan:
			close(self.newTopicRspChan)
			return
		}
	}
}

func (self *TopicProducerMgr) getNextProducerAddr(partProducerInfo *TopicPartProducerInfo, partitionKey []byte) (int, string) {
	addrInfo := AddrPartInfo{"", -1}
	length := len(partProducerInfo.allPartitions)
	if length == 0 {
		return -1, ""
	} else if length == 1 && partitionKey == nil {
		addrInfo = partProducerInfo.getPartitionInfo(0)
		return addrInfo.pid, addrInfo.addr
	}
	retry := 0
	index := uint32(0)
	for addrInfo.addr == "" {
		if partitionKey == nil {
			if self.pubStrategy == PubRR {
				if retry >= length {
					break
				}
				index = atomic.AddUint32(&partProducerInfo.currentIndex, 1)
				addrInfo = partProducerInfo.getPartitionInfo(index % uint32(len(partProducerInfo.allPartitions)))
				retry++
			} else {
				// not supported strategy
				return -1, ""
			}
		} else {
			if !partProducerInfo.isMetaValid || partProducerInfo.meta.PartitionNum <= 0 {
				self.log(LogLevelError, "partition meta info invalid: %v", partProducerInfo.meta)
				return -1, ""
			}

			if self.config.Hasher == nil {
				self.log(LogLevelError, "missing sharding key hasher")
				return -1, ""
			}

			self.hashMtx.Lock()
			self.config.Hasher.Reset()
			self.config.Hasher.Write(partitionKey)
			hashV := self.config.Hasher.Sum32()
			self.hashMtx.Unlock()
			pid := int(hashV) % partProducerInfo.meta.PartitionNum
			addrInfo = partProducerInfo.getSpecificPartitionInfo(pid)
			break
		}
	}
	return addrInfo.pid, addrInfo.addr
}

func (self *TopicProducerMgr) removeProducerForTopic(topic string, pid int, addr string) {
	self.log(LogLevelInfo, "removing producer %v for topic %v-%v ", addr, topic, pid)
	self.topicMtx.Lock()
	v, ok := self.topics[topic]
	if ok {
		newInfo := NewTopicPartProducerInfo(v.meta, v.isMetaValid)
		newInfo.currentIndex = v.currentIndex
		for _, p := range v.allPartitions {
			newInfo.allPartitions = append(newInfo.allPartitions, p)
		}
		changed := false
		for index, info := range newInfo.allPartitions {
			if info.addr == addr && info.pid == pid {
				self.log(LogLevelInfo, "remove producer info %v from topic %v-%v", info, topic, pid)
				newInfo.removePartitionInfo(uint32(index))
				changed = true
			}
		}
		if changed {
			self.topics[topic] = newInfo
		}
	}
	self.topicMtx.Unlock()
}

func (self *TopicProducerMgr) removeProducer(addr string) {
	self.log(LogLevelInfo, "removing producer %v ", addr)
	self.topicMtx.Lock()
	for topic, v := range self.topics {
		newInfo := NewTopicPartProducerInfo(v.meta, v.isMetaValid)
		newInfo.currentIndex = v.currentIndex
		for _, p := range v.allPartitions {
			newInfo.allPartitions = append(newInfo.allPartitions, p)
		}
		for index, info := range newInfo.allPartitions {
			if info.addr == addr {
				self.log(LogLevelInfo, "remove producer info %v from topic %v", info, topic)
				newInfo.removePartitionInfo(uint32(index))
			}
		}
		self.topics[topic] = newInfo
	}
	self.topicMtx.Unlock()
	self.producerMtx.Lock()
	producer, ok := self.producers[addr]
	if ok {
		delete(self.producers, addr)
	}
	delete(self.removingProducers, addr)
	self.producerMtx.Unlock()
	if ok {
		producer.Stop()
	}
}

func (self *TopicProducerMgr) hasAnyProducer(topic string) bool {
	self.topicMtx.RLock()
	partProducerInfo, ok := self.topics[topic]
	self.topicMtx.RUnlock()
	if !ok {
		return false
	}
	for _, info := range partProducerInfo.allPartitions {
		if info.addr != "" {
			return true
		}
	}
	return false
}

func (self *TopicProducerMgr) getProducer(topic string, partitionKey []byte) (*Producer, int, error) {
	self.topicMtx.RLock()
	partProducerInfo, ok := self.topics[topic]
	self.topicMtx.RUnlock()
	if !ok {
		self.newTopicChan <- topic
		select {
		case <-self.newTopicRspChan:
			self.topicMtx.RLock()
			partProducerInfo, ok = self.topics[topic]
			self.topicMtx.RUnlock()
			if !ok {
				return nil, -1, ErrTopicNotSet
			}
		}
	}
	pid, addr := self.getNextProducerAddr(partProducerInfo, partitionKey)
	self.log(LogLevelDebug, "choosing %v producer: %v, %v", topic, pid, addr)
	if addr == "" {
		select {
		case self.lookupdRecheckChan <- 1:
		default:
		}
		return nil, pid, ErrNoProducer
	}
	self.producerMtx.RLock()
	producer, ok := self.producers[addr]
	self.producerMtx.RUnlock()
	if !ok {
		removed, ok := self.removingProducers[addr]
		if !ok {
			return nil, pid, ErrNoProducer
		}
		producer = removed.producer
	}
	return producer, pid, nil
}

func (self *TopicProducerMgr) PublishAsync(topic string, body []byte, doneChan chan *ProducerTransaction,
	args ...interface{}) error {
	return self.doCommandAsyncWithRetry(topic, nil, doneChan, func(pid int) (*Command, error) {
		if pid < 0 || pid == OLD_VERSION_PID {
			// pub to old nsqd that not support partition
			return Publish(topic, body), nil
		}
		return PublishWithPart(topic, strconv.Itoa(pid), body), nil
	}, args)
}

func (self *TopicProducerMgr) PublishAsyncWithJsonExt(topic string, body []byte, ext *MsgExt, doneChan chan *ProducerTransaction,
	args ...interface{}) error {
	return self.doCommandAsyncWithRetry(topic, nil, doneChan, func(pid int) (*Command, error) {
		if pid < 0 {
			return nil, errors.New("pub need partition id")
		}
		return PublishWithJsonExt(topic, strconv.Itoa(pid), body, ext.ToJson())
	}, args)
}

func (self *TopicProducerMgr) MultiPublishAsync(topic string, body [][]byte, doneChan chan *ProducerTransaction,
	args ...interface{}) error {
	return self.doCommandAsyncWithRetry(topic, nil, doneChan, func(pid int) (*Command, error) {
		if pid < 0 || pid == OLD_VERSION_PID {
			// pub to old nsqd that not support partition
			return MultiPublish(topic, body)
		}
		return MultiPublishWithPart(topic, strconv.Itoa(pid), body)
	}, args)
}

func (self *TopicProducerMgr) doCommandAsyncWithRetry(topic string, partitionKey []byte,
	doneChan chan *ProducerTransaction,
	commandFunc func(pid int) (*Command, error), args []interface{}) error {
	retry := uint32(0)
	var err error
	var producer *Producer
	pid := -1

	for retry < 3 {
		retry++
		producer, pid, err = self.getProducer(topic, partitionKey)
		if err != nil {
			if err == ErrNoProducer {
				self.log(LogLevelInfo, "No producer for topic %v", topic)
				time.Sleep(MIN_RETRY_SLEEP + time.Millisecond*time.Duration(10*(2<<retry)))
				continue
			} else {
				break
			}
		}

		cmd, err := commandFunc(pid)
		if err != nil {
			return err
		}
		self.log(LogLevelDebug, "do command to producer %v for topic %v-%v", producer.addr, topic, pid)
		err = producer.sendCommandAsync(cmd, doneChan, args)
		if err != nil {
			self.log(LogLevelInfo, "do command to producer %v for topic %v-%v error: %v", producer.addr, topic, pid, err)
			if atomic.LoadInt32(&producer.failedCnt) > 3 {
				self.removeProducer(producer.addr)
			}
			time.Sleep(MIN_RETRY_SLEEP + time.Millisecond*time.Duration(10*(2<<retry)))
		} else {
			break
		}
	}
	return err
}

func (self *TopicProducerMgr) Publish(topic string, body []byte) error {
	_, err := self.doCommandWithRetry(topic, nil, func(pid int) (*Command, error) {
		if pid < 0 || pid == OLD_VERSION_PID {
			// pub to old nsqd that not support partition
			return Publish(topic, body), nil
		}
		return PublishWithPart(topic, strconv.Itoa(pid), body), nil
	})
	return err
}

func (self *TopicProducerMgr) PublishOrdered(topic string, partitionKey []byte, body []byte) (NewMessageID,
	uint64, uint32, error) {
	if partitionKey == nil {
		return 0, 0, 0, errMissingShardingKey
	}
	resp, err := self.doCommandWithRetry(topic, partitionKey, func(pid int) (*Command, error) {
		if pid < 0 {
			return nil, errors.New("ordered pub need partition id")
		}
		if self.config.EnableTrace {
			self.hashMtx.Lock()
			self.config.Hasher.Reset()
			self.config.Hasher.Write(partitionKey)
			hashV := self.config.Hasher.Sum32()
			self.hashMtx.Unlock()
			return PublishTrace(topic, strconv.Itoa(pid), uint64(hashV), body)
		}
		return PublishWithPart(topic, strconv.Itoa(pid), body), nil
	})
	if err != nil {
		return 0, 0, 0, err
	}
	if !self.config.EnableTrace {
		if len(resp) == 2 && string(resp) == "OK" {
			return 0, 0, 0, nil
		}
		return 0, 0, 0, errors.New("response not valid")
	}
	// response should be : OK+16 bytes id+8bytes offset+4 bytes size
	if len(resp) < 2+MsgIDLength+8+4 {
		self.log(LogLevelError, "trace response invalid: %v", resp)
		return 0, 0, 0, errors.New("trace response not valid")
	}
	id := GetNewMessageID(resp[2 : 2+MsgIDLength])
	offset := binary.BigEndian.Uint64(resp[2+MsgIDLength : 2+MsgIDLength+8])
	rawSize := binary.BigEndian.Uint32(resp[2+MsgIDLength+8 : 2+MsgIDLength+8+4])
	return id, offset, rawSize, err
}

func (self *TopicProducerMgr) PublishOrderedWithJsonExt(topic string, partitionKey []byte, body []byte, ext *MsgExt) (NewMessageID,
	uint64, uint32, error) {
	if partitionKey == nil {
		return 0, 0, 0, errMissingShardingKey
	}
	resp, err := self.doCommandWithRetry(topic, partitionKey, func(pid int) (*Command, error) {
		if pid < 0 {
			return nil, errors.New("ordered pub need partition id")
		}
		return PublishWithJsonExt(topic, strconv.Itoa(pid), body, ext.ToJson())
	})
	if err != nil {
		return 0, 0, 0, err
	}
	if ext.TraceID == 0 {
		if len(resp) == 2 && string(resp) == "OK" {
			return 0, 0, 0, nil
		}
		return 0, 0, 0, errors.New("response not valid")
	}
	// response should be : OK+16 bytes id+8bytes offset+4 bytes size
	if len(resp) < 2+MsgIDLength+8+4 {
		self.log(LogLevelError, "trace response invalid: %v", resp)
		return 0, 0, 0, errors.New("trace response not valid")
	}
	id := GetNewMessageID(resp[2 : 2+MsgIDLength])
	offset := binary.BigEndian.Uint64(resp[2+MsgIDLength : 2+MsgIDLength+8])
	rawSize := binary.BigEndian.Uint32(resp[2+MsgIDLength+8 : 2+MsgIDLength+8+4])
	return id, offset, rawSize, err
}

// pub with trace will return the message id and the message offset and size in the queue
func (self *TopicProducerMgr) PublishAndTrace(topic string, traceID uint64, body []byte) (NewMessageID,
	uint64, uint32, error) {
	resp, err := self.doCommandWithRetry(topic, nil, func(pid int) (*Command, error) {
		return PublishTrace(topic, strconv.Itoa(pid), traceID, body)
	})
	if err != nil {
		return 0, 0, 0, err
	}
	// response should be : OK+16 bytes id+8bytes offset+4 bytes size
	if len(resp) < 2+MsgIDLength+8+4 {
		self.log(LogLevelError, "trace response invalid: %v", resp)
		return 0, 0, 0, errors.New("trace response not valid")
	}
	id := GetNewMessageID(resp[2 : 2+MsgIDLength])
	offset := binary.BigEndian.Uint64(resp[2+MsgIDLength : 2+MsgIDLength+8])
	rawSize := binary.BigEndian.Uint32(resp[2+MsgIDLength+8 : 2+MsgIDLength+8+4])
	return id, offset, rawSize, err
}

type MsgExt struct {
	TraceID     uint64
	DispatchTag string
	Custom      map[string]string
}

func (ext *MsgExt) ToJson() []byte {
	if ext.Custom == nil {
		ext.Custom = make(map[string]string)
	}
	if ext.TraceID > 0 {
		ext.Custom[traceIDExtK] = strconv.FormatUint(ext.TraceID, 10)
	}
	if ext.DispatchTag != "" {
		ext.Custom[dispatchTagExtK] = ext.DispatchTag
	}

	jsonExt, _ := json.Marshal(ext.Custom)
	return jsonExt
}

func (self *TopicProducerMgr) PublishWithJsonExt(topic string, body []byte, ext *MsgExt) (NewMessageID,
	uint64, uint32, error) {
	resp, err := self.doCommandWithRetry(topic, nil, func(pid int) (*Command, error) {
		if pid < 0 {
			return nil, errors.New("pub with tag need partition id")
		}

		return PublishWithJsonExt(topic, strconv.Itoa(pid), body, ext.ToJson())
	})
	if err != nil {
		return 0, 0, 0, err
	}
	if ext.TraceID != 0 {
		// response should be : OK+16 bytes id+8bytes offset+4 bytes size
		if len(resp) < 2+MsgIDLength+8+4 {
			self.log(LogLevelError, "trace response invalid: %v", resp)
			return 0, 0, 0, errors.New("trace response not valid")
		}
		id := GetNewMessageID(resp[2 : 2+MsgIDLength])
		offset := binary.BigEndian.Uint64(resp[2+MsgIDLength : 2+MsgIDLength+8])
		rawSize := binary.BigEndian.Uint32(resp[2+MsgIDLength+8 : 2+MsgIDLength+8+4])
		return id, offset, rawSize, err
	} else {
		if len(resp) < 2 || string(resp) != "OK" {
			return 0, 0, 0, errors.New("response not valid")
		}
		return 0, 0, 0, nil
	}
}

func (self *TopicProducerMgr) MultiPublishV2(topic string, body []*bytes.Buffer) error {
	_, err := self.doCommandWithRetry(topic, nil, func(pid int) (*Command, error) {
		if pid < 0 || pid == OLD_VERSION_PID {
			// pub to old nsqd that not support partition
			return MultiPublishV2(topic, body)
		}
		return MultiPublishWithPartV2(topic, strconv.Itoa(pid), body)
	})
	return err
}

func (self *TopicProducerMgr) MultiPublish(topic string, body [][]byte) error {
	_, err := self.doCommandWithRetry(topic, nil, func(pid int) (*Command, error) {
		if pid < 0 || pid == OLD_VERSION_PID {
			// pub to old nsqd that not support partition
			return MultiPublish(topic, body)
		}
		return MultiPublishWithPart(topic, strconv.Itoa(pid), body)
	})
	return err
}

func (self *TopicProducerMgr) MultiPublishAndTrace(topic string, traceIDList []uint64, body [][]byte) (NewMessageID, uint64, uint32, error) {
	if len(traceIDList) != len(body) {
		return 0, 0, 0, errors.New("arguments error")
	}
	resp, err := self.doCommandWithRetry(topic, nil, func(pid int) (*Command, error) {
		return MultiPublishTrace(topic, strconv.Itoa(pid), traceIDList, body)
	})
	if err != nil {
		return 0, 0, 0, err
	}
	if len(resp) < 2+MsgIDLength+8+4 {
		self.log(LogLevelError, "trace response invalid: %v", resp)
		return 0, 0, 0, errors.New("trace response not valid")
	}

	id := GetNewMessageID(resp[2 : 2+MsgIDLength])
	offset := binary.BigEndian.Uint64(resp[2+MsgIDLength : 2+MsgIDLength+8])
	rawSize := binary.BigEndian.Uint32(resp[2+MsgIDLength+8 : 2+MsgIDLength+8+4])
	return id, offset, rawSize, err
}

func (self *TopicProducerMgr) doCommandWithRetry(topic string, partitionKey []byte,
	commandFunc func(pid int) (*Command, error)) ([]byte, error) {
	retry := uint32(0)
	var err error
	var producer *Producer
	var resp []byte
	pid := -1
	for retry < 3 {
		retry++
		producer, pid, err = self.getProducer(topic, partitionKey)
		if err != nil {
			if err == ErrNoProducer {
				self.log(LogLevelInfo, "No producer for topic %v", topic)
				time.Sleep(MIN_RETRY_SLEEP + time.Millisecond*time.Duration(10*(2<<retry)))
				continue
			} else {
				break
			}
		}
		cmd, err := commandFunc(pid)
		if err != nil {
			self.log(LogLevelError, "get command err: %v", err)
			return nil, err
		}
		self.log(LogLevelDebug, "do command to producer %v for topic %v-%v", producer.addr, topic, pid)
		resp, err = producer.sendCommand(cmd)
		if err != nil {
			self.log(LogLevelInfo, "do command to producer %v for topic %v-%v error: %v", producer.addr, topic, pid, err)
			if atomic.LoadInt32(&producer.failedCnt) > 3 {
				self.removeProducer(producer.addr)
			} else if IsFailedOnNotLeader(err) || IsFailedOnNotWritable(err) ||
				IsTopicNotExist(err) {
				self.removeProducerForTopic(topic, pid, producer.addr)
			}
			if self.hasAnyProducer(topic) {
				go self.TriggerCheckForError(err, time.Second*time.Duration(retry+1))
			} else {
				self.TriggerCheckForError(err, time.Millisecond*100*time.Duration(retry))
			}
			time.Sleep(MIN_RETRY_SLEEP + time.Millisecond*time.Duration(10*(2<<retry)))
		} else {
			break
		}
	}
	return resp, err
}

func (self *TopicProducerMgr) log(lvl LogLevel, line string, args ...interface{}) {
	logger, logLvl := self.getLogger()

	if logger == nil {
		return
	}

	if logLvl > lvl {
		return
	}

	logger.Output(2, fmt.Sprintf("%-4s %s", lvl, fmt.Sprintf(line, args...)))
}

func (self *TopicProducerMgr) SetLogger(l logger, lvl LogLevel) {
	self.logGuard.Lock()
	defer self.logGuard.Unlock()

	self.logger = l
	self.logLvl = lvl
}

func (self *TopicProducerMgr) SetLoggerLevel(lvl LogLevel) {
	self.logGuard.Lock()
	defer self.logGuard.Unlock()
	self.logLvl = lvl
}

func (self *TopicProducerMgr) getLogger() (logger, LogLevel) {
	self.logGuard.RLock()
	lg, lv := self.logger, self.logLvl
	self.logGuard.RUnlock()
	return lg, lv
}
