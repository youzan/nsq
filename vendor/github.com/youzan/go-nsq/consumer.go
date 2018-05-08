package nsq

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
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
	// for old nsqd it may return the special pid for compatible
	OLD_VERSION_PID = -11
)

// Handler is the message processing interface for Consumer
//
// Implement this interface for handlers that return whether or not message
// processing completed successfully.
//
// When the return value is nil Consumer will automatically handle FINishing.
//
// When the returned value is non-nil Consumer will automatically handle REQueing.
type Handler interface {
	HandleMessage(message *Message) error
}

// HandlerFunc is a convenience type to avoid having to declare a struct
// to implement the Handler interface, it can be used like this:
//
// 	consumer.AddHandler(nsq.HandlerFunc(func(m *Message) error {
// 		// handle the message
// 	}))
type HandlerFunc func(message *Message) error

// HandleMessage implements the Handler interface
func (h HandlerFunc) HandleMessage(m *Message) error {
	return h(m)
}

// DiscoveryFilter is an interface accepted by `SetBehaviorDelegate()`
// for filtering the nsqds returned from discovery via nsqlookupd
type DiscoveryFilter interface {
	Filter([]string) []string
}

// FailedMessageLogger is an interface that can be implemented by handlers that wish
// to receive a callback when a message is deemed "failed" (i.e. the number of attempts
// exceeded the Consumer specified MaxAttemptCount)
type FailedMessageLogger interface {
	LogFailedMessage(message *Message)
}

// ConsumerStats represents a snapshot of the state of a Consumer's connections and the messages
// it has seen
type ConsumerStats struct {
	MessagesReceived uint64
	MessagesFinished uint64
	MessagesRequeued uint64
	Connections      int
}

var instCount int64

type backoffSignal int

const (
	backoffFlag backoffSignal = iota
	continueFlag
	resumeFlag
)

// Consumer is a high-level type to consume from NSQ.
//
// A Consumer instance is supplied a Handler that will be executed
// concurrently via goroutines to handle processing the stream of messages
// consumed from the specified topic/channel. See: Handler/HandlerFunc
// for details on implementing the interface to create handlers.
//
// If configured, it will poll nsqlookupd instances and handle connection (and
// reconnection) to any discovered nsqds.
type Consumer struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	messagesReceived uint64
	messagesFinished uint64
	messagesRequeued uint64
	totalRdyCount    int64
	backoffDuration  int64
	backoffCounter   int32
	maxInFlight      int32

	consume_ext int32

	mtx sync.RWMutex

	logger   logger
	logLvl   LogLevel
	logGuard sync.RWMutex

	behaviorDelegate interface{}

	id        int64
	topic     string
	partition int
	channel   string
	config    Config

	rngMtx sync.Mutex
	rng    *rand.Rand

	needRDYRedistributed int32

	backoffMtx sync.RWMutex

	incomingMessages chan *Message

	rdyRetryMtx    sync.RWMutex
	rdyRetryTimers map[string]*time.Timer

	pendingConnections map[string]*Conn
	connections        map[string]*Conn

	nsqdTCPAddrs []AddrPartInfo

	// used at connection close to force a possible reconnect
	lookupdRecheckChan chan int
	lookupdHTTPAddrs   []string
	lookupdQueryIndex  int

	wg              sync.WaitGroup
	runningHandlers int32
	stopFlag        int32
	connectedFlag   int32
	stopHandler     sync.Once
	exitHandler     sync.Once

	// read from this channel to block until consumer is cleanly stopped
	StopChan chan int
	exitChan chan int
	// the offset will be set only once at the first sub to nsqd
	offsetMutex   sync.Mutex
	consumeOffset map[int]ConsumeOffset
}

// NewConsumer creates a new instance of Consumer for the specified topic/channel
//
// The only valid way to create a Config is via NewConfig, using a struct literal will panic.
// After Config is passed into NewConsumer the values are no longer mutable (they are copied).
func NewConsumer(topic string, channel string, config *Config) (*Consumer, error) {
	return NewPartitionConsumer(topic, -1, channel, config)
}

func NewPartitionConsumer(topic string, part int, channel string, config *Config) (*Consumer, error) {
	config.assertInitialized()

	if err := config.Validate(); err != nil {
		return nil, err
	}

	if !IsValidTopicName(topic) {
		return nil, errors.New("invalid topic name")
	}

	if !IsValidChannelName(channel) {
		return nil, errors.New("invalid channel name")
	}

	r := &Consumer{
		id: atomic.AddInt64(&instCount, 1),

		topic:     topic,
		partition: part,
		channel:   channel,
		config:    *config,

		logger:      log.New(os.Stderr, "", log.Flags()),
		logLvl:      LogLevelInfo,
		maxInFlight: int32(config.MaxInFlight),

		incomingMessages: make(chan *Message),

		rdyRetryTimers:     make(map[string]*time.Timer),
		pendingConnections: make(map[string]*Conn),
		connections:        make(map[string]*Conn),

		lookupdRecheckChan: make(chan int, 1),

		rng: rand.New(rand.NewSource(time.Now().UnixNano())),

		StopChan:      make(chan int),
		exitChan:      make(chan int),
		consumeOffset: make(map[int]ConsumeOffset),
	}
	r.wg.Add(1)
	go r.rdyLoop()
	return r, nil
}

func (r *Consumer) SetConsumeExt(topicExt bool) bool {
	if topicExt == true {
		return atomic.CompareAndSwapInt32(&r.consume_ext, 0, 1)
	} else {
		return atomic.CompareAndSwapInt32(&r.consume_ext, 1, 0)
	}
}

func (r *Consumer) IsConsumeExt() bool {
	return atomic.LoadInt32(&r.consume_ext) == int32(1)
}

func (r *Consumer) SetConsumeOffset(partition int, offset ConsumeOffset) error {
	if !r.config.EnableTrace {
		return errors.New("trace must be enabled to allow set consume offset")
	}
	r.offsetMutex.Lock()
	r.consumeOffset[partition] = offset
	r.offsetMutex.Unlock()
	return nil
}

// Stats retrieves the current connection and message statistics for a Consumer
func (r *Consumer) Stats() *ConsumerStats {
	return &ConsumerStats{
		MessagesReceived: atomic.LoadUint64(&r.messagesReceived),
		MessagesFinished: atomic.LoadUint64(&r.messagesFinished),
		MessagesRequeued: atomic.LoadUint64(&r.messagesRequeued),
		Connections:      len(r.conns()),
	}
}

func (r *Consumer) conns() []*Conn {
	r.mtx.RLock()
	conns := make([]*Conn, 0, len(r.connections))
	for _, c := range r.connections {
		conns = append(conns, c)
	}
	r.mtx.RUnlock()
	return conns
}

// SetLogger assigns the logger to use as well as a level
//
// The logger parameter is an interface that requires the following
// method to be implemented (such as the the stdlib log.Logger):
//
//    Output(calldepth int, s string)
//
func (r *Consumer) SetLogger(l logger, lvl LogLevel) {
	r.logGuard.Lock()
	defer r.logGuard.Unlock()

	r.logger = l
	r.logLvl = lvl
}

func (r *Consumer) getLogger() (logger, LogLevel) {
	r.logGuard.RLock()
	defer r.logGuard.RUnlock()

	return r.logger, r.logLvl
}

// SetBehaviorDelegate takes a type implementing one or more
// of the following interfaces that modify the behavior
// of the `Consumer`:
//
//    DiscoveryFilter
//
func (r *Consumer) SetBehaviorDelegate(cb interface{}) {
	matched := false

	if _, ok := cb.(DiscoveryFilter); ok {
		matched = true
	}

	if !matched {
		panic("behavior delegate does not have any recognized methods")
	}

	r.behaviorDelegate = cb
}

// perConnMaxInFlight calculates the per-connection max-in-flight count.
//
// This may change dynamically based on the number of connections to nsqd the Consumer
// is responsible for.
func (r *Consumer) perConnMaxInFlight() int64 {
	b := float64(r.getMaxInFlight())
	s := b / float64(len(r.conns()))
	return int64(math.Min(math.Max(1, s), b))
}

// IsStarved indicates whether any connections for this consumer are blocked on processing
// before being able to receive more messages (ie. RDY count of 0 and not exiting)
func (r *Consumer) IsStarved() bool {
	for _, conn := range r.conns() {
		threshold := int64(float64(atomic.LoadInt64(&conn.lastRdyCount)) * 0.85)
		inFlight := atomic.LoadInt64(&conn.messagesInFlight)
		if inFlight >= threshold && inFlight > 0 && !conn.IsClosing() {
			return true
		}
	}
	return false
}

func (r *Consumer) getMaxInFlight() int32 {
	return atomic.LoadInt32(&r.maxInFlight)
}

// ChangeMaxInFlight sets a new maximum number of messages this comsumer instance
// will allow in-flight, and updates all existing connections as appropriate.
//
// For example, ChangeMaxInFlight(0) would pause message flow
//
// If already connected, it updates the reader RDY state for each connection.
func (r *Consumer) ChangeMaxInFlight(maxInFlight int) {
	if r.getMaxInFlight() == int32(maxInFlight) {
		return
	}

	atomic.StoreInt32(&r.maxInFlight, int32(maxInFlight))

	for _, c := range r.conns() {
		r.maybeUpdateRDY(c)
	}
}

func (r *Consumer) ConnectToSeeds() error {
	for _, lookup := range r.config.LookupdSeeds {
		err := r.ConnectToNSQLookupd(lookup)
		if err != nil {
			return err
		}
	}
	return nil
}

// ConnectToNSQLookupd adds an nsqlookupd address to the list for this Consumer instance.
//
// If it is the first to be added, it initiates an HTTP request to discover nsqd
// producers for the configured topic.
//
// A goroutine is spawned to handle continual polling.
func (r *Consumer) ConnectToNSQLookupd(addr string) error {
	if atomic.LoadInt32(&r.stopFlag) == 1 {
		return errors.New("consumer stopped")
	}
	if atomic.LoadInt32(&r.runningHandlers) == 0 {
		return errors.New("no handlers")
	}

	if err := validatedLookupAddr(addr); err != nil {
		return err
	}

	atomic.StoreInt32(&r.connectedFlag, 1)

	r.mtx.Lock()
	for _, x := range r.lookupdHTTPAddrs {
		if x == addr {
			r.mtx.Unlock()
			return nil
		}
	}
	r.lookupdHTTPAddrs = append(r.lookupdHTTPAddrs, addr)
	numLookupd := len(r.lookupdHTTPAddrs)
	r.mtx.Unlock()

	r.log(LogLevelInfo, "new lookupd address added: %s", addr)
	// if this is the first one, kick off the go loop
	if numLookupd == 1 {
		r.queryLookupd()
		r.wg.Add(1)
		go r.lookupdLoop()
	}

	return nil
}

func (r *Consumer) AddEtcdServiceAddr(address []string, cluster string, key string) {
	// TODO: get the lookup address from etcd service.
}

// ConnectToNSQLookupds adds multiple nsqlookupd address to the list for this Consumer instance.
//
// If adding the first address it initiates an HTTP request to discover nsqd
// producers for the configured topic.
//
// A goroutine is spawned to handle continual polling.
func (r *Consumer) ConnectToNSQLookupds(addresses []string) error {
	for _, addr := range addresses {
		err := r.ConnectToNSQLookupd(addr)
		if err != nil {
			return err
		}
	}
	return nil
}

func validatedLookupAddr(addr string) error {
	if strings.Contains(addr, "/") {
		_, err := url.Parse(addr)
		if err != nil {
			return err
		}
		return nil
	}
	if !strings.Contains(addr, ":") {
		return errors.New("missing port")
	}
	return nil
}

// poll all known lookup servers every LookupdPollInterval
func (r *Consumer) lookupdLoop() {
	// add some jitter so that multiple consumers discovering the same topic,
	// when restarted at the same time, dont all connect at once.
	r.rngMtx.Lock()
	jitter := time.Duration(int64(r.rng.Float64() *
		r.config.LookupdPollJitter * float64(r.config.LookupdPollInterval)))
	r.rngMtx.Unlock()
	var ticker *time.Ticker

	select {
	case <-time.After(jitter):
	case <-r.exitChan:
		goto exit
	}

	ticker = time.NewTicker(r.config.LookupdPollInterval)

	for {
		select {
		case <-ticker.C:
			r.queryLookupd()
		case <-r.lookupdRecheckChan:
			r.queryLookupd()
		case <-r.exitChan:
			goto exit
		}
	}

exit:
	if ticker != nil {
		ticker.Stop()
	}
	r.log(LogLevelInfo, "exiting lookupdLoop")
	r.wg.Done()
}

// return the next lookupd endpoint to query
// keeping track of which one was last used
func (r *Consumer) nextLookupdEndpoint() (string, string, string) {
	r.mtx.RLock()
	if r.lookupdQueryIndex >= len(r.lookupdHTTPAddrs) {
		r.lookupdQueryIndex = 0
	}
	addr := r.lookupdHTTPAddrs[r.lookupdQueryIndex]
	num := len(r.lookupdHTTPAddrs)
	r.mtx.RUnlock()
	r.lookupdQueryIndex = (r.lookupdQueryIndex + 1) % num

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

	v, err := url.ParseQuery(u.RawQuery)
	v.Add("topic", r.topic)
	v.Add("metainfo", "true")
	v.Add("access", "r")
	if r.partition >= 0 {
		v.Add("partition", strconv.Itoa(r.partition))
	}
	u.RawQuery = v.Encode()
	return addr, u.String(), listUrl.String()
}

type metaInfo struct {
	PartitionNum  int  `json:"partition_num"`
	Replica       int  `json:"replica"`
	ExtendSupport bool `json:"extend_support"`
}

type lookupResp struct {
	Channels   []string             `json:"channels"`
	Producers  []*peerInfo          `json:"producers"`
	Partitions map[string]*peerInfo `json:"partitions"`
	Meta       metaInfo             `json:"meta"`
}

type NsqLookupdNodeInfo struct {
	ID       string
	NodeIp   string
	TcpPort  string
	HttpPort string
	RpcPort  string
	Epoch    int64
}

type lookupListResp struct {
	LookupdNodes  []NsqLookupdNodeInfo `json:"lookupdnodes"`
	LookupdLeader NsqLookupdNodeInfo   `json:"lookupdleader"`
}

type peerInfo struct {
	RemoteAddress    string `json:"remote_address"`
	Hostname         string `json:"hostname"`
	BroadcastAddress string `json:"broadcast_address"`
	TCPPort          int    `json:"tcp_port"`
	HTTPPort         int    `json:"http_port"`
	Version          string `json:"version"`
}

func getConnectionUID(addr string, pid int) string {
	return addr + "-" + strconv.Itoa(pid)
}

// make an HTTP req to one of the configured nsqlookupd instances to discover
// which nsqd's provide the topic we are consuming.
//
// initiate a connection to any new producers that are identified.
func (r *Consumer) queryLookupd() {
	addr, endpoint, discoveryUrl := r.nextLookupdEndpoint()
	// discovery other lookupd nodes from current lookupd or from etcd
	r.log(LogLevelDebug, "discovery nsqlookupd %s", discoveryUrl)
	var lookupdList lookupListResp
	err := apiRequestNegotiateV1("GET", discoveryUrl, nil, &lookupdList)
	if err != nil {
		r.log(LogLevelError, "error discovery nsqlookupd (%s) - %s", discoveryUrl, err)
		if strings.Contains(strings.ToLower(err.Error()), "connection refused") &&
			FindString(r.config.LookupdSeeds, addr) == -1 {
			r.mtx.Lock()
			// remove failed
			r.log(LogLevelInfo, "removing failed lookup : %v", addr)
			newLookupList := make([]string, 0)
			for _, v := range r.lookupdHTTPAddrs {
				if v == addr {
					continue
				} else {
					newLookupList = append(newLookupList, v)
				}
			}
			if len(newLookupList) > 0 {
				r.lookupdHTTPAddrs = newLookupList
			}
			r.mtx.Unlock()
			select {
			case r.lookupdRecheckChan <- 1:
				r.log(LogLevelInfo, "trigger tend for err: %v", err)
			default:
			}
			return
		}
	} else {
		for _, node := range lookupdList.LookupdNodes {
			addr := net.JoinHostPort(node.NodeIp, node.HttpPort)
			r.ConnectToNSQLookupd(addr)
		}
	}

	r.log(LogLevelDebug, "querying nsqlookupd %s", endpoint)
	var data lookupResp
	err = apiRequestNegotiateV1("GET", endpoint, nil, &data)
	if err != nil {
		r.log(LogLevelError, "error querying nsqlookupd (%s) - %s", endpoint, err)
		return
	}
	if data.Meta.ExtendSupport && !r.IsConsumeExt() {
		r.SetConsumeExt(true)
	}

	var nsqdAddrs []string
	partInfo := make(map[string]map[int]struct{})
	r.log(LogLevelInfo, "producer partitions: %v", len(data.Partitions))

	for pidStr, partProducer := range data.Partitions {
		pid, err := strconv.Atoi(pidStr)
		if err != nil {
			r.log(LogLevelError, "node partition string invalid: %v, %v", pidStr, err)
			continue
		}
		broadcastAddress := partProducer.BroadcastAddress
		port := partProducer.TCPPort
		nsqdAddr := net.JoinHostPort(broadcastAddress, strconv.Itoa(port))
		nsqdAddrs = append(nsqdAddrs, nsqdAddr)
		nodeParts, ok := partInfo[nsqdAddr]
		if !ok {
			nodeParts = make(map[int]struct{})
			partInfo[nsqdAddr] = nodeParts
		}
		nodeParts[pid] = struct{}{}
		r.log(LogLevelDebug, "producer found %s , partition: %v", nsqdAddr, pid)
	}

	if len(data.Partitions) == 0 {
		// for old lookup, no partition info for node.
		// we always treat partition as -1
		for _, producer := range data.Producers {
			broadcastAddress := producer.BroadcastAddress
			port := producer.TCPPort
			joined := net.JoinHostPort(broadcastAddress, strconv.Itoa(port))
			nsqdAddrs = append(nsqdAddrs, joined)
			r.log(LogLevelDebug, "producer found %s without partition", joined)
		}
	}
	// apply filter
	if discoveryFilter, ok := r.behaviorDelegate.(DiscoveryFilter); ok {
		nsqdAddrs = discoveryFilter.Filter(nsqdAddrs)
	}
	for _, addr := range nsqdAddrs {
		pidList, ok := partInfo[addr]
		if !ok {
			pid := -1
			err = r.ConnectToNSQD(addr, pid)
			if err != nil && err != ErrAlreadyConnected {
				r.log(LogLevelError, "(%s) error connecting to nsqd - %s", addr, err)
				continue
			}
		} else {
			for pid, _ := range pidList {
				err = r.ConnectToNSQD(addr, pid)
				if err != nil && err != ErrAlreadyConnected {
					r.log(LogLevelError, "(%s) error connecting to nsqd - %s", addr, err)
					continue
				}
			}
		}
	}
}

// ConnectToNSQDs takes multiple nsqd addresses to connect directly to.
//
// It is recommended to use ConnectToNSQLookupd so that topics are discovered
// automatically.  This method is useful when you want to connect to local instance.
func (r *Consumer) ConnectToNSQDs(addresses []AddrPartInfo) error {
	for _, addr := range addresses {
		err := r.ConnectToNSQD(addr.addr, addr.pid)
		if err != nil {
			return err
		}
	}
	return nil
}

// ConnectToNSQD takes a nsqd address to connect directly to.
//
// It is recommended to use ConnectToNSQLookupd so that topics are discovered
// automatically.  This method is useful when you want to connect to a single, local,
// instance.
func (r *Consumer) ConnectToNSQD(addr string, part int) error {
	if atomic.LoadInt32(&r.stopFlag) == 1 {
		return errors.New("consumer stopped")
	}

	if atomic.LoadInt32(&r.runningHandlers) == 0 {
		return errors.New("no handlers")
	}

	if r.partition != -1 && r.partition != part {
		return errors.New("nsqd partition not matched with consumer partition")
	}

	atomic.StoreInt32(&r.connectedFlag, 1)

	logger, logLvl := r.getLogger()

	conn := NewConn(addr, &r.config, &consumerConnDelegate{r})
	conn.consumePart = strconv.Itoa(part)
	conn.ext = r.IsConsumeExt()
	conn.SetLogger(logger, logLvl,
		fmt.Sprintf("%3d [%s(%v)/%s] (%%s)", r.id, r.topic, part, r.channel))

	r.mtx.Lock()
	// here we assume only partition for each topic on node
	// if more than one partitions, in order to consume all partitions,
	// we need more connections to the same node
	cid := conn.GetConnUID()
	_, pendingOk := r.pendingConnections[cid]
	_, ok := r.connections[cid]
	if ok || pendingOk {
		r.mtx.Unlock()
		return ErrAlreadyConnected
	}
	r.pendingConnections[cid] = conn
	if idx := indexOfAddrPartInfo(addr, conn.consumePart, r.nsqdTCPAddrs); idx == -1 {
		r.nsqdTCPAddrs = append(r.nsqdTCPAddrs, AddrPartInfo{addr, part})
	}
	r.mtx.Unlock()

	r.log(LogLevelInfo, "consumer init new connection to nsqd: (%s) for topic %v-%v, channel: %v",
		addr, r.topic, part, r.channel)

	cleanupConnection := func() {
		r.mtx.Lock()
		delete(r.pendingConnections, cid)
		r.mtx.Unlock()
		r.log(LogLevelInfo, "consumer cleanup connection: (%s) ", cid)
		conn.Close()
	}

	resp, err := conn.Connect()
	if err != nil {
		cleanupConnection()
		return err
	}

	if resp != nil {
		if resp.MaxRdyCount < int64(r.getMaxInFlight()) {
			r.log(LogLevelWarning,
				"(%s) max RDY count %d < consumer max in flight %d, truncation possible",
				conn.String(), resp.MaxRdyCount, r.getMaxInFlight())
		}
	}

	var cmd *Command
	var offset ConsumeOffset
	var hasConsumeOffset bool
	r.offsetMutex.Lock()
	if offset, hasConsumeOffset = r.consumeOffset[part]; hasConsumeOffset {
		r.log(LogLevelInfo, "topic %v partition %v consume offset at offset: %v", r.topic, part, offset)
	}
	r.offsetMutex.Unlock()

	if part == -1 || part == OLD_VERSION_PID {
		if hasConsumeOffset || r.config.EnableOrdered {
			r.log(LogLevelError, "partition must be given for ordered consumer ")
			return errors.New("missing partition for ordered consumer")
		}
		// consume from old nsqd with no partition
		if r.config.EnableTrace {
			cmd = SubscribeAndTrace(r.topic, r.channel)
		} else {
			cmd = Subscribe(r.topic, r.channel)
		}
	} else {
		if hasConsumeOffset {
			cmd = SubscribeAdvanced(r.topic, r.channel, strconv.Itoa(part), offset)
		} else if r.config.EnableOrdered {
			cmd = SubscribeOrdered(r.topic, r.channel, strconv.Itoa(part))
		} else if r.config.EnableTrace {
			cmd = SubscribeWithPartAndTrace(r.topic, r.channel, strconv.Itoa(part))
		} else {
			cmd = SubscribeWithPart(r.topic, r.channel, strconv.Itoa(part))
		}
	}
	err = conn.WriteCommand(cmd)
	if err != nil {
		cleanupConnection()
		return fmt.Errorf("%v [%s] failed to subscribe to %s(%v):%s - %s",
			addr, conn, r.topic, part, r.channel, err.Error())
	}
	if hasConsumeOffset {
		delete(r.consumeOffset, part)
	}

	r.mtx.Lock()
	delete(r.pendingConnections, cid)
	r.connections[cid] = conn
	r.mtx.Unlock()

	// pre-emptive signal to existing connections to lower their RDY count
	for _, c := range r.conns() {
		r.maybeUpdateRDY(c)
	}

	return nil
}

func indexOfAddrPartInfo(n string, pid string, h []AddrPartInfo) int {
	for i, a := range h {
		if n == a.addr && pid == strconv.Itoa(a.pid) {
			return i
		}
	}
	return -1
}

func indexOf(n string, h []string) int {
	for i, a := range h {
		if n == a {
			return i
		}
	}
	return -1
}

// DisconnectFromNSQD closes the connection to and removes the specified
// `nsqd` address from the list
func (r *Consumer) DisconnectFromNSQD(addr string, part string) error {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	idx := indexOfAddrPartInfo(addr, part, r.nsqdTCPAddrs)
	if idx == -1 {
		return ErrNotConnected
	}

	// slice delete
	r.nsqdTCPAddrs = append(r.nsqdTCPAddrs[:idx], r.nsqdTCPAddrs[idx+1:]...)

	pendingConn, pendingOk := r.pendingConnections[addr]
	conn, ok := r.connections[addr]

	if ok {
		conn.Close()
	} else if pendingOk {
		pendingConn.Close()
	}

	return nil
}

// DisconnectFromNSQLookupd removes the specified `nsqlookupd` address
// from the list used for periodic discovery.
func (r *Consumer) DisconnectFromNSQLookupd(addr string) error {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	idx := indexOf(addr, r.lookupdHTTPAddrs)
	if idx == -1 {
		return ErrNotConnected
	}

	if len(r.lookupdHTTPAddrs) == 1 {
		return fmt.Errorf("cannot disconnect from only remaining nsqlookupd HTTP address %s", addr)
	}

	r.lookupdHTTPAddrs = append(r.lookupdHTTPAddrs[:idx], r.lookupdHTTPAddrs[idx+1:]...)

	return nil
}

func (r *Consumer) onConnMessage(c *Conn, msg *Message) {
	atomic.AddInt64(&r.totalRdyCount, -1)
	atomic.AddUint64(&r.messagesReceived, 1)
	if r.config.EnableOrdered || r.config.EnableTrace {
		if len(msg.Body) < 12 {
			r.log(LogLevelError, "invalid message body length: %v, %v", len(msg.Body), msg)
		}
		// get the offset and rawSize from body
		msg.Offset = uint64(binary.BigEndian.Uint64(msg.Body[:8]))
		msg.RawSize = uint32(binary.BigEndian.Uint32(msg.Body[8 : 8+4]))
		msg.Body = msg.Body[8+4:]
	}
	r.incomingMessages <- msg
	r.maybeUpdateRDY(c)
}

func (r *Consumer) onConnMessageFinished(c *Conn, msg *Message) {
	atomic.AddUint64(&r.messagesFinished, 1)
}

func (r *Consumer) onConnMessageRequeued(c *Conn, msg *Message) {
	atomic.AddUint64(&r.messagesRequeued, 1)
}

func (r *Consumer) onConnBackoff(c *Conn) {
	r.startStopContinueBackoff(c, backoffFlag)
}

func (r *Consumer) onConnContinue(c *Conn) {
	r.startStopContinueBackoff(c, continueFlag)
}

func (r *Consumer) onConnResume(c *Conn) {
	r.startStopContinueBackoff(c, resumeFlag)
}

func (r *Consumer) onConnResponse(c *Conn, data []byte) {
	switch {
	case bytes.Equal(data, []byte("CLOSE_WAIT")):
		// server is ready for us to close (it ack'd our StartClose)
		// we can assume we will not receive any more messages over this channel
		// (but we can still write back responses)
		r.log(LogLevelInfo, "(%s) received CLOSE_WAIT from nsqd", c.String())
		c.Close()
	}
}

func (r *Consumer) onConnError(c *Conn, data []byte) {
	r.log(LogLevelInfo, "conn %v error response : %v", c.RemoteAddr(), string(data))
	if IsFailedOnNotLeaderBytes(data) || IsTopicNotExistBytes(data) || IsFailedOnNotWritableBytes(data) {
		addr := c.RemoteAddr()
		r.log(LogLevelInfo, "removing nsqd address %v for error: %v", addr, string(data))
		r.DisconnectFromNSQD(addr.String(), c.consumePart)
		go func() {
			time.Sleep(time.Second)
			select {
			case r.lookupdRecheckChan <- 1:
			default:
			}
		}()
		r.log(LogLevelInfo, "removed for error response")
	}
}

func (r *Consumer) onConnHeartbeat(c *Conn) {}

func (r *Consumer) onConnIOError(c *Conn, err error) {
	c.Close()
}

func (r *Consumer) onConnClose(c *Conn) {
	var hasRDYRetryTimer bool

	// remove this connections RDY count from the consumer's total
	rdyCount := c.RDY()
	atomic.AddInt64(&r.totalRdyCount, -rdyCount)

	r.rdyRetryMtx.Lock()
	if timer, ok := r.rdyRetryTimers[c.GetConnUID()]; ok {
		// stop any pending retry of an old RDY update
		timer.Stop()
		delete(r.rdyRetryTimers, c.GetConnUID())
		hasRDYRetryTimer = true
	}
	r.rdyRetryMtx.Unlock()

	r.mtx.Lock()
	delete(r.connections, c.GetConnUID())
	left := len(r.connections)
	r.mtx.Unlock()

	r.log(LogLevelWarning, "there are %d connections left alive", left)

	if (hasRDYRetryTimer || rdyCount > 0) &&
		(int32(left) == r.getMaxInFlight() || r.inBackoff()) {
		// we're toggling out of (normal) redistribution cases and this conn
		// had a RDY count...
		//
		// trigger RDY redistribution to make sure this RDY is moved
		// to a new connection
		atomic.StoreInt32(&r.needRDYRedistributed, 1)
	}

	// we were the last one (and stopping)
	if atomic.LoadInt32(&r.stopFlag) == 1 {
		if left == 0 {
			r.stopHandlers()
		}
		return
	}

	r.mtx.RLock()
	numLookupd := len(r.lookupdHTTPAddrs)
	reconnect := indexOfAddrPartInfo(c.String(), c.consumePart, r.nsqdTCPAddrs) >= 0
	r.mtx.RUnlock()
	if numLookupd > 0 {
		// trigger a poll of the lookupd
		go func() {
			time.Sleep(time.Second)
			select {
			case r.lookupdRecheckChan <- 1:
			default:
			}
		}()
	} else if reconnect {
		// there are no lookupd and we still have this nsqd TCP address in our list...
		// try to reconnect after a bit
		go func(addr string, pid string) {
			for {
				r.log(LogLevelInfo, "(%s) re-connecting in %s", addr, r.config.LookupdPollInterval)
				time.Sleep(r.config.LookupdPollInterval)
				if atomic.LoadInt32(&r.stopFlag) == 1 {
					break
				}
				r.mtx.RLock()
				index := indexOfAddrPartInfo(addr, pid, r.nsqdTCPAddrs)
				reconnect := index >= 0
				var addrInfo AddrPartInfo
				if index != -1 {
					addrInfo = r.nsqdTCPAddrs[index]
				}
				r.mtx.RUnlock()
				if !reconnect {
					r.log(LogLevelWarning, "(%s) skipped reconnect after removal...", addr)
					return
				}
				err := r.ConnectToNSQD(addrInfo.addr, addrInfo.pid)
				if err != nil && err != ErrAlreadyConnected {
					r.log(LogLevelError, "(%s) error connecting to nsqd - %s", addr, err)
					continue
				}
				break
			}
		}(c.String(), c.consumePart)
	}
}

func (r *Consumer) startStopContinueBackoff(conn *Conn, signal backoffSignal) {
	// prevent many async failures/successes from immediately resulting in
	// max backoff/normal rate (by ensuring that we dont continually incr/decr
	// the counter during a backoff period)
	r.backoffMtx.Lock()
	if r.inBackoffTimeout() {
		r.backoffMtx.Unlock()
		return
	}
	defer r.backoffMtx.Unlock()

	// update backoff state
	backoffUpdated := false
	backoffCounter := atomic.LoadInt32(&r.backoffCounter)
	switch signal {
	case resumeFlag:
		if backoffCounter > 0 {
			backoffCounter--
			backoffUpdated = true
		}
	case backoffFlag:
		nextBackoff := r.config.BackoffStrategy.Calculate(int(backoffCounter) + 1)
		if nextBackoff <= r.config.MaxBackoffDuration {
			backoffCounter++
			backoffUpdated = true
		}
	}
	atomic.StoreInt32(&r.backoffCounter, backoffCounter)

	if r.backoffCounter == 0 && backoffUpdated {
		// exit backoff
		count := r.perConnMaxInFlight()
		r.log(LogLevelWarning, "exiting backoff, returning all to RDY %d", count)
		for _, c := range r.conns() {
			r.updateRDY(c, count)
		}
	} else if r.backoffCounter > 0 {
		// start or continue backoff
		backoffDuration := r.config.BackoffStrategy.Calculate(int(backoffCounter))

		if backoffDuration > r.config.MaxBackoffDuration {
			backoffDuration = r.config.MaxBackoffDuration
		}

		r.log(LogLevelWarning, "backing off for %.04f seconds (backoff level %d), setting all to RDY 0",
			backoffDuration.Seconds(), backoffCounter)

		// send RDY 0 immediately (to *all* connections)
		for _, c := range r.conns() {
			r.updateRDY(c, 0)
		}

		r.backoff(backoffDuration)
	}
}

func (r *Consumer) backoff(d time.Duration) {
	atomic.StoreInt64(&r.backoffDuration, d.Nanoseconds())
	time.AfterFunc(d, r.resume)
}

func (r *Consumer) resume() {
	if atomic.LoadInt32(&r.stopFlag) == 1 {
		atomic.StoreInt64(&r.backoffDuration, 0)
		return
	}

	// pick a random connection to test the waters
	conns := r.conns()
	if len(conns) == 0 {
		r.log(LogLevelWarning, "no connection available to resume")
		r.log(LogLevelWarning, "backing off for %.04f seconds", 1)
		r.backoff(time.Second)
		return
	}
	r.rngMtx.Lock()
	idx := r.rng.Intn(len(conns))
	r.rngMtx.Unlock()
	choice := conns[idx]

	r.log(LogLevelWarning,
		"(%s) backoff timeout expired, sending RDY 1",
		choice.String())

	// while in backoff only ever let 1 message at a time through
	err := r.updateRDY(choice, 1)
	if err != nil {
		r.log(LogLevelWarning, "(%s) error resuming RDY 1 - %s", choice.String(), err)
		r.log(LogLevelWarning, "backing off for %.04f seconds", 1)
		r.backoff(time.Second)
		return
	}

	atomic.StoreInt64(&r.backoffDuration, 0)
}

func (r *Consumer) inBackoff() bool {
	return atomic.LoadInt32(&r.backoffCounter) > 0
}

func (r *Consumer) inBackoffTimeout() bool {
	return atomic.LoadInt64(&r.backoffDuration) > 0
}

func (r *Consumer) maybeUpdateRDY(conn *Conn) {
	inBackoff := r.inBackoff()
	inBackoffTimeout := r.inBackoffTimeout()
	if inBackoff || inBackoffTimeout {
		r.log(LogLevelDebug, "(%s) skip sending RDY inBackoff:%v || inBackoffTimeout:%v",
			conn, inBackoff, inBackoffTimeout)
		return
	}

	remain := conn.RDY()
	lastRdyCount := conn.LastRDY()
	count := r.perConnMaxInFlight()

	// refill when at 1, or at 25%, or if connections have changed and we're imbalanced
	if remain <= 1 || remain < (lastRdyCount/4) || (count > 0 && count < remain) {
		r.log(LogLevelDebug, "(%s) sending RDY %d (%d remain from last RDY %d)",
			conn, count, remain, lastRdyCount)
		r.updateRDY(conn, count)
	} else {
		r.log(LogLevelDebug, "(%s) skip sending RDY %d (%d remain out of last RDY %d)",
			conn, count, remain, lastRdyCount)
	}
}

func (r *Consumer) rdyLoop() {
	redistributeTicker := time.NewTicker(r.config.RDYRedistributeInterval)

	for {
		select {
		case <-redistributeTicker.C:
			r.redistributeRDY()
		case <-r.exitChan:
			goto exit
		}
	}

exit:
	redistributeTicker.Stop()
	r.log(LogLevelInfo, "rdyLoop exiting")
	r.wg.Done()
}

func (r *Consumer) updateRDY(c *Conn, count int64) error {
	if c.IsClosing() {
		return ErrClosing
	}

	// never exceed the nsqd's configured max RDY count
	if count > c.MaxRDY() {
		count = c.MaxRDY()
	}

	// stop any pending retry of an old RDY update
	r.rdyRetryMtx.Lock()
	if timer, ok := r.rdyRetryTimers[c.GetConnUID()]; ok {
		timer.Stop()
		delete(r.rdyRetryTimers, c.GetConnUID())
	}
	r.rdyRetryMtx.Unlock()

	// never exceed our global max in flight. truncate if possible.
	// this could help a new connection get partial max-in-flight
	rdyCount := c.RDY()
	maxPossibleRdy := int64(r.getMaxInFlight()) - atomic.LoadInt64(&r.totalRdyCount) + rdyCount
	if maxPossibleRdy > 0 && maxPossibleRdy < count {
		count = maxPossibleRdy
	}
	if maxPossibleRdy <= 0 && count > 0 {
		if rdyCount == 0 {
			// we wanted to exit a zero RDY count but we couldn't send it...
			// in order to prevent eternal starvation we reschedule this attempt
			// (if any other RDY update succeeds this timer will be stopped)
			r.rdyRetryMtx.Lock()
			r.rdyRetryTimers[c.GetConnUID()] = time.AfterFunc(5*time.Second,
				func() {
					r.updateRDY(c, count)
				})
			r.rdyRetryMtx.Unlock()
		}
		return ErrOverMaxInFlight
	}

	return r.sendRDY(c, count)
}

func (r *Consumer) sendRDY(c *Conn, count int64) error {
	if count == 0 && c.LastRDY() == 0 {
		// no need to send. It's already that RDY count
		return nil
	}

	atomic.AddInt64(&r.totalRdyCount, -c.RDY()+count)
	c.SetRDY(count)
	err := c.WriteCommand(Ready(int(count)))
	if err != nil {
		r.log(LogLevelError, "(%s) error sending RDY %d - %s", c.String(), count, err)
		return err
	}
	return nil
}

func (r *Consumer) redistributeRDY() {
	if r.inBackoffTimeout() {
		return
	}

	// if an external heuristic set needRDYRedistributed we want to wait
	// until we can actually redistribute to proceed
	conns := r.conns()
	if len(conns) == 0 {
		return
	}

	maxInFlight := r.getMaxInFlight()
	if len(conns) > int(maxInFlight) {
		r.log(LogLevelDebug, "redistributing RDY state (%d conns > %d max_in_flight)",
			len(conns), maxInFlight)
		atomic.StoreInt32(&r.needRDYRedistributed, 1)
	}

	if r.inBackoff() && len(conns) > 1 {
		r.log(LogLevelDebug, "redistributing RDY state (in backoff and %d conns > 1)", len(conns))
		atomic.StoreInt32(&r.needRDYRedistributed, 1)
	}

	if !atomic.CompareAndSwapInt32(&r.needRDYRedistributed, 1, 0) {
		return
	}

	possibleConns := make([]*Conn, 0, len(conns))
	for _, c := range conns {
		lastMsgDuration := time.Now().Sub(c.LastMessageTime())
		rdyCount := c.RDY()
		r.log(LogLevelDebug, "(%s) rdy: %d (last message received %s)",
			c.String(), rdyCount, lastMsgDuration)
		if rdyCount > 0 && lastMsgDuration > r.config.LowRdyIdleTimeout {
			r.log(LogLevelDebug, "(%s) idle connection, giving up RDY", c.String())
			r.updateRDY(c, 0)
		}
		possibleConns = append(possibleConns, c)
	}

	availableMaxInFlight := int64(maxInFlight) - atomic.LoadInt64(&r.totalRdyCount)
	if r.inBackoff() {
		availableMaxInFlight = 1 - atomic.LoadInt64(&r.totalRdyCount)
	}

	for len(possibleConns) > 0 && availableMaxInFlight > 0 {
		availableMaxInFlight--
		r.rngMtx.Lock()
		i := r.rng.Int() % len(possibleConns)
		r.rngMtx.Unlock()
		c := possibleConns[i]
		// delete
		possibleConns = append(possibleConns[:i], possibleConns[i+1:]...)
		r.log(LogLevelDebug, "(%s) redistributing RDY", c.String())
		r.updateRDY(c, 1)
	}
}

// Stop will initiate a graceful stop of the Consumer (permanent)
//
// NOTE: receive on StopChan to block until this process completes
func (r *Consumer) Stop() {
	if !atomic.CompareAndSwapInt32(&r.stopFlag, 0, 1) {
		return
	}

	r.log(LogLevelInfo, "stopping...")

	if len(r.conns()) == 0 {
		r.stopHandlers()
	} else {
		for _, c := range r.conns() {
			err := c.WriteCommand(StartClose())
			if err != nil {
				r.log(LogLevelError, "(%s) error sending CLS - %s", c.String(), err)
			}
		}

		time.AfterFunc(time.Second*30, func() {
			// if we've waited this long handlers are blocked on processing messages
			// so we can't just stopHandlers (if any adtl. messages were pending processing
			// we would cause a panic on channel close)
			//
			// instead, we just bypass handler closing and skip to the final exit
			r.exit()
		})
	}
}

func (r *Consumer) stopHandlers() {
	r.stopHandler.Do(func() {
		r.log(LogLevelInfo, "stopping handlers")
		close(r.incomingMessages)
	})
}

// AddHandler sets the Handler for messages received by this Consumer. This can be called
// multiple times to add additional handlers. Handler will have a 1:1 ratio to message handling goroutines.
//
// This panics if called after connecting to NSQD or NSQ Lookupd
//
// (see Handler or HandlerFunc for details on implementing this interface)
func (r *Consumer) AddHandler(handler Handler) {
	r.AddConcurrentHandlers(handler, 1)
}

// AddConcurrentHandlers sets the Handler for messages received by this Consumer.  It
// takes a second argument which indicates the number of goroutines to spawn for
// message handling.
//
// This panics if called after connecting to NSQD or NSQ Lookupd
//
// (see Handler or HandlerFunc for details on implementing this interface)
func (r *Consumer) AddConcurrentHandlers(handler Handler, concurrency int) {
	if atomic.LoadInt32(&r.connectedFlag) == 1 {
		panic("already connected")
	}

	atomic.AddInt32(&r.runningHandlers, int32(concurrency))
	for i := 0; i < concurrency; i++ {
		go r.handlerLoop(handler)
	}
}

func (r *Consumer) handlerLoop(handler Handler) {
	r.log(LogLevelDebug, "starting Handler")

	for {
		message, ok := <-r.incomingMessages
		if !ok {
			goto exit
		}

		if r.shouldFailMessage(message, handler) {
			message.Finish()
			continue
		}

		err := handler.HandleMessage(message)
		if err != nil {
			r.log(LogLevelError, "Handler returned error (%s) for msg %s", err, message.ID)
			if !message.IsAutoResponseDisabled() {
				message.Requeue(-1)
			}
			continue
		}

		if !message.IsAutoResponseDisabled() {
			message.Finish()
		}
	}

exit:
	r.log(LogLevelDebug, "stopping Handler")
	if atomic.AddInt32(&r.runningHandlers, -1) == 0 {
		r.exit()
	}
}

func (r *Consumer) shouldFailMessage(message *Message, handler interface{}) bool {
	// message passed the max number of attempts
	if r.config.MaxAttempts > 0 && message.Attempts > r.config.MaxAttempts {
		r.log(LogLevelWarning, "msg %s attempted %d times, giving up",
			message.ID, message.Attempts)

		logger, ok := handler.(FailedMessageLogger)
		if ok {
			logger.LogFailedMessage(message)
		}

		return true
	}
	return false
}

func (r *Consumer) exit() {
	r.exitHandler.Do(func() {
		close(r.exitChan)
		r.wg.Wait()
		close(r.StopChan)
	})
}

func (r *Consumer) log(lvl LogLevel, line string, args ...interface{}) {
	logger, logLvl := r.getLogger()

	if logger == nil {
		return
	}

	if logLvl > lvl {
		return
	}

	logger.Output(2, fmt.Sprintf("%-4s %3d [%s(%v)/%s] %s",
		lvl, r.id, r.topic, r.partition, r.channel,
		fmt.Sprintf(line, args...)))
}
