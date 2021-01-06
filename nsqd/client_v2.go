package nsqd

import (
	"bufio"
	"compress/flate"
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/snappy"
	"github.com/youzan/nsq/internal/auth"
	"github.com/youzan/nsq/internal/ext"
	"github.com/youzan/nsq/internal/levellogger"
)

// default is used for producer
const (
	defaultWriteBufferSize         = 100
	defaultConsumerWriteBufferSize = 4 * 1024
	defaultLimitedRdy              = 50000
)

const slowDownThreshold = 5

const (
	stateInit = iota
	stateDisconnected
	stateConnected
	stateSubscribed
	stateClosing
)

type IdentifyDataV2 struct {
	ShortID string `json:"short_id"` // TODO: deprecated, remove in 1.0
	LongID  string `json:"long_id"`  // TODO: deprecated, remove in 1.0

	ClientID            string        `json:"client_id"`
	Hostname            string        `json:"hostname"`
	HeartbeatInterval   int           `json:"heartbeat_interval"`
	OutputBufferSize    int           `json:"output_buffer_size"`
	OutputBufferTimeout int           `json:"output_buffer_timeout"`
	FeatureNegotiation  bool          `json:"feature_negotiation"`
	TLSv1               bool          `json:"tls_v1"`
	Deflate             bool          `json:"deflate"`
	DeflateLevel        int           `json:"deflate_level"`
	Snappy              bool          `json:"snappy"`
	SampleRate          int32         `json:"sample_rate"`
	UserAgent           string        `json:"user_agent"`
	MsgTimeout          int           `json:"msg_timeout"`
	DesiredTag          string        `json:"desired_tag,omitempty"`
	ExtendSupport       bool          `json:"extend_support"`
	ExtFilter           ExtFilterData `json:"ext_filter"`
}

type identifyEvent struct {
	OutputBufferTimeout time.Duration
	HeartbeatInterval   time.Duration
	SampleRate          int32
	MsgTimeout          time.Duration
	ExtFilter           ExtFilterData
}

type ClientV2 struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	ReadyCount    int64
	InFlightCount int64
	MessageCount  uint64
	FinishCount   uint64
	RequeueCount  uint64
	TimeoutCount  uint64

	// this lock used only for connection writer
	// do not use it while get/set stats for client, use meta lock instead
	writeLock sync.RWMutex
	metaLock  sync.RWMutex

	ID        int64
	ctxOpts   *Options
	UserAgent string

	// original connection
	net.Conn

	// connections based on negotiated features
	tlsConn     *tls.Conn
	flateWriter *flate.Writer

	// reading/writing interfaces
	Reader *bufio.Reader
	Writer *bufio.Writer

	outputBufferSize    int64
	outputBufferTimeout int64

	heartbeatInterval int64

	msgTimeout int64

	State          int32
	ConnectTime    time.Time
	Channel        *Channel
	ReadyStateChan chan int
	// this is only used by notify messagebump to quit
	// and should be closed by the read loop only
	ExitChan chan int

	ClientID string
	Hostname string

	SampleRate int32

	IdentifyEventChan chan identifyEvent
	SubEventChan      chan *Channel

	TLS     int32
	Snappy  int32
	Deflate int32

	// re-usable buffer for reading the 4-byte lengths off the wire
	lenBuf   [4]byte
	LenSlice []byte

	AuthSecret  string
	AuthState   *auth.State
	tlsConfig   *tls.Config
	EnableTrace bool

	//PubTimeout         *time.Timer
	remoteAddr         string
	subErrCnt          int64
	lastConsumeTimeout int64

	desiredTag      string
	isExtendSupport int32
	TagMsgChannel   chan *Message
	extFilter       ExtFilterData
	statsMu         sync.RWMutex
	pubStats        map[string]*ClientPubStats
	LimitedRdy      int32
}

func NewClientV2(id int64, conn net.Conn, opts *Options, tls *tls.Config) *ClientV2 {
	var identifier string
	if conn != nil {
		identifier = conn.RemoteAddr().String()
	}
	if tcpC, ok := conn.(*net.TCPConn); ok {
		tcpC.SetNoDelay(true)
	}

	c := &ClientV2{
		ID:      id,
		ctxOpts: opts,

		Conn: conn,

		Reader: NewBufioReader(conn),
		// use small write buffer to reduce memory by default, and
		// increase it if really consume happened
		Writer: newBufioWriterSize(conn, defaultWriteBufferSize),

		outputBufferTimeout: int64(50 * time.Millisecond),

		msgTimeout: int64(opts.MsgTimeout),

		// ReadyStateChan has a buffer of 1 to guarantee that in the event
		// there is a race the state update is not lost
		ReadyStateChan: make(chan int, 1),
		ExitChan:       make(chan int),
		ConnectTime:    time.Now(),
		State:          stateInit,

		ClientID: identifier,
		Hostname: identifier,

		SubEventChan:      make(chan *Channel, 1),
		IdentifyEventChan: make(chan identifyEvent, 1),

		// heartbeats are client configurable but default to 30s
		heartbeatInterval: int64(opts.ClientTimeout / 2),
		tlsConfig:         tls,
		LimitedRdy:        defaultLimitedRdy,
		pubStats:          make(map[string]*ClientPubStats),
	}
	if c.outputBufferTimeout > int64(opts.MaxOutputBufferTimeout) {
		c.outputBufferTimeout = int64(opts.MaxOutputBufferTimeout)
	}
	c.LenSlice = c.lenBuf[:]
	c.remoteAddr = identifier
	return c
}

func (c *ClientV2) GetTcpPubStats(topic *Topic) *ClientPubStats {
	c.statsMu.RLock()
	ps, ok := c.pubStats[topic.GetTopicName()]
	c.statsMu.RUnlock()
	if ok {
		return ps
	}
	ps = topic.GetDetailStats().InitPubClientStats(c.String(), c.UserAgent, "tcp")
	if ps == nil {
		return nil
	}
	c.statsMu.Lock()
	c.pubStats[topic.GetTopicName()] = ps
	c.statsMu.Unlock()
	return ps
}

func (c *ClientV2) String() string {
	return c.remoteAddr
}

func (c *ClientV2) GetID() int64 {
	return c.ID
}

func (c *ClientV2) Exit() {
	atomic.StoreInt64(&c.InFlightCount, 0)
	c.Conn.Close()
	nsqLog.Logf("client [%s] force exit", c)
}

func (c *ClientV2) FinalClose() {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()
	if c.Reader != nil {
		PutBufioReader(c.Reader)
		c.Reader = nil
	}
	if c.Writer != nil {
		putBufioWriter(c.Writer)
		c.Writer = nil
	}
	if c.tlsConn != nil {
		c.tlsConn.Close()
		c.tlsConn = nil
	}
	c.Conn.Close()
}

func (c *ClientV2) LockWrite() {
	c.writeLock.Lock()
}

func (c *ClientV2) UnlockWrite() {
	c.writeLock.Unlock()
}

func (c *ClientV2) Identify(data IdentifyDataV2) error {
	nsqLog.LogDebugf("[%s]-%v IDENTIFY: %+v", c, c.ID, data)

	// TODO: for backwards compatibility, remove in 1.0
	hostname := data.Hostname
	if hostname == "" {
		hostname = data.LongID
	}
	// TODO: for backwards compatibility, remove in 1.0
	clientID := data.ClientID
	if clientID == "" {
		clientID = data.ShortID
	}

	c.metaLock.Lock()
	c.ClientID = clientID
	c.Hostname = hostname
	c.UserAgent = data.UserAgent
	c.metaLock.Unlock()

	err := c.SetHeartbeatInterval(data.HeartbeatInterval)
	if err != nil {
		return err
	}

	err = c.SetOutputBufferSize(data.OutputBufferSize)
	if err != nil {
		return err
	}

	if data.OutputBufferTimeout < 10 && data.OutputBufferTimeout > 0 {
		nsqLog.Infof("output buffer timeout should not less than 10ms, set to 10ms by force: %v", data)
		data.OutputBufferTimeout = 10
	}

	err = c.SetOutputBufferTimeout(data.OutputBufferTimeout)
	if err != nil {
		return err
	}

	err = c.SetSampleRate(data.SampleRate)
	if err != nil {
		return err
	}

	err = c.SetMsgTimeout(data.MsgTimeout)
	if err != nil {
		return err
	}

	err = c.SetDesiredTag(data.DesiredTag)
	if err != nil {
		return err
	}
	if data.ExtendSupport {
		c.SetExtendSupport()
	}
	c.SetExtFilter(data.ExtFilter)

	c.metaLock.RLock()
	ie := identifyEvent{
		OutputBufferTimeout: time.Duration(atomic.LoadInt64(&c.outputBufferTimeout)),
		HeartbeatInterval:   time.Duration(atomic.LoadInt64(&c.heartbeatInterval)),
		SampleRate:          atomic.LoadInt32(&c.SampleRate),
		MsgTimeout:          time.Duration(atomic.LoadInt64(&c.msgTimeout)),
		ExtFilter:           c.extFilter,
	}
	c.metaLock.RUnlock()

	// update the client's message pump
	select {
	case c.IdentifyEventChan <- ie:
	default:
	}

	return nil
}

func (c *ClientV2) Stats() ClientStats {
	c.metaLock.RLock()
	// TODO: deprecated, remove in 1.0
	name := c.ClientID

	clientID := c.ClientID
	hostname := c.Hostname
	userAgent := c.UserAgent
	var identity string
	var identityURL string
	if c.AuthState != nil {
		identity = c.AuthState.Identity
		identityURL = c.AuthState.IdentityURL
	}
	extFilter := c.extFilter
	c.metaLock.RUnlock()
	stats := ClientStats{
		// TODO: deprecated, remove in 1.0
		Name: name,

		Version:             "V2",
		RemoteAddress:       c.RemoteAddr().String(),
		ClientID:            clientID,
		Hostname:            hostname,
		UserAgent:           userAgent,
		State:               atomic.LoadInt32(&c.State),
		ReadyCount:          atomic.LoadInt64(&c.ReadyCount),
		InFlightCount:       atomic.LoadInt64(&c.InFlightCount),
		MessageCount:        atomic.LoadUint64(&c.MessageCount),
		FinishCount:         atomic.LoadUint64(&c.FinishCount),
		RequeueCount:        atomic.LoadUint64(&c.RequeueCount),
		TimeoutCount:        int64(atomic.LoadUint64(&c.TimeoutCount)),
		ConnectTime:         c.ConnectTime.Unix(),
		SampleRate:          atomic.LoadInt32(&c.SampleRate),
		TLS:                 atomic.LoadInt32(&c.TLS) == 1,
		Deflate:             atomic.LoadInt32(&c.Deflate) == 1,
		Snappy:              atomic.LoadInt32(&c.Snappy) == 1,
		Authed:              c.HasAuthorizations(),
		AuthIdentity:        identity,
		AuthIdentityURL:     identityURL,
		DesiredTag:          c.GetDesiredTag(),
		OutputBufferSize:    c.GetOutputBufferSize(),
		OutputBufferTimeout: int64(c.GetOutputBufferTimeout()),
		ExtFilter:           extFilter,
		MsgTimeout:          int64(c.GetMsgTimeout()),
		LimitedRdy:          atomic.LoadInt32(&c.LimitedRdy),
	}
	if stats.TLS {
		p := prettyConnectionState{c.tlsConn.ConnectionState()}
		stats.CipherSuite = p.GetCipherSuite()
		stats.TLSVersion = p.GetVersion()
		stats.TLSNegotiatedProtocol = p.NegotiatedProtocol
		stats.TLSNegotiatedProtocolIsMutual = p.NegotiatedProtocolIsMutual
	}
	return stats
}

// struct to convert from integers to the human readable strings
type prettyConnectionState struct {
	tls.ConnectionState
}

func (p *prettyConnectionState) GetCipherSuite() string {
	switch p.CipherSuite {
	case tls.TLS_RSA_WITH_RC4_128_SHA:
		return "TLS_RSA_WITH_RC4_128_SHA"
	case tls.TLS_RSA_WITH_3DES_EDE_CBC_SHA:
		return "TLS_RSA_WITH_3DES_EDE_CBC_SHA"
	case tls.TLS_RSA_WITH_AES_128_CBC_SHA:
		return "TLS_RSA_WITH_AES_128_CBC_SHA"
	case tls.TLS_RSA_WITH_AES_256_CBC_SHA:
		return "TLS_RSA_WITH_AES_256_CBC_SHA"
	case tls.TLS_ECDHE_ECDSA_WITH_RC4_128_SHA:
		return "TLS_ECDHE_ECDSA_WITH_RC4_128_SHA"
	case tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA:
		return "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA"
	case tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA:
		return "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA"
	case tls.TLS_ECDHE_RSA_WITH_RC4_128_SHA:
		return "TLS_ECDHE_RSA_WITH_RC4_128_SHA"
	case tls.TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA:
		return "TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA"
	case tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA:
		return "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA"
	case tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA:
		return "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA"
	case tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256:
		return "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"
	case tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256:
		return "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256"
	}
	return fmt.Sprintf("Unknown %d", p.CipherSuite)
}

func (p *prettyConnectionState) GetVersion() string {
	switch p.Version {
	case tls.VersionSSL30:
		return "SSL30"
	case tls.VersionTLS10:
		return "TLS1.0"
	case tls.VersionTLS11:
		return "TLS1.1"
	case tls.VersionTLS12:
		return "TLS1.2"
	default:
		return fmt.Sprintf("Unknown %d", p.Version)
	}
}

func (c *ClientV2) IsReadyForMessages() bool {
	if c.Channel.IsPaused() {
		return false
	}

	readyCount := atomic.LoadInt64(&c.ReadyCount)
	inFlightCount := atomic.LoadInt64(&c.InFlightCount)
	errCnt := atomic.LoadInt64(&c.subErrCnt)
	if readyCount > 1 && errCnt >= slowDownThreshold {
		// slow down this client if has some error
		adjustReadyCount := readyCount - errCnt + slowDownThreshold
		if adjustReadyCount <= 0 {
			lct := atomic.LoadInt64(&c.lastConsumeTimeout)
			msgTimeout := time.Duration(atomic.LoadInt64(&c.msgTimeout))
			readyCount = 1
			if time.Now().Add(-2*msgTimeout).Unix() < lct {
				// the wait time maybe large than the msgtimeout (maybe need wait heartbeat to trigge channel wakeup)
				nsqLog.LogDebugf("[%s] state inflt: %4d, errCnt: %d temporarily slow down since last timeout %v",
					c, inFlightCount, errCnt, lct)
			}
		} else if adjustReadyCount < readyCount {
			readyCount = adjustReadyCount
		}
	}

	if nsqLog.Level() >= levellogger.LOG_DETAIL {
		nsqLog.LogDebugf("[%s] state rdy: %4d inflt: %4d, errCnt: %d",
			c, readyCount, inFlightCount, errCnt)
	}

	if inFlightCount >= readyCount || readyCount <= 0 {
		return false
	}
	limitedRdy := atomic.LoadInt32(&c.LimitedRdy)
	if inFlightCount >= int64(limitedRdy) || limitedRdy <= 0 {
		return false
	}
	return true
}

func (c *ClientV2) SetLimitedRdy(cnt int) {
	atomic.StoreInt32(&c.LimitedRdy, int32(cnt))
	c.tryUpdateReadyState()
}

func (c *ClientV2) SetReadyCount(count int64) {
	atomic.StoreInt64(&c.ReadyCount, count)
	c.tryUpdateReadyState()
}

func (c *ClientV2) tryUpdateReadyState() {
	// you can always *try* to write to ReadyStateChan because in the cases
	// where you cannot the message pump loop would have iterated anyway.
	// the atomic integer operations guarantee correctness of the value.
	select {
	case c.ReadyStateChan <- 1:
	default:
	}
}

func (c *ClientV2) IncrSubError(delta int64) {
	if delta < 0 {
		if atomic.LoadInt64(&c.subErrCnt) <= 0 {
			return
		}
	}
	newCnt := atomic.AddInt64(&c.subErrCnt, delta)
	if newCnt < 0 {
		atomic.StoreInt64(&c.subErrCnt, 0)
	} else if newCnt > 0 {
		rdy := atomic.LoadInt64(&c.ReadyCount)
		if newCnt > rdy+4*slowDownThreshold {
			atomic.StoreInt64(&c.subErrCnt, rdy+4*slowDownThreshold)
		} else if delta < 0 && newCnt < rdy {
			// try speed up error recover
			newCnt = atomic.AddInt64(&c.subErrCnt, -1*(rdy-newCnt)/4)
			if newCnt < 0 {
				atomic.StoreInt64(&c.subErrCnt, 0)
			}
			//atomic.StoreInt64(&c.subErrCnt, newCnt/2)
		}
	}

}

func (c *ClientV2) FinishedMessage() {
	atomic.AddUint64(&c.FinishCount, 1)
	atomic.AddInt64(&c.InFlightCount, -1)
	c.IncrSubError(int64(-2))
	c.tryUpdateReadyState()
}

func (c *ClientV2) Empty() {
	atomic.StoreInt64(&c.InFlightCount, 0)
	atomic.StoreInt64(&c.subErrCnt, 0)
	c.tryUpdateReadyState()
}

func (c *ClientV2) SendingMessage() {
	atomic.AddInt64(&c.InFlightCount, 1)
	atomic.AddUint64(&c.MessageCount, 1)
}

func (c *ClientV2) TimedOutMessage() {
	atomic.AddInt64(&c.InFlightCount, -1)
	atomic.AddUint64(&c.TimeoutCount, 1)
	c.IncrSubError(int64(1))
	atomic.StoreInt64(&c.lastConsumeTimeout, time.Now().Unix())
	c.tryUpdateReadyState()
}

func (c *ClientV2) RequeuedMessage() {
	atomic.AddUint64(&c.RequeueCount, 1)
	atomic.AddInt64(&c.InFlightCount, -1)
	c.tryUpdateReadyState()
}

func (c *ClientV2) StartClose() {
	// Force the client into ready 0
	c.SetReadyCount(0)
	// mark this client as closing
	atomic.StoreInt32(&c.State, stateClosing)
}

func (c *ClientV2) SkipZanTest() {
	c.tryUpdateReadyState()
}

func (c *ClientV2) UnskipZanTest() {
	c.tryUpdateReadyState()
}

func (c *ClientV2) Pause() {
	c.tryUpdateReadyState()
}

func (c *ClientV2) UnPause() {
	c.tryUpdateReadyState()
}

func (c *ClientV2) GetHeartbeatInterval() time.Duration {
	return time.Duration(atomic.LoadInt64(&c.heartbeatInterval))
}

func (c *ClientV2) SwitchToConsumer(isEphemeral bool) error {
	// client default is optimized for producer (use more reader buffer and less writer buffer)
	// consumer optimize for write, use more writer buffer by default if not changed by client
	// for ephemeral channel client we keep use small buffer
	if isEphemeral {
		return nil
	}

	s := atomic.LoadInt64(&c.outputBufferSize)
	nsqLog.Logf("[%s]-%v switch to consumer: %v, %v", c, c.ID, s, c.GetOutputBufferTimeout())
	if s > 0 {
		return nil
	}
	// change default
	c.writeLock.Lock()
	defer c.writeLock.Unlock()
	if c.tlsConn != nil {
		return nil
	}
	if atomic.LoadInt32(&c.Deflate) == 1 || atomic.LoadInt32(&c.Snappy) == 1 {
		return nil
	}
	sz := int64(defaultConsumerWriteBufferSize)
	if sz > c.ctxOpts.MaxOutputBufferSize {
		sz = c.ctxOpts.MaxOutputBufferSize
	}
	atomic.StoreInt64(&c.outputBufferSize, sz)
	err := c.Writer.Flush()
	if err != nil {
		return err
	}
	c.Writer = newBufioWriterSize(c.Conn, int(c.GetOutputBufferSize()))
	return nil
}

func (c *ClientV2) SetHeartbeatInterval(desiredInterval int) error {
	switch {
	case desiredInterval == -1:
		atomic.StoreInt64(&c.heartbeatInterval, 0)
	case desiredInterval == 0:
		// do nothing (use default)
	case desiredInterval >= 1000 &&
		desiredInterval <= int(c.ctxOpts.MaxHeartbeatInterval/time.Millisecond):
		atomic.StoreInt64(&c.heartbeatInterval, int64(time.Duration(desiredInterval)*time.Millisecond))
	default:
		return fmt.Errorf("heartbeat interval (%d) is invalid", desiredInterval)
	}

	return nil
}

func (c *ClientV2) GetOutputBufferSize() int64 {
	s := atomic.LoadInt64(&c.outputBufferSize)
	if s <= 0 {
		return defaultWriteBufferSize
	}
	return s
}

func (c *ClientV2) SetOutputBufferSize(desiredSize int) error {
	var size int

	switch {
	case desiredSize == -1:
		// effectively no buffer (every write will go directly to the wrapped net.Conn)
		size = 1
	case desiredSize == 0:
		// do nothing (use default)
	case desiredSize >= 64 && desiredSize <= int(c.ctxOpts.MaxOutputBufferSize):
		size = desiredSize
	case desiredSize > int(c.ctxOpts.MaxOutputBufferSize):
		size = int(c.ctxOpts.MaxOutputBufferSize)
	default:
		return fmt.Errorf("output buffer size (%d) is invalid", desiredSize)
	}

	if size > 0 {
		c.writeLock.Lock()
		defer c.writeLock.Unlock()
		atomic.StoreInt64(&c.outputBufferSize, int64(size))
		err := c.Writer.Flush()
		if err != nil {
			return err
		}
		c.Writer = newBufioWriterSize(c.Conn, size)
	}

	return nil
}

func (c *ClientV2) SetExtFilter(filter ExtFilterData) {
	c.metaLock.Lock()
	defer c.metaLock.Unlock()
	c.extFilter = filter
}

func (c *ClientV2) GetOutputBufferTimeout() time.Duration {
	return time.Duration(atomic.LoadInt64(&c.outputBufferTimeout))
}

func (c *ClientV2) SetOutputBufferTimeout(desiredTimeout int) error {
	switch {
	case desiredTimeout == -1:
		atomic.StoreInt64(&c.outputBufferTimeout, 0)
	case desiredTimeout == 0:
		// do nothing (use default)
	case desiredTimeout >= 1 &&
		desiredTimeout <= int(c.ctxOpts.MaxOutputBufferTimeout/time.Millisecond):
		atomic.StoreInt64(&c.outputBufferTimeout, int64(desiredTimeout)*int64(time.Millisecond))
	default:
		return fmt.Errorf("output buffer timeout (%d) is invalid", desiredTimeout)
	}

	return nil
}

func (c *ClientV2) SetSampleRate(sampleRate int32) error {
	if sampleRate < 0 || sampleRate > 99 {
		return fmt.Errorf("sample rate (%d) is invalid", sampleRate)
	}
	atomic.StoreInt32(&c.SampleRate, sampleRate)
	return nil
}

func (c *ClientV2) GetDesiredTag() string {
	c.metaLock.RLock()
	defer c.metaLock.RUnlock()
	return c.desiredTag
}

func (c *ClientV2) ExtendSupport() bool {
	es := atomic.LoadInt32(&c.isExtendSupport)
	return es != 0
}

func (c *ClientV2) SetExtendSupport() {
	atomic.StoreInt32(&c.isExtendSupport, 1)
}

func (c *ClientV2) GetMsgTimeout() time.Duration {
	return time.Duration(atomic.LoadInt64(&c.msgTimeout))
}

func (c *ClientV2) SetMsgTimeout(msgTimeout int) error {
	switch {
	case msgTimeout == 0:
		// do nothing (use default)
	case msgTimeout >= 1000 &&
		msgTimeout <= int(c.ctxOpts.MaxMsgTimeout/time.Millisecond):
		atomic.StoreInt64(&c.msgTimeout, int64(msgTimeout)*int64(time.Millisecond))
	default:
		return fmt.Errorf("msg timeout (%d) is invalid", msgTimeout)
	}

	return nil
}

func (c *ClientV2) UnsetDesiredTag() {
	c.metaLock.Lock()
	defer c.metaLock.Unlock()
	c.desiredTag = ""
}

func (c *ClientV2) SetDesiredTag(tagStr string) error {
	if tagStr == "" {
		return nil
	}
	if len(tagStr) > ext.MAX_TAG_LEN {
		return fmt.Errorf("invalid tag len %v, exceed limit %v", len(tagStr), ext.MAX_TAG_LEN)
	}
	c.metaLock.Lock()
	defer c.metaLock.Unlock()
	if tagStr != "" && c.desiredTag != tagStr {
		c.desiredTag = tagStr
	}
	return nil
}

// since only used in messagePump loop, no lock needed
func (c *ClientV2) SetTagMsgChannel(tagMsgChan chan *Message) error {
	c.TagMsgChannel = tagMsgChan
	return nil
}

func (c *ClientV2) GetTagMsgChannel() chan *Message {
	return c.TagMsgChannel
}

func (c *ClientV2) UpgradeTLS() error {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	tlsConn := tls.Server(c.Conn, c.tlsConfig)
	tlsConn.SetDeadline(time.Now().Add(5 * time.Second))
	err := tlsConn.Handshake()
	if err != nil {
		return err
	}
	c.tlsConn = tlsConn

	c.Reader = NewBufioReader(c.tlsConn)
	c.Writer = newBufioWriterSize(c.tlsConn, int(c.GetOutputBufferSize()))

	atomic.StoreInt32(&c.TLS, 1)

	return nil
}

func (c *ClientV2) UpgradeDeflate(level int) error {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	conn := c.Conn
	if c.tlsConn != nil {
		conn = c.tlsConn
	}

	c.Reader = NewBufioReader(flate.NewReader(conn))

	fw, _ := flate.NewWriter(conn, level)
	c.flateWriter = fw
	c.Writer = newBufioWriterSize(fw, int(c.GetOutputBufferSize()))

	atomic.StoreInt32(&c.Deflate, 1)

	return nil
}

func (c *ClientV2) UpgradeSnappy() error {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	conn := c.Conn
	if c.tlsConn != nil {
		conn = c.tlsConn
	}

	c.Reader = NewBufioReader(snappy.NewReader(conn))
	c.Writer = newBufioWriterSize(snappy.NewWriter(conn), int(c.GetOutputBufferSize()))

	atomic.StoreInt32(&c.Snappy, 1)

	return nil
}

func (c *ClientV2) Flush() error {
	err := c.Writer.Flush()
	if err != nil {
		return err
	}

	if c.flateWriter != nil {
		return c.flateWriter.Flush()
	}

	return nil
}

func (c *ClientV2) QueryAuthd() error {
	remoteIP, _, err := net.SplitHostPort(c.String())
	if err != nil {
		return err
	}

	tls := atomic.LoadInt32(&c.TLS) == 1
	tlsEnabled := "false"
	if tls {
		tlsEnabled = "true"
	}

	authState, err := auth.QueryAnyAuthd(c.ctxOpts.AuthHTTPAddresses,
		remoteIP, tlsEnabled, c.AuthSecret)
	if err != nil {
		return err
	}
	c.AuthState = authState
	return nil
}

func (c *ClientV2) Auth(secret string) error {
	c.AuthSecret = secret
	return c.QueryAuthd()
}

func (c *ClientV2) IsAuthorized(topic, channel string) (bool, error) {
	if c.AuthState == nil {
		return false, nil
	}
	if c.AuthState.IsExpired() {
		err := c.QueryAuthd()
		if err != nil {
			return false, err
		}
	}
	if c.AuthState.IsAllowed(topic, channel) {
		return true, nil
	}
	return false, nil
}

func (c *ClientV2) HasAuthorizations() bool {
	if c.AuthState != nil {
		return len(c.AuthState.Authorizations) != 0
	}
	return false
}
