package nsqd

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/youzan/nsq/internal/protocol"

	"github.com/youzan/nsq/internal/ext"
	"github.com/youzan/nsq/internal/levellogger"
	"github.com/youzan/nsq/internal/quantile"
	"golang.org/x/time/rate"
)

const (
	resetReaderTimeoutSec      = 60
	MaxMemReqTimes             = 10
	maxTimeoutCntToReq         = 20
	MaxWaitingDelayed          = 100
	MaxDepthReqToEnd           = 1000000
	ZanTestSkip                = 0
	ZanTestUnskip              = 1
	memSizeForSmall            = 2
	delayedReqToEndMinInterval = time.Millisecond * 128
	DefaultMaxChDelayedQNum    = 10000 * 16
	limitSmallMsgBytes         = 1024
)

func ChangeIntervalForTest() {
	timeoutBlockingWait = time.Second
}

var (
	timeoutBlockingWait = time.Hour / 2
)

var (
	ErrMsgNotInFlight                 = errors.New("Message ID not in flight")
	ErrMsgTooMuchReq                  = errors.New("Message too much requeue")
	ErrMsgDeferredTooMuch             = errors.New("Too much deferred messages in flight")
	ErrMsgAlreadyInFlight             = errors.New("Message ID already in flight")
	ErrConsumeDisabled                = errors.New("Consume is disabled currently")
	ErrMsgDeferred                    = errors.New("Message is deferred")
	ErrSetConsumeOffsetNotFirstClient = errors.New("consume offset can only be changed by the first consume client")
	ErrNotDiskQueueReader             = errors.New("the consume channel is not disk queue reader")
)

type Consumer interface {
	SkipZanTest()
	UnskipZanTest()
	UnPause()
	Pause()
	TimedOutMessage()
	RequeuedMessage()
	FinishedMessage()
	Stats() ClientStats
	Exit()
	String() string
	GetID() int64
	SetLimitedRdy(cnt int)
}

type resetChannelData struct {
	Offset         BackendOffset
	Cnt            int64
	ClearConfirmed bool
}

type MsgChanData struct {
	MsgChan   chan *Message
	ClientCnt int64
}

type delayedMessage struct {
	msg        Message
	deliveryTs time.Time
}

// Channel represents the concrete type for a NSQ channel (and also
// implements the Queue interface)
//
// There can be multiple channels per topic, each with there own unique set
// of subscribers (clients).
//
// Channels maintain all client and message metadata, orchestrating in-flight
// messages, timeouts, requeuing, etc.
type Channel struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	requeueCount      uint64
	timeoutCount      uint64
	deferredCount     int64
	deferredFromDelay int64

	sync.RWMutex

	topicName  string
	topicPart  int
	name       string
	nsqdNotify INsqdNotify
	option     *Options

	backend BackendQueueReader

	requeuedMsgChan        chan *Message
	waitingRequeueChanMsgs map[MessageID]*Message
	waitingRequeueMsgs     map[MessageID]*Message
	tagMsgChansMutex       sync.RWMutex
	//mapping from tag to messages chan
	tagMsgChans        map[string]*MsgChanData
	tagChanInitChan    chan string
	tagChanRemovedChan chan string
	clientMsgChan      chan *Message

	exitChan     chan int
	exitSyncChan chan bool
	exitFlag     int32
	exitMutex    sync.RWMutex

	// state tracking
	clients          map[int64]Consumer
	paused           int32
	skipped          int32
	zanTestSkip      int32
	ephemeral        bool
	deleteCallback   func(*Channel)
	deleter          sync.Once
	moreDataCallback func(*Channel)

	// Stats tracking
	e2eProcessingLatencyStream *quantile.Quantile

	inFlightMessages map[MessageID]*Message
	inFlightPQ       inFlightPqueue
	inFlightMutex    sync.Mutex

	confirmedMsgs   *IntervalSkipList
	confirmMutex    sync.Mutex
	waitingConfirm  int32
	tryReadBackend  chan bool
	readerChanged   chan resetChannelData
	endUpdatedChan  chan bool
	needNotifyRead  int32
	consumeDisabled int32
	// stat counters
	EnableTrace     int32
	EnableSlowTrace int32
	Ext             int32

	requireOrder int32
	// 1 - reset
	// 2 - reset and clear confirmed
	needResetReader        int32
	processResetReaderTime int64
	waitingProcessMsgTs    int64
	waitingDeliveryState   int32
	delayedLock            sync.RWMutex
	delayedQueue           *DelayQueue
	delayedConfirmedMsgs   map[MessageID]Message

	//channel msg stats
	channelStatsInfo      *ChannelStatsInfo
	topicOrdered          bool
	lastDelayedReqToEndTs int64
	limiter               *rate.Limiter
	chLog                 *levellogger.LevelLogger
	lastQueueReadTs       int64
	ackOldThanTime        time.Duration
	ackRetryCnt           int
}

// NewChannel creates a new instance of the Channel type and returns a pointer
func NewChannel(topicName string, part int, topicOrdered bool, channelName string, chEnd BackendQueueEnd, opt *Options,
	deleteCallback func(*Channel), moreDataCallback func(*Channel), consumeDisabled int32,
	notify INsqdNotify, ext int32, queueStart BackendQueueEnd, metaStorage IMetaStorage,
	kvTopic *KVTopic, forceReload bool) *Channel {

	c := &Channel{
		topicName:          topicName,
		topicPart:          part,
		topicOrdered:       topicOrdered,
		name:               channelName,
		clientMsgChan:      make(chan *Message),
		tagMsgChans:        make(map[string]*MsgChanData),
		tagChanInitChan:    make(chan string, 2),
		tagChanRemovedChan: make(chan string, 2),
		exitChan:           make(chan int),
		exitSyncChan:       make(chan bool),
		clients:            make(map[int64]Consumer),
		confirmedMsgs:      NewIntervalSkipList(),
		tryReadBackend:     make(chan bool, 1),
		readerChanged:      make(chan resetChannelData, 10),
		endUpdatedChan:     make(chan bool, 1),
		deleteCallback:     deleteCallback,
		moreDataCallback:   moreDataCallback,
		option:             opt,
		nsqdNotify:         notify,
		consumeDisabled:    consumeDisabled,
		Ext:                ext,
		// we use KB as tokens, and ignore any less than 1KB messages
		// so the limiter only limit the large messages.
		limiter: rate.NewLimiter(rate.Limit(opt.ChannelRateLimitKB), int(opt.ChannelRateLimitKB)*4),
	}
	if opt.AckOldThanTime > 0 {
		c.ackOldThanTime = opt.AckOldThanTime
	}
	if opt.AckRetryCnt > 0 {
		c.ackRetryCnt = opt.AckRetryCnt
	}

	c.chLog = nsqLog.WrappedWithPrefix("["+c.GetTopicName()+"("+c.GetName()+")]", 0)
	if protocol.IsEphemeral(channelName) {
		c.ephemeral = true
	}
	if topicOrdered || c.IsEphemeral() {
		c.requeuedMsgChan = make(chan *Message, memSizeForSmall)
	} else {
		c.requeuedMsgChan = make(chan *Message, opt.MaxRdyCount/10+1)
	}
	c.waitingRequeueChanMsgs = make(map[MessageID]*Message, memSizeForSmall)
	c.waitingRequeueMsgs = make(map[MessageID]*Message, memSizeForSmall)
	c.delayedConfirmedMsgs = make(map[MessageID]Message, memSizeForSmall)

	if len(opt.E2EProcessingLatencyPercentiles) > 0 {
		c.e2eProcessingLatencyStream = quantile.New(
			opt.E2EProcessingLatencyWindowTime,
			opt.E2EProcessingLatencyPercentiles,
		)
	}
	// channel no need sync so much.
	syncEvery := opt.SyncEvery * 1000
	if syncEvery < 1 {
		syncEvery = 1
	}

	//initialize channel stats
	c.channelStatsInfo = &ChannelStatsInfo{
		topicName:   c.topicName,
		topicPart:   strconv.Itoa(c.topicPart),
		channelName: channelName,
	}

	c.initPQ()

	// backend names, for uniqueness, automatically include the topic...
	backendReaderName := getBackendReaderName(c.topicName, c.topicPart, channelName)
	backendName := getBackendName(c.topicName, c.topicPart)
	c.backend = newDiskQueueReader(backendName, backendReaderName,
		path.Join(opt.DataPath, c.topicName),
		opt.MaxBytesPerFile,
		int32(minValidMsgLength),
		int32(opt.MaxMsgSize)+minValidMsgLength,
		syncEvery,
		opt.SyncTimeout,
		chEnd,
		false,
		metaStorage,
		kvTopic,
		forceReload,
	)

	if queueStart != nil {
		// The old closed channel (on slave) may have the invalid read start if the
		// topic disk data is cleaned already. So we check here for new opened channel
		c.checkAndFixStart(queueStart)
	}
	go c.messagePump()

	c.nsqdNotify.NotifyStateChanged(c, false)

	return c
}

func (c *Channel) GetName() string {
	return c.name
}

func (c *Channel) GetTopicName() string {
	return c.topicName
}

func (c *Channel) GetTopicPart() int {
	return c.topicPart
}

func (c *Channel) ChangeLimiterBytes(kb int64) {
	if kb <= 0 {
		return
	}
	c.limiter.SetLimit(rate.Limit(kb))
	c.limiter.SetBurst(int(kb) * 4)
}

func (c *Channel) checkAndFixStart(start BackendQueueEnd) {
	dr, _ := c.backend.(*diskQueueReader)
	dstart, _ := start.(*diskQueueEndInfo)
	if dr != nil && dstart != nil && dstart.EndOffset.GreatThan(&dr.confirmedQueueInfo.EndOffset) {
		c.chLog.Infof("confirm start need be fixed since file number is greater %v, %v",
			start, c.GetConfirmed())
		// new queue start file name is great than the confirmed, we move confirmed to new start
		oldConfirmed := c.GetConfirmed()
		dr.ResetReadToQueueStart(*dstart)
		if c.GetConfirmed().Offset() < oldConfirmed.Offset() {
			c.chLog.Infof("confirm start need be fixed %v, %v, %v",
				start, oldConfirmed, c.GetConfirmed())
			c.backend.SkipReadToOffset(oldConfirmed.Offset(), oldConfirmed.TotalMsgCnt())
		}
		return
	}
	if c.GetConfirmed().Offset() >= start.Offset() {
		return
	}
	c.chLog.Infof("confirm start need be fixed %v, %v",
		start, c.GetConfirmed())
	_, err := c.backend.SkipReadToOffset(start.Offset(), start.TotalMsgCnt())
	if err != nil {
		c.chLog.Warningf("skip to new start failed: %v",
			err)
		if dr != nil && dstart != nil {
			oldConfirmed := c.GetConfirmed()
			dr.ResetReadToQueueStart(*dstart)
			if c.GetConfirmed().Offset() < oldConfirmed.Offset() {
				c.chLog.Infof("confirm start need be fixed %v, %v, %v",
					start, oldConfirmed, c.GetConfirmed())
				c.backend.SkipReadToOffset(oldConfirmed.Offset(), oldConfirmed.TotalMsgCnt())
			}
			c.chLog.Warningf("fix channel read to queue start: %v",
				dstart)
		} else {
			newRead, _ := c.backend.SkipReadToEnd()
			c.chLog.Warningf("fix channel read to : %v",
				newRead)
		}
	}
}

func (c *Channel) closeClientMsgChannels() {
	c.tagMsgChansMutex.Lock()
	defer c.tagMsgChansMutex.Unlock()

	for tag := range c.tagMsgChans {
		delete(c.tagMsgChans, tag)
	}
}

func (c *Channel) RemoveTagClientMsgChannel(tag string) {
	c.tagMsgChansMutex.Lock()
	defer c.tagMsgChansMutex.Unlock()
	tagCh, ok := c.tagMsgChans[tag]
	if !ok {
		return
	}

	cnt := tagCh.ClientCnt
	if cnt-1 > int64(0) {
		tagCh.ClientCnt = cnt - 1
	} else {
		tagCh.ClientCnt = 0
		delete(c.tagMsgChans, tag)
		select {
		case c.tagChanRemovedChan <- tag:
		default:
			select {
			case c.tagChanRemovedChan <- tag:
			case <-time.After(time.Millisecond * 10):
				c.chLog.Infof("timeout sending tag channel remove signal for %v", tag)
			}
		}
	}
}

//get or create tag message chanel, invoked from protocol_v2.messagePump()
func (c *Channel) GetOrCreateClientMsgChannel(tag string) chan *Message {
	c.tagMsgChansMutex.Lock()
	defer c.tagMsgChansMutex.Unlock()
	tagMsgChanData, exist := c.tagMsgChans[tag]

	if exist {
		tagMsgChanData.ClientCnt = tagMsgChanData.ClientCnt + 1
	} else {
		//initialize tag channel
		c.tagMsgChans[string(tag)] = &MsgChanData{
			make(chan *Message),
			1,
		}
		select {
		case c.tagChanInitChan <- tag:
		case <-time.After(time.Millisecond * 10):
			c.chLog.Infof("timeout sending tag channel init signal for %v", tag)
		}
	}

	return c.tagMsgChans[tag].MsgChan
}

func (c *Channel) GetClientMsgChan() chan *Message {
	return c.clientMsgChan
}

/**
get active tag channel or default message channel from tag channel map
*/
func (c *Channel) GetClientTagMsgChan(tag string) (chan *Message, bool) {
	c.tagMsgChansMutex.RLock()
	defer c.tagMsgChansMutex.RUnlock()
	msgChanData, exist := c.tagMsgChans[tag]
	if !exist {
		c.chLog.Debugf("channel tag %v not found.", tag)
		return nil, false
	}
	return msgChanData.MsgChan, true
}

func (c *Channel) isNeedParseMsgExt() bool {
	c.tagMsgChansMutex.RLock()
	tagChLen := len(c.tagMsgChans)
	c.tagMsgChansMutex.RUnlock()
	if tagChLen > 0 {
		return true
	}
	// TODO: if the channel need filter, we need parse ext
	return false
}

func (c *Channel) IsSlowTraced() bool {
	return atomic.LoadInt32(&c.EnableSlowTrace) == 1
}

func (c *Channel) IsTraced() bool {
	return atomic.LoadInt32(&c.EnableTrace) == 1
}

func (c *Channel) IsEphemeral() bool {
	return c.ephemeral
}

func (c *Channel) SetDelayedQueue(dq *DelayQueue) {
	c.delayedLock.Lock()
	c.delayedQueue = dq
	c.delayedLock.Unlock()
}

func (c *Channel) GetDelayedQueue() *DelayQueue {
	c.delayedLock.RLock()
	dq := c.delayedQueue
	c.delayedLock.RUnlock()
	return dq
}

func (c *Channel) SetExt(isExt bool) {
	if isExt {
		atomic.StoreInt32(&c.Ext, 1)
	} else {
		atomic.StoreInt32(&c.Ext, 0)
	}
}

func (c *Channel) IsExt() bool {
	return atomic.LoadInt32(&c.Ext) == 1
}

func (c *Channel) SetSlowTrace(enable bool) {
	if enable {
		atomic.StoreInt32(&c.EnableSlowTrace, 1)
	} else {
		atomic.StoreInt32(&c.EnableSlowTrace, 0)
	}
}

func (c *Channel) SetTrace(enable bool) {
	if enable {
		atomic.StoreInt32(&c.EnableTrace, 1)
	} else {
		atomic.StoreInt32(&c.EnableTrace, 0)
	}
}

func (c *Channel) SetConsumeOffset(offset BackendOffset, cnt int64, force bool) error {
	c.Lock()
	defer c.Unlock()
	num := len(c.clients)
	if num > 1 && !force {
		return ErrSetConsumeOffsetNotFirstClient
	}
	if c.IsConsumeDisabled() {
		return ErrConsumeDisabled
	}

	_, ok := c.backend.(*diskQueueReader)
	if ok {
		select {
		case c.readerChanged <- resetChannelData{offset, cnt, true}:
		default:
			c.chLog.Logf("ignored the reader reset: %v:%v", offset, cnt)
			if offset > 0 && cnt > 0 {
				select {
				case c.readerChanged <- resetChannelData{offset, cnt, true}:
				case <-time.After(time.Millisecond * 10):
					c.chLog.Logf("ignored the reader reset finally: %v:%v", offset, cnt)
				}
			}
		}
	} else {
		return ErrNotDiskQueueReader
	}
	return nil
}

func (c *Channel) SetOrdered(enable bool) {
	if enable {
		if !atomic.CompareAndSwapInt32(&c.requireOrder, 0, 1) {
			return
		}
		select {
		case c.readerChanged <- resetChannelData{BackendOffset(-1), 0, true}:
		default:
		}
	} else {
		c.RLock()
		defer c.RUnlock()
		if !c.IsOrdered() {
			return
		}
		for _, c := range c.clients {
			c.Exit()
		}
		atomic.StoreInt32(&c.requireOrder, 0)
		select {
		case c.tryReadBackend <- true:
		default:
		}
	}
}

func (c *Channel) IsOrdered() bool {
	return atomic.LoadInt32(&c.requireOrder) == 1
}

func (c *Channel) initPQ() {
	pqSize := int(math.Max(1, float64(c.option.MemQueueSize)/10))
	if c.topicOrdered || c.IsEphemeral() {
		pqSize = memSizeForSmall
	}

	for _, m := range c.inFlightMessages {
		if m.belongedConsumer != nil {
			// Do we need empty client here?
			// Req may be better for counter stats (any race case that make inflight count not 0?)
			m.belongedConsumer.RequeuedMessage()
			m.belongedConsumer = nil
		}

		if m.DelayedType == ChannelDelayed {
			atomic.AddInt64(&c.deferredFromDelay, -1)
		}
	}
	c.inFlightMessages = make(map[MessageID]*Message, pqSize)
	c.inFlightPQ = newInFlightPqueue(pqSize)
	atomic.StoreInt64(&c.deferredCount, 0)
}

// Exiting returns a boolean indicating if this channel is closed/exiting
func (c *Channel) Exiting() bool {
	return atomic.LoadInt32(&c.exitFlag) == 1
}

// Delete empties the channel and closes
func (c *Channel) Delete() error {
	return c.exit(true)
}

// Close cleanly closes the Channel
func (c *Channel) Close() error {
	return c.exit(false)
}

// waiting more data may include :
// waiting more disk read
// waiting in memory inflight
// waiting in delayed inflight
// waiting requeued

// waiting read more data from disk, this means the reader has reached end of queue.
// However, there may be some other messages in memory waiting confirm.
func (c *Channel) IsWaitingMoreDiskData() bool {
	if c.IsPaused() || c.IsConsumeDisabled() || c.IsSkipped() {
		return false
	}
	d, ok := c.backend.(*diskQueueReader)
	if ok {
		return d.isReadToEnd()
	}
	return false
}

// waiting more data is indicated all msgs are consumed and confirmed
// if some delayed message in channel, waiting more data is not true
func (c *Channel) IsWaitingMoreData() bool {
	if c.IsPaused() || c.IsConsumeDisabled() || c.IsSkipped() {
		return false
	}
	d, ok := c.backend.(*diskQueueReader)
	if ok {
		return d.IsWaitingMoreData()
	}
	return false
}

func (c *Channel) exit(deleted bool) error {
	c.exitMutex.Lock()
	defer c.exitMutex.Unlock()

	if !atomic.CompareAndSwapInt32(&c.exitFlag, 0, 1) {
		return ErrExiting
	}

	if deleted {
		c.chLog.Logf("CHANNEL deleting")

		// since we are explicitly deleting a channel (not just at system exit time)
		// de-register this from the lookupd
		c.nsqdNotify.NotifyStateChanged(c, false)
	} else {
		c.chLog.Logf("CHANNEL closing")
	}

	// this forceably closes clients, client will be removed by client before the
	// client read loop exit.
	c.RLock()
	for _, client := range c.clients {
		client.Exit()
	}
	c.RUnlock()

	close(c.exitChan)
	<-c.exitSyncChan

	// write anything leftover to disk
	c.Flush(true)
	if deleted {
		// empty the queue (deletes the backend files, too)
		if c.GetDelayedQueue() != nil {
			c.GetDelayedQueue().EmptyDelayedChannel(c.GetName())
		}

		c.skipChannelToEnd()
		return c.backend.Delete()
	}

	return c.backend.Close()
}

func (c *Channel) skipChannelToEnd() (BackendQueueEnd, error) {
	c.Lock()
	defer c.Unlock()
	e, err := c.backend.SkipReadToEnd()
	if err != nil {
		c.chLog.Infof("failed to reset reader to end %v", err)
	} else {
		c.drainChannelWaiting(true, nil, nil)
	}
	return e, nil
}

func (c *Channel) Flush(fsync bool) error {
	if c.IsSkipped() && !c.IsConsumeDisabled() && c.Depth() > 0 {
		// skipped channel will not read the disk queue, so we need skip to end periodically if we have new data
		e := c.GetChannelEnd()
		select {
		case c.readerChanged <- resetChannelData{e.Offset(), e.TotalMsgCnt(), true}:
		default:
		}
	}
	if c.ephemeral {
		return nil
	}
	d, ok := c.backend.(*diskQueueReader)
	if ok {
		d.Flush(fsync)
	}
	return nil
}

func (c *Channel) Backlogs() int64 {
	depth := c.Depth()
	//short cut for order topic
	if c.IsOrdered() {
		return depth
	}

	delayedCnt := atomic.LoadInt64(&c.deferredCount)
	backlogsNotRead := c.GetChannelNotReadCnt()
	if backlogsNotRead < 0 {
		backlogsNotRead = 0
	}
	return delayedCnt + backlogsNotRead
}

func (c *Channel) Depth() int64 {
	return c.backend.Depth()
}

func (c *Channel) DepthSize() int64 {
	if d, ok := c.backend.(*diskQueueReader); ok {
		return d.DepthSize()
	}
	return 0
}

func (c *Channel) DepthTimestamp() int64 {
	return atomic.LoadInt64(&c.waitingProcessMsgTs)
}

func (c *Channel) IsZanTestSkipped() bool {
	return c.IsExt() && c.option.AllowZanTestSkip && atomic.LoadInt32(&c.zanTestSkip) == ZanTestSkip
}

func (c *Channel) SkipZanTest() {
	c.doSkipZanTest(true)
}

func (c *Channel) UnskipZanTest() {
	c.doSkipZanTest(false)
}

func (c *Channel) doSkipZanTest(skipped bool) {
	if skipped {
		atomic.StoreInt32(&c.zanTestSkip, ZanTestSkip)
		//
	} else {
		atomic.StoreInt32(&c.zanTestSkip, ZanTestUnskip)
	}

	c.RLock()
	for _, client := range c.clients {
		if skipped {
			client.SkipZanTest()
		} else {
			client.UnskipZanTest()
		}
	}
	c.RUnlock()
}

func (c *Channel) Pause() {
	c.doPause(true)
}

func (c *Channel) UnPause() {
	c.doPause(false)
}

func (c *Channel) doPause(pause bool) {
	if pause {
		atomic.StoreInt32(&c.paused, 1)
	} else {
		atomic.StoreInt32(&c.paused, 0)
	}

	c.RLock()
	for _, client := range c.clients {
		if pause {
			client.Pause()
		} else {
			client.UnPause()
		}
	}
	c.RUnlock()
}

func (c *Channel) IsPaused() bool {
	return atomic.LoadInt32(&c.paused) == 1
}

func (c *Channel) Skip() {
	c.doSkip(true)
}

func (c *Channel) UnSkip() {
	c.doSkip(false)
}

func (c *Channel) IsSkipped() bool {
	return atomic.LoadInt32(&c.skipped) == 1
}

func (c *Channel) doSkip(skipped bool) {
	if skipped {
		atomic.StoreInt32(&c.skipped, 1)
		if c.GetDelayedQueue() != nil {
			c.GetDelayedQueue().EmptyDelayedChannel(c.GetName())
		}
	} else {
		atomic.StoreInt32(&c.skipped, 0)
		c.TryRefreshChannelEnd()
	}
}

// When topic message is put, update the new end of the queue
func (c *Channel) UpdateQueueEnd(end BackendQueueEnd, forceReload bool) error {
	if end == nil {
		return nil
	}
	changed, err := c.backend.UpdateQueueEnd(end, forceReload)
	if !changed || err != nil {
		return err
	}

	if c.IsConsumeDisabled() {
	} else {
		select {
		case c.endUpdatedChan <- true:
		default:
		}
	}
	return err
}

// TouchMessage resets the timeout for an in-flight message
func (c *Channel) TouchMessage(clientID int64, id MessageID, clientMsgTimeout time.Duration) error {
	c.inFlightMutex.Lock()
	msg, ok := c.inFlightMessages[id]
	if !ok {
		c.inFlightMutex.Unlock()
		c.chLog.Logf("failed while touch: %v, msg not exist", id)
		return ErrMsgNotInFlight
	}
	if msg.GetClientID() != clientID {
		c.inFlightMutex.Unlock()
		return fmt.Errorf("client does not own message : %v vs %v",
			msg.GetClientID(), clientID)
	}
	newTimeout := time.Now().Add(clientMsgTimeout)
	if newTimeout.Sub(msg.deliveryTS) >=
		c.option.MaxMsgTimeout {
		// we would have gone over, set to the max
		newTimeout = msg.deliveryTS.Add(c.option.MaxMsgTimeout)
	}
	msg.pri = newTimeout.UnixNano()
	if msg.index != -1 {
		c.inFlightPQ.Remove(msg.index)
	}
	c.inFlightPQ.Push(msg)
	c.inFlightMutex.Unlock()
	return nil
}

func (c *Channel) ConfirmBackendQueueOnSlave(offset BackendOffset, cnt int64, allowBackward bool) error {
	if cnt == 0 && offset != 0 {
		c.chLog.LogWarningf("channel count is not valid: %v:%v. (This may happen while upgrade from old)", offset, cnt)
		return nil
	}
	// confirm on slave may exceed the current end, because the buffered write
	// may need to be flushed on slave.
	c.confirmMutex.Lock()
	defer c.confirmMutex.Unlock()
	var err error
	var newConfirmed BackendQueueEnd
	if offset < c.GetConfirmed().Offset() {
		if c.chLog.Level() > levellogger.LOG_DEBUG {
			c.chLog.LogDebugf("confirm offset less than current: %v, %v", offset, c.GetConfirmed())
		}
		if allowBackward {
			d, ok := c.backend.(*diskQueueReader)
			if ok {
				newConfirmed, err = d.ResetReadToOffset(offset, cnt)
				c.chLog.LogDebugf("channel reset to backward: %v", newConfirmed)
			}
		}
	} else {
		if allowBackward {
			d, ok := c.backend.(*diskQueueReader)
			if ok {
				newConfirmed, err = d.ResetReadToOffset(offset, cnt)
				c.chLog.LogDebugf("channel reset to backward: %v", newConfirmed)
			}
		} else {
			_, err = c.backend.SkipReadToOffset(offset, cnt)
			c.confirmedMsgs.DeleteLower(int64(offset))
			atomic.StoreInt32(&c.waitingConfirm, int32(c.confirmedMsgs.Len()))
		}
	}
	if err != nil {
		if err != ErrExiting {
			c.chLog.Logf("confirm read failed: %v, offset: %v", err, offset)
		}
	}

	return err
}

// if a message confirmed without goto inflight first, then we
// should clean the waiting state from requeue
func (c *Channel) ConfirmMsgWithoutGoInflight(msg *Message) (BackendOffset, int64, bool) {
	var offset BackendOffset
	var cnt int64
	var changed bool
	c.inFlightMutex.Lock()
	if msg.DelayedType == ChannelDelayed {
		offset, cnt, changed = c.ConfirmDelayedMessage(msg)
	} else {
		offset, cnt, changed = c.ConfirmBackendQueue(msg)
	}
	if _, ok := c.waitingRequeueChanMsgs[msg.ID]; ok {
		c.waitingRequeueChanMsgs[msg.ID] = nil
		delete(c.waitingRequeueChanMsgs, msg.ID)
	}
	c.inFlightMutex.Unlock()
	c.TryRefreshChannelEnd()
	c.ContinueConsumeForOrder()
	return offset, cnt, changed
}

// must be called to clean the state if this msg will be ignored after poped from requeue
// must not called if the msg will be delivery to inflight or will be confirmed later
func (c *Channel) cleanWaitingRequeueChan(msg *Message) {
	c.inFlightMutex.Lock()
	c.cleanWaitingRequeueChanNoLock(msg)
	c.inFlightMutex.Unlock()
}

func (c *Channel) cleanWaitingRequeueChanNoLock(msg *Message) {
	if _, ok := c.waitingRequeueChanMsgs[msg.ID]; ok {
		c.waitingRequeueChanMsgs[msg.ID] = nil
		delete(c.waitingRequeueChanMsgs, msg.ID)
	}
	if msg.DelayedType == ChannelDelayed {
		atomic.AddInt64(&c.deferredFromDelay, -1)
		if c.isTracedOrDebugTraceLog(msg) {
			nsqMsgTracer.TraceSub(c.GetTopicName(), c.GetName(), "CLEAN_DELAYED", msg.TraceID, msg, "", 0)
		}
	}
}

func (c *Channel) ConfirmDelayedMessage(msg *Message) (BackendOffset, int64, bool) {
	c.confirmMutex.Lock()
	defer c.confirmMutex.Unlock()
	needNotify := false
	curConfirm := c.GetConfirmed()
	if msg.DelayedOrigID > 0 && msg.DelayedType == ChannelDelayed && c.GetDelayedQueue() != nil {
		tn := time.Now()
		c.GetDelayedQueue().ConfirmedMessage(msg)
		cost := time.Since(tn)
		if cost > time.Second {
			nsqMsgTracer.TraceSub(c.GetTopicName(), c.GetName(), "FIN_DELAYED_SLOW", msg.TraceID, msg, "", cost.Nanoseconds())
		}
		c.delayedConfirmedMsgs[msg.ID] = *msg
		if c.isTracedOrDebugTraceLog(msg) {
			nsqMsgTracer.TraceSub(c.GetTopicName(), c.GetName(), "FIN_DELAYED", msg.TraceID, msg, "", cost.Nanoseconds())
		}
		if atomic.AddInt64(&c.deferredFromDelay, -1) <= 0 {
			c.nsqdNotify.NotifyScanChannel(c, false)
			needNotify = true
		}
	}
	return curConfirm.Offset(), curConfirm.TotalMsgCnt(), needNotify
}

// in order not to make the confirm map too large,
// we need handle this case: a old message is not confirmed,
// and we keep all the newer confirmed messages so we can confirm later.
// indicated weather the confirmed offset is changed
func (c *Channel) ConfirmBackendQueue(msg *Message) (BackendOffset, int64, bool) {
	c.confirmMutex.Lock()
	defer c.confirmMutex.Unlock()
	curConfirm := c.GetConfirmed()
	if msg.DelayedType == ChannelDelayed {
		c.chLog.Logf("should not confirm delayed here: %v", msg)
		return curConfirm.Offset(), curConfirm.TotalMsgCnt(), false
	}
	if msg.Offset < curConfirm.Offset() {
		c.chLog.LogDebugf("confirmed msg is less than current confirmed offset: %v-%v, %v", msg.ID, msg.Offset, curConfirm)
		return curConfirm.Offset(), curConfirm.TotalMsgCnt(), false
	}
	//c.confirmedMsgs[int64(msg.offset)] = msg
	mergedInterval := c.confirmedMsgs.AddOrMerge(&queueInterval{start: int64(msg.Offset),
		end:    int64(msg.Offset) + int64(msg.RawMoveSize),
		endCnt: uint64(msg.queueCntIndex),
	})
	reduced := false
	newConfirmed := curConfirm.Offset()
	confirmedCnt := curConfirm.TotalMsgCnt()
	if mergedInterval.End() <= int64(newConfirmed) {
		c.confirmedMsgs.DeleteLower(int64(newConfirmed))
	} else if mergedInterval.Start() <= int64(newConfirmed) {
		newConfirmed = BackendOffset(mergedInterval.End())
		confirmedCnt = int64(mergedInterval.EndCnt())
		reduced = true
	} else {
	}
	//atomic.StoreInt32(&c.waitingConfirm, int32(len(c.confirmedMsgs)))
	atomic.StoreInt32(&c.waitingConfirm, int32(c.confirmedMsgs.Len()))
	if reduced {
		err := c.backend.ConfirmRead(newConfirmed, confirmedCnt)
		if err != nil {
			if err != ErrExiting {
				c.chLog.LogWarningf("channel confirm read failed: %v, msg: %v",
					err, msg.ID)
				// rollback removed confirmed messages
				//for _, m := range c.tmpRemovedConfirmed {
				//	c.confirmedMsgs[int64(msg.offset)] = m
				//}
				//atomic.StoreInt32(&c.waitingConfirm, int32(len(c.confirmedMsgs)))
			}
			return curConfirm.Offset(), curConfirm.TotalMsgCnt(), reduced
		} else {
			if c.chLog.Level() >= levellogger.LOG_DETAIL {
				c.chLog.Debugf("channel merge msg %v( %v) to interval %v, confirmed to %v",
					msg.Offset, msg.queueCntIndex, mergedInterval, newConfirmed)
			}
			c.confirmedMsgs.DeleteLower(int64(newConfirmed))
			atomic.StoreInt32(&c.waitingConfirm, int32(c.confirmedMsgs.Len()))
		}
		if int64(c.confirmedMsgs.Len()) < c.option.MaxConfirmWin/2 &&
			atomic.LoadInt32(&c.needNotifyRead) == 1 &&
			!c.IsOrdered() {
			select {
			case c.tryReadBackend <- true:
			default:
			}
		}
	}
	if c.chLog.Level() >= levellogger.LOG_DEBUG && int64(c.confirmedMsgs.Len()) > c.option.MaxConfirmWin {
		curConfirm = c.GetConfirmed()
		flightCnt := len(c.inFlightMessages)
		if flightCnt == 0 {
			c.chLog.LogDebugf("lots of confirmed messages : %v, %v, %v",
				c.confirmedMsgs.Len(), curConfirm, flightCnt)
		}
	}
	return newConfirmed, confirmedCnt, reduced
	// TODO: if some messages lost while re-queue, it may happen that some messages not
	// in inflight queue and also not wait confirm. In this way, we need reset
	// backend queue to force read the data from disk again.
}

func (c *Channel) ShouldWaitDelayed(msg *Message) bool {
	if c.IsOrdered() || c.IsEphemeral() {
		return false
	}
	// while there are some waiting confirmed messages and some disk delayed messages, if we
	// switched leader, we reset the read offset to the oldest confirmed. This will cause lots of
	// messages read from normal disk queue but these messages should be delayed.
	dq := c.GetDelayedQueue()
	if msg.DelayedOrigID > 0 && msg.DelayedType == ChannelDelayed && dq != nil {
		return false
	}
	// check if this is in delayed queue
	// (it may happen while the reader is reset to the confirmed for leader changed or other event trigger)
	if dq != nil {
		if dq.IsChannelMessageDelayed(msg.ID, c.GetName()) {
			if c.isTracedOrDebugTraceLog(msg) {
				c.chLog.LogDebugf("non-delayed msg %v should be delayed since in delayed queue", msg)
				nsqMsgTracer.TraceSub(c.GetTopicName(), c.GetName(), "IGNORE_DELAY_CONFIRMED", msg.TraceID, msg, "", 0)
			}
			return true
		}
	}

	return false
}

func (c *Channel) IsConfirmed(msg *Message) bool {
	if msg.DelayedOrigID > 0 && msg.DelayedType == ChannelDelayed && c.GetDelayedQueue() != nil {
		return false
	}
	c.confirmMutex.Lock()
	ok := c.confirmedMsgs.IsCompleteOverlap(&queueInterval{start: int64(msg.Offset),
		end:    int64(msg.Offset) + int64(msg.RawMoveSize),
		endCnt: uint64(msg.queueCntIndex)})
	c.confirmMutex.Unlock()

	if ok {
		if c.isTracedOrDebugTraceLog(msg) {
			c.chLog.LogDebugf("msg %v is already confirmed", msg)
			nsqMsgTracer.TraceSub(c.GetTopicName(), c.GetName(), "IGNORE_CONFIRMED", msg.TraceID, msg, "", 0)
		}
	}
	return ok
}

func (c *Channel) FinishMessage(clientID int64, clientAddr string,
	id MessageID) (BackendOffset, int64, bool, *Message, error) {
	return c.internalFinishMessage(clientID, clientAddr, id, false)
}

func (c *Channel) FinishMessageForce(clientID int64, clientAddr string,
	id MessageID, forceFin bool) (BackendOffset, int64, bool, *Message, error) {
	if forceFin {
		c.chLog.Logf("force finish msg %v", id)
	}
	return c.internalFinishMessage(clientID, clientAddr, id, forceFin)
}

// FinishMessage successfully discards an in-flight message
func (c *Channel) internalFinishMessage(clientID int64, clientAddr string,
	id MessageID, forceFin bool) (BackendOffset, int64, bool, *Message, error) {
	now := time.Now()
	c.inFlightMutex.Lock()
	defer c.inFlightMutex.Unlock()
	if forceFin {
		oldMsg, ok := c.inFlightMessages[id]
		if ok {
			clientID = oldMsg.GetClientID()
		}
	}
	msg, err := c.popInFlightMessage(clientID, id, true)
	if err != nil {
		c.chLog.LogDebugf("channel message %v fin error: %v from client %v", id, err,
			clientID)
		return 0, 0, false, nil, err
	}
	ackCost := now.UnixNano() - msg.deliveryTS.UnixNano()
	isOldDeferred := msg.IsDeferred()
	if msg.TraceID != 0 || c.IsTraced() || c.chLog.Level() >= levellogger.LOG_DETAIL {
		// if fin by no client address, means fin by internal delayed queue or by http api
		if clientAddr != "" {
			nsqMsgTracer.TraceSub(c.GetTopicName(), c.GetName(), "FIN", msg.TraceID, msg, clientAddr, ackCost)
		} else {
			nsqMsgTracer.TraceSub(c.GetTopicName(), c.GetName(), "FIN_INTERNAL", msg.TraceID, msg, clientAddr, ackCost)
		}
	}
	if c.e2eProcessingLatencyStream != nil {
		c.e2eProcessingLatencyStream.Insert(msg.Timestamp)
	}
	if ackCost >= time.Second.Nanoseconds() &&
		(c.isTracedOrDebugTraceLog(msg) || c.IsSlowTraced()) {
		if clientAddr != "" {
			nsqMsgTracer.TraceSub(c.GetTopicName(), c.GetName(), "SLOW_ACK", msg.TraceID, msg, clientAddr, ackCost)
		}
	}
	c.channelStatsInfo.UpdateDelivery2ACKStats(ackCost / int64(time.Millisecond))
	c.channelStatsInfo.UpdateChannelStats((now.UnixNano() - msg.Timestamp) / int64(time.Millisecond))
	var offset BackendOffset
	var cnt int64
	var changed bool

	// confirm should be no error, since the inflight has been poped
	if msg.DelayedType == ChannelDelayed {
		offset, cnt, changed = c.ConfirmDelayedMessage(msg)
	} else {
		offset, cnt, changed = c.ConfirmBackendQueue(msg)
	}
	if msg.belongedConsumer != nil {
		if clientAddr != "" {
			msg.belongedConsumer.FinishedMessage()
		} else {
			msg.belongedConsumer.RequeuedMessage()
		}
		msg.belongedConsumer = nil
	}

	if isOldDeferred {
		atomic.AddInt64(&c.deferredCount, -1)
		atomic.StoreInt32(&msg.deferredCnt, 0)
		if clientAddr != "" {
			// delayed message should be requeued and then send to client
			// if some client finish delayed message directly, something may be wrong.
			c.chLog.Infof("channel delayed msg %v finished by client %v ",
				msg, clientAddr)
		}
		if c.chLog.Level() >= levellogger.LOG_DEBUG {
			if clientAddr == "" {
				c.chLog.Debugf("channel delay msg %v finished by force ",
					msg)
			}
		}
	}
	newDeferCnt := atomic.LoadInt64(&c.deferredCount)
	if (int64(len(c.inFlightMessages))-newDeferCnt <= 0) && len(c.requeuedMsgChan) == 0 && c.IsWaitingMoreDiskData() {
		c.moreDataCallback(c)
	}
	return offset, cnt, changed, msg, nil
}

// if some message is skipped, we should try refresh channel end
// to get more possible new messages, since the end will only be updated when new message come in first time
func (c *Channel) TryRefreshChannelEnd() {
	if c.IsWaitingMoreDiskData() {
		c.moreDataCallback(c)
	}
}

func (c *Channel) ContinueConsumeForOrder() {
	if c.IsOrdered() && atomic.LoadInt32(&c.needNotifyRead) == 1 {
		select {
		case c.tryReadBackend <- true:
		default:
		}
	}
}

func (c *Channel) isTooMuchDeferredInMem(deCnt int64) bool {
	// if requeued by deferred is more than half of the all messages handled,
	// it may be a bug in client which can not handle any more, so we just wait
	// timeout not requeue to defer
	cnt := c.GetChannelWaitingConfirmCnt()
	if cnt >= c.option.MaxConfirmWin && float64(deCnt) > float64(cnt)*0.5 {
		c.chLog.Debugf("too much delayed in memory: %v vs %v", deCnt, cnt)
		return true
	}
	return false
}

func (c *Channel) ShouldRequeueToEnd(clientID int64, clientAddr string, id MessageID,
	timeout time.Duration, byClient bool) (*Message, bool) {
	if !byClient {
		return nil, false
	}
	if c.IsOrdered() || c.IsEphemeral() {
		return nil, false
	}

	c.inFlightMutex.Lock()
	defer c.inFlightMutex.Unlock()
	// change the timeout for inflight
	msg, ok := c.inFlightMessages[id]
	if !ok {
		return nil, false
	}
	if msg.GetClientID() != clientID || msg.IsDeferred() {
		return nil, false
	}

	return c.checkMsgRequeueToEnd(msg, timeout)
}

func (c *Channel) checkMsgRequeueToEnd(msg *Message,
	timeout time.Duration) (*Message, bool) {
	id := msg.ID
	threshold := time.Minute
	if c.option.ReqToEndThreshold >= time.Millisecond {
		threshold = c.option.ReqToEndThreshold
	}
	if c.chLog.Level() >= levellogger.LOG_DEBUG || c.IsTraced() {
		c.chLog.Logf("check requeue to end, timeout:%v, msg timestamp:%v, depth ts:%v, msg attempt:%v, waiting :%v",
			timeout, msg.Timestamp,
			c.DepthTimestamp(), msg.Attempts(), atomic.LoadInt32(&c.waitingConfirm))
	}

	// too much delayed queue will impact the read/write on boltdb, so we refuse to write if too large
	dqCnt := int64(c.GetDelayedQueueCnt())
	if dqCnt > c.option.MaxChannelDelayedQNum*10 {
		return nil, false
	}
	tn := time.Now()
	// check if delayed queue is blocked too much, and try increase delay and put this
	// delayed message to delay-queue again.
	if msg.DelayedType == ChannelDelayed {
		//to avoid some delayed messages req again and again (bad for boltdb performance )
		// we should control the req timeout for some failed message with too much attempts
		// and this may cause some delay more time than expected.
		dqDepthTs, dqCnt := c.GetDelayedQueueConsumedState()
		blockingTooLong := tn.UnixNano()-dqDepthTs > 10*threshold.Nanoseconds()
		waitingDelayCnt := atomic.LoadInt64(&c.deferredFromDelay)
		if (blockingTooLong || (msg.Attempts() < MaxMemReqTimes*10)) && (tn.UnixNano()-atomic.LoadInt64(&c.lastDelayedReqToEndTs) > delayedReqToEndMinInterval.Nanoseconds()) {
			// if the message is peeked from disk delayed queue,
			// we can try to put it back to end if there are some other
			// delayed queue messages waiting.
			// mostly, delayed depth ts should be in future, if it is less than now,
			// means there are some others delayed timeout need to be peeked immediately.

			// waitingDelayCnt is the counter for memory waiting from delay diskqueue,
			// dqCnt is all the delayed diskqueue counter, so
			// if all delayed messages are in memory, we no need to put them back to disk.
			blocking := tn.UnixNano()-dqDepthTs > threshold.Nanoseconds()
			if dqDepthTs > 0 && blocking && int64(dqCnt) > waitingDelayCnt && int64(dqCnt) > MaxWaitingDelayed {
				if c.isTracedOrDebugTraceLog(msg) {
					c.chLog.Logf("delayed queue message %v req to end, timestamp:%v, attempt:%v, delayed depth timestamp: %v, delay waiting : %v, %v",
						id, msg.Timestamp,
						msg.Attempts(), dqDepthTs, waitingDelayCnt, dqCnt)
				}
				atomic.StoreInt64(&c.lastDelayedReqToEndTs, tn.UnixNano())
				return msg.GetCopy(), true
			}
		}
		if c.isTracedOrDebugTraceLog(msg) {
			c.chLog.Logf("check delayed queue message %v req to end, timestamp:%v, depth ts:%v, attempt:%v, delay waiting :%v, delayed depth timestamp: %v, cnt: %v",
				id, msg.Timestamp,
				c.DepthTimestamp(), msg.Attempts(), waitingDelayCnt, dqDepthTs, dqCnt)
		}
		// the ChannelDelayed message already in delayed queue, so we can ignore other checks
		return nil, false
	}

	msgAtp := msg.Attempts()
	depDiffTs := tn.UnixNano() - c.DepthTimestamp()
	if msgAtp >= MaxAttempts-1 {
		if (c.Depth() > c.option.MaxConfirmWin) ||
			depDiffTs > time.Hour.Nanoseconds() {
			return msg.GetCopy(), true
		}
	}

	newTimeout := tn.Add(timeout)
	if newTimeout.Sub(msg.deliveryTS) >=
		c.option.MaxReqTimeout || timeout > threshold {
		if c.option.MaxChannelDelayedQNum == 0 || int64(dqCnt) < c.option.MaxChannelDelayedQNum {
			return msg.GetCopy(), true
		}
	}

	deCnt := atomic.LoadInt64(&c.deferredCount)
	if (msgAtp > MaxMemReqTimes*10) &&
		(c.Depth() > MaxDepthReqToEnd) &&
		(c.DepthTimestamp() > msg.Timestamp+time.Hour.Nanoseconds()) &&
		!c.isTooMuchDeferredInMem(deCnt) {
		// too much deferred means most messages are requeued, to avoid too much in disk delay queue,
		// we just ignore requeue.
		c.chLog.Logf("msg %v req to end, attemptted %v and created at %v, current processing %v",
			id, msgAtp, msg.Timestamp, c.DepthTimestamp())
		return msg.GetCopy(), true
	}

	if (deCnt >= c.option.MaxConfirmWin) &&
		(timeout > threshold/2) {
		// if requeued by deferred is more than half of the all messages handled,
		// it may be a bug in client which can not handle any more, so we just wait
		// timeout not requeue to defer
		cnt := c.GetChannelWaitingConfirmCnt()
		if cnt >= c.option.MaxConfirmWin && float64(deCnt) <= float64(cnt)*0.5 {
			c.chLog.Logf("requeue msg to end %v, since too much delayed in memory: %v vs %v", id, deCnt, cnt)
			return msg.GetCopy(), true
		}
	}

	isBlocking := atomic.LoadInt32(&c.waitingConfirm) >= int32(c.option.MaxConfirmWin)
	if isBlocking {
		if timeout > threshold/2 || (timeout > 2*time.Minute) {
			return msg.GetCopy(), true
		}

		if msgAtp <= MaxMemReqTimes/2 {
			return nil, false
		}

		// check if req this message can avoid the blocking
		if msg.Timestamp > c.DepthTimestamp()+threshold.Nanoseconds() {
			return nil, false
		}

		if msgAtp > MaxMemReqTimes && depDiffTs > threshold.Nanoseconds() {
			return msg.GetCopy(), true
		}
		if depDiffTs > 2*threshold.Nanoseconds() && timeout > time.Second {
			return msg.GetCopy(), true
		}
		return nil, false
	} else {
		if msg.Timestamp > c.DepthTimestamp()+threshold.Nanoseconds()/10 {
			return nil, false
		}
		if msgAtp < MaxMemReqTimes {
			return nil, false
		}

		if depDiffTs < threshold.Nanoseconds() {
			// the newest confirmed is near now, we
			// check if the message is too old and depth is large
			// to avoid re-consume too old messages if leader changed
			if msg.Timestamp < c.DepthTimestamp()-10*threshold.Nanoseconds() &&
				c.Depth() > MaxDepthReqToEnd {
				return msg.GetCopy(), true
			}
			return nil, false
		}
		if msgAtp > MaxMemReqTimes*10 && msg.Timestamp <= c.DepthTimestamp() && depDiffTs > 2*threshold.Nanoseconds() && c.Depth() > 100 {
			return msg.GetCopy(), true
		}
		if c.Depth() < MaxDepthReqToEnd/10 {
			return nil, false
		}
		// avoid req to end if req is for rate limit for consuming
		if timeout < threshold/2 {
			return nil, false
		}
		// req to end since depth is large and the depth timestamp is also old
		return msg.GetCopy(), true
	}
}

// RequeueMessage requeues a message based on `time.Duration`, ie:
//
// `timeoutMs` == 0 - requeue a message immediately
// `timeoutMs`  > 0 - asynchronously wait for the specified timeout
//     and requeue a message
//
func (c *Channel) RequeueMessage(clientID int64, clientAddr string, id MessageID, timeout time.Duration, byClient bool) error {
	c.inFlightMutex.Lock()
	defer c.inFlightMutex.Unlock()
	msg, ok := c.inFlightMessages[id]
	if !ok {
		c.chLog.LogDebugf("failed requeue for delay: %v, msg not exist", id)
		return ErrMsgNotInFlight
	}
	if timeout == 0 {
		// we should avoid too frequence 0 req if by client
		if msg.Attempts() >= MaxAttempts/2 && byClient {
			return ErrMsgTooMuchReq
		}
		if msg.Attempts() > MaxMemReqTimes*10 && byClient && time.Now().Sub(msg.deliveryTS) < time.Second/10 {
			// avoid too short req for the message
			return ErrMsgTooMuchReq
		}

		// remove from inflight first
		msg, err := c.popInFlightMessage(clientID, id, false)
		if err != nil {
			c.chLog.LogDebugf("channel message %v requeue error: %v from client %v", id, err,
				clientID)
			return err
		}
		// requeue by intend should treat as not fail attempt
		if msg.Attempts() > 0 && !byClient {
			msg.IncrAttempts(-1)
		}
		if msg.belongedConsumer != nil {
			msg.belongedConsumer.RequeuedMessage()
			msg.belongedConsumer = nil
		}
		return c.doRequeue(msg, clientAddr)
	}
	// one message should not defer again before the old defer timeout
	if msg.IsDeferred() {
		return ErrMsgDeferred
	}

	if msg.GetClientID() != clientID {
		c.chLog.LogDebugf("failed requeue for client not own message: %v: %v vs %v", id, msg.GetClientID(), clientID)
		return fmt.Errorf("client does not own message %v: %v vs %v", id,
			msg.GetClientID(), clientID)
	}
	// change the timeout for inflight
	newTimeout := time.Now().Add(timeout)
	if msg.Attempts() > MaxMemReqTimes*10 && newTimeout.Sub(msg.deliveryTS) < time.Second {
		// avoid too short req for the message
		c.chLog.LogDebugf("too short req %v, %v, %v, %v, %v",
			newTimeout, msg.deliveryTS, timeout, id, msg.Attempts())
		newTimeout = newTimeout.Add(time.Second)
	}
	if (timeout > c.option.ReqToEndThreshold) ||
		(newTimeout.Sub(msg.deliveryTS) >=
			c.option.MaxReqTimeout) {
		c.chLog.LogDebugf("too long timeout %v, %v, %v, should req message: %v to delayed queue",
			newTimeout, msg.deliveryTS, timeout, id)
	}
	reqWaiting := len(c.requeuedMsgChan)
	deCnt := atomic.LoadInt64(&c.deferredCount)
	if c.isTooMuchDeferredInMem(deCnt) {
		if newTimeout.UnixNano() >= msg.pri {
			c.chLog.Logf("failed to requeue msg %v, since too much delayed in memory: %v", id, deCnt)
			return ErrMsgDeferredTooMuch
		}
	}
	if int64(atomic.LoadInt32(&c.waitingConfirm)) > c.option.MaxConfirmWin {
		// too much req, we only allow early req than timeout to speed up retry
		if newTimeout.UnixNano() >= msg.pri {
			c.chLog.Logf("failed to requeue msg %v, %v since too much waiting confirmed: %v, req waiting: %v",
				id, msg.Attempts(), atomic.LoadInt32(&c.waitingConfirm), reqWaiting)
			return ErrMsgDeferredTooMuch
		}
	}

	atomic.AddInt64(&c.deferredCount, 1)
	msg.pri = newTimeout.UnixNano()
	atomic.AddInt32(&msg.deferredCnt, 1)

	if c.isTracedOrDebugTraceLog(msg) {
		nsqMsgTracer.TraceSub(c.GetTopicName(), c.GetName(), "REQ_DEFER", msg.TraceID, msg, clientAddr, 0)
	}

	// defered message do not belong to any client
	if msg.belongedConsumer != nil {
		msg.belongedConsumer.RequeuedMessage()
	}
	msg.belongedConsumer = nil
	if msg.index != -1 {
		c.inFlightPQ.Remove(msg.index)
	}
	c.inFlightPQ.Push(msg)

	c.chLog.LogDebugf("client %v requeue with delayed %v message: %v", clientID, timeout, id)
	return nil
}

func (c *Channel) RequeueClientMessages(clientID int64, clientAddr string) {
	if c.Exiting() {
		return
	}
	if c.IsConsumeDisabled() {
		return
	}
	idList := make([]MessageID, 0)
	c.inFlightMutex.Lock()
	for id, msg := range c.inFlightMessages {
		if msg.GetClientID() == clientID {
			idList = append(idList, id)
		}
	}
	c.inFlightMutex.Unlock()
	for _, id := range idList {
		c.RequeueMessage(clientID, clientAddr, id, 0, false)
	}
	if len(idList) > 0 {
		c.chLog.Logf("client: %v requeued %v messages ",
			clientID, len(idList))
	}
}

func (c *Channel) GetClientsCount() int {
	c.RLock()
	defer c.RUnlock()
	return len(c.clients)
}

func (c *Channel) GetClients() map[int64]Consumer {
	c.RLock()
	defer c.RUnlock()

	results := make(map[int64]Consumer)
	for k, c := range c.clients {
		results[k] = c
	}
	return results
}

func (c *Channel) SetClientLimitedRdy(clientAddr string, rdy int) {
	c.RLock()
	defer c.RUnlock()
	for _, client := range c.clients {
		if strings.HasPrefix(client.String(), clientAddr) {
			client.SetLimitedRdy(rdy)
		}
	}
}

func (c *Channel) AddClient(clientID int64, client Consumer) error {
	c.Lock()
	defer c.Unlock()

	if c.IsConsumeDisabled() {
		return ErrConsumeDisabled
	}
	_, ok := c.clients[clientID]
	if ok {
		return nil
	}
	c.clients[clientID] = client
	return nil
}

// RemoveClient removes a client from the Channel's client list
func (c *Channel) RemoveClient(clientID int64, clientTag string) {

	if clientTag != "" {
		c.RemoveTagClientMsgChannel(clientTag)
	}

	c.Lock()
	defer c.Unlock()

	_, ok := c.clients[clientID]
	if !ok {
		return
	}
	c.clients[clientID] = nil
	delete(c.clients, clientID)

	if len(c.clients) == 0 && c.ephemeral == true {
		go c.deleter.Do(func() { c.deleteCallback(c) })
	}
}

func (c *Channel) StartInFlightTimeout(msg *Message, client Consumer, clientAddr string, timeout time.Duration) (bool, error) {
	now := time.Now()
	old, err := c.pushInFlightMessage(msg, client, now, now.Add(timeout).UnixNano())
	shouldSend := true
	if err != nil {
		if old != nil && old.IsDeferred() {
			shouldSend = false
		} else if old != nil && old.DelayedType == ChannelDelayed {
			shouldSend = false
		} else {
			c.chLog.LogWarningf("push message in flight failed: %v, %v, old: %v", err,
				PrintMessageNoBody(msg), PrintMessageNoBody(old))
		}
		return shouldSend, err
	}

	if msg.TraceID != 0 || c.IsTraced() || c.chLog.Level() >= levellogger.LOG_DETAIL {
		nsqMsgTracer.TraceSub(c.GetTopicName(), c.GetName(), "START", msg.TraceID, msg, clientAddr, now.UnixNano()-msg.Timestamp)
	}

	return shouldSend, nil
}

func (c *Channel) GetInflightNum() int {
	c.inFlightMutex.Lock()
	n := len(c.inFlightMessages)
	c.inFlightMutex.Unlock()
	return n
}

func (c *Channel) UpdateConfirmedInterval(intervals []MsgQueueInterval) {
	c.confirmMutex.Lock()
	defer c.confirmMutex.Unlock()
	if c.chLog.Level() >= levellogger.LOG_DETAIL {
		c.chLog.Logf("update confirmed interval, before: %v", c.confirmedMsgs.ToString())
	}
	if c.confirmedMsgs.Len() != 0 {
		c.confirmedMsgs = NewIntervalSkipList()
	}
	for _, qi := range intervals {
		c.confirmedMsgs.AddOrMerge(&queueInterval{start: qi.Start, end: qi.End, endCnt: qi.EndCnt})
	}
	if c.chLog.Level() >= levellogger.LOG_DETAIL {
		c.chLog.Logf("update confirmed interval, after: %v", c.confirmedMsgs.ToString())
	}
}

func (c *Channel) GetConfirmedIntervalLen() int {
	c.confirmMutex.Lock()
	l := c.confirmedMsgs.Len()
	c.confirmMutex.Unlock()
	return l
}

func (c *Channel) GetConfirmedInterval() []MsgQueueInterval {
	c.confirmMutex.Lock()
	ret := c.confirmedMsgs.ToIntervalList()
	c.confirmMutex.Unlock()
	return ret
}

func (c *Channel) GetConfirmed() BackendQueueEnd {
	return c.backend.GetQueueConfirmed()
}

func (c *Channel) GetChannelEnd() BackendQueueEnd {
	return c.backend.GetQueueReadEnd()
}

//message count from queue end to queue read end
func (c *Channel) GetChannelNotReadCnt() int64 {
	d, ok := c.backend.(*diskQueueReader)
	if ok {
		return d.GetQueueReadEnd().TotalMsgCnt() - d.GetQueueCurrentRead().TotalMsgCnt()
	}
	return 0
}

func (c *Channel) GetChannelWaitingConfirmCnt() int64 {
	d, ok := c.backend.(*diskQueueReader)
	if ok {
		return d.GetQueueCurrentRead().TotalMsgCnt() - d.GetQueueConfirmed().TotalMsgCnt()
	}
	return 0
}

// doRequeue performs the low level operations to requeue a message
// should protect by inflight lock
func (c *Channel) doRequeue(m *Message, clientAddr string) error {
	if c.Exiting() {
		return ErrExiting
	}
	ChannelRequeuedCnt.With(prometheus.Labels{
		"topic":     c.GetTopicName(),
		"partition": strconv.Itoa(c.GetTopicPart()),
		"channel":   c.GetName(),
	}).Inc()
	atomic.AddUint64(&c.requeueCount, 1)
	if c.isTracedOrDebugTraceLog(m) {
		nsqMsgTracer.TraceSub(c.GetTopicName(), c.GetName(), "REQ", m.TraceID, m, clientAddr, 0)
	}
	select {
	case <-c.exitChan:
		c.chLog.Logf("requeue message failed for existing: %v ", m.ID)
		return ErrExiting
	case c.requeuedMsgChan <- m:
		c.waitingRequeueChanMsgs[m.ID] = m
	default:
		c.waitingRequeueMsgs[m.ID] = m
	}
	return nil
}

// pushInFlightMessage atomically adds a message to the in-flight dictionary
func (c *Channel) pushInFlightMessage(msg *Message, client Consumer, now time.Time, pri int64) (*Message, error) {
	c.inFlightMutex.Lock()
	defer c.inFlightMutex.Unlock()
	msg.belongedConsumer = client
	msg.deliveryTS = now
	msg.pri = pri
	if msg.Attempts() < MaxAttempts {
		msg.IncrAttempts(1)
	}
	if c.IsConsumeDisabled() {
		// we should clean req message if it is not go into inflight, if not
		// we leave a orphen message not in requeue chan and not in inflight
		c.cleanWaitingRequeueChanNoLock(msg)
		return nil, ErrConsumeDisabled
	}
	oldm, ok := c.inFlightMessages[msg.ID]
	if ok {
		// It may conflict if both normal diskqueue and the delayed queue has the same message (because of the rpc timeout)
		// We need check and clean the state to make sure we can have the right stats
		c.cleanWaitingRequeueChanNoLock(msg)
		// Since the belongedConsumer on the new msg is not actually used for sending, so we do not need handle it
		// and the old message will continue be handled in next peek
		if msg.DelayedType == 0 {
			if oldm.DelayedType == ChannelDelayed {
				c.ConfirmBackendQueue(msg)
				nsqMsgTracer.TraceSub(c.GetTopicName(), c.GetName(), "IGNORE_DELAY_CONFIRMED", msg.TraceID, msg, "", 0)
			}
			c.chLog.Infof("non-delayed msg %v conflict while add inflight, old msg type: %v", msg, oldm.DelayedType)
		} else if msg.DelayedType == ChannelDelayed {
			c.chLog.Infof("delayed msg %v conflict while add inflight", msg)
		}
		return oldm, ErrMsgAlreadyInFlight
	}
	c.inFlightMessages[msg.ID] = msg
	c.inFlightPQ.Push(msg)
	if _, ok := c.waitingRequeueChanMsgs[msg.ID]; ok {
		c.waitingRequeueChanMsgs[msg.ID] = nil
		delete(c.waitingRequeueChanMsgs, msg.ID)
	}
	return nil, nil
}

// popInFlightMessage atomically removes a message from the in-flight dictionary
func (c *Channel) popInFlightMessage(clientID int64, id MessageID, force bool) (*Message, error) {
	msg, ok := c.inFlightMessages[id]
	if !ok {
		return nil, ErrMsgNotInFlight
	}
	if msg.GetClientID() != clientID {
		return nil, fmt.Errorf("client does not own message : %v vs %v",
			msg.GetClientID(), clientID)
	}
	if !force && msg.IsDeferred() {
		c.chLog.Logf("channel should never pop a deferred message here unless the timeout : %v", msg.ID)
		return nil, ErrMsgDeferred
	}
	c.inFlightMessages[id] = nil
	delete(c.inFlightMessages, id)
	if msg.index != -1 {
		c.inFlightPQ.Remove(msg.index)
	}
	return msg, nil
}

func (c *Channel) IsConsumeDisabled() bool {
	return atomic.LoadInt32(&c.consumeDisabled) == 1
}

func (c *Channel) DisableConsume(disable bool) {
	c.Lock()
	defer c.Unlock()
	if disable {
		if !atomic.CompareAndSwapInt32(&c.consumeDisabled, 0, 1) {
			return
		}
		c.chLog.Logf("channel %v disabled for consume", c.name)
		for cid, client := range c.clients {
			client.Exit()
			delete(c.clients, cid)
		}
		needClearConfirm := false
		if c.IsOrdered() {
			needClearConfirm = true
		}
		c.drainChannelWaiting(needClearConfirm, nil, nil)
		if c.ephemeral {
			go c.deleter.Do(func() { c.deleteCallback(c) })
		}
	} else {
		if !atomic.CompareAndSwapInt32(&c.consumeDisabled, 1, 0) {
			select {
			case c.tryReadBackend <- true:
			default:
			}
		} else {
			c.chLog.Logf("channel %v enabled for consume", c.name)
			// we need reset backend read position to confirm position
			// since we dropped all inflight and requeue data while disable consume.
			if c.chLog.Level() >= levellogger.LOG_DEBUG {
				c.confirmMutex.Lock()
				c.chLog.Logf("confirmed interval while enable: %v", c.confirmedMsgs.ToString())
				c.confirmMutex.Unlock()
			}

			done := false
			for !done {
				select {
				case m, ok := <-c.clientMsgChan:
					if !ok {
						done = true
						break
					}
					c.chLog.Logf("ignored a read message %v at offset %v while enable consume", m.ID, m.Offset)
					c.cleanWaitingRequeueChan(m)
				case m := <-c.requeuedMsgChan:
					c.chLog.Logf("ignored a requeued message %v at offset %v while enable consume", m.ID, m.Offset)
					c.cleanWaitingRequeueChan(m)
				default:
					done = true
				}
			}
			select {
			case c.readerChanged <- resetChannelData{BackendOffset(-1), 0, false}:
			default:
			}
		}
	}
	c.nsqdNotify.NotifyStateChanged(c, false)
}

// should not drain outside of the messagepump loop
// if drain outside loop, readerChanged channel should be triggered
// TODO: check the counter for deferredFromDelay, should make sure not concurrent with client ack
func (c *Channel) drainChannelWaiting(clearConfirmed bool, lastDataNeedRead *bool, origReadChan chan ReadResult) error {
	// skipped channel will reset to end period, so just ignore log
	if !c.IsSkipped() {
		c.chLog.Logf("draining channel waiting %v", clearConfirmed)
	}
	c.inFlightMutex.Lock()
	defer c.inFlightMutex.Unlock()
	c.initPQ()
	c.confirmMutex.Lock()
	if clearConfirmed {
		c.confirmedMsgs = NewIntervalSkipList()
		atomic.StoreInt32(&c.waitingConfirm, 0)
	} else {
		curConfirm := c.GetConfirmed()
		c.confirmedMsgs.DeleteLower(int64(curConfirm.Offset()))
		atomic.StoreInt32(&c.waitingConfirm, int32(c.confirmedMsgs.Len()))
		c.chLog.Logf("current confirmed interval %v ", c.confirmedMsgs.ToString())
	}
	c.confirmMutex.Unlock()
	atomic.StoreInt64(&c.waitingProcessMsgTs, 0)

	if c.Exiting() {
		return nil
	}

	done := false
	clientMsgChan := c.clientMsgChan
	for !done {
		select {
		case m, ok := <-clientMsgChan:
			if !ok {
				clientMsgChan = nil
				continue
			}
			c.chLog.Logf("ignored a read message %v (%v) at Offset %v while drain channel", m.ID, m.DelayedType, m.Offset)
			c.cleanWaitingRequeueChanNoLock(m)
		case m := <-c.requeuedMsgChan:
			c.chLog.Logf("ignored a message %v (%v) at Offset %v while drain channel", m.ID, m.DelayedType, m.Offset)
			c.cleanWaitingRequeueChanNoLock(m)
		default:
			done = true
		}
	}
	reqCnt := len(c.waitingRequeueMsgs)
	delayed := 0
	for k, m := range c.waitingRequeueMsgs {
		if m.DelayedType == ChannelDelayed {
			delayed++
		}
		c.waitingRequeueMsgs[k] = nil
		delete(c.waitingRequeueMsgs, k)
	}

	// skipped channel will reset to end period, so just ignore log
	if !c.IsSkipped() {
		c.chLog.Logf("drained channel waiting req %v, %v, delay: %v, %v", reqCnt,
			len(c.waitingRequeueChanMsgs),
			atomic.LoadInt64(&c.deferredFromDelay), delayed)
	}
	// should in inFlightMutex to avoid concurrent with confirming from client
	// we can not set it to 0, because there may be a message read out from req chan but still not
	// begin start inflight.
	atomic.AddInt64(&c.deferredFromDelay, -1*int64(delayed))

	if lastDataNeedRead != nil {
		*lastDataNeedRead = false
	}
	// since the reader is reset, we should drain the previous data.
	select {
	case <-origReadChan:
	default:
	}

	return nil
}

func (c *Channel) TryWakeupRead() {
	if c.IsConsumeDisabled() {
		return
	}
	if c.IsOrdered() || c.IsEphemeral() {
		return
	}
	select {
	case c.tryReadBackend <- true:
	default:
	}
	if c.chLog.Level() >= levellogger.LOG_DETAIL {
		c.chLog.LogDebugf("channel consume try wakeup : %v", c.name)
	}
}

func (c *Channel) resetReaderToConfirmed() error {
	atomic.StoreInt64(&c.waitingProcessMsgTs, 0)
	atomic.StoreInt32(&c.needResetReader, 0)
	confirmed, err := c.backend.ResetReadToConfirmed()
	if err != nil {
		c.chLog.LogWarningf("channel reset read to confirmed error: %v", err)
		return err
	}
	c.chLog.Logf("reset channel reader to confirm: %v", confirmed)
	return nil
}

func (c *Channel) resetChannelReader(resetOffset resetChannelData, lastDataNeedRead *bool, origReadChan chan ReadResult,
	lastMsg *Message, needReadBackend *bool, readBackendWait *bool) {
	var err error
	if resetOffset.Offset == BackendOffset(-1) {
		if resetOffset.ClearConfirmed {
			atomic.StoreInt32(&c.needResetReader, 2)
		} else {
			atomic.StoreInt32(&c.needResetReader, 1)
		}
	} else {
		d := c.backend.(*diskQueueReader)
		_, err = d.ResetReadToOffset(resetOffset.Offset, resetOffset.Cnt)
		if err != nil {
			c.chLog.Warningf("failed to reset reader to %v, %v", resetOffset, err)
		} else {
			c.drainChannelWaiting(true, lastDataNeedRead, origReadChan)
			*lastMsg = Message{}
			// skipped channel will reset to end period, so just ignore log
			if !c.IsSkipped() {
				c.chLog.Infof("reset reader to %v", resetOffset)
			}
		}
		*needReadBackend = true
		*readBackendWait = false
	}
}

func (c *Channel) TryFixConfirmedByResetRead() error {
	if c.IsSkipped() || c.IsConsumeDisabled() {
		return nil
	}
	if c.IsPaused() || c.IsOrdered() {
		return nil
	}
	c.chLog.Warningf("channel try fix confirmed by reset %v, waiting %v, %v",
		c.GetConfirmed(), c.GetChannelWaitingConfirmCnt(), c.GetChannelDebugStats())
	select {
	case c.readerChanged <- resetChannelData{BackendOffset(-1), 0, false}:
	default:
	}
	return nil
}

func (c *Channel) CheckIfNeedResetRead() bool {
	if c.IsSkipped() || c.IsConsumeDisabled() {
		return false
	}
	if c.IsPaused() || c.IsOrdered() {
		return false
	}
	if c.GetClientsCount() <= 0 {
		return false
	}
	c.inFlightMutex.Lock()
	inflightCnt := len(c.inFlightMessages)
	inflightCnt += len(c.waitingRequeueMsgs)
	inflightCnt += len(c.waitingRequeueChanMsgs)
	inflightCnt += len(c.requeuedMsgChan)
	inflightCnt += int(atomic.LoadInt64(&c.deferredCount))
	c.inFlightMutex.Unlock()
	if inflightCnt <= 0 && c.Depth() > 10 &&
		c.GetChannelWaitingConfirmCnt() >= c.option.MaxConfirmWin {
		c.chLog.Warningf("channel has depth %v but no inflight, confirmed %v, waiting %v, %v",
			c.Depth(), c.GetConfirmed(), c.GetChannelWaitingConfirmCnt(), c.GetChannelDebugStats())
		return true
	}
	return false
}

// messagePump reads messages from either memory or backend and sends
// messages to clients over a go chan
func (c *Channel) messagePump() {
	var msg *Message
	var data ReadResult
	var err error
	var lastMsg Message
	var lastDataResult ReadResult
	isSkipped := false
	origReadChan := make(chan ReadResult, 1)
	var readChan <-chan ReadResult
	var waitEndUpdated chan bool

	maxWin := int32(c.option.MaxConfirmWin)
	resumedFirst := true
	d := c.backend
	needReadBackend := true
	lastDataNeedRead := false
	readBackendWait := false
	backendErr := 0
LOOP:
	for {
		// do an extra check for closed exit before we select on all the memory/backend/exitChan
		// this solves the case where we are closed and something else is draining clientMsgChan into
		// backend. we don't want to reverse that
		if atomic.LoadInt32(&c.exitFlag) == 1 {
			goto exit
		}

		if c.IsExt() {
			// clean notify to avoid block notify while get/remove tag channel
			select {
			case <-c.tagChanInitChan:
			case <-c.tagChanRemovedChan:
			default:
			}
		}

		resetReaderFlag := atomic.LoadInt32(&c.needResetReader)
		deCnt := atomic.LoadInt64(&c.deferredCount)
		if resetReaderFlag > 0 {
			c.chLog.Infof("reset the reader to confirmed: %v", c.GetConfirmed())
			err = c.resetReaderToConfirmed()
			// if reset failed, we should not drain the waiting data
			if err == nil {
				needClearConfirm := false
				if atomic.LoadInt32(&c.waitingConfirm) > maxWin {
					c.inFlightMutex.Lock()
					inflightCnt := len(c.inFlightMessages)
					inflightCnt += len(c.waitingRequeueMsgs)
					inflightCnt += len(c.waitingRequeueChanMsgs)
					c.inFlightMutex.Unlock()
					if inflightCnt <= 0 {
						c.chLog.Warningf("reset need clear confirmed since no inflight")
						needClearConfirm = true
					}
				}
				if c.IsOrdered() || resetReaderFlag == 2 {
					needClearConfirm = true
				}
				c.drainChannelWaiting(needClearConfirm, &lastDataNeedRead, origReadChan)
				lastMsg = Message{}
			}
			readChan = origReadChan
			needReadBackend = true
			readBackendWait = false
		} else if readBackendWait {
			readChan = nil
			needReadBackend = false
		} else if atomic.LoadInt32(&c.waitingConfirm) > maxWin ||
			c.isTooMuchDeferredInMem(deCnt) {
			if c.chLog.Level() >= levellogger.LOG_DEBUG {
				c.chLog.LogDebugf("channel reader is holding: %v, %v,  mem defer: %v",
					atomic.LoadInt32(&c.waitingConfirm),
					c.GetConfirmed(), deCnt)
			}
			atomic.StoreInt32(&c.needNotifyRead, 1)

			readChan = nil
			needReadBackend = false

			c.inFlightMutex.Lock()
			inflightCnt := len(c.inFlightMessages)
			inflightCnt += len(c.waitingRequeueMsgs)
			inflightCnt += len(c.waitingRequeueChanMsgs)
			c.inFlightMutex.Unlock()
			if inflightCnt <= 0 {
				c.chLog.Warningf("many confirmed but no inflight: %v",
					atomic.LoadInt32(&c.waitingConfirm))
			}
		} else {
			readChan = origReadChan
			needReadBackend = true
		}

		if c.IsConsumeDisabled() {
			readChan = nil
			needReadBackend = false
			c.chLog.Logf("channel consume is disabled : %v", c.name)
			if lastMsg.ID > 0 {
				c.chLog.Logf("consume disabled at last read message: %v:%v", lastMsg.ID, lastMsg.Offset)
				lastMsg = Message{}
			}
		}
		if c.IsSkipped() {
			readChan = nil
			needReadBackend = false
		}

		if needReadBackend {
			if !lastDataNeedRead {
				dataRead, hasData := d.TryReadOne()
				if hasData {
					atomic.StoreInt64(&c.lastQueueReadTs, time.Now().Unix())
					lastDataNeedRead = true
					origReadChan <- dataRead
					readChan = origReadChan
					waitEndUpdated = nil
				} else {
					readChan = nil
					waitEndUpdated = c.endUpdatedChan
					if c.moreDataCallback != nil {
						c.moreDataCallback(c)
					}
				}
			} else {
				readChan = origReadChan
				waitEndUpdated = nil
			}
		} else {
			waitEndUpdated = nil
		}

		atomic.StoreInt32(&c.waitingDeliveryState, 0)
		// read from requeue chan first to avoid disk data blocking requeue chan
		select {
		case msg = <-c.requeuedMsgChan:
			if msg.TraceID != 0 || c.IsTraced() || c.chLog.Level() >= levellogger.LOG_DETAIL {
				c.chLog.LogDebugf("read message %v from requeue", msg.ID)
				nsqMsgTracer.TraceSub(c.GetTopicName(), c.GetName(), "READ_REQ", msg.TraceID, msg, "0", time.Now().UnixNano()-msg.Timestamp)
			}
		default:
			// notify to refill requeue chan if any waiting
			c.inFlightMutex.Lock()
			waitingReq := len(c.waitingRequeueMsgs)
			c.inFlightMutex.Unlock()
			if waitingReq > 0 {
				notified := c.nsqdNotify.NotifyScanChannel(c, false)
				if !notified {
					// try later, avoid too much
					c.nsqdNotify.PushTopicJob(c.GetTopicName(), func() {
						c.nsqdNotify.NotifyScanChannel(c, true)
						c.chLog.LogDebugf("notify refill req done from requeue")
					})
				} else {
					c.chLog.LogDebugf("notify refill req done from requeue")
				}
			}
			select {
			case <-c.exitChan:
				goto exit
			case msg = <-c.requeuedMsgChan:
				if msg.TraceID != 0 || c.IsTraced() || c.chLog.Level() >= levellogger.LOG_DETAIL {
					c.chLog.LogDebugf("read message %v from requeue", msg.ID)
					nsqMsgTracer.TraceSub(c.GetTopicName(), c.GetName(), "READ_REQ", msg.TraceID, msg, "0", time.Now().UnixNano()-msg.Timestamp)
				}
			case data = <-readChan:
				lastDataNeedRead = false
				if data.Err != nil {
					c.chLog.Errorf("channel failed to read message - %s", data.Err)
					if data.Err == ErrReadQueueCountMissing {
						time.Sleep(time.Second)
					} else {
						// TODO: fix corrupt file from other replica.
						// and should handle the confirm offset, since some skipped data
						// may never be confirmed any more
						if backendErr > 10 {
							_, skipErr := c.backend.(*diskQueueReader).SkipToNext()
							if skipErr != nil {
							}
							c.chLog.Errorf("channel skip to next because of backend error: %v", backendErr)
							isSkipped = true
							backendErr = 0
						} else {
							backendErr++
							time.Sleep(time.Second)
							// it may happen the read position for (filenum, fileoffset) is invalid (the segment file is changed or cleaned)
							// but the virtual offset is still valid, so we try fix this
							// it will be fixed while updating the channel end.
						}
					}
					time.Sleep(time.Millisecond * 100)
					continue LOOP
				}
				if backendErr > 0 {
					c.chLog.Infof("channel backend error auto recovery: %v", backendErr)
				}
				backendErr = 0
				msg, err = decodeMessage(data.Data, c.IsExt())
				if err != nil {
					c.chLog.Errorf("channel failed to decode message - %s - %v", err, data)
					continue LOOP
				}
				msg.Offset = data.Offset
				msg.RawMoveSize = data.MovedSize
				msg.queueCntIndex = data.CurCnt
				if msg.TraceID != 0 || c.IsTraced() || c.chLog.Level() >= levellogger.LOG_DETAIL {
					nsqMsgTracer.TraceSub(c.GetTopicName(), c.GetName(), "READ_QUEUE", msg.TraceID, msg, "0", time.Now().UnixNano()-msg.Timestamp)
				}

				if lastMsg.ID > 0 && msg.ID < lastMsg.ID {
					// note: this may happen if the reader pefetch some data not committed by the disk writer
					// we need read it again later.
					c.chLog.Warningf("read a message with less message ID: %v vs %v, raw data: %v", msg.ID, lastMsg.ID, data)
					c.chLog.Warningf("last raw data: %v", lastDataResult)
					time.Sleep(time.Millisecond * 5)
					if diskQ, ok := c.backend.(*diskQueueReader); ok {
						diskQ.ResetLastReadOne(data.Offset, data.CurCnt-1, int32(data.MovedSize))
					}
					lastMsg = *msg
					lastDataResult = data
					continue LOOP
				}

				atomic.StoreInt64(&c.waitingProcessMsgTs, msg.Timestamp)
				lastDataResult = data
				if isSkipped {
					// TODO: store the skipped info to retry error if possible.
					c.chLog.LogWarningf("channel skipped message from %v:%v to the : %v:%v",
						lastMsg.ID, lastMsg.Offset, msg.ID, msg.Offset)
				}
				if resumedFirst {
					if c.chLog.Level() > levellogger.LOG_DEBUG || c.IsTraced() {
						c.chLog.LogDebugf("channel resumed first message %v at Offset: %v", msg.ID, msg.Offset)
					}
					resumedFirst = false
				}
				lastMsg = *msg
				isSkipped = false
			case <-c.tryReadBackend:
				atomic.StoreInt32(&c.needNotifyRead, 0)
				readBackendWait = false
				resumedFirst = true
				continue LOOP
			case resetOffset := <-c.readerChanged:
				// skipped channel will reset to end period, so just ignore log
				if !c.IsSkipped() {
					c.chLog.Infof("got reader reset notify:%v ", resetOffset)
				}
				c.resetChannelReader(resetOffset, &lastDataNeedRead, origReadChan, &lastMsg, &needReadBackend, &readBackendWait)
				continue LOOP
			case <-waitEndUpdated:
				continue LOOP
			}
		}

		if msg == nil {
			continue
		}

		if c.IsConsumeDisabled() {
			c.cleanWaitingRequeueChan(msg)
			continue
		}
		if c.IsOrdered() {
			curConfirm := c.GetConfirmed()
			if msg.Offset != curConfirm.Offset() {
				c.chLog.Infof("read a message not in ordered: %v, %v", msg.Offset, curConfirm)
				atomic.StoreInt32(&c.needResetReader, 2)
				continue
			}
		}

		//let timer sync to update backend in replicas' channels
		needSkip := c.IsSkipped()
		isZanTestSkip := false
		if !needSkip {
			isZanTestSkip = c.shouldSkipZanTest(msg)
			needSkip = isZanTestSkip
		}
		if needSkip || c.shouldAckForIgnore(msg) {
			c.ConfirmMsgWithoutGoInflight(msg)
			continue LOOP
		}

		atomic.StoreInt32(&c.waitingDeliveryState, 1)
		//atomic.StoreInt32(&msg.deferredCnt, 0)
		if c.IsOrdered() {
			atomic.StoreInt32(&c.needNotifyRead, 1)
			readBackendWait = true
		}

		var msgTag string
		var extParsed bool
		if c.IsExt() {
			// clean notify to avoid block notify while get/remove tag channel
			select {
			case <-c.tagChanInitChan:
			case <-c.tagChanRemovedChan:
			default:
			}
		}

	tagMsgLoop:
		needParseExt := !extParsed && c.isNeedParseMsgExt()
		if needParseExt {
			//deliver according to tag value in message
			msgTag, err = parseTagIfAny(msg)
			if err != nil {
				c.chLog.Errorf("error parse tag from message %v, offset %v, ext data: %s, err: %v",
					msg.ID, msg.Offset, msg.ExtBytes, err.Error())
			}
			extParsed = true
		}

		if msgTag != "" {
			tagMsgChan, chanExist := c.GetClientTagMsgChan(msgTag)
			if chanExist {
				select {
				case tagMsgChan <- msg:
					msg = nil
					continue LOOP
				case <-c.tagChanRemovedChan:
					//do not go to msgDefaultLoop, as tag chan remove event may invoked from previously deleted client
					goto tagMsgLoop
				case <-c.exitChan:
					goto exit
				}
			}
		}

	msgDefaultLoop:
		// for large message, check if we need limit the network bandwidth for this channel
		if len(msg.Body) > limitSmallMsgBytes {
			now := time.Now()
			re := c.limiter.ReserveN(now, len(msg.Body)/limitSmallMsgBytes)
			if re.OK() {
				du := re.DelayFrom(now)
				if du > 0 {
					if du > time.Second {
						du = time.Second
					}
					ChannelRateLimitCnt.With(prometheus.Labels{
						"topic":   c.GetTopicName(),
						"channel": c.GetName(),
					}).Inc()
					// we allow small exceed to avoid sleep too often in short time
					if du > time.Millisecond {
						time.Sleep(du)
					}
				}
			}
		}
		select {
		case newTag := <-c.tagChanInitChan:
			if !extParsed || newTag == msgTag {
				c.chLog.Infof("client with tag %v initialized, try deliver in tag loop", newTag)
				goto tagMsgLoop
			} else {
				goto msgDefaultLoop
			}
		case c.clientMsgChan <- msg:
		case resetOffset := <-c.readerChanged:
			c.chLog.Infof("got reader reset notify while dispatch message:%v ", resetOffset)
			c.resetChannelReader(resetOffset, &lastDataNeedRead, origReadChan, &lastMsg, &needReadBackend, &readBackendWait)
			//retry delivering msg to client, regardless reset result
			goto msgDefaultLoop
		case <-c.exitChan:
			goto exit
		}

		msg = nil
		// the client will call back to mark as in-flight w/ its info
	}

exit:
	c.chLog.Logf("CHANNEL closing ... messagePump")
	close(c.clientMsgChan)
	close(c.exitSyncChan)
}

func (c *Channel) shouldAckForIgnore(msg *Message) bool {
	if c.ackRetryCnt == 0 || c.ackOldThanTime == 0 {
		return false
	}
	if (msg.Attempts() > uint16(c.ackRetryCnt)) && (msg.Timestamp < time.Now().UnixNano()-c.ackOldThanTime.Nanoseconds()) {
		nsqMsgTracer.TraceSub(c.GetTopicName(), c.GetName(), "ACK_AUTO_RETRY_TOO_LONG", msg.TraceID, msg, "", 0)
		ChannelAutoAckCnt.With(prometheus.Labels{
			"topic":   c.GetTopicName(),
			"channel": c.GetName(),
			"reason":  "ACK_AUTO_RETRY_TOO_LONG",
		}).Inc()
		return true
	}
	return false
}

func (c *Channel) shouldSkipZanTest(msg *Message) bool {
	if c.IsZanTestSkipped() && msg.ExtVer == ext.JSON_HEADER_EXT_VER {
		//check if zan_test header contained in json header
		extHeader, err := NewJsonExt(msg.ExtBytes)
		if err != nil {
			return false
		}
		zanTest, _ := extHeader.GetBoolOrStringBool(ext.ZAN_TEST_KEY)
		return zanTest
	}
	return false
}

func parseTagIfAny(msg *Message) (string, error) {
	var msgTag string
	var err error
	switch msg.ExtVer {
	case ext.TAG_EXT_VER:
		msgTag = string(msg.ExtBytes)
	case ext.JSON_HEADER_EXT_VER:
		var extHeader IJsonExt
		extHeader, err = NewJsonExt(msg.ExtBytes)
		if err == nil {
			msgTag, _ = extHeader.GetString(ext.CLIENT_DISPATCH_TAG_KEY)
		} else if len(msg.ExtBytes) == 0 {
			err = nil
			msgTag = ""
		}
	}
	return msgTag, err
}

func (c *Channel) GetChannelDebugStats() string {
	c.inFlightMutex.Lock()
	inFlightCount := len(c.inFlightMessages)
	debugStr := fmt.Sprintf("topic %v channel %v \ninflight %v, req %v, %v, %v, deferred: %v , %v, ",
		c.GetTopicName(), c.GetName(), inFlightCount, len(c.waitingRequeueMsgs),
		len(c.requeuedMsgChan), len(c.waitingRequeueChanMsgs), atomic.LoadInt64(&c.deferredCount), atomic.LoadInt64(&c.deferredFromDelay))

	if c.chLog.Level() >= levellogger.LOG_DEBUG || c.IsTraced() {
		for _, msg := range c.inFlightMessages {
			debugStr += fmt.Sprintf("%v(offset: %v, cnt: %v), %v,", msg.ID, msg.Offset,
				msg.queueCntIndex, msg.DelayedType)
		}
	}
	debugStr += "\n"
	if len(c.requeuedMsgChan) != len(c.waitingRequeueChanMsgs) {
		debugStr += "requeue mismatch: "
		for _, msg := range c.waitingRequeueChanMsgs {
			debugStr += fmt.Sprintf("%v(%v),", msg.ID, msg.Offset)
		}
	}
	c.inFlightMutex.Unlock()
	debugStr += "\n"
	d, ok := c.backend.(*diskQueueReader)
	var curRead BackendQueueEnd
	if ok {
		curRead = d.GetQueueCurrentRead()
	}
	c.confirmMutex.Lock()
	debugStr += fmt.Sprintf("channel end : %v,current read:%v, current confirm %v, confirmed %v messages: %s\n",
		c.GetChannelEnd(), curRead,
		c.GetConfirmed(), c.confirmedMsgs.Len(), c.confirmedMsgs.ToString())
	c.confirmMutex.Unlock()
	debugStr += "\n"
	return debugStr
}

func (c *Channel) CheckIfTimeoutToomuch(msg *Message, msgTimeout time.Duration) {
	toCnt := atomic.LoadInt32(&msg.timedoutCnt)
	// check is it a normal slow or a unnormal message caused timeout too much times
	// In order to avoid timeout blocking normal, we check and req it to delayed queue
	if toCnt > maxTimeoutCntToReq && !c.IsEphemeral() && !c.IsOrdered() {
		tnow := time.Now().UnixNano()
		if tnow-c.DepthTimestamp() > timeoutBlockingWait.Nanoseconds() {
			c.inFlightMutex.Lock()
			defer c.inFlightMutex.Unlock()
			nmsg, ok := c.checkMsgRequeueToEnd(msg, msgTimeout)
			if ok {
				if c.isTracedOrDebugTraceLog(msg) {
					c.chLog.Logf("msg %v timeout too much, requeue to end: %v", msg.ID, toCnt)
				}
				c.nsqdNotify.ReqToEnd(c, nmsg, msgTimeout*10)
			}
		}
	}
}

func (c *Channel) doPeekInFlightQueue(tnow int64) (bool, bool) {
	dirty := false
	flightCnt := 0
	requeuedCnt := 0
	for {
		c.inFlightMutex.Lock()
		if c.IsConsumeDisabled() {
			c.inFlightMutex.Unlock()
			goto exit
		}

		msg, _ := c.inFlightPQ.PeekAndShift(tnow)
		flightCnt = len(c.inFlightMessages)
		if msg == nil {
			// TODO: also check if delayed queue is blocked too much, and try increase delay and put this
			// delayed message to delay-queue again.
			if atomic.LoadInt32(&c.waitingConfirm) > 1 || flightCnt > 1 {
				c.chLog.LogDebugf("tnow %v channel no timeout, inflight %v, waiting confirm: %v, confirmed: %v",
					tnow, flightCnt, atomic.LoadInt32(&c.waitingConfirm),
					c.GetConfirmed())
				canReqEnd := tnow-atomic.LoadInt64(&c.lastDelayedReqToEndTs) > delayedReqToEndMinInterval.Nanoseconds()
				if !c.IsEphemeral() && !c.IsOrdered() && atomic.LoadInt32(&c.waitingConfirm) >= int32(c.option.MaxConfirmWin) && canReqEnd {
					confirmed := c.GetConfirmed().Offset()
					var blockingMsg *Message
					for _, m := range c.inFlightMessages {
						if m.Offset != confirmed {
							continue
						}
						threshold := time.Minute
						if c.option.ReqToEndThreshold >= time.Millisecond {
							threshold = c.option.ReqToEndThreshold
						}
						if m.Attempts() >= MaxMemReqTimes {
							// if the blocking message still need waiting too long,
							// we requeue to end or just timeout it immediately
							if m.pri > time.Now().Add(threshold/2).UnixNano() {
								blockingMsg = m
							} else if tnow-c.DepthTimestamp() > 2*int64(threshold) {
								// check if blocking too long time
								blockingMsg = m
							}
						}
						break
					}
					if blockingMsg != nil {
						c.chLog.Logf("msg %v is blocking confirm, requeue to end, inflight %v, waiting confirm: %v, confirmed: %v",
							PrintMessage(blockingMsg),
							flightCnt, atomic.LoadInt32(&c.waitingConfirm), confirmed)
						toEnd := true
						deCnt := atomic.LoadInt64(&c.deferredCount)
						if c.isTooMuchDeferredInMem(deCnt) {
							c.chLog.Logf("too much delayed in memory: %v", deCnt)
							toEnd = false
						}
						if toEnd {
							copyMsg := blockingMsg.GetCopy()
							c.nsqdNotify.ReqToEnd(c, copyMsg, time.Duration(copyMsg.pri-time.Now().UnixNano()))
						}
					}
				}
			}
			c.inFlightMutex.Unlock()
			goto exit
		}
		dirty = true

		_, ok := c.inFlightMessages[msg.ID]
		if !ok {
			c.inFlightMutex.Unlock()
			goto exit
		}
		c.inFlightMessages[msg.ID] = nil
		delete(c.inFlightMessages, msg.ID)
		// note: if this message is deferred by client, we treat it as a delay message,
		// so we consider it is by demanded to delay not timeout of message.
		if msg.IsDeferred() {
			atomic.AddInt64(&c.deferredCount, -1)
		} else {
			atomic.AddUint64(&c.timeoutCount, 1)
			ChannelTimeoutCnt.With(prometheus.Labels{
				"topic":     c.GetTopicName(),
				"partition": strconv.Itoa(c.GetTopicPart()),
				"channel":   c.GetName(),
			}).Inc()
			atomic.AddInt32(&msg.timedoutCnt, 1)
		}
		if msg.Attempts() >= MaxAttempts {
			ChannelRetryToomuchCnt.With(prometheus.Labels{
				"topic":   c.GetTopicName(),
				"channel": c.GetName(),
			}).Inc()
		}
		client := msg.belongedConsumer
		if msg.belongedConsumer != nil {
			msg.belongedConsumer.TimedOutMessage()
			msg.belongedConsumer = nil
		}
		requeuedCnt++
		msgCopy := *msg
		atomic.StoreInt32(&msg.deferredCnt, 0)
		c.doRequeue(msg, strconv.Itoa(int(msg.GetClientID())))
		c.inFlightMutex.Unlock()

		if msgCopy.TraceID != 0 || c.IsTraced() || c.chLog.Level() >= levellogger.LOG_INFO {
			clientAddr := ""
			if client != nil {
				clientAddr = client.String()
			}
			cost := tnow - msgCopy.deliveryTS.UnixNano()
			if msgCopy.IsDeferred() {
				c.chLog.LogDebugf("msg %v defer timeout, expect at %v ",
					msgCopy.ID, msgCopy.pri)
				if c.isTracedOrDebugTraceLog(&msgCopy) || msgCopy.Attempts() > 5 {
					nsqMsgTracer.TraceSub(c.GetTopicName(), c.GetName(), "DELAY_TIMEOUT", msgCopy.TraceID, &msgCopy, clientAddr, cost)
				}
			} else {
				nsqMsgTracer.TraceSub(c.GetTopicName(), c.GetName(), "TIMEOUT", msgCopy.TraceID, &msgCopy, clientAddr, cost)
			}
		}
	}

exit:
	// try requeue the messages that waiting.
	stopScan := false
	c.inFlightMutex.Lock()
	reqLen := len(c.requeuedMsgChan) + len(c.waitingRequeueMsgs)
	if !c.IsConsumeDisabled() {
		if len(c.waitingRequeueMsgs) > 1 {
			c.chLog.LogDebugf("channel requeue waiting messages: %v", len(c.waitingRequeueMsgs))
		}

		for k, m := range c.waitingRequeueMsgs {
			select {
			case c.requeuedMsgChan <- m:
				c.waitingRequeueMsgs[k] = nil
				delete(c.waitingRequeueMsgs, k)
				c.waitingRequeueChanMsgs[m.ID] = m
				requeuedCnt++
			default:
				stopScan = true
				// we need check next time soon to refill while the requeue chan pop empty
			}
			if stopScan {
				break
			}
		}
	}
	c.inFlightMutex.Unlock()
	isInflightEmpty := (flightCnt == 0) && (reqLen == 0) && (requeuedCnt <= 0)
	return dirty, isInflightEmpty
}

func (c *Channel) processInFlightQueue(tnow int64) (bool, bool) {
	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()

	if c.Exiting() {
		return false, false
	}

	dirty, _ := c.doPeekInFlightQueue(tnow)
	c.RLock()
	clientNum := len(c.clients)
	c.RUnlock()
	checkFast := false
	waitingDelayCnt := atomic.LoadInt64(&c.deferredFromDelay)
	if waitingDelayCnt < 0 {
		c.chLog.Logf("delayed waiting count error %v ", waitingDelayCnt)
	}
	// Since all messages in delayed queue stored in sorted by delayed timestamp,
	// all old unconfirmed messages in delayed queue will be peeked at each time.
	// So peek is no need if last peeked batch is not all confirmed.
	// However, some of them may retry too long time in memory.
	// If we do not confirm them soon, it may block too long time for some messages in delayed queue.
	// So we need requeue them to the end of the delayed queue again (while req command received) if blocking too long time.
	needPeekDelay := waitingDelayCnt <= 0

	if !c.IsConsumeDisabled() && !c.IsOrdered() &&
		needPeekDelay && clientNum > 0 {
		newAdded, cnt, err := c.peekAndReqDelayedMessages(tnow)
		if err == nil {
			if newAdded > 0 && c.chLog.Level() >= levellogger.LOG_DEBUG {
				c.chLog.LogDebugf("channel delayed waiting peeked %v added %v new : %v",
					cnt, newAdded, waitingDelayCnt)
			}
		}
	}
	if waitingDelayCnt > 0 && clientNum > 0 {
		if c.chLog.Level() >= levellogger.LOG_DEBUG {
			c.chLog.LogDebugf("channel delayed waiting : %v", waitingDelayCnt)
		}
		c.inFlightMutex.Lock()
		allWaiting := len(c.inFlightMessages) + len(c.waitingRequeueChanMsgs) + len(c.waitingRequeueMsgs)
		c.inFlightMutex.Unlock()
		if waitingDelayCnt > int64(allWaiting) {
			c.chLog.Logf("channel delayed waiting : %v, more than all waiting delivery: %v", waitingDelayCnt, allWaiting)
		}
	}
	// for tagged client, it may happen if no any un-tagged client.
	// we may read to end, but the last message is normal message.
	// in this way, we should block and wait un-tagged client.
	// However, wait un-tagged client should have inflight or requeue waiting, and
	// should waiting delivery state
	// another new case: while a lot of confirmed messages go to confirmed without go into inflight (sampled or already confirmed),
	// we should handle this case to avoid reset the channel. In this case we need check if current read pos is changed.
	if (!dirty) && (atomic.LoadInt32(&c.waitingDeliveryState) == 0) && c.CheckIfNeedResetRead() && (tnow-atomic.LoadInt64(&c.lastQueueReadTs) > resetReaderTimeoutSec) {
		diff := tnow - atomic.LoadInt64(&c.processResetReaderTime)
		if diff > resetReaderTimeoutSec && atomic.LoadInt64(&c.processResetReaderTime) > 0 {
			c.chLog.LogWarningf("try reset reader since no inflight and requeued for too long (%v): %v, %v, %v",
				diff,
				atomic.LoadInt32(&c.waitingConfirm), c.GetConfirmed(), c.GetChannelDebugStats())

			atomic.StoreInt64(&c.processResetReaderTime, tnow)
			select {
			case c.readerChanged <- resetChannelData{BackendOffset(-1), 0, false}:
			default:
			}
		}
	} else {
		atomic.StoreInt64(&c.processResetReaderTime, tnow)
	}

	return dirty, checkFast
}

func (c *Channel) peekAndReqDelayedMessages(tnow int64) (int, int, error) {
	if c.IsEphemeral() {
		return 0, 0, nil
	}
	delayedQueue := c.GetDelayedQueue()
	if delayedQueue == nil {
		return 0, 0, nil
	}
	newAdded := 0
	peekedMsgs := peekBufPoolGet()
	defer peekBufPoolPut(peekedMsgs)
	cnt, err := delayedQueue.PeekRecentChannelTimeout(tnow, peekedMsgs, c.GetName())
	if err != nil {
		return newAdded, 0, err
	}
	c.inFlightMutex.Lock()
	defer c.inFlightMutex.Unlock()
	if c.IsConsumeDisabled() {
		return 0, 0, nil
	}
	for _, tmpMsg := range peekedMsgs[:cnt] {
		m := tmpMsg
		c.confirmMutex.Lock()
		oldMsg, cok := c.delayedConfirmedMsgs[m.DelayedOrigID]
		c.confirmMutex.Unlock()
		if cok {
			// avoid to pop some recently confirmed delayed messages, avoid possible duplicate
			// the delayed confirmed message may be requeue to end again
			// so we check if the delayed ts is the same.
			if m.DelayedTs != oldMsg.DelayedTs {
				c.chLog.LogDebugf("delayed message already confirmed with different ts %v, %v ", PrintMessageNoBody(&m), oldMsg.DelayedTs)
			} else {
				c.chLog.Logf("delayed message already confirmed %v ", m)
			}
		} else {
			// while reset reader, the message may reload from disk which not in inflight or requeue
			// and it will add to the inflight from startinflight
			oldMsg2, ok2 := c.inFlightMessages[m.DelayedOrigID]
			_, ok3 := c.waitingRequeueChanMsgs[m.DelayedOrigID]
			_, ok4 := c.waitingRequeueMsgs[m.DelayedOrigID]
			// it may happen the delayed message is synced to slave but the
			// consume offset is not synced yet (since it is async), and then
			// the leader changed to the slave. The new leader will read the
			// old message from disk queue and put into the inflight queue.
			// The inflight may be conflicted with the delayed and the attempts will be
			// lost. we need handle this.
			if ok2 {
				// the delayed orig message id in delayed message is the actual id in the topic queue
				if oldMsg2.ID != m.DelayedOrigID || oldMsg2.DelayedTs != m.DelayedTs {
					c.chLog.Debugf("old msg %v in flight mismatch peek from delayed queue, new %v ",
						oldMsg2, m)
					if oldMsg2.ID == m.DelayedOrigID &&
						oldMsg2.DelayedChannel == "" &&
						oldMsg2.DelayedOrigID == 0 && oldMsg2.DelayedTs == 0 &&
						oldMsg2.DelayedType == 0 {
						// this inflight message is caused by leader changed, and the
						// new leader read from disk queue (which will be treated as non delayed message)
						if bytes.Equal(oldMsg2.Body, m.Body) {
							// just fin it
							// we do not handle inflight remove here, it will be handled while timeout and will check confirmed before send to client
							c.chLog.LogDebugf("old msg %v in flight confirmed since in delayed queue",
								PrintMessage(oldMsg2))
							c.ConfirmBackendQueue(oldMsg2)
						}
					}
				}
			} else if ok3 || ok4 {
				// already waiting requeue
				c.chLog.LogDebugf("delayed waiting in requeued %v ", m)
			} else {
				tmpID := m.ID
				m.ID = m.DelayedOrigID
				m.DelayedOrigID = tmpID

				if tnow > m.DelayedTs+int64(c.option.QueueScanInterval*2) {
					c.chLog.LogDebugf("delayed is too late now %v for message: %v",
						tnow, m)
				}
				if tnow < m.DelayedTs {
					c.chLog.LogDebugf("delayed is too early now %v for message: %v",
						tnow, m)
				}

				nsqMsgTracer.TraceSub(c.GetTopicName(), c.GetName(), "DELAY_QUEUE_TIMEOUT", m.TraceID, &m, "", tnow-m.Timestamp)

				newAdded++
				if m.belongedConsumer != nil {
					m.belongedConsumer.RequeuedMessage()
					m.belongedConsumer = nil
				}

				atomic.AddInt64(&c.deferredFromDelay, 1)

				atomic.StoreInt32(&m.deferredCnt, 0)
				c.doRequeue(&m, "")
			}
		}
	}
	c.confirmMutex.Lock()
	c.delayedConfirmedMsgs = make(map[MessageID]Message, MaxWaitingDelayed)
	c.confirmMutex.Unlock()
	return newAdded, cnt, nil
}

func (c *Channel) GetDelayedQueueConsumedDetails() (int64, RecentKeyList, map[int]uint64, map[string]uint64) {
	dq := c.GetDelayedQueue()
	if dq == nil {
		return 0, nil, nil, nil
	}
	ts := time.Now().UnixNano()
	kl, cntList, chList := dq.GetOldestConsumedState([]string{c.GetName()}, false)
	return ts, kl, cntList, chList
}

func (c *Channel) GetDelayedQueueCnt() uint64 {
	dqCnt := uint64(0)
	dq := c.GetDelayedQueue()
	if dq == nil {
		return dqCnt
	}
	dqCnt, _ = dq.GetCurrentDelayedCnt(ChannelDelayed, c.GetName())
	return dqCnt
}

func (c *Channel) GetDelayedQueueConsumedState() (int64, uint64) {
	var recentTs int64
	dqCnt := uint64(0)
	dq := c.GetDelayedQueue()
	if dq == nil {
		return recentTs, dqCnt
	}

	recentList, _, chCntList := dq.GetOldestConsumedState([]string{c.GetName()}, false)
	if len(recentList) > 0 {
		for _, k := range recentList {
			_, ts, _, ch, err := decodeDelayedMsgDBKey(k)
			if err != nil || ch != c.GetName() {
				continue
			}
			recentTs = ts
			break
		}
	}
	if len(chCntList) > 0 {
		dqCnt, _ = chCntList[c.GetName()]
	}
	return recentTs, dqCnt
}

func (c *Channel) GetMemDelayedMsgs() []MessageID {
	idList := make([]MessageID, 0)
	c.inFlightMutex.Lock()
	for _, msg := range c.inFlightMessages {
		if msg.IsDeferred() {
			idList = append(idList, msg.ID)
		}
	}
	c.inFlightMutex.Unlock()
	return idList
}

func (c *Channel) isTracedOrDebugTraceLog(msg *Message) bool {
	if msg.TraceID != 0 || c.IsTraced() || c.chLog.Level() >= levellogger.LOG_DEBUG {
		return true
	}
	return false
}
