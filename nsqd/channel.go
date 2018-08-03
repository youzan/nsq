package nsqd

import (
	"errors"
	"fmt"
	"math"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/youzan/nsq/internal/protocol"

	simpleJson "github.com/bitly/go-simplejson"
	"github.com/youzan/nsq/internal/ext"
	"github.com/youzan/nsq/internal/levellogger"
	"github.com/youzan/nsq/internal/quantile"
)

const (
	resetReaderTimeoutSec = 10
	MAX_MEM_REQ_TIMES     = 10
	MaxWaitingDelayed     = 100
)

var (
	ErrMsgNotInFlight                 = errors.New("Message ID not in flight")
	ErrMsgDeferredTooMuch             = errors.New("Too much deferred messages in flight")
	ErrMsgAlreadyInFlight             = errors.New("Message ID already in flight")
	ErrConsumeDisabled                = errors.New("Consume is disabled currently")
	ErrMsgDeferred                    = errors.New("Message is deferred")
	ErrSetConsumeOffsetNotFirstClient = errors.New("consume offset can only be changed by the first consume client")
	ErrNotDiskQueueReader             = errors.New("the consume channel is not disk queue reader")
)

type Consumer interface {
	UnPause()
	Pause()
	TimedOutMessage()
	RequeuedMessage()
	FinishedMessage()
	Stats() ClientStats
	Exit()
	Empty()
	String() string
	GetID() int64
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
	peekedMsgs             []Message

	//channel msg stats
	channelStatsInfo *ChannelStatsInfo
}

// NewChannel creates a new instance of the Channel type and returns a pointer
func NewChannel(topicName string, part int, channelName string, chEnd BackendQueueEnd, opt *Options,
	deleteCallback func(*Channel), moreDataCallback func(*Channel), consumeDisabled int32,
	notify INsqdNotify, ext int32, queueStart BackendQueueEnd) *Channel {

	c := &Channel{
		topicName:              topicName,
		topicPart:              part,
		name:                   channelName,
		requeuedMsgChan:        make(chan *Message, opt.MaxRdyCount+1),
		waitingRequeueChanMsgs: make(map[MessageID]*Message, 100),
		waitingRequeueMsgs:     make(map[MessageID]*Message, 100),
		clientMsgChan:          make(chan *Message),
		tagMsgChans:            make(map[string]*MsgChanData),
		tagChanInitChan:        make(chan string, 2),
		tagChanRemovedChan:     make(chan string, 2),
		exitChan:               make(chan int),
		exitSyncChan:           make(chan bool),
		clients:                make(map[int64]Consumer),
		confirmedMsgs:          NewIntervalSkipList(),
		tryReadBackend:         make(chan bool, 1),
		readerChanged:          make(chan resetChannelData, 10),
		endUpdatedChan:         make(chan bool, 1),
		deleteCallback:         deleteCallback,
		moreDataCallback:       moreDataCallback,
		option:                 opt,
		nsqdNotify:             notify,
		consumeDisabled:        consumeDisabled,
		delayedConfirmedMsgs:   make(map[MessageID]Message, MaxWaitingDelayed),
		peekedMsgs:             make([]Message, MaxWaitingDelayed),
		Ext:                    ext,
	}
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
	c.channelStatsInfo = &ChannelStatsInfo{}

	c.initPQ()

	if protocol.IsEphemeral(channelName) {
		c.ephemeral = true
	}
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
		false)

	if queueStart != nil {
		// The old closed channel (on slave) may have the invalid read start if the
		// topic disk data is cleaned already. So we check here for new opened channel
		c.checkAndFixStart(queueStart)
	}
	go c.messagePump()

	c.nsqdNotify.NotifyStateChanged(c, true)

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

func (c *Channel) checkAndFixStart(start BackendQueueEnd) {
	if c.GetConfirmed().Offset() >= start.Offset() {
		return
	}
	nsqLog.Infof("%v-%v confirm start need be fixed %v, %v", c.GetTopicName(), c.GetName(),
		start, c.GetConfirmed())
	newStart, err := c.backend.SkipReadToOffset(start.Offset(), start.TotalMsgCnt())
	if err != nil {
		nsqLog.Warningf("%v-%v skip to new start failed: %v", c.GetTopicName(), c.GetName(),
			err)
		newStart, err = c.backend.SkipReadToEnd()
		if err != nil {
			nsqLog.Warningf("%v-%v skip to new start failed: %v", c.GetTopicName(), c.GetName(),
				err)
		} else {
			nsqLog.Infof("%v-%v skip to end : %v", c.GetTopicName(), c.GetName(),
				newStart)
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
			case <-time.After(time.Millisecond):
				nsqLog.Infof("%v-%v timeout sending tag channel remove signal for %v", c.GetTopicName(), c.GetName(), tag)
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
		case <-time.After(time.Millisecond):
			nsqLog.Infof("%v-%v timeout sending tag channel init signal for %v", c.GetTopicName(), c.GetName(), tag)
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
		nsqLog.Debugf("channel %v for tag %v not found.", c.GetName(), tag)
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
			nsqLog.Logf("ignored the reader reset: %v:%v", offset, cnt)
			if offset > 0 && cnt > 0 {
				select {
				case c.readerChanged <- resetChannelData{offset, cnt, true}:
				case <-time.After(time.Millisecond * 10):
					nsqLog.Logf("ignored the reader reset finally: %v:%v", offset, cnt)
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
		if c.GetClientsCount() == 0 {
			atomic.StoreInt32(&c.requireOrder, 0)
			select {
			case c.tryReadBackend <- true:
			default:
			}
		} else {
			nsqLog.Logf("can not set ordered to false while the channel is still consuming by client")
		}
	}
}

func (c *Channel) IsOrdered() bool {
	return atomic.LoadInt32(&c.requireOrder) == 1
}

func (c *Channel) initPQ() {
	pqSize := int(math.Max(1, float64(c.option.MemQueueSize)/10))

	c.inFlightMutex.Lock()
	for _, m := range c.inFlightMessages {
		if m.belongedConsumer != nil {
			m.belongedConsumer.Empty()
		}
	}
	c.inFlightMessages = make(map[MessageID]*Message, pqSize)
	c.inFlightPQ = newInFlightPqueue(pqSize)
	atomic.StoreInt64(&c.deferredCount, 0)
	c.inFlightMutex.Unlock()
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

// waiting more data is indicated all msgs are consumed
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
		nsqLog.Logf("CHANNEL(%s): deleting", c.name)

		// since we are explicitly deleting a channel (not just at system exit time)
		// de-register this from the lookupd
		c.nsqdNotify.NotifyStateChanged(c, true)
	} else {
		nsqLog.Logf("CHANNEL(%s): closing", c.name)
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
	c.flush()
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
		nsqLog.Warningf("failed to reset reader to end %v", err)
	} else {
		c.drainChannelWaiting(true, nil, nil)
	}
	return e, nil
}

func (c *Channel) flush() error {
	if c.ephemeral {
		return nil
	}
	d, ok := c.backend.(*diskQueueReader)
	if ok {
		d.Flush()
	}
	return nil
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

func (c *Channel) Pause() error {
	return c.doPause(true)
}

func (c *Channel) UnPause() error {
	return c.doPause(false)
}

func (c *Channel) doPause(pause bool) error {
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
	return nil
}

func (c *Channel) IsPaused() bool {
	return atomic.LoadInt32(&c.paused) == 1
}

func (c *Channel) Skip() error {
	return c.doSkip(true)
}

func (c *Channel) UnSkip() error {
	return c.doSkip(false)
}

func (c *Channel) IsSkipped() bool {
	return atomic.LoadInt32(&c.skipped) == 1
}

func (c *Channel) doSkip(skipped bool) error {
	if skipped {
		atomic.StoreInt32(&c.skipped, 1)
		if c.GetDelayedQueue() != nil {
			c.GetDelayedQueue().EmptyDelayedChannel(c.GetName())
		}
	} else {
		atomic.StoreInt32(&c.skipped, 0)
	}
	return nil
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
		nsqLog.Logf("failed while touch: %v, msg not exist", id)
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
		nsqLog.LogWarningf("channel (%v) the count is not valid: %v:%v. (This may happen while upgrade from old)", c.GetName(), offset, cnt)
		return nil
	}
	// confirm on slave may exceed the current end, because the buffered write
	// may need to be flushed on slave.
	c.confirmMutex.Lock()
	defer c.confirmMutex.Unlock()
	var err error
	var newConfirmed BackendQueueEnd
	if offset < c.GetConfirmed().Offset() {
		if nsqLog.Level() > levellogger.LOG_DEBUG {
			nsqLog.LogDebugf("confirm offset less than current: %v, %v", offset, c.GetConfirmed())
		}
		if allowBackward {
			d, ok := c.backend.(*diskQueueReader)
			if ok {
				newConfirmed, err = d.ResetReadToOffset(offset, cnt)
				nsqLog.LogDebugf("channel (%v) reset to backward: %v", c.GetName(), newConfirmed)
			}
		}
	} else {
		if allowBackward {
			d, ok := c.backend.(*diskQueueReader)
			if ok {
				newConfirmed, err = d.ResetReadToOffset(offset, cnt)
				nsqLog.LogDebugf("channel (%v) reset to backward: %v", c.GetName(), newConfirmed)
			}
		} else {
			_, err = c.backend.SkipReadToOffset(offset, cnt)
			c.confirmedMsgs.DeleteLower(int64(offset))
			atomic.StoreInt32(&c.waitingConfirm, int32(c.confirmedMsgs.Len()))
		}
	}
	if err != nil {
		if err != ErrExiting {
			nsqLog.Logf("confirm read failed: %v, offset: %v", err, offset)
		}
	}

	return err
}

// if a message confirmed without goto inflight first, then we
// should clean the waiting state from requeue
func (c *Channel) CleanWaitingRequeueChan(msg *Message) {
	c.inFlightMutex.Lock()
	if _, ok := c.waitingRequeueChanMsgs[msg.ID]; ok {
		c.waitingRequeueChanMsgs[msg.ID] = nil
		delete(c.waitingRequeueChanMsgs, msg.ID)
	}
	c.inFlightMutex.Unlock()
}

func (c *Channel) ConfirmDelayedMessage(msg *Message) (BackendOffset, int64, bool) {
	c.confirmMutex.Lock()
	defer c.confirmMutex.Unlock()
	needNotify := false
	curConfirm := c.GetConfirmed()
	if msg.DelayedOrigID > 0 && msg.DelayedType == ChannelDelayed && c.GetDelayedQueue() != nil {
		c.GetDelayedQueue().ConfirmedMessage(msg)
		c.delayedConfirmedMsgs[msg.ID] = *msg
		if atomic.AddInt64(&c.deferredFromDelay, -1) <= 0 {
			c.nsqdNotify.NotifyScanDelayed(c)
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
		nsqLog.Logf("should not confirm delayed here: %v", msg)
		return curConfirm.Offset(), curConfirm.TotalMsgCnt(), false
	}
	if msg.Offset < curConfirm.Offset() {
		nsqLog.LogDebugf("confirmed msg is less than current confirmed offset: %v-%v, %v", msg.ID, msg.Offset, curConfirm)
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
				nsqLog.LogWarningf("channel (%v): confirm read failed: %v, msg: %v", c.GetName(), err, msg)
				// rollback removed confirmed messages
				//for _, m := range c.tmpRemovedConfirmed {
				//	c.confirmedMsgs[int64(msg.offset)] = m
				//}
				//atomic.StoreInt32(&c.waitingConfirm, int32(len(c.confirmedMsgs)))
			}
			return curConfirm.Offset(), curConfirm.TotalMsgCnt(), reduced
		} else {
			if nsqLog.Level() >= levellogger.LOG_DETAIL || c.IsTraced() {
				nsqLog.Logf("channel %v merge msg %v( %v) to interval %v, confirmed to %v", c.GetName(),
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
	if nsqLog.Level() >= levellogger.LOG_DEBUG && int64(c.confirmedMsgs.Len()) > c.option.MaxConfirmWin {
		curConfirm = c.GetConfirmed()
		flightCnt := len(c.inFlightMessages)
		if flightCnt == 0 {
			nsqLog.LogDebugf("lots of confirmed messages : %v, %v, %v",
				c.confirmedMsgs.Len(), curConfirm, flightCnt)
		}
	}
	return newConfirmed, confirmedCnt, reduced
	// TODO: if some messages lost while re-queue, it may happen that some messages not
	// in inflight queue and also not wait confirm. In this way, we need reset
	// backend queue to force read the data from disk again.
}

func (c *Channel) ShouldWaitDelayed(msg *Message) bool {
	if c.IsOrdered() {
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
			if msg.TraceID != 0 || c.IsTraced() || nsqLog.Level() >= levellogger.LOG_DEBUG {
				nsqLog.LogDebugf("non-delayed msg %v should be delayed since in delayed queue", msg)
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
		if msg.TraceID != 0 || c.IsTraced() || nsqLog.Level() >= levellogger.LOG_DEBUG {
			nsqLog.LogDebugf("msg %v is already confirmed", msg)
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
		nsqLog.Logf("topic %v channel %v force finish msg %v", c.GetTopicName(), c.GetName(), id)
	}
	return c.internalFinishMessage(clientID, clientAddr, id, forceFin)
}

// FinishMessage successfully discards an in-flight message
func (c *Channel) internalFinishMessage(clientID int64, clientAddr string,
	id MessageID, forceFin bool) (BackendOffset, int64, bool, *Message, error) {
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
		nsqLog.LogDebugf("channel (%v): message %v fin error: %v from client %v", c.GetName(), id, err,
			clientID)
		return 0, 0, false, nil, err
	}
	now := time.Now()
	ackCost := now.UnixNano() - msg.deliveryTS.UnixNano()
	isOldDeferred := msg.IsDeferred()
	if msg.TraceID != 0 || c.IsTraced() || nsqLog.Level() >= levellogger.LOG_DETAIL {
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
	expectTimeout := msg.pri - msg.deliveryTS.UnixNano()
	if ackCost >= time.Second.Nanoseconds() &&
		(c.IsTraced() || msg.TraceID != 0 || c.IsSlowTraced() ||
			ackCost >= expectTimeout/10 || nsqLog.Level() >= levellogger.LOG_DEBUG) {
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
			nsqLog.Infof("channel %v delayed msg %v finished by client %v ", c.GetName(),
				msg, clientAddr)
		}
		if nsqLog.Level() >= levellogger.LOG_DEBUG {
			if clientAddr == "" {
				nsqLog.Debugf("channel %v delay msg %v finished by force ", c.GetName(),
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
		nsqLog.Logf("too much delayed in memory: %v vs %v", deCnt, cnt)
		return true
	}
	return false
}

func (c *Channel) ShouldRequeueToEnd(clientID int64, clientAddr string, id MessageID,
	timeout time.Duration, byClient bool) (*Message, bool) {
	if !byClient {
		return nil, false
	}
	if c.IsOrdered() {
		return nil, false
	}
	threshold := time.Minute
	if c.option.ReqToEndThreshold >= time.Millisecond {
		threshold = c.option.ReqToEndThreshold
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

	if nsqLog.Level() >= levellogger.LOG_DEBUG || c.IsTraced() {
		nsqLog.LogDebugf("channel %v check requeue to end, timeout:%v, msg timestamp:%v, depth ts:%v, msg attempt:%v, waiting :%v",
			c.GetName(), timeout, msg.Timestamp,
			c.DepthTimestamp(), msg.Attempts, atomic.LoadInt32(&c.waitingConfirm))
	}

	newTimeout := time.Now().Add(timeout)
	if newTimeout.Sub(msg.deliveryTS) >=
		c.option.MaxReqTimeout {
		return msg.GetCopy(), true
	}
	if timeout > threshold {
		return msg.GetCopy(), true
	}

	deCnt := atomic.LoadInt64(&c.deferredCount)
	if (deCnt >= c.option.MaxConfirmWin) &&
		(timeout > threshold/2) {
		// if requeued by deferred is more than half of the all messages handled,
		// it may be a bug in client which can not handle any more, so we just wait
		// timeout not requeue to defer
		cnt := c.GetChannelWaitingConfirmCnt()
		if cnt >= c.option.MaxConfirmWin && float64(deCnt) <= float64(cnt)*0.5 {
			nsqLog.Logf("requeue msg to end %v, since too much delayed in memory: %v vs %v", id, deCnt, cnt)
			return msg.GetCopy(), true
		}
	}

	ts := time.Now().UnixNano() - c.DepthTimestamp()
	isBlocking := atomic.LoadInt32(&c.waitingConfirm) >= int32(c.option.MaxConfirmWin)
	if isBlocking {
		if timeout > threshold/2 || (timeout > 2*time.Minute) {
			return msg.GetCopy(), true
		}

		if msg.Attempts < 3 {
			return nil, false
		}

		if msg.Timestamp > c.DepthTimestamp()+threshold.Nanoseconds() {
			return nil, false
		}

		if msg.Attempts > MAX_MEM_REQ_TIMES && ts > threshold.Nanoseconds() {
			return msg.GetCopy(), true
		}
		if ts > 20*threshold.Nanoseconds() {
			return msg.GetCopy(), true
		}
		return nil, false
	} else {
		if msg.Timestamp > c.DepthTimestamp()+threshold.Nanoseconds()/10 {
			return nil, false
		}

		if msg.Attempts < MAX_MEM_REQ_TIMES {
			return nil, false
		}
		if ts < 20*threshold.Nanoseconds() {
			return nil, false
		}
		if c.Depth() < 100 {
			return nil, false
		}
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
	if timeout == 0 {
		// remove from inflight first
		msg, err := c.popInFlightMessage(clientID, id, false)
		if err != nil {
			nsqLog.LogDebugf("channel (%v): message %v requeue error: %v from client %v", c.GetName(), id, err,
				clientID)
			return err
		}
		// requeue by intend should treat as not fail attempt
		if msg.Attempts > 0 && !byClient {
			msg.Attempts--
		}
		if msg.belongedConsumer != nil {
			msg.belongedConsumer.RequeuedMessage()
			msg.belongedConsumer = nil
		}
		return c.doRequeue(msg, clientAddr)
	}
	// change the timeout for inflight
	msg, ok := c.inFlightMessages[id]
	if !ok {
		nsqLog.LogDebugf("failed requeue for delay: %v, msg not exist", id)
		return ErrMsgNotInFlight
	}
	// one message should not defer again before the old defer timeout
	if msg.IsDeferred() {
		return ErrMsgDeferred
	}

	if msg.GetClientID() != clientID {
		nsqLog.LogDebugf("failed requeue for client not own message: %v: %v vs %v", id, msg.GetClientID(), clientID)
		return fmt.Errorf("client does not own message %v: %v vs %v", id,
			msg.GetClientID(), clientID)
	}
	newTimeout := time.Now().Add(timeout)
	if (timeout > c.option.ReqToEndThreshold) ||
		(newTimeout.Sub(msg.deliveryTS) >=
			c.option.MaxReqTimeout) {
		nsqLog.Logf("ch %v too long timeout %v, %v, %v, should req message: %v to delayed queue", c.GetName(),
			newTimeout, msg.deliveryTS, timeout, id)
	}
	deCnt := atomic.LoadInt64(&c.deferredCount)
	if c.isTooMuchDeferredInMem(deCnt) {
		nsqLog.Logf("failed to requeue msg %v, since too much delayed in memory: %v", id, deCnt)
		return ErrMsgDeferredTooMuch
	}
	if int64(atomic.LoadInt32(&c.waitingConfirm)) > c.option.MaxConfirmWin {
		nsqLog.Logf("failed to requeue msg %v, since too much waiting confirmed: %v", id, atomic.LoadInt32(&c.waitingConfirm))
		return ErrMsgDeferredTooMuch
	}

	atomic.AddInt64(&c.deferredCount, 1)
	msg.pri = newTimeout.UnixNano()
	atomic.AddInt32(&msg.deferredCnt, 1)

	if msg.TraceID != 0 || c.IsTraced() || nsqLog.Level() >= levellogger.LOG_DEBUG {
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

	nsqLog.LogDebugf("channel %v client %v requeue with delayed %v message: %v", c.GetName(), clientID, timeout, id)
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
		nsqLog.Logf("client: %v requeued %v messages ",
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
	msg.belongedConsumer = client
	msg.deliveryTS = now
	msg.pri = now.Add(timeout).UnixNano()
	msg.Attempts++
	old, err := c.pushInFlightMessage(msg)
	shouldSend := true
	if err != nil {
		if old != nil && old.IsDeferred() {
			shouldSend = false
		} else if old != nil && old.DelayedType == ChannelDelayed {
			shouldSend = false
		} else {
			nsqLog.LogWarningf("push message in flight failed: %v, %v", err,
				msg.GetFullMsgID())
		}
		return shouldSend, err
	}

	if msg.TraceID != 0 || c.IsTraced() || nsqLog.Level() >= levellogger.LOG_DETAIL {
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
	if nsqLog.Level() >= levellogger.LOG_DETAIL {
		nsqLog.Logf("update confirmed interval, before: %v", c.confirmedMsgs.ToString())
	}
	if c.confirmedMsgs.Len() != 0 {
		c.confirmedMsgs = NewIntervalSkipList()
	}
	for _, qi := range intervals {
		c.confirmedMsgs.AddOrMerge(&queueInterval{start: qi.Start, end: qi.End, endCnt: qi.EndCnt})
	}
	if nsqLog.Level() >= levellogger.LOG_DETAIL {
		nsqLog.Logf("update confirmed interval, after: %v", c.confirmedMsgs.ToString())
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
	atomic.AddUint64(&c.requeueCount, 1)
	if m.TraceID != 0 || c.IsTraced() || nsqLog.Level() >= levellogger.LOG_DEBUG {
		nsqMsgTracer.TraceSub(c.GetTopicName(), c.GetName(), "REQ", m.TraceID, m, clientAddr, 0)
	}
	select {
	case <-c.exitChan:
		nsqLog.Logf("requeue message failed for existing: %v ", m.ID)
		return ErrExiting
	case c.requeuedMsgChan <- m:
		c.waitingRequeueChanMsgs[m.ID] = m
	default:
		c.waitingRequeueMsgs[m.ID] = m
	}
	return nil
}

// pushInFlightMessage atomically adds a message to the in-flight dictionary
func (c *Channel) pushInFlightMessage(msg *Message) (*Message, error) {
	c.inFlightMutex.Lock()
	defer c.inFlightMutex.Unlock()
	if c.IsConsumeDisabled() {
		return nil, ErrConsumeDisabled
	}
	m, ok := c.inFlightMessages[msg.ID]
	if ok {
		return m, ErrMsgAlreadyInFlight
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
		nsqLog.Logf("channel (%v): should never pop a deferred message here unless the timeout : %v", c.GetName(), msg.ID)
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
		nsqLog.Logf("channel %v disabled for consume", c.name)
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
		nsqLog.Logf("channel %v enabled for consume", c.name)
		if !atomic.CompareAndSwapInt32(&c.consumeDisabled, 1, 0) {
			select {
			case c.tryReadBackend <- true:
			default:
			}
			nsqLog.Logf("channel %v already enabled for consume", c.name)
		} else {
			// we need reset backend read position to confirm position
			// since we dropped all inflight and requeue data while disable consume.
			if nsqLog.Level() >= levellogger.LOG_DEBUG {
				c.confirmMutex.Lock()
				nsqLog.Logf("channel %v confirmed interval while enable: %v", c.GetName(), c.confirmedMsgs.ToString())
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
					nsqLog.Logf("ignored a read message %v at offset %v while enable consume", m.ID, m.Offset)
					c.CleanWaitingRequeueChan(m)
					if m.DelayedType == ChannelDelayed {
						atomic.AddInt64(&c.deferredFromDelay, -1)
					}
				case m := <-c.requeuedMsgChan:
					nsqLog.Logf("ignored a requeued message %v at offset %v while enable consume", m.ID, m.Offset)
					c.CleanWaitingRequeueChan(m)
					if m.DelayedType == ChannelDelayed {
						atomic.AddInt64(&c.deferredFromDelay, -1)
					}
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
func (c *Channel) drainChannelWaiting(clearConfirmed bool, lastDataNeedRead *bool, origReadChan chan ReadResult) error {
	nsqLog.Logf("draining channel %v waiting %v", c.GetName(), clearConfirmed)
	c.initPQ()
	c.confirmMutex.Lock()
	if clearConfirmed {
		c.confirmedMsgs = NewIntervalSkipList()
		atomic.StoreInt32(&c.waitingConfirm, 0)
	} else {
		curConfirm := c.GetConfirmed()
		c.confirmedMsgs.DeleteLower(int64(curConfirm.Offset()))
		atomic.StoreInt32(&c.waitingConfirm, int32(c.confirmedMsgs.Len()))
		nsqLog.Logf("channel %v current confirmed interval %v ", c.GetName(), c.confirmedMsgs.ToString())
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
			nsqLog.Logf("ignored a read message %v at Offset %v while drain channel", m.ID, m.Offset)
		case m := <-c.requeuedMsgChan:
			nsqLog.Logf("ignored a message %v at Offset %v while drain channel", m.ID, m.Offset)
		default:
			done = true
		}
	}
	reqCnt := 0
	c.inFlightMutex.Lock()
	reqCnt += len(c.waitingRequeueMsgs) + len(c.waitingRequeueChanMsgs)
	for k := range c.waitingRequeueMsgs {
		c.waitingRequeueMsgs[k] = nil
		delete(c.waitingRequeueMsgs, k)
	}
	for k := range c.waitingRequeueChanMsgs {
		c.waitingRequeueChanMsgs[k] = nil
		delete(c.waitingRequeueChanMsgs, k)
	}
	c.inFlightMutex.Unlock()
	nsqLog.Logf("drained channel %v waiting req %v, delay: %v", c.GetName(), reqCnt,
		atomic.LoadInt64(&c.deferredFromDelay))
	atomic.StoreInt64(&c.deferredFromDelay, 0)

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
	if c.IsOrdered() {
		return
	}
	select {
	case c.tryReadBackend <- true:
	default:
	}
	if nsqLog.Level() >= levellogger.LOG_DETAIL {
		nsqLog.LogDebugf("channel consume try wakeup : %v", c.name)
	}
}

func (c *Channel) resetReaderToConfirmed() error {
	atomic.StoreInt64(&c.waitingProcessMsgTs, 0)
	atomic.StoreInt32(&c.needResetReader, 0)
	confirmed, err := c.backend.ResetReadToConfirmed()
	if err != nil {
		nsqLog.LogWarningf("channel(%v): reset read to confirmed error: %v", c.GetName(), err)
		return err
	}
	nsqLog.Logf("reset channel %v reader to confirm: %v", c.name, confirmed)
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
			nsqLog.Warningf("failed to reset reader to %v, %v", resetOffset, err)
		} else {
			c.drainChannelWaiting(true, lastDataNeedRead, origReadChan)
			*lastMsg = Message{}
		}
		*needReadBackend = true
		*readBackendWait = false
	}
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
		if resetReaderFlag > 0 {
			nsqLog.Infof("reset the reader : %v", c.GetConfirmed())
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
						nsqLog.Warningf("reset need clear confirmed since no inflight: %v, %v",
							c.GetTopicName(), c.GetName())
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
		} else if atomic.LoadInt32(&c.waitingConfirm) > maxWin {
			if nsqLog.Level() >= levellogger.LOG_DEBUG {
				nsqLog.LogDebugf("channel %v reader is holding: %v, %v",
					c.GetName(),
					atomic.LoadInt32(&c.waitingConfirm),
					c.GetConfirmed())
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
				nsqLog.Warningf("many confirmed but no inflight: %v, %v, %v",
					c.GetTopicName(), c.GetName(), atomic.LoadInt32(&c.waitingConfirm))
			}
		} else {
			readChan = origReadChan
			needReadBackend = true
		}

		if c.IsConsumeDisabled() {
			readChan = nil
			needReadBackend = false
			nsqLog.Logf("channel consume is disabled : %v", c.name)
			if lastMsg.ID > 0 {
				nsqLog.Logf("consume disabled at last read message: %v:%v", lastMsg.ID, lastMsg.Offset)
				lastMsg = Message{}
			}
		}

		if needReadBackend {
			if !lastDataNeedRead {
				dataRead, hasData := d.TryReadOne()
				if hasData {
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
		select {
		case <-c.exitChan:
			goto exit
		case msg = <-c.requeuedMsgChan:
			if msg.TraceID != 0 || c.IsTraced() || nsqLog.Level() >= levellogger.LOG_DETAIL {
				nsqLog.LogDebugf("read message %v from requeue", msg.ID)
				nsqMsgTracer.TraceSub(c.GetTopicName(), c.GetName(), "READ_REQ", msg.TraceID, msg, "0", 0)
			}
		case data = <-readChan:
			lastDataNeedRead = false
			if data.Err != nil {
				nsqLog.LogErrorf("channel (%v): failed to read message - %s", c.GetName(), data.Err)
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
						nsqLog.Warningf("channel %v skip to next because of backend error: %v", c.GetName(), backendErr)
						isSkipped = true
						backendErr = 0
					} else {
						backendErr++
						time.Sleep(time.Second)
					}
				}
				time.Sleep(time.Millisecond * 100)
				continue LOOP
			}
			if backendErr > 0 {
				nsqLog.Infof("channel %v backend error auto recovery: %v", c.GetName(), backendErr)
			}
			backendErr = 0
			msg, err = decodeMessage(data.Data, c.IsExt())
			if err != nil {
				nsqLog.LogErrorf("channel (%v): failed to decode message - %s - %v", c.GetName(), err, data)
				continue LOOP
			}
			msg.Offset = data.Offset
			msg.RawMoveSize = data.MovedSize
			msg.queueCntIndex = data.CurCnt
			if msg.TraceID != 0 || c.IsTraced() || nsqLog.Level() >= levellogger.LOG_DETAIL {
				nsqMsgTracer.TraceSub(c.GetTopicName(), c.GetName(), "READ_QUEUE", msg.TraceID, msg, "0", 0)
			}

			if lastMsg.ID > 0 && msg.ID < lastMsg.ID {
				// note: this may happen if the reader pefetch some data not committed by the disk writer
				// we need read it again later.
				nsqLog.Warningf("read a message with less message ID: %v vs %v, raw data: %v", msg.ID, lastMsg.ID, data)
				nsqLog.Warningf("last raw data: %v", lastDataResult)
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
				nsqLog.LogWarningf("channel (%v): skipped message from %v:%v to the : %v:%v",
					c.GetName(), lastMsg.ID, lastMsg.Offset, msg.ID, msg.Offset)
			}
			if resumedFirst {
				if nsqLog.Level() > levellogger.LOG_DEBUG || c.IsTraced() {
					nsqLog.LogDebugf("channel %v resumed first messsage %v at Offset: %v", c.GetName(), msg.ID, msg.Offset)
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
			nsqLog.Infof("got reader reset notify:%v ", resetOffset)
			c.resetChannelReader(resetOffset, &lastDataNeedRead, origReadChan, &lastMsg, &needReadBackend, &readBackendWait)
			continue LOOP
		case <-waitEndUpdated:
			continue LOOP
		}

		if msg == nil {
			continue
		}

		if c.IsConsumeDisabled() {
			continue
		}
		if c.IsOrdered() {
			curConfirm := c.GetConfirmed()
			if msg.Offset != curConfirm.Offset() {
				nsqLog.Infof("read a message not in ordered: %v, %v", msg.Offset, curConfirm)
				atomic.StoreInt32(&c.needResetReader, 2)
				continue
			}
		}

		//let timer sync to update backend in replicas' channels
		if c.IsSkipped() {
			if msg.DelayedType == ChannelDelayed {
				c.ConfirmDelayedMessage(msg)
			} else {
				c.ConfirmBackendQueue(msg)
			}
			c.CleanWaitingRequeueChan(msg)
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
				nsqLog.Errorf("error parse tag from message %v, offset %v, data: %v", msg.ID, msg.Offset, PrintMessage(msg))
			}
			extParsed = true
		}

		if msgTag != "" {
			tagMsgChan, chanExist := c.GetClientTagMsgChan(msgTag)
			if chanExist {
				select {
				case tagMsgChan <- msg:
					msg = nil
					continue
				case <-c.tagChanRemovedChan:
					//do not go to msgDefaultLoop, as tag chan remove event may invoked from previously deleted client
					goto tagMsgLoop
				case resetOffset := <-c.readerChanged:
					nsqLog.Infof("got reader reset notify while dispatch message:%v ", resetOffset)
					c.resetChannelReader(resetOffset, &lastDataNeedRead, origReadChan, &lastMsg, &needReadBackend, &readBackendWait)
					continue
				case <-c.exitChan:
					goto exit
				}
			}
		}

	msgDefaultLoop:
		select {
		case newTag := <-c.tagChanInitChan:
			if !extParsed || newTag == msgTag {
				nsqLog.Infof("client with tag %v initialized, try deliver in tag loop", newTag)
				goto tagMsgLoop
			} else {
				goto msgDefaultLoop
			}
		case c.clientMsgChan <- msg:
		case resetOffset := <-c.readerChanged:
			nsqLog.Infof("got reader reset notify while dispatch message:%v ", resetOffset)
			c.resetChannelReader(resetOffset, &lastDataNeedRead, origReadChan, &lastMsg, &needReadBackend, &readBackendWait)
		case <-c.exitChan:
			goto exit
		}

		msg = nil
		// the client will call back to mark as in-flight w/ its info
	}

exit:
	nsqLog.Logf("CHANNEL(%s): closing ... messagePump", c.name)
	close(c.clientMsgChan)
	close(c.exitSyncChan)
}

func parseTagIfAny(msg *Message) (string, error) {
	var msgTag string
	var err error
	switch msg.ExtVer {
	case ext.TAG_EXT_VER:
		msgTag = string(msg.ExtBytes)
	case ext.JSON_HEADER_EXT_VER:
		var jsonExt *simpleJson.Json
		jsonExt, err = simpleJson.NewJson(msg.ExtBytes)
		if err == nil {
			if tagJson, exist := jsonExt.CheckGet(ext.CLIENT_DISPATCH_TAG_KEY); exist {
				msgTag, err = tagJson.String()
			}
		}
	}
	return msgTag, err
}

func (c *Channel) GetChannelDebugStats() string {
	c.inFlightMutex.Lock()
	inFlightCount := len(c.inFlightMessages)
	debugStr := fmt.Sprintf("topic %v channel %v \ninflight %v, req %v, %v, %v, ",
		c.GetTopicName(), c.GetName(), inFlightCount, len(c.waitingRequeueMsgs),
		len(c.requeuedMsgChan), len(c.waitingRequeueChanMsgs))

	if nsqLog.Level() >= levellogger.LOG_DEBUG || c.IsTraced() {
		for _, msg := range c.inFlightMessages {
			debugStr += fmt.Sprintf("%v(%v, %v),", msg.ID, msg.Offset, msg.DelayedType)
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

func (c *Channel) processInFlightQueue(tnow int64) (bool, bool) {
	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()

	if c.Exiting() {
		return false, false
	}

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
			if atomic.LoadInt32(&c.waitingConfirm) > 1 || flightCnt > 1 {
				nsqLog.LogDebugf("channel %v no timeout, inflight %v, waiting confirm: %v, confirmed: %v",
					c.GetName(), flightCnt, atomic.LoadInt32(&c.waitingConfirm),
					c.GetConfirmed())
				if !c.IsOrdered() && atomic.LoadInt32(&c.waitingConfirm) >= int32(c.option.MaxConfirmWin) {
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
						// if the blocking message still need waiting too long,
						// we requeue to end or just timeout it immediately
						if m.pri > time.Now().Add(threshold/2).UnixNano() {
							blockingMsg = m
						}
						break
					}
					if blockingMsg != nil {
						nsqLog.Logf("msg %v is blocking confirm, requeue to end, inflight %v, waiting confirm: %v, confirmed: %v",
							PrintMessage(blockingMsg),
							flightCnt, atomic.LoadInt32(&c.waitingConfirm), confirmed)
						toEnd := true
						deCnt := atomic.LoadInt64(&c.deferredCount)
						if c.isTooMuchDeferredInMem(deCnt) {
							nsqLog.Logf("channel %v too much delayed in memory: %v", c.GetName(), deCnt)
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

		if msgCopy.TraceID != 0 || c.IsTraced() || nsqLog.Level() >= levellogger.LOG_INFO {
			clientAddr := ""
			if client != nil {
				clientAddr = client.String()
			}
			cost := tnow - msgCopy.deliveryTS.UnixNano()
			if msgCopy.IsDeferred() {
				nsqLog.LogDebugf("msg %v defer timeout, expect at %v ",
					msgCopy.ID, msgCopy.pri)
				nsqMsgTracer.TraceSub(c.GetTopicName(), c.GetName(), "DELAY_TIMEOUT", msgCopy.TraceID, &msgCopy, clientAddr, cost)
			} else {
				nsqMsgTracer.TraceSub(c.GetTopicName(), c.GetName(), "TIMEOUT", msgCopy.TraceID, &msgCopy, clientAddr, cost)
			}
		}
	}

exit:
	// try requeue the messages that waiting.
	stopScan := false
	c.inFlightMutex.Lock()
	oldWaitingDeliveryState := atomic.LoadInt32(&c.waitingDeliveryState)
	reqLen := len(c.requeuedMsgChan) + len(c.waitingRequeueMsgs)
	if !c.IsConsumeDisabled() {
		if len(c.waitingRequeueMsgs) > 1 {
			nsqLog.LogDebugf("channel %v requeue waiting messages: %v", c.GetName(), len(c.waitingRequeueMsgs))
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
			}
			if stopScan {
				break
			}
		}
	}
	c.inFlightMutex.Unlock()
	c.RLock()
	clientNum := len(c.clients)
	c.RUnlock()
	delayedQueue := c.GetDelayedQueue()
	checkFast := false
	waitingDelayCnt := atomic.LoadInt64(&c.deferredFromDelay)
	if waitingDelayCnt < 0 {
		nsqLog.Logf("delayed waiting count error %v ", waitingDelayCnt)
	}
	needPeekDelay := waitingDelayCnt <= 0
	if !c.IsConsumeDisabled() && !c.IsOrdered() && delayedQueue != nil &&
		needPeekDelay && clientNum > 0 {
		peekStart := time.Now()
		newAdded := 0
		cnt, err := delayedQueue.PeekRecentChannelTimeout(tnow, c.peekedMsgs, c.GetName())
		if err == nil {
			for _, tmpMsg := range c.peekedMsgs[:cnt] {
				m := tmpMsg
				c.inFlightMutex.Lock()
				c.confirmMutex.Lock()
				_, cok := c.delayedConfirmedMsgs[m.DelayedOrigID]
				c.confirmMutex.Unlock()
				if cok {
					nsqLog.LogDebugf("delayed message already confirmed %v ", m)
				} else {
					oldMsg2, ok2 := c.inFlightMessages[m.DelayedOrigID]
					_, ok3 := c.waitingRequeueChanMsgs[m.DelayedOrigID]
					_, ok4 := c.waitingRequeueMsgs[m.DelayedOrigID]
					if ok2 {
						// the delayed orig message id in delayed message is the actual id in the topic queue
						if oldMsg2.ID != m.DelayedOrigID || oldMsg2.DelayedTs != m.DelayedTs {
							nsqLog.Logf("old msg %v in flight mismatch peek from delayed queue, new %v ",
								oldMsg2, m)
						}
					} else if ok3 || ok4 {
						// already waiting requeue
						nsqLog.LogDebugf("delayed waiting in requeued %v ", m)
					} else {
						tmpID := m.ID
						m.ID = m.DelayedOrigID
						m.DelayedOrigID = tmpID

						if tnow > m.DelayedTs+int64(c.option.QueueScanInterval*2) {
							nsqLog.LogDebugf("channel %v delayed is too late now %v for message: %v, peeking time: %v",
								c.GetName(), tnow, m, peekStart)
						}
						if tnow < m.DelayedTs {
							nsqLog.LogDebugf("channel %v delayed is too early now %v for message: %v, peeking time: %v",
								c.GetName(), tnow, m, peekStart)
						}
						if m.TraceID != 0 || c.IsTraced() || nsqLog.Level() >= levellogger.LOG_DEBUG {
							nsqMsgTracer.TraceSub(c.GetTopicName(), c.GetName(), "DELAY_QUEUE_TIMEOUT", m.TraceID, &m, "", 0)
						}

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
				c.inFlightMutex.Unlock()
			}
			c.confirmMutex.Lock()
			c.delayedConfirmedMsgs = make(map[MessageID]Message, MaxWaitingDelayed)
			c.confirmMutex.Unlock()
			if newAdded > 0 && nsqLog.Level() >= levellogger.LOG_DEBUG {
				nsqLog.LogDebugf("channel %v delayed waiting peeked %v added %v new : %v",
					c.GetName(), cnt, newAdded, waitingDelayCnt)
			}
		}
	} else if clientNum > 0 {
		if waitingDelayCnt > 0 {
			if nsqLog.Level() >= levellogger.LOG_DEBUG {
				nsqLog.LogDebugf("channel %v delayed waiting : %v", c.GetName(), waitingDelayCnt)
			}
			c.inFlightMutex.Lock()
			allWaiting := len(c.inFlightMessages) + len(c.waitingRequeueChanMsgs) + len(c.waitingRequeueMsgs)
			c.inFlightMutex.Unlock()
			if waitingDelayCnt > int64(allWaiting) {
				nsqLog.Logf("channel %v delayed waiting : %v, more than all waiting delivery: %v", c.GetName(), waitingDelayCnt, allWaiting)
			}
		}
	}
	isInflightEmpty := (flightCnt == 0) && (reqLen == 0) && (requeuedCnt <= 0)
	noReadDataFromDisk := atomic.LoadInt32(&c.waitingConfirm) >= int32(c.option.MaxConfirmWin)
	if isInflightEmpty && !noReadDataFromDisk && !c.IsPaused() {
		// for tagged client, it may happen if no any un-tagged client.
		// we may read to end, but the last message is normal message.
		// in this way, we should block and wait un-tagged client.
		c.tagMsgChansMutex.RLock()
		tagChLen := len(c.tagMsgChans)
		c.tagMsgChansMutex.RUnlock()
		e := c.GetChannelEnd()
		if c.GetConfirmed().Offset() < e.Offset() && tagChLen == 0 {
			d, ok := c.backend.(*diskQueueReader)
			if ok && d.GetQueueCurrentRead() == e {
				noReadDataFromDisk = true
			}
		}
	}
	if isInflightEmpty && (!dirty) && clientNum > 0 &&
		(oldWaitingDeliveryState == 0) && noReadDataFromDisk &&
		(atomic.LoadInt32(&c.waitingDeliveryState) == 0) {
		diff := time.Now().Unix() - atomic.LoadInt64(&c.processResetReaderTime)
		if diff > resetReaderTimeoutSec && atomic.LoadInt64(&c.processResetReaderTime) > 0 {
			nsqLog.LogWarningf("try reset reader since no inflight and requeued for too long (%v): %v, %v, %v",
				diff,
				atomic.LoadInt32(&c.waitingConfirm), c.GetConfirmed(), c.GetChannelDebugStats())

			atomic.StoreInt64(&c.processResetReaderTime, time.Now().Unix())
			select {
			case c.readerChanged <- resetChannelData{BackendOffset(-1), 0, true}:
			default:
			}
		}
	} else {
		atomic.StoreInt64(&c.processResetReaderTime, time.Now().Unix())
	}

	return dirty, checkFast
}

func (c *Channel) GetDelayedQueueConsumedState() (RecentKeyList, map[int]uint64, map[string]uint64) {
	dq := c.GetDelayedQueue()
	if dq == nil {
		return nil, nil, nil
	}

	return dq.GetOldestConsumedState([]string{c.GetName()}, false)
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
