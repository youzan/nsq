package nsqd

import (
	//"github.com/youzan/nsq/internal/levellogger"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	simpleJson "github.com/bitly/go-simplejson"
	ast "github.com/stretchr/testify/assert"
	"github.com/youzan/nsq/internal/ext"
)

var shortWaitTime = time.Second
var longWaitTime = time.Second * 10

type fakeConsumer struct {
	cid         int64
	inFlightCnt int64
}

func NewFakeConsumer(id int64) *fakeConsumer {
	return &fakeConsumer{cid: id}
}

func (c *fakeConsumer) UnPause() {
}
func (c *fakeConsumer) Pause() {
}
func (c *fakeConsumer) SetLimitedRdy(cnt int) {
}
func (c *fakeConsumer) GetInflightCnt() int64 {
	return atomic.LoadInt64(&c.inFlightCnt)
}

func (c *fakeConsumer) SendingMessage() {
	atomic.AddInt64(&c.inFlightCnt, 1)
}
func (c *fakeConsumer) TimedOutMessage() {
	atomic.AddInt64(&c.inFlightCnt, -1)
}
func (c *fakeConsumer) RequeuedMessage() {
	atomic.AddInt64(&c.inFlightCnt, -1)
}
func (c *fakeConsumer) FinishedMessage() {
	atomic.AddInt64(&c.inFlightCnt, -1)
}
func (c *fakeConsumer) Stats() ClientStats {
	return ClientStats{}
}
func (c *fakeConsumer) Exit() {
}
func (c *fakeConsumer) Empty() {
}
func (c *fakeConsumer) String() string {
	return ""
}
func (c *fakeConsumer) GetID() int64 {
	return c.cid
}

func (c *fakeConsumer) SkipZanTest() {

}

func (c *fakeConsumer) UnskipZanTest() {

}

func waitUntil(t *testing.T, to time.Duration, f func() bool) {
	end := time.Now().UnixNano() + int64(to)
	for time.Now().UnixNano() < end {
		if f() {
			return
		}
		time.Sleep(time.Millisecond * 10)
	}
	t.Errorf("condition not met after timeout")
}

// ensure that we can push a message through a topic and get it out of a channel
func TestPutMessage(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_put_message" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	channel1 := topic.GetChannel("ch")

	var id MessageID
	msg := NewMessage(id, []byte("test"))
	topic.PutMessage(msg)
	topic.ForceFlush()

	select {
	case outputMsg := <-channel1.clientMsgChan:
		equal(t, msg.ID, outputMsg.ID)
		equal(t, msg.Body, outputMsg.Body)
	case <-time.After(shortWaitTime):
		t.Logf("timeout wait")
	}
}

// ensure that both channels get the same message
func TestPutMessage2Chan(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_put_message_2chan" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	channel1 := topic.GetChannel("ch1")
	channel2 := topic.GetChannel("ch2")

	var id MessageID
	msg := NewMessage(id, []byte("test"))
	topic.PutMessage(msg)
	topic.flushBuffer(true)

	timer := time.NewTimer(shortWaitTime)
	select {
	case outputMsg1 := <-channel1.clientMsgChan:
		equal(t, msg.ID, outputMsg1.ID)
		equal(t, msg.Body, outputMsg1.Body)
	case <-timer.C:
		t.Errorf("timeout waiting consume")
		return
	}

	select {
	case outputMsg2 := <-channel2.clientMsgChan:
		equal(t, msg.ID, outputMsg2.ID)
		equal(t, msg.Body, outputMsg2.Body)
	case <-timer.C:
		t.Errorf("timeout waiting consume")
		return
	}
}

func TestChannelBackendMaxMsgSize(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_channel_backend_maxmsgsize" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)

	equal(t, topic.backend.maxMsgSize, int32(opts.MaxMsgSize+minValidMsgLength))
}

func TestInFlightWorker(t *testing.T) {
	count := 250

	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_in_flight_worker" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	channel := topic.GetChannel("channel")

	for i := 0; i < count; i++ {
		msg := NewMessage(topic.nextMsgID(), []byte("test"))
		channel.StartInFlightTimeout(msg, NewFakeConsumer(0), "", opts.MsgTimeout)
	}

	channel.Lock()
	inFlightMsgs := len(channel.inFlightMessages)
	channel.Unlock()
	equal(t, inFlightMsgs, count)

	channel.inFlightMutex.Lock()
	inFlightPQMsgs := len(channel.inFlightPQ)
	channel.inFlightMutex.Unlock()
	equal(t, inFlightPQMsgs, count)

	// the in flight worker has a resolution of 100ms so we need to wait
	// at least that much longer than our msgTimeout (in worst case)
	time.Sleep(4*opts.MsgTimeout + opts.QueueScanInterval)

	channel.Lock()
	inFlightMsgs = len(channel.inFlightMessages)
	channel.Unlock()
	equal(t, inFlightMsgs, 0)

	channel.inFlightMutex.Lock()
	inFlightPQMsgs = len(channel.inFlightPQ)
	channel.inFlightMutex.Unlock()
	equal(t, inFlightPQMsgs, 0)
}

func TestChannelEmpty(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_channel_empty" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	channel := topic.GetChannel("channel")

	msgs := make([]*Message, 0, 25)
	for i := 0; i < 25; i++ {
		msg := NewMessage(topic.nextMsgID(), []byte("test"))
		channel.StartInFlightTimeout(msg, NewFakeConsumer(0), "", opts.MsgTimeout)
		msgs = append(msgs, msg)
	}

	channel.RequeueMessage(0, "", msgs[len(msgs)-1].ID, 0, true)
	equal(t, len(channel.inFlightMessages), 24)
	equal(t, len(channel.inFlightPQ), 24)

	channel.skipChannelToEnd()

	equal(t, len(channel.inFlightMessages), 0)
	equal(t, len(channel.inFlightPQ), 0)
	equal(t, channel.Depth(), int64(0))
}

func TestChannelReqRefillSoon(t *testing.T) {
	// test the overflow requeued in waiting map should be refilled soon after the requeue chan is empty
	count := 50
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.MaxRdyCount = 100
	// use large to delay the period scan
	opts.QueueScanRefreshInterval = shortWaitTime * 2
	opts.QueueScanInterval = shortWaitTime * 2
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_requeued_refill" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	channel := topic.GetChannel("channel")

	for i := 0; i < count; i++ {
		msg := NewMessage(0, []byte("test"))
		_, _, _, _, err := topic.PutMessage(msg)
		equal(t, err, nil)
	}

	channel.AddClient(1, NewFakeConsumer(1))
	lastTime := time.Now()
	start := time.Now()
	recvCnt := 0
	for time.Since(start) < opts.QueueScanInterval*2 {
		select {
		case <-time.After(shortWaitTime):
			t.Error("timeout recv")
			return
		case outputMsg, ok := <-channel.clientMsgChan:
			if !ok {
				t.Error("eror recv")
				return
			}
			channel.inFlightMutex.Lock()
			waitingReq := len(channel.waitingRequeueMsgs)
			reqChanLen := len(channel.requeuedMsgChan)
			channel.inFlightMutex.Unlock()

			recvCnt++
			now := time.Now()
			t.Logf("recv: %v, consume %v at %s last %s, %v, %v", recvCnt, outputMsg.ID, now, lastTime, waitingReq, reqChanLen)
			if time.Since(lastTime) > shortWaitTime {
				t.Errorf("too long waiting")
			}
			lastTime = now
			channel.StartInFlightTimeout(outputMsg, NewFakeConsumer(1), "", opts.MsgTimeout)
			if recvCnt == count {
				go func() {
					time.Sleep(shortWaitTime / 10)
					channel.nsqdNotify.NotifyScanChannel(channel, false)
				}()
			}
			if recvCnt >= count {
				channel.FinishMessage(1, "", outputMsg.ID)
			} else {
				channel.RequeueMessage(1, "", outputMsg.ID, time.Millisecond, true)
			}
			if channel.Depth() == 0 {
				return
			}
		}
	}
	t.Errorf("should return early")
}

func TestChannelReqNowTooMuch(t *testing.T) {
	// test the 0 requeued message should be avoided if too much attempts
	count := 5
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.MaxRdyCount = 100
	// use large to delay the period scan
	opts.QueueScanRefreshInterval = shortWaitTime * 2
	opts.QueueScanInterval = shortWaitTime * 2
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_requeued_refill" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	channel := topic.GetChannel("channel")

	for i := 0; i < count; i++ {
		msg := NewMessage(0, []byte("test"))
		_, _, _, _, err := topic.PutMessage(msg)
		equal(t, err, nil)
	}

	channel.AddClient(1, NewFakeConsumer(1))
	start := time.Now()
	reqCnt := 0
	timeout := 0
	for time.Since(start) < opts.QueueScanInterval*2 {
		select {
		case <-time.After(opts.MsgTimeout):
			timeout++
		case outputMsg, ok := <-channel.clientMsgChan:
			if !ok {
				t.Error("eror recv")
				return
			}
			channel.inFlightMutex.Lock()
			waitingReq := len(channel.waitingRequeueMsgs)
			reqChanLen := len(channel.requeuedMsgChan)
			channel.inFlightMutex.Unlock()

			reqCnt++
			now := time.Now()
			t.Logf("consume %v at %s , %v, %v", outputMsg.ID, now, waitingReq, reqChanLen)
			channel.StartInFlightTimeout(outputMsg, NewFakeConsumer(1), "", opts.MsgTimeout)
			err := channel.RequeueMessage(1, "", outputMsg.ID, 0, true)
			ast.True(t, outputMsg.Attempts() <= MaxAttempts)
			if outputMsg.Attempts() >= MaxAttempts/2 {
				ast.NotNil(t, err)
			}
		}
	}

	t.Logf("total req cnt: %v, timeout: %v", reqCnt, timeout)
	if timeout <= count/2 {
		t.Errorf("should waiting for req 0 too much")
	}
	ast.True(t, reqCnt >= count*MaxMemReqTimes*10)
	ast.True(t, reqCnt < count*MaxMemReqTimes*20)
}

func TestChannelReqShortTooMuch(t *testing.T) {
	// test the 0 requeued message should be avoided if too much attempts
	count := 5
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.MaxRdyCount = 100
	// use large to delay the period scan
	opts.QueueScanRefreshInterval = longWaitTime / 2
	opts.QueueScanInterval = time.Millisecond * 2
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_requeued_short" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	channel := topic.GetChannel("channel")

	for i := 0; i < count; i++ {
		msg := NewMessage(0, []byte("test"))
		_, _, _, _, err := topic.PutMessage(msg)
		equal(t, err, nil)
	}

	channel.AddClient(1, NewFakeConsumer(1))
	start := time.Now()
	reqCnt := 0
	timeout := 0
	for time.Since(start) < longWaitTime/2+time.Second*time.Duration(count) {
		select {
		case <-time.After(opts.MsgTimeout * 2):
			timeout++
		case outputMsg, ok := <-channel.clientMsgChan:
			if !ok {
				t.Error("eror recv")
				return
			}
			channel.inFlightMutex.Lock()
			waitingReq := len(channel.waitingRequeueMsgs)
			reqChanLen := len(channel.requeuedMsgChan)
			channel.inFlightMutex.Unlock()

			reqCnt++
			now := time.Now()
			t.Logf("consume %v at %s , %v, %v", outputMsg.ID, now, waitingReq, reqChanLen)
			channel.StartInFlightTimeout(outputMsg, NewFakeConsumer(1), "", opts.MsgTimeout)
			err := channel.RequeueMessage(1, "", outputMsg.ID, time.Millisecond, true)
			ast.True(t, outputMsg.Attempts() <= MaxAttempts)
			if outputMsg.Attempts() >= MaxAttempts/2 {
				ast.NotNil(t, err)
			}
		}
	}

	t.Logf("total req cnt: %v, timeout: %v", reqCnt, timeout)
	ast.True(t, reqCnt >= count*MaxMemReqTimes*10)
	ast.True(t, timeout > 1)
	ast.True(t, reqCnt < count*MaxMemReqTimes*20)
}

func TestChannelReqTooMuchInDeferShouldNotContinueReadBackend(t *testing.T) {
	count := 30
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.MaxConfirmWin = 10
	opts.MaxRdyCount = 100
	opts.MsgTimeout = opts.QueueScanInterval * 10
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_requeued_toomuch" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	channel := topic.GetChannel("channel")

	for i := 0; i < count; i++ {
		msg := NewMessage(0, []byte("test"))
		_, _, _, _, err := topic.PutMessage(msg)
		equal(t, err, nil)
	}

	channel.AddClient(1, NewFakeConsumer(1))
	start := time.Now()
	reqCnt := 0
	timeout := 0
	lastDelay := time.Now()
	for time.Since(start) < opts.QueueScanInterval*5 {
		select {
		case <-time.After(opts.MsgTimeout * 2):
			timeout++
		case outputMsg, ok := <-channel.clientMsgChan:
			if !ok {
				t.Error("eror recv")
				return
			}
			channel.inFlightMutex.Lock()
			waitingReq := len(channel.waitingRequeueMsgs)
			reqChanLen := len(channel.requeuedMsgChan)
			channel.inFlightMutex.Unlock()

			reqCnt++
			now := time.Now()
			t.Logf("consume %v at %s , %v, %v", outputMsg.ID, now, waitingReq, reqChanLen)
			// should not read too much from backend
			ast.True(t, int64(outputMsg.ID) <= opts.MaxConfirmWin+1, "should not read backend too much")
			channel.StartInFlightTimeout(outputMsg, NewFakeConsumer(1), "", opts.MsgTimeout)
			// requeue with different timeout to make sure the memory deferred cnt is high
			// since after timeout deferred cnt will be reset
			lastDelay = lastDelay.Add(opts.QueueScanInterval)
			delay := lastDelay.Sub(now)
			t.Logf("consume %v delay to %s, %s", outputMsg.ID, lastDelay, delay)
			channel.RequeueMessage(1, "", outputMsg.ID, delay, false)
		}
	}

	t.Logf("total req cnt: %v, timeout: %v", reqCnt, timeout)
	ast.True(t, int64(reqCnt) > opts.MaxConfirmWin)
	ast.Equal(t, 0, timeout)
	start = time.Now()
	for time.Since(start) < shortWaitTime {
		if channel.Depth() == 0 {
			break
		}
		select {
		case <-time.After(opts.MsgTimeout):
			timeout++
		case outputMsg, ok := <-channel.clientMsgChan:
			if !ok {
				t.Error("eror recv")
				return
			}
			channel.inFlightMutex.Lock()
			waitingReq := len(channel.waitingRequeueMsgs)
			reqChanLen := len(channel.requeuedMsgChan)
			channel.inFlightMutex.Unlock()

			reqCnt++
			now := time.Now()
			t.Logf("consume %v at %s , %v, %v", outputMsg.ID, now, waitingReq, reqChanLen)
			channel.StartInFlightTimeout(outputMsg, NewFakeConsumer(1), "", opts.MsgTimeout)
			channel.FinishMessage(1, "", outputMsg.ID)
		}
	}
	ast.Equal(t, int64(0), channel.Depth())
}

func TestChannelTimeoutTooMuchIShouldNotBlockingToolong(t *testing.T) {
	oldWait := timeoutBlockingWait
	defer func() {
		timeoutBlockingWait = oldWait
	}()
	timeoutBlockingWait = time.Second
	count := 300
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.MsgTimeout = time.Millisecond
	opts.MaxRdyCount = 100
	opts.MaxConfirmWin = 10
	// make it more chance to req the timeout to end
	opts.ReqToEndThreshold = time.Second
	_, _, nsqd := mustStartNSQD(opts)
	nsqd.SetReqToEndCB(func(ch *Channel, m *Message, to time.Duration) error {
		_, _, _, _, err := ch.FinishMessage(m.GetClientID(), "", m.ID)
		if err != nil {
			t.Logf("fin timeout failed: %v", m.ID)
		}
		return nil
	})
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_timeouted_toomuch" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	channel := topic.GetChannel("channel")

	for i := 0; i < count; i++ {
		msg := NewMessage(0, []byte("test"))
		_, _, _, _, err := topic.PutMessage(msg)
		equal(t, err, nil)
	}

	fc := NewFakeConsumer(1)
	channel.AddClient(1, fc)
	start := time.Now()
	reqCnt := 0
	timeout := 0
	checkTimeout := 0
	finCnt := 0
	for time.Since(start) < timeoutBlockingWait*maxTimeoutCntToReq*2 {
		recvChan := channel.clientMsgChan
		if fc.GetInflightCnt() > 2 {
			recvChan = nil
		}
		if channel.Depth() == 0 {
			break
		}
		select {
		case <-time.After(opts.MsgTimeout):
			timeout++
		case outputMsg, ok := <-recvChan:
			if !ok {
				t.Error("eror recv")
				return
			}
			reqCnt++
			now := time.Now()
			t.Logf("consume %v at %s , %v, inflight: %v", outputMsg.ID, now, outputMsg.timedoutCnt, fc.GetInflightCnt())
			channel.StartInFlightTimeout(outputMsg, fc, "", opts.MsgTimeout)
			fc.SendingMessage()
			channel.CheckIfTimeoutToomuch(outputMsg, opts.MsgTimeout)
			// requeue with different timeout to make sure the memory deferred cnt is high
			// since after timeout deferred cnt will be reset
			if outputMsg.ID <= 5 {
				checkTimeout++
			} else {
				channel.FinishMessage(1, "", outputMsg.ID)
				finCnt++
			}
		}
	}

	t.Logf("total req cnt: %v, timeout: %v, fin %v, depth: %v, check %v", reqCnt, timeout, finCnt, channel.Depth(), checkTimeout)
	ast.True(t, finCnt > 1)
	ast.Equal(t, int64(0), channel.Depth())
	ast.True(t, checkTimeout > 1)
}

func TestChannelEmptyWhileConfirmDelayMsg(t *testing.T) {
	// test confirm delay counter while empty channel
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_channel_empty_delay" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	channel := topic.GetChannel("channel")
	dq, err := topic.GetOrCreateDelayedQueueNoLock(nil)
	atomic.StoreInt64(&dq.SyncEvery, opts.SyncEvery*100)
	equal(t, err, nil)
	stopC := make(chan bool)
	for i := 0; i < 100; i++ {
		msg := NewMessage(0, []byte("test"))
		id, _, _, _, err := topic.PutMessage(msg)
		equal(t, err, nil)
		newMsg := msg.GetCopy()
		newMsg.ID = 0
		newMsg.DelayedType = ChannelDelayed

		newTimeout := time.Now().Add(time.Millisecond)
		newMsg.DelayedTs = newTimeout.UnixNano()

		newMsg.DelayedOrigID = id
		newMsg.DelayedChannel = channel.GetName()

		_, _, _, _, err = dq.PutDelayMessage(newMsg)
		equal(t, err, nil)
	}
	dq.ForceFlush()

	channel.skipChannelToEnd()
	channel.AddClient(1, NewFakeConsumer(1))
	go func() {
		for {
			select {
			case <-stopC:
				return
			default:
			}
			outputMsg, ok := <-channel.clientMsgChan

			if !ok {
				return
			}
			t.Logf("consume %v", outputMsg)
			channel.StartInFlightTimeout(outputMsg, NewFakeConsumer(0), "", opts.MsgTimeout)
			channel.FinishMessageForce(0, "", outputMsg.ID, true)
			time.Sleep(time.Millisecond)
		}
	}()

	go func() {
		for {
			select {
			case <-stopC:
				return
			default:
			}
			channel.skipChannelToEnd()
			_, dqCnt := channel.GetDelayedQueueConsumedState()
			if int64(dqCnt) == 0 && atomic.LoadInt64(&channel.deferredFromDelay) == 0 && channel.Depth() == 0 {
				close(stopC)
				return
			}
			time.Sleep(time.Millisecond * 10)
		}
	}()

	go func() {
		for {
			select {
			case <-stopC:
				return
			default:
			}
			checkOK := atomic.LoadInt64(&channel.deferredFromDelay) >= int64(0)
			equal(t, checkOK, true)
			if !checkOK {
				close(stopC)
				return
			}
			time.Sleep(time.Microsecond * 10)
		}
	}()
	done := false
	for done {
		select {
		case <-stopC:
			done = true
		case <-time.After(time.Second * 3):
			_, dqCnt := channel.GetDelayedQueueConsumedState()
			if int64(dqCnt) == 0 && atomic.LoadInt64(&channel.deferredFromDelay) == 0 && channel.Depth() == 0 {
				close(stopC)
				done = true
			}
		}
	}
	t.Logf("stopped %v, %v", atomic.LoadInt64(&channel.deferredFromDelay), channel.GetChannelDebugStats())
	time.Sleep(time.Second * 3)
	// make sure all delayed counter is not more or less
	equal(t, atomic.LoadInt64(&channel.deferredFromDelay) == int64(0), true)
}

func TestChannelEmptyWhileReqDelayedMessageWaitingDispatchToClient(t *testing.T) {
	// test confirm delay counter while empty channel
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_channel_empty_req_delay" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	channel := topic.GetChannel("channel")
	dq, err := topic.GetOrCreateDelayedQueueNoLock(nil)
	atomic.StoreInt64(&dq.SyncEvery, opts.SyncEvery*100)
	equal(t, err, nil)
	// write at least 2*MaxWaitingDelayed to test the second peek from delayed queue
	for i := 0; i < 3*MaxWaitingDelayed+1; i++ {
		msg := NewMessage(0, []byte("test"))
		id, _, _, _, err := topic.PutMessage(msg)
		equal(t, err, nil)
		newMsg := msg.GetCopy()
		newMsg.ID = 0
		newMsg.DelayedType = ChannelDelayed

		newTimeout := time.Now().Add(time.Millisecond)
		newMsg.DelayedTs = newTimeout.UnixNano()

		newMsg.DelayedOrigID = id
		newMsg.DelayedChannel = channel.GetName()

		_, _, _, _, err = dq.PutDelayMessage(newMsg)
		equal(t, err, nil)
	}
	dq.ForceFlush()

	t.Logf("current %v, %v", atomic.LoadInt64(&channel.deferredFromDelay), channel.GetChannelDebugStats())
	channel.AddClient(1, NewFakeConsumer(1))
	channel.skipChannelToEnd()
	// wait peek delayed messages
	time.Sleep(opts.QueueScanInterval * 2)
	// the clientMsgChan should block waiting
	channel.inFlightMutex.Lock()
	waitChCnt := len(channel.waitingRequeueChanMsgs)
	realWaitChCnt := len(channel.requeuedMsgChan)
	waitReqMoreCnt := len(channel.waitingRequeueMsgs)
	channel.inFlightMutex.Unlock()
	t.Logf("current %v, %v", atomic.LoadInt64(&channel.deferredFromDelay), channel.GetChannelDebugStats())
	ast.True(t, waitChCnt > 0, "should have wait req count")
	ast.Equal(t, waitChCnt, realWaitChCnt+1)
	ast.Equal(t, int64(waitChCnt+waitReqMoreCnt), atomic.LoadInt64(&channel.deferredFromDelay))

	e := channel.GetChannelEnd()
	queueOffset := int64(e.Offset())
	cnt := e.TotalMsgCnt()
	channel.SetConsumeOffset(BackendOffset(queueOffset), cnt, true)
	time.Sleep(time.Millisecond)

	s := time.Now()
	// continue consume
	done := false
	for !done {
		ticker := time.NewTimer(time.Second * 3)
		select {
		case outputMsg, ok := <-channel.clientMsgChan:
			if !ok {
				done = true
				break
			}
			t.Logf("consume %v", outputMsg)
			channel.StartInFlightTimeout(outputMsg, NewFakeConsumer(0), "", opts.MsgTimeout)
			channel.FinishMessageForce(0, "", outputMsg.ID, true)
		case <-ticker.C:
			_, dqCnt := channel.GetDelayedQueueConsumedState()
			if dqCnt == 0 {
				done = true
				break
			}
			if time.Since(s) > time.Minute {
				t.Errorf("timeout waiting consume delayed messages: %v", dqCnt)
				done = true
				break
			}
			continue
		}
	}

	t.Logf("stopped %v, %v", atomic.LoadInt64(&channel.deferredFromDelay), channel.GetChannelDebugStats())
	_, dqCnt := channel.GetDelayedQueueConsumedState()
	ast.Equal(t, uint64(0), dqCnt)
	// make sure all delayed counter is not more or less
	equal(t, atomic.LoadInt64(&channel.deferredFromDelay), int64(0))
	equal(t, atomic.LoadInt64(&channel.deferredCount), int64(0))
}

func TestChannelEmptyWhileReqDelayedMessageWaitingInReq(t *testing.T) {
	// test confirm delay counter while empty channel
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	// use small max ready count to make sure the req chan to be full and more delayed messages is waiting in requeued map
	opts.MaxRdyCount = 2
	opts.SyncEvery = 100
	opts.SyncTimeout = time.Second * 10
	// make sure scan for full delayed, so we need scan slow to make sure all delayed messages can be poped
	opts.QueueScanInterval = time.Second * 2
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_channel_empty_req_delay" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	channel := topic.GetChannel("channel")
	dq, err := topic.GetOrCreateDelayedQueueNoLock(nil)
	atomic.StoreInt64(&dq.SyncEvery, opts.SyncEvery)
	equal(t, err, nil)
	// write at least 2*MaxWaitingDelayed to test the second peek from delayed queue
	for i := 0; i < 2*MaxWaitingDelayed+1; i++ {
		msg := NewMessage(0, []byte("test"))
		id, _, _, _, err := topic.PutMessage(msg)
		equal(t, err, nil)
		newMsg := msg.GetCopy()
		newMsg.ID = 0
		newMsg.DelayedType = ChannelDelayed

		newTimeout := time.Now().Add(time.Second * 4)
		newMsg.DelayedTs = newTimeout.UnixNano()

		newMsg.DelayedOrigID = id
		newMsg.DelayedChannel = channel.GetName()

		_, _, _, _, err = dq.PutDelayMessage(newMsg)
		equal(t, err, nil)
	}

	dq.ForceFlush()
	topic.ForceFlush()
	t.Logf("current %v, %v", atomic.LoadInt64(&channel.deferredFromDelay), channel.GetChannelDebugStats())
	channel.AddClient(1, NewFakeConsumer(1))
	channel.skipChannelToEnd()
	// write a message for req in mem
	for i := 0; i < 1; i++ {
		msg := NewMessage(0, []byte("testreq"))
		_, _, _, _, err := topic.PutMessage(msg)
		equal(t, err, nil)
	}
	topic.ForceFlush()
	select {
	case outputMsg, _ := <-channel.clientMsgChan:
		t.Logf("consume %v", outputMsg)
		// make sure we are not requeue the delayed messages
		ast.NotEqual(t, ChannelDelayed, outputMsg.DelayedType)
		channel.StartInFlightTimeout(outputMsg, NewFakeConsumer(1), "", opts.MsgTimeout)
		//  use short req to make sure the mem req will be poped first to req chan
		channel.RequeueMessage(1, "", outputMsg.ID, opts.QueueScanInterval/10, false)
	}

	t.Logf("current %v, %v", atomic.LoadInt64(&channel.deferredFromDelay), channel.GetChannelDebugStats())
	equal(t, atomic.LoadInt64(&channel.deferredCount), int64(1))
	s := time.Now()
	for {
		if time.Since(s) > longWaitTime*2 {
			t.Error("timeout waiting req")
			return
		}
		// wait peek delayed messages
		time.Sleep(opts.QueueScanInterval)
		// the clientMsgChan should block waiting
		channel.inFlightMutex.Lock()
		waitChCnt := len(channel.waitingRequeueChanMsgs)
		realWaitChCnt := len(channel.requeuedMsgChan)
		waitReqMoreCnt := len(channel.waitingRequeueMsgs)
		inflightCnt := len(channel.inFlightMessages)
		channel.inFlightMutex.Unlock()
		t.Logf("current %v, %v", atomic.LoadInt64(&channel.deferredFromDelay), channel.GetChannelDebugStats())
		if waitChCnt <= 0 || waitReqMoreCnt <= 0 || waitChCnt != realWaitChCnt+1 {
			continue
		}
		ast.True(t, waitChCnt > 0, "should have wait req count")
		ast.True(t, waitReqMoreCnt > 0, "should have wait more req count")
		ast.Equal(t, waitChCnt, realWaitChCnt+1)
		// should pop memory requeued message first
		ast.Equal(t, 0, inflightCnt)
		ast.InDelta(t, int64(waitChCnt+waitReqMoreCnt), atomic.LoadInt64(&channel.deferredFromDelay), 1)
		break
	}

	equal(t, atomic.LoadInt64(&channel.deferredCount), int64(0))
	e := channel.GetChannelEnd()
	queueOffset := int64(e.Offset())
	cnt := e.TotalMsgCnt()
	channel.SetConsumeOffset(BackendOffset(queueOffset), cnt, true)
	time.Sleep(time.Millisecond * 10)

	t.Logf("current state %v, %v", atomic.LoadInt64(&channel.deferredFromDelay), channel.GetChannelDebugStats())
	s = time.Now()
	// continue consume
	done := false
	for !done {
		ticker := time.NewTimer(time.Second * 3)
		select {
		case outputMsg, ok := <-channel.clientMsgChan:
			if !ok {
				done = true
				break
			}
			t.Logf("consume %v", outputMsg)
			channel.StartInFlightTimeout(outputMsg, NewFakeConsumer(0), "", opts.MsgTimeout)
			channel.FinishMessageForce(0, "", outputMsg.ID, true)
		case <-ticker.C:
			_, dqCnt := channel.GetDelayedQueueConsumedState()
			if dqCnt == 0 {
				done = true
				break
			}
			if time.Since(s) > time.Minute {
				t.Errorf("timeout waiting consume delayed messages: %v", dqCnt)
				done = true
				break
			}
			continue
		}
	}

	t.Logf("stopped %v, %v", atomic.LoadInt64(&channel.deferredFromDelay), channel.GetChannelDebugStats())
	_, dqCnt := channel.GetDelayedQueueConsumedState()
	ast.Equal(t, uint64(0), dqCnt)
	// make sure all delayed counter is not more or less
	equal(t, atomic.LoadInt64(&channel.deferredFromDelay), int64(0))
	equal(t, atomic.LoadInt64(&channel.deferredCount), int64(0))
}

func TestChannelResetConfirmedWhileDelayedQueuePop(t *testing.T) {
	// When the channel reset to confirmed (leader changed), the
	// message will reload from disk queue. Before the reload message go into inflight,
	// if the delayed queue pop a message and try requeued to requeued chan (since the
	// reload message has not go into inflight by now, it will success), then the reload message
	// go into inflight, after that the delayed message later will conflict while try go into inflight.
	// In this case, the delayed count should be handled
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.QueueScanInterval = time.Second
	// use small max ready count to make sure the req chan to be full and more delayed messages is waiting in requeued map
	opts.MaxRdyCount = 2
	opts.MsgTimeout = time.Second * 60
	if testing.Verbose() {
		opts.LogLevel = 2
		SetLogger(opts.Logger)
	}
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_channel_reset_while_delayed_pop" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	channel := topic.GetChannel("channel")
	dq, err := topic.GetOrCreateDelayedQueueNoLock(nil)
	atomic.StoreInt64(&dq.SyncEvery, opts.SyncEvery*100)
	equal(t, err, nil)
	// write at least 2*MaxWaitingDelayed to test the second peek from delayed queue
	var resetOffset BackendQueueEnd
	for i := 0; i < 2*MaxWaitingDelayed+1; i++ {
		msg := NewMessage(0, []byte("test"))
		id, _, _, qe, err := topic.PutMessage(msg)
		equal(t, err, nil)
		if i == MaxWaitingDelayed/2 {
			// make sure the reset offset message can be poped from delayed queue in the first batch
			resetOffset = qe
		}
		newMsg := msg.GetCopy()
		newMsg.ID = 0
		newMsg.DelayedType = ChannelDelayed

		newTimeout := time.Now().Add(time.Second * 2)
		newMsg.DelayedTs = newTimeout.UnixNano()

		newMsg.DelayedOrigID = id
		newMsg.DelayedChannel = channel.GetName()

		_, _, _, _, err = dq.PutDelayMessage(newMsg)
		equal(t, err, nil)
	}
	dq.ForceFlush()

	t.Logf("current %v, %v", atomic.LoadInt64(&channel.deferredFromDelay), channel.GetChannelDebugStats())
	channel.AddClient(1, NewFakeConsumer(1))
	channel.AddClient(2, NewFakeConsumer(2))
	channel.SetConsumeOffset(resetOffset.Offset(), resetOffset.TotalMsgCnt(), true)
	// make sure the reload message from disk queue first pumped
	time.Sleep(time.Second)

	// before we consume from reset confirmed, we wait the delayed queue pop
	s := time.Now()
	for {
		if time.Since(s) > time.Minute {
			t.Error("timeout waiting")
			return
		}
		// wait peek delayed messages
		time.Sleep(time.Second)
		// the clientMsgChan should block waiting
		channel.inFlightMutex.Lock()
		waitChCnt := len(channel.waitingRequeueChanMsgs)
		realWaitChCnt := len(channel.requeuedMsgChan)
		waitReqMoreCnt := len(channel.waitingRequeueMsgs)
		inflightCnt := len(channel.inFlightMessages)
		channel.inFlightMutex.Unlock()
		t.Logf("current %v, %v", atomic.LoadInt64(&channel.deferredFromDelay), channel.GetChannelDebugStats())
		// make sure the reset message is poped from delayed queue
		if waitChCnt <= 0 || waitReqMoreCnt <= 0 || waitChCnt+waitReqMoreCnt < int(resetOffset.TotalMsgCnt()) {
			continue
		}
		ast.True(t, waitChCnt > 0, "should have wait req count")
		ast.True(t, waitReqMoreCnt > 0, "should have wait more req count")
		ast.Equal(t, waitChCnt, realWaitChCnt)
		ast.Equal(t, 0, inflightCnt)
		ast.Equal(t, int64(waitChCnt+waitReqMoreCnt), atomic.LoadInt64(&channel.deferredFromDelay))
		break
	}

	var resetFirstMsgID MessageID
	select {
	case outputMsg, _ := <-channel.clientMsgChan:
		t.Logf("consume %v", outputMsg)
		// make sure we are not requeue the delayed messages
		ast.NotEqual(t, ChannelDelayed, outputMsg.DelayedType)
		channel.StartInFlightTimeout(outputMsg, NewFakeConsumer(1), "", opts.MsgTimeout)
		resetFirstMsgID = outputMsg.ID
	}
	t.Logf("reset msg id %v", resetFirstMsgID)
	// wait conflict from delayed message (which has the same id) try go into inflight
	done := false
	s = time.Now()
	for !done {
		ticker := time.NewTimer(time.Second)
		select {
		case outputMsg, _ := <-channel.clientMsgChan:
			t.Logf("consume %v", outputMsg)
			if outputMsg.DelayedType == ChannelDelayed {
				_, err = channel.StartInFlightTimeout(outputMsg, NewFakeConsumer(2), "", opts.MsgTimeout)
				if outputMsg.ID == resetFirstMsgID {
					t.Logf("got the same id from delayed queue")
					ast.NotNil(t, err)
					done = true
					break
				} else {
					ast.Nil(t, err)
					channel.FinishMessageForce(0, "", outputMsg.ID, true)
				}
			} else {
				if outputMsg.ID == resetFirstMsgID {
					t.Errorf("should not got the waiting handle message for timeout")
				}
				channel.StartInFlightTimeout(outputMsg, NewFakeConsumer(0), "", opts.MsgTimeout)
				channel.FinishMessageForce(0, "", outputMsg.ID, true)
			}
		case <-ticker.C:
			if time.Since(s) > time.Minute {
				t.Errorf("timeout waiting")
				done = true
				break
			}
		}
	}

	channel.FinishMessageForce(1, "", resetFirstMsgID, true)
	s = time.Now()
	done = false
	// continue consume
	for !done {
		ticker := time.NewTimer(time.Second)
		select {
		case outputMsg, ok := <-channel.clientMsgChan:
			if !ok {
				done = true
				break
			}
			t.Logf("consume %v", outputMsg)
			channel.StartInFlightTimeout(outputMsg, NewFakeConsumer(0), "", opts.MsgTimeout)
			channel.FinishMessageForce(0, "", outputMsg.ID, true)
		case <-ticker.C:
			_, dqCnt := channel.GetDelayedQueueConsumedState()
			if dqCnt == 0 {
				done = true
				break
			}
			if time.Since(s) > time.Minute {
				t.Errorf("timeout waiting consume delayed messages: %v", dqCnt)
				done = true
				break
			}
			continue
		}
	}

	t.Logf("stopped %v, %v", atomic.LoadInt64(&channel.deferredFromDelay), channel.GetChannelDebugStats())
	_, dqCnt := channel.GetDelayedQueueConsumedState()
	ast.Equal(t, uint64(0), dqCnt)
	// make sure all delayed counter is not more or less
	equal(t, atomic.LoadInt64(&channel.deferredFromDelay), int64(0))
	equal(t, atomic.LoadInt64(&channel.deferredCount), int64(0))
	equal(t, channel.Depth(), int64(0))
	channel.inFlightMutex.Lock()
	waitChCnt := len(channel.waitingRequeueChanMsgs)
	realWaitChCnt := len(channel.requeuedMsgChan)
	waitReqMoreCnt := len(channel.waitingRequeueMsgs)
	inflightCnt := len(channel.inFlightMessages)
	channel.inFlightMutex.Unlock()
	equal(t, waitChCnt+realWaitChCnt+waitReqMoreCnt+inflightCnt, int(0))
}

func TestChannelResetConfirmedWhileDelayedQueueMsgInflight(t *testing.T) {
	// While there is a delayed message in inflight waiting ack,
	// the channel reset to confirmed (leader changed after draining) and the
	// message reload from disk queue will conflict while try go into inflight.
	// In this case, the normal message may never go into inflight which leave the confirmed offset
	// blocking. So we need handle this by checking conflict
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.QueueScanInterval = time.Second
	// use small max ready count to make sure the req chan to be full and more delayed messages is waiting in requeued map
	opts.MaxRdyCount = 2
	opts.MsgTimeout = time.Second * 60
	if testing.Verbose() {
		opts.LogLevel = 2
		SetLogger(opts.Logger)
	}
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_channel_reset_while_delayed_pop" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	channel := topic.GetChannel("channel")
	dq, err := topic.GetOrCreateDelayedQueueNoLock(nil)
	atomic.StoreInt64(&dq.SyncEvery, opts.SyncEvery*100)
	equal(t, err, nil)
	// write at least 2*MaxWaitingDelayed to test the second peek from delayed queue
	var resetOffset BackendQueueEnd
	for i := 0; i < 2*MaxWaitingDelayed+1; i++ {
		msg := NewMessage(0, []byte("test"))
		id, msgOffset, _, qe, err := topic.PutMessage(msg)
		equal(t, err, nil)
		if i == MaxWaitingDelayed/2 {
			// make sure the reset offset message can be poped from delayed queue in the first batch
			resetOffset = qe
		}
		msg.Offset = msgOffset
		newMsg := msg.GetCopy()
		newMsg.ID = 0
		newMsg.DelayedType = ChannelDelayed

		newTimeout := time.Now().Add(time.Second)
		newMsg.DelayedTs = newTimeout.UnixNano()

		newMsg.DelayedOrigID = id
		newMsg.DelayedChannel = channel.GetName()

		_, _, _, _, err = dq.PutDelayMessage(newMsg)
		equal(t, err, nil)
	}
	dq.ForceFlush()

	t.Logf("current %v, %v", atomic.LoadInt64(&channel.deferredFromDelay), channel.GetChannelDebugStats())
	channel.AddClient(1, NewFakeConsumer(1))
	channel.AddClient(2, NewFakeConsumer(2))
	channel.SetConsumeOffset(resetOffset.Offset(), resetOffset.TotalMsgCnt(), true)
	time.Sleep(time.Second)
	// before we consume from reset confirmed, we wait the delayed queue pop, make sure the delayed message first go into inflight
	s := time.Now()
	for {
		if time.Since(s) > time.Minute {
			t.Error("timeout waiting")
			return
		}
		// wait peek delayed messages
		time.Sleep(time.Second)
		// the clientMsgChan should block waiting
		channel.inFlightMutex.Lock()
		waitChCnt := len(channel.waitingRequeueChanMsgs)
		realWaitChCnt := len(channel.requeuedMsgChan)
		waitReqMoreCnt := len(channel.waitingRequeueMsgs)
		inflightCnt := len(channel.inFlightMessages)
		channel.inFlightMutex.Unlock()
		t.Logf("current %v, %v", atomic.LoadInt64(&channel.deferredFromDelay), channel.GetChannelDebugStats())
		// make sure the reset message is poped from delayed queue
		if waitChCnt <= 0 || waitReqMoreCnt <= 0 || waitChCnt+waitReqMoreCnt < int(resetOffset.TotalMsgCnt()) {
			continue
		}
		ast.True(t, waitChCnt > 0, "should have wait req count")
		ast.True(t, waitReqMoreCnt > 0, "should have wait more req count")
		ast.Equal(t, waitChCnt, realWaitChCnt)
		ast.Equal(t, 0, inflightCnt)
		ast.Equal(t, int64(waitChCnt+waitReqMoreCnt), atomic.LoadInt64(&channel.deferredFromDelay))
		break
	}
	var resetFirstMsgID MessageID
	done := false
	s = time.Now()
	firstPoped := true
	for !done {
		ticker := time.NewTimer(time.Second)
		select {
		case outputMsg, _ := <-channel.clientMsgChan:
			t.Logf("consume %v", outputMsg)
			// ignore first message, if it is the message before reset
			if firstPoped && outputMsg.Offset != resetOffset.Offset() {
				_, err = channel.StartInFlightTimeout(outputMsg, NewFakeConsumer(1), "", opts.MsgTimeout)
				channel.FinishMessageForce(1, "", outputMsg.ID, true)
				continue
			}
			firstPoped = false
			// the first non-delayed queue message should be the reset message
			if resetFirstMsgID <= 0 && outputMsg.DelayedType != ChannelDelayed {
				_, err = channel.StartInFlightTimeout(outputMsg, NewFakeConsumer(1), "", time.Second)
				ast.Nil(t, err)
				resetFirstMsgID = outputMsg.ID
				// just ignore to make it timeout later
				continue
			}
			if outputMsg.Offset == resetOffset.Offset() {
				if outputMsg.DelayedType == ChannelDelayed {
					// make sure the normal message is removed in inflight
					channel.RequeueMessage(1, "", outputMsg.ID, 0, false)
					_, err = channel.StartInFlightTimeout(outputMsg, NewFakeConsumer(1), "", opts.MsgTimeout)
					ast.Nil(t, err)
					resetFirstMsgID = outputMsg.ID
					t.Logf("found the reset message %v", outputMsg)
					done = true
					break
				} else {
					// we use small timeout to allow trigger requeue(delayed message will ignore pop if this is inflight)
					_, err = channel.StartInFlightTimeout(outputMsg, NewFakeConsumer(1), "", time.Second)
					ast.Nil(t, err)
					channel.inFlightMutex.Lock()
					realWaitChCnt := len(channel.requeuedMsgChan)
					waitReqMoreCnt := len(channel.waitingRequeueMsgs)
					channel.inFlightMutex.Unlock()
					if realWaitChCnt < 2 && waitReqMoreCnt <= 0 {
						// requeue to make it remove out of inflight, so later the delayed message with same id can go into inflight
						channel.RequeueMessage(1, "", outputMsg.ID, 0, false)
					}
					continue
				}
			} else {
				_, err = channel.StartInFlightTimeout(outputMsg, NewFakeConsumer(1), "", opts.MsgTimeout)
				ast.Nil(t, err)
			}
			channel.FinishMessageForce(1, "", outputMsg.ID, true)
		case <-ticker.C:
			if time.Since(s) > time.Minute {
				t.Errorf("timeout waiting")
				done = true
				break
			}
		}
	}
	_, dqCnt := channel.GetDelayedQueueConsumedState()
	t.Logf("reset msg id %v, expect offset: %v, current left: %v, %v", resetFirstMsgID, resetOffset, dqCnt, channel.Depth())
	// wait conflict from normal queue (which has the same id) try go into inflight
	done = false
	s = time.Now()
	for !done {
		ticker := time.NewTimer(time.Second)
		select {
		case outputMsg, _ := <-channel.clientMsgChan:
			t.Logf("consume %v", outputMsg)
			if outputMsg.DelayedType != ChannelDelayed {
				_, err = channel.StartInFlightTimeout(outputMsg, NewFakeConsumer(2), "", opts.MsgTimeout)
				if outputMsg.ID == resetFirstMsgID {
					t.Logf("got the same id from reset queue")
					ast.NotNil(t, err)
					done = true
					break
				} else {
					ast.Nil(t, err)
					channel.FinishMessageForce(0, "", outputMsg.ID, true)
				}
			} else {
				if outputMsg.ID == resetFirstMsgID {
					t.Errorf("should not got the waiting handle message for timeout")
				}
				channel.StartInFlightTimeout(outputMsg, NewFakeConsumer(0), "", opts.MsgTimeout)
				channel.FinishMessageForce(0, "", outputMsg.ID, true)
			}
		case <-ticker.C:
			_, dqCnt := channel.GetDelayedQueueConsumedState()
			if dqCnt == 0 && channel.Depth() == 0 {
				t.Errorf("all message consumed before got conflict message")
				done = true
				break
			}
			if time.Since(s) > time.Minute {
				t.Errorf("timeout waiting")
				done = true
				break
			}
		}
	}

	channel.FinishMessageForce(1, "", resetFirstMsgID, true)
	s = time.Now()
	done = false
	// continue consume
	for !done {
		ticker := time.NewTimer(time.Second)
		select {
		case outputMsg, ok := <-channel.clientMsgChan:
			if !ok {
				done = true
				break
			}
			t.Logf("consume %v", outputMsg)
			channel.StartInFlightTimeout(outputMsg, NewFakeConsumer(0), "", opts.MsgTimeout)
			channel.FinishMessageForce(0, "", outputMsg.ID, true)
		case <-ticker.C:
			_, dqCnt := channel.GetDelayedQueueConsumedState()
			if dqCnt == 0 && channel.Depth() == 0 {
				done = true
				break
			}
			if time.Since(s) > time.Minute {
				t.Errorf("timeout waiting consume delayed messages: %v", dqCnt)
				done = true
				break
			}
			continue
		}
	}

	t.Logf("stopped %v, %v", atomic.LoadInt64(&channel.deferredFromDelay), channel.GetChannelDebugStats())
	_, dqCnt = channel.GetDelayedQueueConsumedState()
	ast.Equal(t, uint64(0), dqCnt)
	// make sure all delayed counter is not more or less
	equal(t, atomic.LoadInt64(&channel.deferredFromDelay), int64(0))
	equal(t, atomic.LoadInt64(&channel.deferredCount), int64(0))
	equal(t, channel.Depth(), int64(0))
	channel.inFlightMutex.Lock()
	waitChCnt := len(channel.waitingRequeueChanMsgs)
	realWaitChCnt := len(channel.requeuedMsgChan)
	waitReqMoreCnt := len(channel.waitingRequeueMsgs)
	inflightCnt := len(channel.inFlightMessages)
	channel.inFlightMutex.Unlock()
	equal(t, waitChCnt+realWaitChCnt+waitReqMoreCnt+inflightCnt, int(0))
}

func TestChannelHealth(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.MemQueueSize = 2
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topic := nsqd.GetTopicIgnPart("test")

	channel := topic.GetChannel("channel")
	// cause channel.messagePump to exit so we can set channel.backend without
	// a data race. side effect is it closes clientMsgChan, and messagePump is
	// never restarted. note this isn't the intended usage of exitChan but gets
	// around the data race without more invasive changes to how channel.backend
	// is set/loaded.
	channel.exitChan <- 1
}

func TestChannelSkip(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_channel_skip" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	channel := topic.GetChannel("channel")

	msgs := make([]*Message, 0, 10)
	for i := 0; i < 10; i++ {
		var msgId MessageID
		msgBytes := []byte(strconv.Itoa(i))
		msg := NewMessage(msgId, msgBytes)
		msgs = append(msgs, msg)
	}
	topic.PutMessages(msgs)

	var msgId MessageID
	msgBytes := []byte(strconv.Itoa(10))
	msg := NewMessage(msgId, msgBytes)
	_, backendOffsetMid, _, _, _ := topic.PutMessage(msg)
	topic.ForceFlush()
	equal(t, channel.Depth(), int64(11))

	msgs = make([]*Message, 0, 9)
	//put another 10 messages
	for i := 0; i < 9; i++ {
		var msgId MessageID
		msgBytes := []byte(strconv.Itoa(i + 11))
		msg := NewMessage(msgId, msgBytes)
		msgs = append(msgs, msg)
	}
	topic.PutMessages(msgs)
	topic.flushBuffer(true)
	time.Sleep(time.Millisecond)
	equal(t, channel.Depth(), int64(20))

	//skip forward to message 10
	t.Logf("backendOffsetMid: %d", backendOffsetMid)
	channel.SetConsumeOffset(backendOffsetMid, 10, true)
	time.Sleep(time.Second)
	// should first read the old read pos message
	for i := 0; i < 10; i++ {
		outputMsg := <-channel.clientMsgChan
		if i == 0 && outputMsg.Offset != backendOffsetMid {
			//this is message before reset, we can ignore and try next
			equal(t, string(outputMsg.Body[:]), strconv.Itoa(0))
			outputMsg = <-channel.clientMsgChan
		}
		equal(t, string(outputMsg.Body[:]), strconv.Itoa(i+10))
	}
}

func checkChannelCleanStats(t *testing.T, channel *Channel, expectedDepth int64) {
	chStats := NewChannelStats(channel, nil, 0)
	equal(t, chStats.InFlightCount, int(0))
	equal(t, chStats.DeferredCount, int(0))
	equal(t, chStats.Depth, expectedDepth)
	equal(t, chStats.DeferredFromDelayCount, int(0))
}

func TestChannelResetMaybeReadOneOldMsg(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_channel_reset_read_old" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	channel := topic.GetChannel("channel")

	msgs := make([]*Message, 0, 10)
	for i := 0; i < 10; i++ {
		var msgId MessageID
		msgBytes := []byte(strconv.Itoa(i))
		msg := NewMessage(msgId, msgBytes)
		msgs = append(msgs, msg)
	}
	topic.PutMessages(msgs)

	var msgId MessageID
	msgBytes := []byte(strconv.Itoa(10))
	msg := NewMessage(msgId, msgBytes)
	_, backendOffsetMid, _, _, _ := topic.PutMessage(msg)
	topic.ForceFlush()
	equal(t, channel.Depth(), int64(11))

	msgs = make([]*Message, 0, 9)
	//put another 10 messages
	for i := 0; i < 9; i++ {
		var msgId MessageID
		msgBytes := []byte(strconv.Itoa(i + 11))
		msg := NewMessage(msgId, msgBytes)
		msgs = append(msgs, msg)
	}
	topic.PutMessages(msgs)
	topic.flushBuffer(true)
	time.Sleep(time.Millisecond)
	equal(t, channel.Depth(), int64(20))

	//skip forward to message 10
	t.Logf("backendOffsetMid: %d", backendOffsetMid)
	channel.SetConsumeOffset(backendOffsetMid, 10, true)
	time.Sleep(time.Second)
	checkChannelCleanStats(t, channel, 10)
	for i := 0; i < 10; i++ {
		outputMsg := <-channel.clientMsgChan
		if i == 0 && outputMsg.Offset != backendOffsetMid {
			// first read the old read pos message
			equal(t, string(outputMsg.Body[:]), strconv.Itoa(0))
			channel.StartInFlightTimeout(outputMsg, NewFakeConsumer(0), "", time.Second)
			channel.FinishMessage(0, "", outputMsg.ID)
			outputMsg = <-channel.clientMsgChan
		}
		equal(t, string(outputMsg.Body[:]), strconv.Itoa(i+10))
		channel.StartInFlightTimeout(outputMsg, NewFakeConsumer(0), "", time.Second)
		channel.FinishMessage(0, "", outputMsg.ID)
	}
	checkChannelCleanStats(t, channel, 0)
}

func TestChannelResetWhileOneSingleMemDelayedMsg(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)

	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_channel_reset_delayed" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	channel := topic.GetChannel("channel")

	channel.AddClient(1, NewFakeConsumer(1))

	var msgId MessageID
	msgBytes := []byte(strconv.Itoa(10))
	msg := NewMessage(msgId, msgBytes)
	_, backendOffsetMid, _, _, _ := topic.PutMessage(msg)
	topic.ForceFlush()
	topic.flushBuffer(true)
	time.Sleep(time.Millisecond)
	equal(t, channel.Depth(), int64(1))
	// make sure we have one mem delayed msg
	outputMsg := <-channel.clientMsgChan
	channel.StartInFlightTimeout(outputMsg, NewFakeConsumer(0), "", time.Second)
	channel.RequeueMessage(0, "", outputMsg.ID, time.Second, true)
	equal(t, atomic.LoadInt64(&channel.deferredCount), int64(1))
	// wait until timeout
	time.Sleep(time.Second * 2)
	checkChannelCleanStats(t, channel, 1)
	//skip forward
	t.Logf("backendOffsetMid: %d", backendOffsetMid)
	channel.SetConsumeOffset(channel.GetChannelEnd().Offset(), 1, true)
	time.Sleep(time.Second)
	select {
	case outputMsg = <-channel.clientMsgChan:
		equal(t, string(outputMsg.Body[:]), strconv.Itoa(10))
		channel.StartInFlightTimeout(outputMsg, NewFakeConsumer(0), "", time.Second)
		channel.FinishMessage(0, "", outputMsg.ID)
	case <-time.After(time.Second):
	}

	checkChannelCleanStats(t, channel, 0)
}

func TestChannelResetWhileOneSingleDiskDelayedMsg(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)

	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_channel_reset_delayed" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	channel := topic.GetChannel("channel")
	channel.AddClient(1, NewFakeConsumer(1))

	var msgId MessageID
	msgBytes := []byte(strconv.Itoa(10))
	msg := NewMessage(msgId, msgBytes)
	_, backendOffsetMid, _, _, _ := topic.PutMessage(msg)
	topic.ForceFlush()
	topic.flushBuffer(true)
	time.Sleep(time.Millisecond)
	equal(t, channel.Depth(), int64(1))
	// make sure we have one disk delayed msg
	outputMsg := <-channel.clientMsgChan
	channel.StartInFlightTimeout(outputMsg, NewFakeConsumer(0), "", time.Second)

	newMsg := outputMsg.GetCopy()
	newMsg.ID = 0
	newMsg.DelayedType = ChannelDelayed
	newTimeout := time.Now().Add(time.Second)
	newMsg.DelayedTs = newTimeout.UnixNano()

	newMsg.DelayedOrigID = outputMsg.ID
	newMsg.DelayedChannel = channel.GetName()

	topic.Lock()
	dq, _ := topic.GetOrCreateDelayedQueueNoLock(nil)
	dq.PutDelayMessage(newMsg)
	topic.Unlock()

	channel.FinishMessage(0, "", outputMsg.ID)
	checkChannelCleanStats(t, channel, 0)

	// wait until timeout
	time.Sleep(time.Second * 2)
	chStats := NewChannelStats(channel, nil, 0)
	equal(t, chStats.DeferredFromDelayCount, int(1))
	//skip forward
	t.Logf("backendOffsetMid: %d", backendOffsetMid)
	channel.SetConsumeOffset(channel.GetChannelEnd().Offset(), 1, true)
	time.Sleep(time.Second)
	chStats = NewChannelStats(channel, nil, 0)
	equal(t, chStats.DeferredFromDelayCount, int(1))

	select {
	case outputMsg = <-channel.clientMsgChan:
		equal(t, string(outputMsg.Body[:]), strconv.Itoa(10))

		channel.StartInFlightTimeout(outputMsg, NewFakeConsumer(0), "", time.Second)
		channel.FinishMessage(0, "", outputMsg.ID)
	case <-time.After(time.Second):
	}

	checkChannelCleanStats(t, channel, 0)
}

func TestChannelSkipZanTestForOrdered(t *testing.T) {
	// while the ordered message is timeouted and requeued,
	// change the state to skip zan test may block waiting the next
	// we test this case for ordered topic
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.MsgTimeout = time.Second * 2
	opts.AllowZanTestSkip = true
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_channel_skiptest_order" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicWithExt(topicName, 0, true)
	channel := topic.GetChannel("order_channel")
	channel.SetOrdered(true)
	channel.doSkipZanTest(false)

	msgs := make([]*Message, 0, 3)
	for i := 0; i < 3; i++ {
		var msgId MessageID
		msgBytes := []byte(strconv.Itoa(i))
		msg := NewMessage(msgId, msgBytes)
		msgs = append(msgs, msg)
	}
	topic.PutMessages(msgs)

	msgs = make([]*Message, 0, 3)
	//put another 10 zan test messages
	for i := 0; i < 3; i++ {
		var msgId MessageID
		msgBytes := []byte("zan_test")
		extJ := simpleJson.New()
		extJ.Set(ext.ZAN_TEST_KEY, true)
		extBytes, _ := extJ.Encode()
		msg := NewMessageWithExt(msgId, msgBytes, ext.JSON_HEADER_EXT_VER, extBytes)
		msgs = append(msgs, msg)
	}
	topic.PutMessages(msgs)
	topic.flushBuffer(true)
	time.Sleep(time.Millisecond)

	msgs = make([]*Message, 0, 3)
	// put another normal messsages
	for i := 0; i < 3; i++ {
		var msgId MessageID
		msgBytes := []byte(strconv.Itoa(i + 11 + 10))
		msg := NewMessage(msgId, msgBytes)
		msgs = append(msgs, msg)
	}
	topic.PutMessages(msgs)
	topic.flushBuffer(true)
	time.Sleep(time.Second)
	// consume normal message and some test message
	for i := 0; i < 3; i++ {
		outputMsg := <-channel.clientMsgChan
		t.Logf("consume %v", string(outputMsg.Body))
		channel.StartInFlightTimeout(outputMsg, NewFakeConsumer(0), "", opts.MsgTimeout)
		channel.FinishMessageForce(0, "", outputMsg.ID, true)
		channel.ContinueConsumeForOrder()
	}
	time.Sleep(time.Millisecond * 10)
	// make sure zan test timeout
	outputMsg := <-channel.clientMsgChan
	t.Logf("consume %v", string(outputMsg.Body))
	channel.StartInFlightTimeout(outputMsg, NewFakeConsumer(0), "", opts.MsgTimeout)
	equal(t, []byte("zan_test"), outputMsg.Body)
	time.Sleep(time.Millisecond * 10)
	// skip zan test soon to make sure the zan test is inflight
	channel.doSkipZanTest(true)
	time.Sleep(time.Second * 3)
	toC := time.After(time.Second * 30)

	// set zan test skip and should continue consume normal messages
	for i := 0; i < 3; i++ {
		select {
		case outputMsg = <-channel.clientMsgChan:
		case <-toC:
			t.Errorf("timeout waiting consume")
			return
		}
		t.Logf("consume %v, %v, %v", string(outputMsg.Body), string(outputMsg.ExtBytes), outputMsg.ExtVer)
		channel.StartInFlightTimeout(outputMsg, NewFakeConsumer(0), "", opts.MsgTimeout)
		channel.FinishMessageForce(0, "", outputMsg.ID, true)
		channel.ContinueConsumeForOrder()
		nequal(t, []byte("zan_test"), outputMsg.Body)
		if channel.Depth() == 0 {
			break
		}
	}
	equal(t, channel.Depth(), int64(0))
}

func TestChannelSkipMsgRetryTooMuchCnt(t *testing.T) {
	// while the ordered message is timeouted and requeued,
	// change the state to skip zan test may block waiting the next
	// we test this case for ordered topic
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.QueueScanInterval = time.Millisecond * 10
	opts.MsgTimeout = time.Millisecond
	opts.AckOldThanTime = opts.MsgTimeout
	opts.AckRetryCnt = 2
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_channel_skiptest_toomuch_retry" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicWithExt(topicName, 0, false)
	channel := topic.GetChannel("channel")

	msgs := make([]*Message, 0, 3)
	for i := 0; i < 3; i++ {
		var msgId MessageID
		msgBytes := []byte(strconv.Itoa(i))
		msg := NewMessage(msgId, msgBytes)
		msgs = append(msgs, msg)
	}
	topic.PutMessages(msgs)
	topic.flushBuffer(true)
	time.Sleep(time.Millisecond)
	// consume and wait timeout
	toC := time.After(time.Second)
	for i := 0; i < 3*(opts.AckRetryCnt+2); i++ {
		if channel.Depth() == 0 {
			return
		}
		var outputMsg *Message
		select {
		case outputMsg = <-channel.clientMsgChan:
		case <-toC:
			if channel.Depth() == 0 {
				return
			}
			t.Errorf("timeout waiting consume")
			return
		}
		t.Logf("consume %v attempt %v, limit: %v", outputMsg.ID, outputMsg.Attempts(), opts.AckRetryCnt)
		equal(t, int(outputMsg.Attempts()) <= opts.AckRetryCnt, true)
		channel.StartInFlightTimeout(outputMsg, NewFakeConsumer(0), "", opts.MsgTimeout)
		// wait timeout
	}
	equal(t, channel.Depth(), int64(0))
}

func TestChannelSkipMsgRetryTooLongAgo(t *testing.T) {
	// while the ordered message is timeouted and requeued,
	// change the state to skip zan test may block waiting the next
	// we test this case for ordered topic
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.AckOldThanTime = shortWaitTime
	opts.AckRetryCnt = 1

	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_channel_skiptest_toomuch_retry" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicWithExt(topicName, 0, false)
	channel := topic.GetChannel("channel")

	msgs := make([]*Message, 0, 3)
	for i := 0; i < 3; i++ {
		var msgId MessageID
		msgBytes := []byte(strconv.Itoa(i))
		msg := NewMessage(msgId, msgBytes)
		msgs = append(msgs, msg)
	}
	topic.PutMessages(msgs)
	topic.flushBuffer(true)
	time.Sleep(time.Millisecond)
	// consume and wait timeout
	toC := time.After(opts.AckOldThanTime * 2)
	cnt := 0
	for {
		var outputMsg *Message
		select {
		case outputMsg = <-channel.clientMsgChan:
		case <-toC:
			if channel.Depth() == 0 {
				equal(t, cnt > 3*(opts.AckRetryCnt+2), true)
				return
			}
			t.Errorf("timeout waiting consume")
			return
		}
		t.Logf("consume %v attempt %v, limit: %v, cnt: %v", outputMsg.ID, outputMsg.Attempts(), opts.AckRetryCnt, cnt)
		cnt++
		channel.StartInFlightTimeout(outputMsg, NewFakeConsumer(0), "", opts.MsgTimeout)
		// wait timeout
	}
}

func TestChannelInitWithOldStart(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.MaxBytesPerFile = 1024
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_channel_init_oldstart" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	channel := topic.GetChannel("channel")
	channel2 := topic.GetChannel("channel2")
	channel3 := topic.GetChannel("channel3")

	msgs := make([]*Message, 0, 10)
	for i := 0; i < 10; i++ {
		var msgId MessageID
		msgBytes := []byte(strconv.Itoa(i))
		msg := NewMessage(msgId, msgBytes)
		msgs = append(msgs, msg)
	}
	topic.PutMessages(msgs)
	topic.flushBuffer(true)

	var msgId MessageID
	msgBytes := []byte(strconv.Itoa(10))
	msg := NewMessage(msgId, msgBytes)
	_, backendOffsetMid, _, _, _ := topic.PutMessage(msg)
	topic.ForceFlush()
	equal(t, channel.Depth(), int64(11))
	channel.SetConsumeOffset(backendOffsetMid, 10, true)
	time.Sleep(time.Second)
	outputMsg := <-channel.clientMsgChan
	if (outputMsg.Offset) != backendOffsetMid {
		//this is message before reset
		equal(t, string(outputMsg.Body[:]), strconv.Itoa(0))
		outputMsg = <-channel.clientMsgChan
	}
	equal(t, string(outputMsg.Body[:]), strconv.Itoa(10))
	topic.CloseExistingChannel("channel", false)
	time.Sleep(time.Second)

	msgs = make([]*Message, 0, 1000-10)
	for i := 10; i < 1000; i++ {
		var msgId MessageID
		msgBytes := []byte(strconv.Itoa(i))
		msg := NewMessage(msgId, msgBytes)
		msgs = append(msgs, msg)
	}
	_, putOffset, _, _, putEnd, _ := topic.PutMessages(msgs)
	topic.ForceFlush()
	t.Log(putEnd)
	t.Log(putOffset)

	var msgId2 MessageID
	msgBytes = []byte(strconv.Itoa(1001))
	msg = NewMessage(msgId2, msgBytes)
	topic.PutMessage(msg)
	topic.ForceFlush()

	channel2.skipChannelToEnd()
	channel3.SetConsumeOffset(putEnd.Offset(), putEnd.TotalMsgCnt(), true)
	topic.CloseExistingChannel(channel3.GetName(), false)
	topic.TryCleanOldData(1024*2, false, topic.backend.GetQueueReadEnd().Offset())
	t.Log(topic.GetQueueReadStart())
	t.Log(topic.backend.GetQueueReadEnd())

	// closed channel reopen should check if old confirmed is not less than cleaned disk segment start
	// and the reopened channel read end can update to the newest topic end
	channel = topic.GetChannel("channel")
	t.Log(channel.GetConfirmed())
	t.Log(channel.GetChannelEnd())

	equal(t, channel.GetConfirmed(), topic.backend.GetQueueReadStart())
	equal(t, channel.GetChannelEnd(), topic.backend.GetQueueReadEnd())
	channel3 = topic.GetChannel(channel3.GetName())

	t.Log(channel3.GetConfirmed())
	t.Log(channel3.GetChannelEnd())

	equal(t, channel3.GetConfirmed(), putEnd)
	equal(t, channel3.GetChannelEnd(), topic.backend.GetQueueReadEnd())
}

func TestChannelResetWithNewStartOverConfirmed(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.MaxBytesPerFile = 1024
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_channel_reset_withnew_start" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	channel := topic.GetChannel("channel")
	channel2 := topic.GetChannel("channel2")
	msgs := make([]*Message, 0, 10)
	for i := 0; i < 10; i++ {
		var msgId MessageID
		msgBytes := []byte(strconv.Itoa(i))
		msg := NewMessage(msgId, msgBytes)
		msgs = append(msgs, msg)
	}
	topic.PutMessages(msgs)
	topic.flushBuffer(true)

	var msgId MessageID
	msgBytes := []byte(strconv.Itoa(10))
	msg := NewMessage(msgId, msgBytes)
	_, backendOffsetMid, _, _, _ := topic.PutMessage(msg)
	topic.ForceFlush()
	equal(t, channel.Depth(), int64(11))
	channel.SetConsumeOffset(backendOffsetMid, 10, true)
	time.Sleep(time.Second)

	outputMsg := <-channel.clientMsgChan
	if (outputMsg.Offset) != backendOffsetMid {
		//this is message before reset
		equal(t, string(outputMsg.Body[:]), strconv.Itoa(0))
		outputMsg = <-channel.clientMsgChan
	}
	equal(t, string(outputMsg.Body[:]), strconv.Itoa(10))
	topic.CloseExistingChannel("channel", false)
	time.Sleep(time.Second)

	msgs = make([]*Message, 0, 1000-10)
	for i := 10; i < 1000; i++ {
		var msgId MessageID
		msgBytes := []byte(strconv.Itoa(i))
		msg := NewMessage(msgId, msgBytes)
		msgs = append(msgs, msg)
	}
	topic.PutMessages(msgs)
	topic.ForceFlush()

	var msgId2 MessageID
	msgBytes = []byte(strconv.Itoa(1001))
	msg = NewMessage(msgId2, msgBytes)
	topic.PutMessage(msg)
	topic.ForceFlush()
	channel2.skipChannelToEnd()

	topic.TryCleanOldData(1024*2, false, topic.backend.GetQueueReadEnd().Offset())
	t.Log(topic.GetQueueReadStart())
	t.Log(topic.backend.GetQueueReadEnd())

	// closed channel reopen should check if old confirmed is not less than cleaned disk segment start
	// and the reopened channel read end can update to the newest topic end
	channel = topic.GetChannel("channel")
	t.Log(channel.GetConfirmed())
	t.Log(channel.GetChannelEnd())

	equal(t, channel.GetConfirmed(), topic.backend.GetQueueReadStart())
	equal(t, channel.GetChannelEnd(), topic.backend.GetQueueReadEnd())
}

func TestChannelResetReadEnd(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_channel_skip" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	channel := topic.GetChannel("channel")

	msgs := make([]*Message, 0, 10)
	for i := 0; i < 10; i++ {
		var msgId MessageID
		msgBytes := []byte(strconv.Itoa(i))
		msg := NewMessage(msgId, msgBytes)
		msgs = append(msgs, msg)
	}
	topic.PutMessages(msgs)

	var msgId MessageID
	msgBytes := []byte(strconv.Itoa(10))
	msg := NewMessage(msgId, msgBytes)
	_, backendOffsetMid, _, _, _ := topic.PutMessage(msg)
	topic.ForceFlush()
	equal(t, channel.Depth(), int64(11))

	msgs = make([]*Message, 0, 9)
	//put another 10 messages
	for i := 0; i < 9; i++ {
		var msgId MessageID
		msgBytes := []byte(strconv.Itoa(i + 11))
		msg := NewMessage(msgId, msgBytes)
		msgs = append(msgs, msg)
	}
	topic.PutMessages(msgs)
	topic.flushBuffer(true)
	time.Sleep(time.Millisecond)
	equal(t, channel.Depth(), int64(20))

	//skip forward to message 10
	t.Logf("backendOffsetMid: %d", backendOffsetMid)
	channel.SetConsumeOffset(backendOffsetMid, 10, true)
	time.Sleep(time.Millisecond)
	for i := 0; i < 10; i++ {
		outputMsg := <-channel.clientMsgChan
		t.Logf("Msg: %s", outputMsg.Body)
		if (outputMsg.Offset) != backendOffsetMid && i == 0 {
			//this is message before reset
			equal(t, string(outputMsg.Body[:]), strconv.Itoa(0))
			outputMsg = <-channel.clientMsgChan
		}
		equal(t, string(outputMsg.Body[:]), strconv.Itoa(i+10))
	}
	equal(t, channel.Depth(), int64(10))

	channel.SetConsumeOffset(0, 0, true)
	time.Sleep(time.Millisecond)
	//equal(t, channel.Depth(), int64(20))
	for i := 0; i < 20; i++ {
		outputMsg := <-channel.clientMsgChan
		t.Logf("Msg: %s", outputMsg.Body)
		equal(t, string(outputMsg.Body[:]), strconv.Itoa(i))
	}
}

// depth timestamp is the next msg time need to be consumed
func TestChannelDepthTimestamp(t *testing.T) {
	// handle read no data, reset, etc
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_channel_depthts" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	channel := topic.GetChannel("channel")

	msgs := make([]*Message, 0, 9)
	//put another 10 messages
	for i := 0; i < 10; i++ {
		var msgId MessageID
		msgBytes := []byte(strconv.Itoa(i + 11))
		msg := NewMessage(msgId, msgBytes)
		time.Sleep(time.Millisecond * 10)
		msgs = append(msgs, msg)
	}
	topic.PutMessages(msgs)
	topic.ForceFlush()

	lastDepthTs := int64(0)
	for i := 0; i < 9; i++ {
		msgOutput := <-channel.clientMsgChan
		time.Sleep(time.Millisecond * 10)
		if lastDepthTs != 0 {
			// next msg timestamp == last depth ts
			equal(t, msgOutput.Timestamp, lastDepthTs)
		}
		lastDepthTs = channel.DepthTimestamp()
	}
	channel.resetReaderToConfirmed()
	equal(t, channel.DepthTimestamp(), int64(0))
}

func TestChannelUpdateEndWhenNeed(t *testing.T) {
	// put will try update channel end if channel need more data
	// and channel will try get newest end while need more data (no new put)
	// consider below:
	// 1. put 1,2,3 update channel end to 3
	// 2. consume 1
	// 3. put 4, no need update end
	// 4. consume 2, 3
	// 5. check consume 4 without topic flush
	// 6. consume end and put 5
	// 7. check consume 5 without flush
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.SyncEvery = 100
	opts.SyncTimeout = time.Second * 10

	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_channel_end_update" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	channel := topic.GetChannel("channel")

	msgs := make([]*Message, 0, 9)
	for i := 0; i < 10; i++ {
		var msgId MessageID
		msgBytes := []byte(strconv.Itoa(i + 11))
		msg := NewMessage(msgId, msgBytes)
		time.Sleep(time.Millisecond)
		msgs = append(msgs, msg)
	}
	topic.PutMessages(msgs)
	topic.flushBuffer(true)

	for i := 0; i < 5; i++ {
		msgOutput := <-channel.clientMsgChan
		channel.StartInFlightTimeout(msgOutput, NewFakeConsumer(0), "", opts.MsgTimeout)
		channel.ConfirmBackendQueue(msgOutput)
		t.Logf("consume %v", string(msgOutput.Body))
	}
	for i := 0; i < 5; i++ {
		var id MessageID
		msg := NewMessage(id, []byte("test"))
		topic.PutMessage(msg)
	}
	for i := 0; i < 10; i++ {
		select {
		case msgOutput := <-channel.clientMsgChan:
			channel.StartInFlightTimeout(msgOutput, NewFakeConsumer(0), "", opts.MsgTimeout)
			channel.ConfirmBackendQueue(msgOutput)
			t.Logf("consume %v", string(msgOutput.Body))
		case <-time.After(time.Second):
			t.Fatalf("timeout consume new messages")
		}
	}
	for i := 0; i < 5; i++ {
		var id MessageID
		msg := NewMessage(id, []byte("test"))
		topic.PutMessage(msg)
		time.Sleep(time.Millisecond)
	}
	for i := 0; i < 5; i++ {
		select {
		case msgOutput := <-channel.clientMsgChan:
			channel.StartInFlightTimeout(msgOutput, NewFakeConsumer(0), "", opts.MsgTimeout)
			channel.ConfirmBackendQueue(msgOutput)
			t.Logf("consume %v", string(msgOutput.Body))
		case <-time.After(time.Second):
			t.Fatalf("timeout consume new messages")
		}
	}
	// test new conn consume start from end queue before new message puts
}

func TestRangeTree(t *testing.T) {
	//tr := NewIntervalTree()
	tr := NewIntervalSkipList()
	//tr := NewIntervalHash()
	m1 := &queueInterval{0, 10, 2}
	m2 := &queueInterval{10, 20, 3}
	m3 := &queueInterval{20, 30, 4}
	m4 := &queueInterval{30, 40, 5}
	m5 := &queueInterval{40, 50, 6}
	m6 := &queueInterval{50, 60, 7}

	ret := tr.Query(m1, false)
	equal(t, len(ret), 0)
	equal(t, m1, tr.AddOrMerge(m1))
	t.Logf("1 %v", tr.ToString())

	ret = tr.Query(m1, false)
	equal(t, len(ret), 1)
	lowest := tr.IsLowestAt(m1.Start())
	equal(t, lowest, m1)
	lowest = tr.IsLowestAt(m1.End())
	equal(t, lowest, nil)
	deleted := tr.DeleteLower(m1.Start() + (m1.End()-m1.Start())/2)
	equal(t, deleted, 0)
	ret = tr.Query(m3, false)
	equal(t, len(ret), 0)
	ret = tr.Query(m3, true)
	equal(t, len(ret), 0)
	ret = tr.Query(m2, true)
	equal(t, len(ret), 0)
	ret = tr.Query(m2, false)
	equal(t, len(ret), 1)
	lowest = tr.IsLowestAt(m1.Start())
	equal(t, lowest, m1)
	tr.AddOrMerge(m3)
	t.Logf("2 %v", tr.ToString())
	ret = tr.Query(m5, false)
	equal(t, len(ret), 0)
	ret = tr.Query(m2, false)
	equal(t, len(ret), 2)
	ret = tr.Query(m4, false)
	equal(t, len(ret), 1)
	tr.AddOrMerge(m5)
	ret = tr.Query(m2, false)
	equal(t, len(ret), 2)
	ret = tr.Query(m4, false)
	equal(t, len(ret), 2)
	ret = tr.Query(m4, true)
	equal(t, len(ret), 0)
	lowest = tr.IsLowestAt(m1.Start())
	equal(t, lowest, m1)
	lowest = tr.IsLowestAt(m3.Start())
	equal(t, lowest, nil)

	deleted = tr.DeleteLower(m1.Start() + (m1.End()-m1.Start())/2)
	equal(t, deleted, 0)

	merged := tr.AddOrMerge(m2)
	t.Logf("4 %v", tr.ToString())
	equal(t, merged.Start(), m1.Start())
	equal(t, merged.End(), m3.End())
	equal(t, merged.EndCnt(), m3.EndCnt())
	equal(t, true, tr.IsCompleteOverlap(m1))
	equal(t, true, tr.IsCompleteOverlap(m2))
	equal(t, true, tr.IsCompleteOverlap(m3))

	ret = tr.Query(m6, false)
	equal(t, len(ret), 1)

	merged = tr.AddOrMerge(m6)
	equal(t, merged.Start(), m5.Start())
	equal(t, merged.End(), m6.End())
	equal(t, merged.EndCnt(), m6.EndCnt())
	equal(t, true, tr.IsCompleteOverlap(m5))
	equal(t, true, tr.IsCompleteOverlap(m6))

	ret = tr.Query(m4, false)
	equal(t, len(ret), 2)

	merged = tr.AddOrMerge(m4)

	equal(t, tr.Len(), int(1))
	equal(t, merged.Start(), int64(0))
	equal(t, merged.End(), int64(60))
	equal(t, merged.EndCnt(), uint64(7))
	equal(t, true, tr.IsCompleteOverlap(m1))
	equal(t, true, tr.IsCompleteOverlap(m2))
	equal(t, true, tr.IsCompleteOverlap(m3))
	equal(t, true, tr.IsCompleteOverlap(m4))
	equal(t, true, tr.IsCompleteOverlap(m5))
	equal(t, true, tr.IsCompleteOverlap(m6))
	merged = tr.AddOrMerge(m1)
	equal(t, merged.Start(), int64(0))
	equal(t, merged.End(), int64(60))
	equal(t, merged.EndCnt(), uint64(7))
	merged = tr.AddOrMerge(m6)
	equal(t, merged.Start(), int64(0))
	equal(t, merged.End(), int64(60))
	equal(t, merged.EndCnt(), uint64(7))

	deleted = tr.DeleteLower(m1.Start() + (m1.End()-m1.Start())/2)
	equal(t, deleted, 0)
	deleted = tr.DeleteLower(int64(60))
	equal(t, deleted, 1)
	equal(t, tr.Len(), int(0))
}

func TestMessageDispatchWhileResetInvalidOffset(t *testing.T) {
	topicName := "test_dispatch_while_rest_invalid" + strconv.Itoa(int(time.Now().Unix()))

	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()
	topic := nsqd.GetTopicIgnPart(topicName)
	topicDynConf := TopicDynamicConf{
		AutoCommit: 1,
		SyncEvery:  1,
		Ext:        true,
	}
	topic.SetDynamicInfo(topicDynConf, nil)

	ch := topic.GetChannel("ch")

	n := 100
	var wg sync.WaitGroup
	wg.Add(1)
	//case 1: tagged message goes to client with tag
	go func(maxReceived int) {
		i := 0
		defer wg.Done()
		timer := time.NewTimer(10 * time.Second)
		for {
			ch.readerChanged <- resetChannelData{BackendOffset(1000000000), 1, true}
			select {
			case <-timer.C:
				//timeout
				t.Fatal("timeout receive all messages before timeout")
				goto exit
			case msgOutput := <-ch.clientMsgChan:
				ch.StartInFlightTimeout(msgOutput, NewFakeConsumer(0), "", opts.MsgTimeout)
				ch.ConfirmBackendQueue(msgOutput)
				t.Logf("consume %v", string(msgOutput.Body))
				i++
				if i == maxReceived {
					goto exit
				}
			}
		}
	exit:
	}(n)
	for i := 0; i < n; i++ {
		var msgId MessageID
		msg := NewMessageWithExt(msgId, []byte("test body tag1"), ext.TAG_EXT_VER, []byte(""))
		_, _, _, _, putErr := topic.PutMessage(msg)
		assert(t, putErr == nil, "fail to put message: %v", putErr)
	}
	wg.Wait()
	t.Logf("subscribe stops.")
}

func TestMessageDispatchWhileReset(t *testing.T) {
	topicName := "test_dispatch_while_rest" + strconv.Itoa(int(time.Now().Unix()))

	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()
	topic := nsqd.GetTopicIgnPart(topicName)
	topicDynConf := TopicDynamicConf{
		AutoCommit: 1,
		SyncEvery:  1,
		Ext:        true,
	}
	topic.SetDynamicInfo(topicDynConf, nil)

	ch := topic.GetChannel("ch")

	n := 100
	var wg sync.WaitGroup
	wg.Add(1)
	//case 1: tagged message goes to client with tag
	go func() {
		defer wg.Done()
		for i := 0; i < n; {
			queueEnd := ch.backend.GetQueueConfirmed()
			ch.readerChanged <- resetChannelData{queueEnd.Offset(), queueEnd.TotalMsgCnt(), true}
			msgOutput := <-ch.clientMsgChan
			if msgOutput == nil {
				continue
			}
			ch.StartInFlightTimeout(msgOutput, NewFakeConsumer(0), "", opts.MsgTimeout)
			ch.ConfirmBackendQueue(msgOutput)
			t.Logf("consume %v", string(msgOutput.Body))
			i++
		}
	}()
	for i := 0; i < n; i++ {
		var msgId MessageID
		msg := NewMessageWithExt(msgId, []byte("test body tag1"), ext.TAG_EXT_VER, []byte(""))
		_, _, _, _, putErr := topic.PutMessage(msg)
		assert(t, putErr == nil, "fail to put message: %v", putErr)
		//reset with invalid offset to cause error
	}
	wg.Wait()
	t.Logf("subscribe stops.")
}

func BenchmarkRangeTree(b *testing.B) {

	mn := make([]*queueInterval, 1000)
	for i := 0; i < 1000; i++ {
		mn[i] = &queueInterval{int64(i) * 10, int64(i)*10 + 10, uint64(i) + 2}
	}

	b.StopTimer()
	b.StartTimer()
	for i := 0; i <= b.N; i++ {
		//tr := NewIntervalTree()
		tr := NewIntervalSkipList()
		//tr := NewIntervalHash()
		for index, q := range mn {
			if index%2 == 0 {
				tr.AddOrMerge(q)
				if index >= 1 {
					tr.IsCompleteOverlap(mn[index/2])
				}
			}
		}
		for index, q := range mn {
			if index%2 == 1 {
				tr.AddOrMerge(q)
				if index >= 1 {
					tr.IsCompleteOverlap(mn[index/2])
				}
			}
		}
		if tr.Len() != int(1) {
			b.Fatal("len not 1 " + tr.ToString())
		}
		l := tr.ToIntervalList()
		tr.DeleteInterval(&queueInterval{
			start:  l[0].Start,
			end:    l[0].End,
			endCnt: l[0].EndCnt,
		})
		for index, q := range mn {
			if index%2 == 1 {
				tr.AddOrMerge(q)
				if index >= 1 {
					tr.DeleteInterval(mn[index/2])
				}
			}
		}

		//if l[0].Start != int64(0) {
		//	b.Fatal("start not 0 " + tr.ToString())
		//}
		//if l[0].End != int64(10000) {
		//	b.Fatal("end not 10000 " + tr.ToString())
		//}
	}
}
