package nsqdserver

import (
	"crypto/tls"
	"errors"
	"net"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/youzan/nsq/consistence"
	"github.com/youzan/nsq/internal/ext"
	"github.com/youzan/nsq/nsqd"
)

const (
	FailedOnNotLeader   = consistence.ErrFailedOnNotLeader
	FailedOnNotWritable = consistence.ErrFailedOnNotWritable
	maxBatchNum         = 1000
)

var (
	serverPubFailedCnt          int64
	testPopQueueTimeout         int32
	testPutMessageTimeout       int32
	testFailedReqToDelayTimeout int32
)

func incrServerPubFailed() {
	atomic.AddInt64(&serverPubFailedCnt, 1)
	nsqd.ServerPubFailedCnt.Inc()
}

func getServerPubFailed() int64 {
	return atomic.LoadInt64(&serverPubFailedCnt)
}

type context struct {
	clientIDSequence int64
	nsqd             *nsqd.NSQD
	nsqdCoord        *consistence.NsqdCoordinator
	tlsConfig        *tls.Config
	httpAddr         *net.TCPAddr
	tcpAddr          *net.TCPAddr
	reverseProxyPort string
	clientConnNum    int64
}

func (c *context) getOpts() *nsqd.Options {
	return c.nsqd.GetOpts()
}

func (c *context) isAuthEnabled() bool {
	return c.nsqd.IsAuthEnabled()
}

func (c *context) nextClientID() int64 {
	return atomic.AddInt64(&c.clientIDSequence, 1)
}

func (c *context) swapOpts(other *nsqd.Options) {
	c.nsqd.SwapOpts(other)
	consistence.SetCoordLogLevel(other.LogLevel)
	consistence.ChangeSleepMsBetweenLogSyncPull(other.SleepMsBetweenLogSyncPull)
}

func (c *context) triggerOptsNotification() {
	c.nsqd.TriggerOptsNotification()
}

func (c *context) realHTTPAddr() *net.TCPAddr {
	return c.httpAddr
}

func (c *context) realTCPAddr() *net.TCPAddr {
	return c.tcpAddr
}

func (c *context) getStartTime() time.Time {
	return c.nsqd.GetStartTime()
}

func (c *context) getHealth() string {
	return c.nsqd.GetHealth()
}

func (c *context) isHealthy() bool {
	return c.nsqd.IsHealthy()
}

func (c *context) setHealth(err error) {
	c.nsqd.SetHealth(err)
}

func (c *context) getStats(leaderOnly bool, selectedTopic string, ch string, filterClients bool) []nsqd.TopicStats {
	if selectedTopic != "" {
		return c.nsqd.GetTopicStatsWithFilter(leaderOnly, selectedTopic, ch, filterClients)
	}
	return c.nsqd.GetStats(leaderOnly, filterClients)
}

func (c *context) GetTlsConfig() *tls.Config {
	return c.tlsConfig
}

func (c *context) getDefaultPartition(topic string) int {
	if c.nsqdCoord != nil {
		pid, _, err := c.nsqdCoord.GetMasterTopicCoordData(topic)
		if err != nil {
			return -1
		}
		return pid
	}
	return c.nsqd.GetTopicDefaultPart(topic)
}

func (c *context) getPartitions(name string) map[int]*nsqd.Topic {
	return c.nsqd.GetTopicPartitions(name)
}

func (c *context) getExistingTopic(name string, part int) (*nsqd.Topic, error) {
	return c.nsqd.GetExistingTopic(name, part)
}

func (c *context) getTopic(name string, part int, ext bool, ordered bool) *nsqd.Topic {
	if ext {
		return c.nsqd.GetTopicWithExt(name, part, ordered)
	} else {
		return c.nsqd.GetTopic(name, part, ordered)
	}
}

func (c *context) deleteExistingTopic(name string, part int) error {
	return c.nsqd.DeleteExistingTopic(name, part)
}

func (c *context) persistMetadata() {
	c.nsqd.NotifyPersistMetadata()
}

func (c *context) GetDistributedID() string {
	if c.nsqdCoord == nil {
		return ""
	}
	return c.nsqdCoord.GetMyID()
}

func (c *context) checkConsumeForMasterWrite(topic string, part int) bool {
	if c.nsqdCoord == nil {
		return true
	}
	return c.nsqdCoord.IsMineConsumeLeaderForTopic(topic, part)
}

func (c *context) checkForMasterWrite(topic string, part int) bool {
	if c.nsqdCoord == nil {
		return true
	}
	if consistence.IsAllClusterWriteDisabled() {
		return false
	}
	return c.nsqdCoord.IsMineLeaderForTopic(topic, part)
}

func (c *context) PutMessageObj(topic *nsqd.Topic,
	msg *nsqd.Message) (nsqd.MessageID, nsqd.BackendOffset, int32, nsqd.BackendQueueEnd, error) {
	if c.nsqdCoord == nil {
		if msg.DelayedType >= nsqd.MinDelayedType {
			topic.Lock()
			dq, err := topic.GetOrCreateDelayedQueueNoLock(nil)
			if err == nil {
				id, offset, writeBytes, dend, err := dq.PutDelayMessage(msg)
				topic.Unlock()
				return id, offset, writeBytes, dend, err
			}
			topic.Unlock()
			return 0, 0, 0, nil, err
		}
		return topic.PutMessage(msg)
	}
	if msg.DelayedType >= nsqd.MinDelayedType {
		if atomic.LoadInt32(&nsqd.EnableDelayedQueue) != int32(1) {
			return 0, 0, 0, nil, errors.New("delayed queue not enabled")
		}
		return c.nsqdCoord.PutDelayedMessageToCluster(topic, msg)
	}
	return c.nsqdCoord.PutMessageToCluster(topic, msg)
}

func (c *context) PutMessage(topic *nsqd.Topic,
	body []byte, extContent ext.IExtContent, traceID uint64) (nsqd.MessageID, nsqd.BackendOffset, int32, nsqd.BackendQueueEnd, error) {

	var msg *nsqd.Message
	if !topic.IsExt() {
		msg = nsqd.NewMessage(0, body)
	} else {
		msg = nsqd.NewMessageWithExt(0, body, extContent.ExtVersion(), extContent.GetBytes())
	}
	msg.TraceID = traceID

	nsqd.TopicPubTotalCnt.With(prometheus.Labels{
		"topic":     topic.GetTopicName(),
		"partition": strconv.Itoa(topic.GetTopicPart()),
	}).Inc()

	if c.nsqdCoord == nil {
		return topic.PutMessage(msg)
	}
	return c.nsqdCoord.PutMessageToCluster(topic, msg)
}

func (c *context) PutMessages(topic *nsqd.Topic, msgs []*nsqd.Message) (nsqd.MessageID, nsqd.BackendOffset, int32, error) {
	nsqd.TopicPubTotalCnt.With(prometheus.Labels{
		"topic":     topic.GetTopicName(),
		"partition": strconv.Itoa(topic.GetTopicPart()),
	}).Add(float64(len(msgs)))

	if c.nsqdCoord == nil {
		id, offset, rawSize, _, _, err := topic.PutMessages(msgs)
		return id, offset, rawSize, err
	}
	return c.nsqdCoord.PutMessagesToCluster(topic, msgs)
}

func (c *context) FinishMessageForce(ch *nsqd.Channel, msgID nsqd.MessageID) error {
	if c.nsqdCoord == nil {
		_, _, _, _, err := ch.FinishMessageForce(0, "", msgID, true)
		if err == nil {
			ch.ContinueConsumeForOrder()
		}
		return err
	}
	return c.nsqdCoord.FinishMessageToCluster(ch, 0, "", msgID)
}

func (c *context) FinishMessage(ch *nsqd.Channel, clientID int64, clientAddr string, msgID nsqd.MessageID) error {
	if c.nsqdCoord == nil {
		_, _, _, _, err := ch.FinishMessage(clientID, clientAddr, msgID)
		if err == nil {
			ch.ContinueConsumeForOrder()
		}
		return err
	}
	return c.nsqdCoord.FinishMessageToCluster(ch, clientID, clientAddr, msgID)
}

func (c *context) SyncChannels(topic *nsqd.Topic) error {
	if c.nsqdCoord == nil {
		return nil
	}
	return c.nsqdCoord.SyncTopicChannels(topic.GetTopicName(), topic.GetTopicPart())
}

func (c *context) DeleteExistingChannel(topic *nsqd.Topic, channelName string) error {
	if c.nsqdCoord == nil {
		err := topic.DeleteExistingChannel(channelName)
		return err
	}
	return c.nsqdCoord.DeleteChannel(topic, channelName)
}

func (c *context) UpdateChannelState(topic *nsqd.Topic, ch *nsqd.Channel, paused int, skipped int, zanTestSkipped int) error {
	var err error
	if c.nsqdCoord == nil {
		err = topic.UpdateChannelMeta(ch, paused, skipped, zanTestSkipped)
	} else {
		err = c.nsqdCoord.UpdateChannelStateToCluster(ch, paused, skipped, zanTestSkipped)
	}
	if err != nil {
		nsqd.NsqLogger().Logf("failed to update channel(%v) state pause: %v, skip: %v, topic %v, err: %v", ch.GetName(), paused, skipped, ch.GetTopicName(), err)
		return err
	}
	return nil
}

func (c *context) EmptyChannelDelayedQueue(ch *nsqd.Channel) error {
	if c.nsqdCoord == nil {
		if ch.GetDelayedQueue() != nil {
			err := ch.GetDelayedQueue().EmptyDelayedChannel(ch.GetName())
			if err != nil {
				nsqd.NsqLogger().Logf("empty delayed queue for channel %v failed: %v", ch.GetName(), err)
				return err
			}
		}
	} else {
		err := c.nsqdCoord.EmptyChannelDelayedStateToCluster(ch)
		if err != nil {
			nsqd.NsqLogger().Logf("failed to empty delayed for channel: %v , err: %v ", ch.GetName(), err)
			return err
		}
	}
	return nil
}

func (c *context) SetChannelOffset(ch *nsqd.Channel, startFrom *ConsumeOffset, force bool) (int64, int64, error) {
	var l *consistence.CommitLogData
	var queueOffset int64
	cnt := int64(0)
	var err error
	if startFrom.OffsetType == offsetTimestampType {
		if c.nsqdCoord != nil {
			l, queueOffset, cnt, err = c.nsqdCoord.SearchLogByMsgTimestamp(ch.GetTopicName(), ch.GetTopicPart(), startFrom.OffsetValue)
		} else {
			err = errors.New("Not supported while coordinator disabled")
		}
	} else if startFrom.OffsetType == offsetSpecialType {
		if startFrom.OffsetValue == -1 {
			e := ch.GetChannelEnd()
			queueOffset = int64(e.Offset())
			cnt = e.TotalMsgCnt()
		} else {
			nsqd.NsqLogger().Logf("not known special offset :%v", startFrom)
			err = errors.New("not supported offset type")
		}
	} else if startFrom.OffsetType == offsetVirtualQueueType {
		queueOffset = startFrom.OffsetValue
		cnt = 0
		if c.nsqdCoord != nil {
			l, queueOffset, cnt, err = c.nsqdCoord.SearchLogByMsgOffset(ch.GetTopicName(), ch.GetTopicPart(), queueOffset)
		} else {
			err = errors.New("Not supported while coordinator disabled")
		}
	} else if startFrom.OffsetType == offsetMsgCountType {
		if c.nsqdCoord != nil {
			l, queueOffset, cnt, err = c.nsqdCoord.SearchLogByMsgCnt(ch.GetTopicName(), ch.GetTopicPart(), startFrom.OffsetValue)
		} else {
			err = errors.New("Not supported while coordinator disabled")
		}
	} else {
		nsqd.NsqLogger().Logf("not supported offset type:%v", startFrom)
		err = errors.New("not supported offset type")
	}
	if err != nil {
		nsqd.NsqLogger().Logf("failed to search the consume offset: %v, err:%v", startFrom, err)
		return 0, 0, err
	}
	nsqd.NsqLogger().Logf("%v searched log : %v, offset: %v:%v", startFrom, l, queueOffset, cnt)
	if c.nsqdCoord == nil {
		err = ch.SetConsumeOffset(nsqd.BackendOffset(queueOffset), cnt, force)
		if err != nil {
			if err != nsqd.ErrSetConsumeOffsetNotFirstClient {
				nsqd.NsqLogger().Logf("failed to set the consume offset: %v, err:%v", startFrom, err)
				return 0, 0, err
			}
			nsqd.NsqLogger().Logf("the consume offset: %v can only be set by the first client", startFrom)
		}
	} else {
		err = c.nsqdCoord.SetChannelConsumeOffsetToCluster(ch, queueOffset, cnt, force)
		if err != nil {
			if coordErr, ok := err.(*consistence.CommonCoordErr); ok {
				if coordErr.IsEqual(consistence.ErrLocalSetChannelOffsetNotFirstClient) {
					nsqd.NsqLogger().Logf("the consume offset: %v can only be set by the first client", startFrom)
					return queueOffset, cnt, nil
				}
			}
			nsqd.NsqLogger().Logf("failed to set the consume offset: %v (%v:%v), err: %v ", startFrom, queueOffset, cnt, err)
			return 0, 0, err
		}
	}
	return queueOffset, cnt, nil
}

func (c *context) internalPubLoop(topic *nsqd.Topic) {
	messages := make([]*nsqd.Message, 0, 100)
	pubInfoList := make([]*nsqd.PubInfo, 0, 100)
	mpubInfoList := make([]*nsqd.MPubInfo, 0, 100)
	topicName := topic.GetTopicName()
	partition := topic.GetTopicPart()
	nsqd.NsqLogger().Logf("start pub loop for topic: %v ", topic.GetFullName())
	defer func() {
		done := false
		for !done {
			select {
			case info := <-topic.GetWaitChan():
				pubInfoList = append(pubInfoList, info)
			case minfo := <-topic.GetMWaitChan():
				mpubInfoList = append(mpubInfoList, minfo)
			default:
				done = true
			}
		}
		nsqd.NsqLogger().Logf("quit pub loop for topic: %v, left: %v, %v ", topic.GetFullName(), len(pubInfoList), len(mpubInfoList))
		for _, info := range pubInfoList {
			info.Err = nsqd.ErrExiting
			close(info.Done)
		}
		for _, info := range mpubInfoList {
			info.Err = nsqd.ErrExiting
			close(info.Done)
		}
	}()
	quitChan := topic.QuitChan()
	infoChan := topic.GetWaitChan()
	minfoChan := topic.GetMWaitChan()
	for {
		if len(messages) > maxBatchNum {
			infoChan = nil
			minfoChan = nil
		} else {
			infoChan = topic.GetWaitChan()
			minfoChan = topic.GetMWaitChan()
		}
		select {
		case <-quitChan:
			return
		case minfo := <-minfoChan:
			if time.Since(minfo.StartPub) >= pubWaitTimeout || atomic.LoadInt32(&testPopQueueTimeout) == 1 {
				topic.IncrPubFailed()
				incrServerPubFailed()
				minfo.Err = ErrPubPopQueueTimeout
				close(minfo.Done)
				nsqd.NsqLogger().LogErrorf("topic %v put message timeout while pop queue, pub start: %s", topic.GetFullName(), minfo.StartPub)
				continue
			}
			messages = append(messages, minfo.Msgs...)
			mpubInfoList = append(mpubInfoList, minfo)
		case info := <-infoChan:
			if len(info.MsgBody) <= 0 {
				nsqd.NsqLogger().Logf("empty msg body")
			}
			if time.Since(info.StartPub) >= pubWaitTimeout || atomic.LoadInt32(&testPopQueueTimeout) == 1 {
				topic.IncrPubFailed()
				incrServerPubFailed()
				info.Err = ErrPubPopQueueTimeout
				close(info.Done)
				nsqd.NsqLogger().LogErrorf("topic %v put message timeout while pop queue, pub start: %s", topic.GetFullName(), info.StartPub)
				continue
			}
			if !topic.IsExt() {
				messages = append(messages, nsqd.NewMessage(0, info.MsgBody))
			} else {
				messages = append(messages, nsqd.NewMessageWithExt(0, info.MsgBody, info.ExtContent.ExtVersion(), info.ExtContent.GetBytes()))
			}
			pubInfoList = append(pubInfoList, info)
		default:
			if len(messages) == 0 {
				select {
				case <-quitChan:
					return
				case info := <-infoChan:
					if time.Since(info.StartPub) >= pubWaitTimeout || atomic.LoadInt32(&testPopQueueTimeout) == 1 {
						topic.IncrPubFailed()
						incrServerPubFailed()
						info.Err = ErrPubPopQueueTimeout
						close(info.Done)
						nsqd.NsqLogger().LogErrorf("topic %v put message timeout while pop queue, pub start: %s", topic.GetFullName(), info.StartPub)
						continue
					}
					if !topic.IsExt() {
						messages = append(messages, nsqd.NewMessage(0, info.MsgBody))
					} else {
						messages = append(messages, nsqd.NewMessageWithExt(0, info.MsgBody, info.ExtContent.ExtVersion(), info.ExtContent.GetBytes()))
					}
					pubInfoList = append(pubInfoList, info)
				case minfo := <-minfoChan:
					if time.Since(minfo.StartPub) >= pubWaitTimeout || atomic.LoadInt32(&testPopQueueTimeout) == 1 {
						topic.IncrPubFailed()
						incrServerPubFailed()
						minfo.Err = ErrPubPopQueueTimeout
						close(minfo.Done)
						nsqd.NsqLogger().LogErrorf("topic %v put message timeout while pop queue, pub start: %s", topic.GetFullName(), minfo.StartPub)
						continue
					}
					messages = append(messages, minfo.Msgs...)
					mpubInfoList = append(mpubInfoList, minfo)
				}
				continue
			}
			if tcnt := atomic.LoadInt32(&testPutMessageTimeout); tcnt >= 1 {
				time.Sleep(time.Second * time.Duration(tcnt))
			}
			var retErr error
			if c.checkForMasterWrite(topicName, partition) {
				s := time.Now()
				_, _, _, err := c.PutMessages(topic, messages)
				if err != nil {
					nsqd.NsqLogger().LogErrorf("topic %v put messages %v failed: %v", topic.GetFullName(), len(messages), err)
					retErr = err
				}
				cost := time.Since(s)
				if cost > time.Second {
					nsqd.NsqLogger().Logf("topic %v put messages %v to cluster slow: %v", topic.GetFullName(), len(messages), cost)
				}
			} else {
				topic.DisableForSlave(c.checkConsumeForMasterWrite(topicName, partition))
				nsqd.NsqLogger().LogDebugf("should put to master: %v",
					topic.GetFullName())
				retErr = consistence.ErrNotTopicLeader.ToErrorType()
			}
			if retErr != nil {
				topic.IncrPubFailed()
				incrServerPubFailed()
			}
			for _, info := range pubInfoList {
				info.Err = retErr
				close(info.Done)
			}
			for _, minfo := range mpubInfoList {
				minfo.Err = retErr
				close(minfo.Done)
			}
			pubInfoList = pubInfoList[:0]
			mpubInfoList = mpubInfoList[:0]
			messages = messages[:0]
		}
	}
}

func (c *context) internalRequeueToEnd(ch *nsqd.Channel,
	oldMsg *nsqd.Message, timeoutDuration time.Duration) error {
	topic, err := c.getExistingTopic(ch.GetTopicName(), ch.GetTopicPart())
	if topic == nil || err != nil {
		nsqd.NsqLogger().LogWarningf("req channel %v topic not found: %v", ch.GetName(), err)
		return err
	}
	if topic.IsOrdered() {
		return errors.New("ordered topic can not requeue to end")
	}
	if ch.Exiting() {
		return nsqd.ErrExiting
	}

	if !c.checkConsumeForMasterWrite(topic.GetTopicName(), topic.GetTopicPart()) {
		return consistence.ErrNotTopicLeader.ToErrorType()
	}

	newMsg := oldMsg.GetCopy()
	newMsg.ID = 0
	newMsg.DelayedType = nsqd.ChannelDelayed

	if newMsg.Attempts() >= nsqd.MaxAttempts/4 {
		// to avoid requeue to end again and again, the message attempts many times should be
		// delayed enough time.
		nto := time.Second * time.Duration(newMsg.Attempts())
		if timeoutDuration <= nto {
			timeoutDuration = nto
		}
	}
	if timeoutDuration > c.nsqd.GetOpts().MaxReqTimeout {
		timeoutDuration = c.nsqd.GetOpts().MaxReqTimeout
	}
	newTimeout := time.Now().Add(timeoutDuration)
	newMsg.DelayedTs = newTimeout.UnixNano()

	newMsg.DelayedOrigID = oldMsg.ID
	newMsg.DelayedChannel = ch.GetName()

	nsqd.NsqLogger().LogDebugf("requeue to end with delayed %v message: %v", timeoutDuration, oldMsg.ID)
	// TODO: maybe use group commit to reduce io, we should use another loop for req, since it will be written to
	// delayed commit log
	_, _, _, _, putErr := c.PutMessageObj(topic, newMsg)
	if putErr != nil {
		nsqd.NsqLogger().Logf("req message %v to end failed, channel %v, put error: %v ",
			newMsg, ch.GetName(), putErr)
		return putErr
	}
	if atomic.LoadInt32(&testFailedReqToDelayTimeout) == 1 {
		time.Sleep(time.Second * 10)
		nsqd.NsqLogger().Logf("req message %v to end failed, channel %v, put error by test",
			newMsg, ch.GetName())
		return errors.New("put delayed timeout error by test")
	}

	// it is possible that before we ack, the delayed message is poped from delayed queue, however it will be checked
	// if there is the same id message still in inflight or in requeue.
	err = c.FinishMessage(ch, oldMsg.GetClientID(), "", oldMsg.ID)
	return err
}

func (c *context) GreedyCleanTopicOldData(topic *nsqd.Topic) error {
	if c.nsqdCoord != nil {
		return c.nsqdCoord.GreedyCleanTopicOldData(topic)
	}
	return nil
}
