package consistence

import (
	"github.com/youzan/nsq/nsqd"
	"os"
	"path"
	"sync"
	"sync/atomic"
)

type ChannelConsumerOffset struct {
	VOffset             int64
	VCnt                int64
	Flush               bool
	AllowBackward       bool
	ConfirmedInterval   []nsqd.MsgQueueInterval
	NeedUpdateConfirmed bool
}

type ChannelConsumeMgr struct {
	sync.Mutex
	channelConsumeOffset map[string]ChannelConsumerOffset
}

func newChannelComsumeMgr() *ChannelConsumeMgr {
	return &ChannelConsumeMgr{
		channelConsumeOffset: make(map[string]ChannelConsumerOffset),
	}
}

type coordData struct {
	topicInfo          TopicPartitionMetaInfo
	topicLeaderSession TopicLeaderSession
	consumeMgr         *ChannelConsumeMgr
	logMgr             *TopicCommitLogMgr
	delayedLogMgr      *TopicCommitLogMgr
	forceLeave         int32
}

func (self *coordData) updateBufferSize(bs int) {
	self.logMgr.updateBufferSize(bs)
	if self.delayedLogMgr != nil {
		self.delayedLogMgr.updateBufferSize(bs)
	}
}

func (self *coordData) flushCommitLogs() {
	self.logMgr.FlushCommitLogs()
	if self.delayedLogMgr != nil {
		self.delayedLogMgr.FlushCommitLogs()
	}
}

func (self *coordData) switchForMaster(master bool) {
	self.logMgr.switchForMaster(master)
	if self.delayedLogMgr != nil {
		self.delayedLogMgr.switchForMaster(master)
	}
}

func (self *coordData) GetCopy() *coordData {
	newCoordData := &coordData{}
	if self == nil {
		return newCoordData
	}
	*newCoordData = *self
	return newCoordData
}

type TopicCoordinator struct {
	dataMutex sync.Mutex
	*coordData
	// hold for write to avoid disable or exiting or catchup
	// lock order: first lock writehold then lock data to avoid deadlock
	writeHold      sync.Mutex
	catchupRunning int32
	disableWrite   int32
	exiting        int32
	basePath       string
}

func NewTopicCoordinator(name string, partition int, basepath string,
	syncEvery int, ordered bool) (*TopicCoordinator, error) {
	tc := &TopicCoordinator{}
	tc.coordData = &coordData{}
	tc.coordData.consumeMgr = newChannelComsumeMgr()
	tc.topicInfo.Name = name
	tc.topicInfo.Partition = partition
	tc.disableWrite = 1
	tc.basePath = basepath
	var err error
	err = os.MkdirAll(basepath, 0755)
	if err != nil {
		coordLog.Errorf("topic(%v) failed to create directory: %v ", name, err)
		return nil, err
	}
	// sync 1 means flush every message
	// all other sync we can make the default buffer, since the commit log
	// is just index of disk data and can be restored from disk queue.
	buf := syncEvery - 1
	if buf != 0 {
		if buf < DEFAULT_COMMIT_BUF_SIZE {
			buf = DEFAULT_COMMIT_BUF_SIZE
		}
	}
	tc.logMgr, err = InitTopicCommitLogMgr(name, partition, basepath, buf)
	if err != nil {
		coordLog.Errorf("topic(%v) failed to init log: %v ", name, err)
		return nil, err
	}

	if !ordered {
		dqPath := path.Join(tc.basePath, "delayed_queue")
		os.MkdirAll(dqPath, 0755)
		tc.delayedLogMgr, err = InitTopicCommitLogMgr(name, partition,
			dqPath, buf)
		if err != nil {
			coordLog.Errorf("topic(%v) failed to init delayed queue log: %v ", name, err)
			return nil, err
		}
	}
	return tc, nil
}

func (self *TopicCoordinator) GetDelayedQueueLogMgr() (*TopicCommitLogMgr, error) {
	logMgr := self.GetData().delayedLogMgr
	if logMgr == nil {
		return nil, ErrTopicMissingDelayedLog.ToErrorType()
	}
	return logMgr, nil
}

func (self *TopicCoordinator) DeleteNoWriteLock(removeData bool) {
	self.Exiting()
	self.SetForceLeave(true)
	self.dataMutex.Lock()
	if removeData {
		self.logMgr.Delete()
		if self.delayedLogMgr != nil {
			self.delayedLogMgr.Delete()
		}
	} else {
		self.logMgr.Close()
		if self.delayedLogMgr != nil {
			self.delayedLogMgr.Close()
		}
	}
	self.dataMutex.Unlock()
}

func (self *TopicCoordinator) DeleteWithLock(removeData bool) {
	self.Exiting()
	self.SetForceLeave(true)
	self.writeHold.Lock()
	self.dataMutex.Lock()
	if removeData {
		self.logMgr.Delete()
		if self.delayedLogMgr != nil {
			self.delayedLogMgr.Delete()
		}
	} else {
		self.logMgr.Close()
		if self.delayedLogMgr != nil {
			self.delayedLogMgr.Close()
		}
	}
	self.dataMutex.Unlock()
	self.writeHold.Unlock()
}

func (self *TopicCoordinator) GetData() *coordData {
	self.dataMutex.Lock()
	d := self.coordData
	self.dataMutex.Unlock()
	return d
}

func (self *TopicCoordinator) IsWriteDisabled() bool {
	return atomic.LoadInt32(&self.disableWrite) == 1
}

func (self *TopicCoordinator) DisableWrite(disable bool) {
	// hold the write lock to wait the current write finish.
	self.writeHold.Lock()
	if disable {
		atomic.StoreInt32(&self.disableWrite, 1)
	} else {
		atomic.StoreInt32(&self.disableWrite, 0)
	}
	self.writeHold.Unlock()
}

func (self *TopicCoordinator) IsExiting() bool {
	return atomic.LoadInt32(&self.exiting) == 1
}

func (self *TopicCoordinator) Exiting() {
	atomic.StoreInt32(&self.exiting, 1)
}

func (self *coordData) GetLeader() string {
	return self.topicInfo.Leader
}

func (self *coordData) GetLeaderSessionID() string {
	if self.topicLeaderSession.LeaderNode == nil {
		return ""
	}
	return self.topicLeaderSession.LeaderNode.GetID()
}

func (self *coordData) IsMineISR(id string) bool {
	return FindSlice(self.topicInfo.ISR, id) != -1
}

func (self *coordData) IsMineLeaderSessionReady(id string) bool {
	if self.topicLeaderSession.LeaderNode != nil &&
		self.topicLeaderSession.LeaderNode.GetID() == id &&
		self.topicLeaderSession.Session != "" {
		return true
	}
	return false
}

func (self *coordData) GetLeaderSession() string {
	return self.topicLeaderSession.Session
}

func (self *coordData) GetLeaderSessionEpoch() EpochType {
	return self.topicLeaderSession.LeaderEpoch
}

func (self *coordData) GetTopicEpochForWrite() EpochType {
	return self.topicInfo.EpochForWrite
}

func (self *TopicCoordinator) checkWriteForLeader(myID string) *CoordErr {
	return self.GetData().checkWriteForLeader(myID)
}

func (self *coordData) checkWriteForLeader(myID string) *CoordErr {
	if self.IsForceLeave() {
		return ErrNotTopicLeader
	}
	if self.GetLeaderSessionID() != myID || self.topicInfo.Leader != myID {
		return ErrNotTopicLeader
	}
	if self.topicLeaderSession.Session == "" {
		return ErrMissingTopicLeaderSession
	}
	return nil
}

func (self *coordData) IsISRReadyForWrite(myID string) bool {
	return (len(self.topicInfo.ISR) > self.topicInfo.Replica/2) && self.IsMineISR(myID)
}

func (self *coordData) SetForceLeave(leave bool) {
	if leave {
		atomic.StoreInt32(&self.forceLeave, 1)
	} else {
		atomic.StoreInt32(&self.forceLeave, 0)
	}
}

func (self *coordData) IsForceLeave() bool {
	return atomic.LoadInt32(&self.forceLeave) == 1
}
