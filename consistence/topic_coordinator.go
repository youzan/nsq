package consistence

import (
	"os"
	"path"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/youzan/nsq/nsqd"
)

func GetTopicFullName(n string, pid int) string {
	return n + "-" + strconv.Itoa(pid)
}

type ChannelConsumerOffset struct {
	VOffset             int64
	VCnt                int64
	Flush               bool
	AllowBackward       bool
	ConfirmedInterval   []nsqd.MsgQueueInterval
	NeedUpdateConfirmed bool
}

func (cco *ChannelConsumerOffset) IsSame(other *ChannelConsumerOffset) bool {
	if cco.VOffset != other.VOffset {
		return false
	}
	if cco.VCnt != other.VCnt {
		return false
	}
	if !other.NeedUpdateConfirmed && !cco.NeedUpdateConfirmed {
		return true
	}

	if len(cco.ConfirmedInterval) != len(other.ConfirmedInterval) {
		return false
	}

	for i, ci := range cco.ConfirmedInterval {
		if ci != other.ConfirmedInterval[i] {
			return false
		}
	}
	return true
}

type ChannelConsumeMgr struct {
	sync.Mutex
	channelNames         []string
	channelConsumeOffset map[string]ChannelConsumerOffset
	delayedQueueSyncedTs int64
}

func isSameStrList(l, r []string) bool {
	if len(l) != len(r) {
		return false
	}
	for i, v := range l {
		if v != r[i] {
			return false
		}
	}
	return true
}

func newChannelComsumeMgr() *ChannelConsumeMgr {
	return &ChannelConsumeMgr{
		channelConsumeOffset: make(map[string]ChannelConsumerOffset),
	}
}

func (cc *ChannelConsumeMgr) Clear() {
	cc.Lock()
	cc.channelNames = []string{}
	cc.channelConsumeOffset = make(map[string]ChannelConsumerOffset)
	cc.delayedQueueSyncedTs = 0
	cc.Unlock()
}

func (cc *ChannelConsumeMgr) Get(ch string) (ChannelConsumerOffset, bool) {
	cc.Lock()
	cco, ok := cc.channelConsumeOffset[ch]
	cc.Unlock()
	return cco, ok
}

func (cc *ChannelConsumeMgr) Update(ch string, cco ChannelConsumerOffset) {
	cc.Lock()
	cc.channelConsumeOffset[ch] = cco
	cc.Unlock()
}

func (cc *ChannelConsumeMgr) GetSyncedDelayedQueueTs() int64 {
	cc.Lock()
	defer cc.Unlock()
	return cc.delayedQueueSyncedTs
}

func (cc *ChannelConsumeMgr) UpdateSyncedDelayedQueueTs(ts int64) {
	cc.Lock()
	defer cc.Unlock()
	cc.delayedQueueSyncedTs = ts
}

func (cc *ChannelConsumeMgr) GetSyncedChs() []string {
	cc.Lock()
	names := cc.channelNames
	cc.Unlock()
	return names
}

func (cc *ChannelConsumeMgr) UpdateSyncedChs(names []string) {
	cc.Lock()
	cc.channelNames = names
	cc.Unlock()
}

type coordData struct {
	topicInfo          TopicPartitionMetaInfo
	topicLeaderSession TopicLeaderSession
	consumeMgr         *ChannelConsumeMgr
	syncedConsumeMgr   *ChannelConsumeMgr
	logMgr             *TopicCommitLogMgr
	delayedLogMgr      *TopicCommitLogMgr
}

func (cd *coordData) updateBufferSize(bs int) {
	cd.logMgr.updateBufferSize(bs)
	if cd.delayedLogMgr != nil {
		cd.delayedLogMgr.updateBufferSize(bs)
	}
}

func (cd *coordData) flushCommitLogs() {
	cd.logMgr.FlushCommitLogs()
	if cd.delayedLogMgr != nil {
		cd.delayedLogMgr.FlushCommitLogs()
	}
}

func (cd *coordData) switchForMaster(master bool) {
	cd.logMgr.switchForMaster(master)
	if cd.delayedLogMgr != nil {
		cd.delayedLogMgr.switchForMaster(master)
	}
	cd.syncedConsumeMgr.Clear()
}

func (cd *coordData) GetCopy() *coordData {
	newCoordData := &coordData{}
	if cd == nil {
		return newCoordData
	}
	*newCoordData = *cd
	return newCoordData
}

type TopicCoordinator struct {
	dataMutex sync.Mutex
	*coordData
	forceLeave int32
	// hold for write to avoid disable or exiting or catchup
	// lock order: first lock writehold then lock data to avoid deadlock
	writeHold      sync.Mutex
	catchupRunning int32
	disableWrite   int32
	exiting        int32
	basePath       string
}

func NewTopicCoordinatorWithFixMode(name string, partition int, basepath string,
	syncEvery int, ordered bool, forceFix bool) (*TopicCoordinator, error) {
	return newTopicCoordinator(name, partition, basepath, syncEvery, ordered, forceFix)
}

func NewTopicCoordinator(name string, partition int, basepath string,
	syncEvery int, ordered bool) (*TopicCoordinator, error) {
	return newTopicCoordinator(name, partition, basepath, syncEvery, ordered, false)
}

func newTopicCoordinator(name string, partition int, basepath string,
	syncEvery int, ordered bool, fixMode bool) (*TopicCoordinator, error) {
	tc := &TopicCoordinator{}
	tc.coordData = &coordData{}
	tc.coordData.consumeMgr = newChannelComsumeMgr()
	tc.coordData.syncedConsumeMgr = newChannelComsumeMgr()
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
	tc.logMgr, err = InitTopicCommitLogMgrWithFixMode(name, partition, basepath, buf, fixMode)
	if err != nil {
		coordLog.Errorf("topic(%v) failed to init log: %v ", name, err)
		return nil, err
	}

	if !ordered {
		dqPath := path.Join(tc.basePath, "delayed_queue")
		os.MkdirAll(dqPath, 0755)
		tc.delayedLogMgr, err = InitTopicCommitLogMgrWithFixMode(name, partition,
			dqPath, buf, fixMode)
		if err != nil {
			coordLog.Errorf("topic(%v) failed to init delayed queue log: %v ", name, err)
			return nil, err
		}
	}
	return tc, nil
}

func (tc *TopicCoordinator) GetDelayedQueueLogMgr() (*TopicCommitLogMgr, error) {
	logMgr := tc.GetData().delayedLogMgr
	if logMgr == nil {
		return nil, ErrTopicMissingDelayedLog.ToErrorType()
	}
	return logMgr, nil
}

func (tc *TopicCoordinator) DeleteNoWriteLock(removeData bool) {
	tc.Exiting()
	tc.SetForceLeave(true)
	tc.dataMutex.Lock()
	if removeData {
		tc.logMgr.Delete()
		if tc.delayedLogMgr != nil {
			tc.delayedLogMgr.Delete()
		}
	} else {
		tc.logMgr.Close()
		if tc.delayedLogMgr != nil {
			tc.delayedLogMgr.Close()
		}
	}
	tc.dataMutex.Unlock()
}

func (tc *TopicCoordinator) DeleteWithLock(removeData bool) {
	tc.Exiting()
	tc.SetForceLeave(true)
	tc.writeHold.Lock()
	tc.dataMutex.Lock()
	if removeData {
		tc.logMgr.Delete()
		if tc.delayedLogMgr != nil {
			tc.delayedLogMgr.Delete()
		}
	} else {
		tc.logMgr.Close()
		if tc.delayedLogMgr != nil {
			tc.delayedLogMgr.Close()
		}
	}
	tc.dataMutex.Unlock()
	tc.writeHold.Unlock()
}

func (tc *TopicCoordinator) GetData() *coordData {
	tc.dataMutex.Lock()
	d := tc.coordData
	tc.dataMutex.Unlock()
	return d
}

func (tc *TopicCoordinator) IsWriteDisabled() bool {
	return atomic.LoadInt32(&tc.disableWrite) == 1
}

func (tc *TopicCoordinator) DisableWrite(disable bool) {
	// hold the write lock to wait the current write finish.
	tc.writeHold.Lock()
	if disable {
		atomic.StoreInt32(&tc.disableWrite, 1)
	} else {
		atomic.StoreInt32(&tc.disableWrite, 0)
	}
	tc.writeHold.Unlock()
}

func (tc *TopicCoordinator) IsExiting() bool {
	return atomic.LoadInt32(&tc.exiting) == 1
}

func (tc *TopicCoordinator) Exiting() {
	atomic.StoreInt32(&tc.exiting, 1)
}

func (tc *TopicCoordinator) checkWriteForLeader(myID string) *CoordErr {
	if tc.IsForceLeave() {
		return ErrNotTopicLeader
	}
	cd := tc.GetData()
	if cd.GetLeaderSessionID() != myID || cd.topicInfo.Leader != myID {
		return ErrNotTopicLeader
	}
	if cd.topicLeaderSession.Session == "" {
		return ErrMissingTopicLeaderSession
	}
	return nil
}

func (tc *TopicCoordinator) SetForceLeave(leave bool) {
	if leave {
		atomic.StoreInt32(&tc.forceLeave, 1)
	} else {
		atomic.StoreInt32(&tc.forceLeave, 0)
	}
}

func (tc *TopicCoordinator) IsForceLeave() bool {
	return atomic.LoadInt32(&tc.forceLeave) == 1
}

func (cd *coordData) GetLeader() string {
	return cd.topicInfo.Leader
}

func (cd *coordData) GetLeaderSessionID() string {
	if cd.topicLeaderSession.LeaderNode == nil {
		return ""
	}
	return cd.topicLeaderSession.LeaderNode.GetID()
}

func (cd *coordData) IsMineISR(id string) bool {
	return FindSlice(cd.topicInfo.ISR, id) != -1
}

func (cd *coordData) IsMineLeaderSessionReady(id string) bool {
	if cd.topicLeaderSession.LeaderNode != nil &&
		cd.topicLeaderSession.LeaderNode.GetID() == id &&
		cd.topicLeaderSession.Session != "" {
		return true
	}
	return false
}

func (cd *coordData) GetLeaderSession() string {
	return cd.topicLeaderSession.Session
}

func (cd *coordData) GetLeaderSessionEpoch() EpochType {
	return cd.topicLeaderSession.LeaderEpoch
}

func (cd *coordData) GetTopicEpochForWrite() EpochType {
	return cd.topicInfo.EpochForWrite
}

func (cd *coordData) IsISRReadyForWrite(myID string) bool {
	return (len(cd.topicInfo.ISR) > cd.topicInfo.Replica/2) && cd.IsMineISR(myID)
}
