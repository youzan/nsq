package consistence

import (
	"bytes"
	"errors"
	"net"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/youzan/nsq/internal/protocol"

	"github.com/absolute8511/gorpc"
	"github.com/youzan/nsq/internal/levellogger"
	"github.com/youzan/nsq/nsqd"
)

type ErrRPCRetCode int

const (
	RpcNoErr ErrRPCRetCode = iota
	RpcCommonErr
)

const (
	RpcErrLeavingISRWait ErrRPCRetCode = iota + 10
	RpcErrNotTopicLeader
	RpcErrNoLeader
	RpcErrEpochMismatch
	RpcErrEpochLessThanCurrent
	RpcErrWriteQuorumFailed
	RpcErrCommitLogIDDup
	RpcErrCommitLogEOF
	RpcErrCommitLogOutofBound
	RpcErrCommitLogLessThanSegmentStart
	RpcErrMissingTopicLeaderSession
	RpcErrLeaderSessionMismatch
	RpcErrWriteDisabled
	RpcErrTopicNotExist
	RpcErrMissingTopicCoord
	RpcErrTopicCoordExistingAndMismatch
	RpcErrTopicCoordConflicted
	RpcErrTopicLeaderChanged
	RpcErrTopicLoading
	RpcErrSlaveStateInvalid
	RpcErrTopicCoordStateInvalid
	RpcErrWriteOnNonISR
)

type SlaveAsyncWriteResult struct {
	ret gorpc.AsyncResult
}

func (self *SlaveAsyncWriteResult) Wait() {
	<-self.ret.Done
}

func (self *SlaveAsyncWriteResult) GetResult() *CoordErr {
	coordErr, ok := self.ret.Response.(*CoordErr)
	if ok {
		return convertRpcError(self.ret.Error, coordErr)
	} else {
		return convertRpcError(self.ret.Error, nil)
	}
}

type NsqdNodeLoadFactor struct {
	nodeLF        float32
	topicLeaderLF map[string]map[int]float32
	topicSlaveLF  map[string]map[int]float32
}

type RpcAdminTopicInfo struct {
	TopicPartitionMetaInfo
	LookupdEpoch EpochType
	DisableWrite bool
}

type RpcAcquireTopicLeaderReq struct {
	RpcTopicData
	LeaderNodeID string
	LookupdEpoch EpochType
}

type RpcReleaseTopicLeaderReq struct {
	RpcTopicData
	LeaderNodeID string
	LookupdEpoch EpochType
}

type RpcTopicLeaderSession struct {
	RpcTopicData
	LeaderNode   NsqdNodeInfo
	LookupdEpoch EpochType
	JoinSession  string
}

type NsqdCoordRpcServer struct {
	nsqdCoord      *NsqdCoordinator
	rpcDispatcher  *gorpc.Dispatcher
	rpcServer      *gorpc.Server
	dataRootPath   string
	disableRpcTest bool
}

func NewNsqdCoordRpcServer(coord *NsqdCoordinator, rootPath string) *NsqdCoordRpcServer {
	return &NsqdCoordRpcServer{
		nsqdCoord:     coord,
		rpcDispatcher: gorpc.NewDispatcher(),
		dataRootPath:  rootPath,
	}
}

// used only for test
func (self *NsqdCoordRpcServer) toggleDisableRpcTest(disable bool) {
	self.disableRpcTest = disable
	coordLog.Infof("rpc is disabled on node: %v", self.nsqdCoord.myNode.GetID())
}

// coord rpc server is used for communication with other replicas and the nsqlookupd
func (self *NsqdCoordRpcServer) start(ip, port string) (string, error) {
	self.rpcDispatcher.AddService("NsqdCoordRpcServer", self)
	self.rpcServer = gorpc.NewTCPServer(net.JoinHostPort(ip, port), self.rpcDispatcher.NewHandlerFunc())

	e := self.rpcServer.Start()
	if e != nil {
		coordLog.Errorf("start rpc server error : %v", e)
		panic(e)
	}

	coordLog.Infof("nsqd coordinator rpc listen at : %v", self.rpcServer.Listener.ListenAddr())
	return self.rpcServer.Listener.ListenAddr().String(), nil
}

func (self *NsqdCoordRpcServer) stop() {
	if self.rpcServer != nil {
		self.rpcServer.Stop()
	}
}

func (self *NsqdCoordRpcServer) checkLookupForWrite(lookupEpoch EpochType) *CoordErr {
	if self.disableRpcTest {
		return &CoordErr{"rpc disabled", RpcCommonErr, CoordNetErr}
	}
	return nil
}

func (self *NsqdCoordRpcServer) NotifyReleaseTopicLeader(rpcTopicReq *RpcReleaseTopicLeaderReq) *CoordErr {
	var ret CoordErr
	if err := self.checkLookupForWrite(rpcTopicReq.LookupdEpoch); err != nil {
		ret = *err
		return &ret
	}
	coordLog.Infof("got release leader session notify : %v, on node: %v", rpcTopicReq, self.nsqdCoord.myNode.GetID())
	topicCoord, err := self.nsqdCoord.getTopicCoord(rpcTopicReq.TopicName, rpcTopicReq.TopicPartition)
	if err != nil {
		coordLog.Infof("topic partition missing.")
		ret = *err
		return &ret
	}
	coordData := topicCoord.GetData()
	origSession := coordData.topicLeaderSession

	if coordData.GetLeaderSessionID() != "" && coordData.GetLeader() != coordData.GetLeaderSessionID() {
		// check if any old leader session acquired by mine is not released
		if self.nsqdCoord.GetMyID() == coordData.GetLeaderSessionID() {
			coordLog.Warningf("old leader session acquired by me is not released, my leader should release: %v", coordData)
			if origSession.LeaderEpoch != rpcTopicReq.TopicLeaderSessionEpoch ||
				origSession.Session != rpcTopicReq.TopicLeaderSession {
				coordLog.Warningf("old topic %v leader old session %v on local is not matched with newest : %v", rpcTopicReq.TopicName,
					origSession, rpcTopicReq)
				if rpcTopicReq.TopicLeaderSession != "" {
					origSession.LeaderEpoch = rpcTopicReq.TopicLeaderSessionEpoch
					origSession.Session = rpcTopicReq.TopicLeaderSession
				}
			}
			err = self.nsqdCoord.releaseTopicLeader(&coordData.topicInfo, &origSession)
			if err != nil {
				ret = *err
			}
			return &ret
		}
	}
	if !topicCoord.IsWriteDisabled() {
		coordLog.Errorf("topic %v release leader should disable write first", coordData.topicInfo.GetTopicDesp())
		ret = *ErrTopicCoordStateInvalid
		return &ret
	}
	if rpcTopicReq.Epoch < coordData.topicInfo.Epoch ||
		coordData.topicLeaderSession.LeaderEpoch != rpcTopicReq.TopicLeaderSessionEpoch {
		coordLog.Infof("topic info epoch mismatch while release leader: %v, %v", coordData,
			rpcTopicReq)
		ret = *ErrEpochMismatch
		return &ret
	}
	if coordData.GetLeader() == self.nsqdCoord.myNode.GetID() {
		coordLog.Infof("my leader should release: %v", coordData)
		err = self.nsqdCoord.releaseTopicLeader(&coordData.topicInfo, &origSession)
		if err != nil {
			ret = *err
		}
	} else {
		coordLog.Infof("Leader is not me while release: %v", coordData)
	}
	return &ret
}

func (self *NsqdCoordRpcServer) NotifyTopicLeaderSession(rpcTopicReq *RpcTopicLeaderSession) *CoordErr {
	s := time.Now().Unix()
	var ret CoordErr
	defer func() {
		coordErrStats.incCoordErr(&ret)
		e := time.Now().Unix()
		if e-s > int64(RPC_TIMEOUT/2) {
			coordLog.Infof("rpc call used: %v", e-s)
		}
	}()

	if err := self.checkLookupForWrite(rpcTopicReq.LookupdEpoch); err != nil {
		ret = *err
		return &ret
	}
	coordLog.Infof("got leader session notify : %v, leader node info:%v on node: %v", rpcTopicReq, rpcTopicReq.LeaderNode, self.nsqdCoord.myNode.GetID())
	topicCoord, err := self.nsqdCoord.getTopicCoord(rpcTopicReq.TopicName, rpcTopicReq.TopicPartition)
	if err != nil {
		coordLog.Infof("topic partition missing.")
		ret = *err
		return &ret
	}
	if !topicCoord.IsWriteDisabled() {
		tcData := topicCoord.GetData()
		if rpcTopicReq.JoinSession != "" {
			if FindSlice(tcData.topicInfo.ISR, self.nsqdCoord.myNode.GetID()) != -1 {
				coordLog.Errorf("join session should disable write first")
				ret = *ErrTopicCoordStateInvalid
				return &ret
			}
		}
		if rpcTopicReq.LeaderNode.ID != tcData.topicInfo.Leader {
			// Not allow to change the leader session to another during write
			// but we can be notified to know the lost leader session
			if rpcTopicReq.TopicLeaderSession != "" {
				coordLog.Errorf("change leader session to another should disable write first")
				ret = *ErrTopicCoordStateInvalid
				return &ret
			}
		}
	}
	newSession := &TopicLeaderSession{
		Topic:       rpcTopicReq.TopicName,
		Partition:   rpcTopicReq.TopicPartition,
		LeaderNode:  &rpcTopicReq.LeaderNode,
		Session:     rpcTopicReq.TopicLeaderSession,
		LeaderEpoch: rpcTopicReq.TopicLeaderSessionEpoch,
	}
	err = self.nsqdCoord.updateTopicLeaderSession(topicCoord, newSession, rpcTopicReq.JoinSession)
	if err != nil {
		ret = *err
	}
	return &ret
}

func (self *NsqdCoordRpcServer) NotifyAcquireTopicLeader(rpcTopicReq *RpcAcquireTopicLeaderReq) *CoordErr {
	s := time.Now().Unix()
	var ret CoordErr
	defer func() {
		coordErrStats.incCoordErr(&ret)
		e := time.Now().Unix()
		if e-s > int64(RPC_TIMEOUT/2) {
			coordLog.Infof("rpc call used: %v", e-s)
		}
	}()

	if err := self.checkLookupForWrite(rpcTopicReq.LookupdEpoch); err != nil {
		ret = *err
		return &ret
	}
	if rpcTopicReq.TopicPartition < 0 || rpcTopicReq.TopicName == "" {
		return ErrTopicArgError
	}
	topicCoord, err := self.nsqdCoord.getTopicCoord(rpcTopicReq.TopicName, rpcTopicReq.TopicPartition)
	if err != nil {
		coordLog.Infof("topic partition missing.")
		ret = *err
		return &ret
	}
	tcData := topicCoord.GetData()
	if tcData.topicInfo.Epoch != rpcTopicReq.Epoch ||
		tcData.GetLeader() != rpcTopicReq.LeaderNodeID ||
		tcData.GetLeader() != self.nsqdCoord.myNode.GetID() {
		coordLog.Infof("not topic leader while acquire leader: %v, %v", tcData, rpcTopicReq)
		return &ret
	}
	err = self.nsqdCoord.notifyAcquireTopicLeader(tcData)
	if err != nil {
		ret = *err
		return &ret
	}
	return &ret

}

func (self *NsqdCoordRpcServer) prepareUpdateTopicInfo(rpcTopicReq *RpcAdminTopicInfo) (bool, *CoordErr) {
	self.nsqdCoord.coordMutex.Lock()
	defer self.nsqdCoord.coordMutex.Unlock()
	coords, ok := self.nsqdCoord.topicCoords[rpcTopicReq.Name]
	myID := self.nsqdCoord.myNode.GetID()
	// the ordered topic can have multi master partitions on the same node
	if rpcTopicReq.Leader == myID && !rpcTopicReq.TopicMetaInfo.AllowMulti() {
		for pid, tc := range coords {
			if tc.GetData().GetLeader() != myID {
				continue
			}
			if pid != rpcTopicReq.Partition {
				coordLog.Infof("found local partition %v already exist master for this topic %v", pid, rpcTopicReq)
				return false, ErrTopicCoordExistingAndMismatch
			}
		}
	}
	if rpcTopicReq.Leader != myID &&
		FindSlice(rpcTopicReq.ISR, myID) == -1 &&
		FindSlice(rpcTopicReq.CatchupList, myID) == -1 {
		// a topic info not belong to me,
		// check if we need to delete local
		coordLog.Infof("Not a topic(%s) related to me. isr is : %v", rpcTopicReq.Name, rpcTopicReq.ISR)
		var tc *TopicCoordinator
		var ok2 bool
		if ok {
			tc, ok2 = coords[rpcTopicReq.Partition]
			if ok2 {
				delete(coords, rpcTopicReq.Partition)
				coordLog.Infof("topic(%s) is removing from local node since not related", rpcTopicReq.Name)
				tc.DeleteWithLock(false)
				tcData := tc.GetData()
				go func() {
					coordLog.Infof("topic(%s) local topic data is removed from local node since not related", rpcTopicReq.Name)
					if tcData.topicInfo.Leader == myID {
						self.nsqdCoord.releaseTopicLeader(&tcData.topicInfo, &tc.topicLeaderSession)
					}
					// never hold any coordinator lock while close or delete local topic
					self.nsqdCoord.localNsqd.CloseExistingTopic(rpcTopicReq.Name, rpcTopicReq.Partition)
				}()
			}
		}
		coordLog.Infof("topic(%s) is removed from local node since not related", rpcTopicReq.Name)
		return false, nil
	}

	return true, nil
}

func (self *NsqdCoordRpcServer) UpdateTopicInfo(rpcTopicReq *RpcAdminTopicInfo) *CoordErr {
	s := time.Now().Unix()
	var ret CoordErr
	defer func() {
		coordErrStats.incCoordErr(&ret)
		e := time.Now().Unix()
		if e-s > int64(RPC_TIMEOUT/2) {
			coordLog.Infof("UpdateTopicInfo rpc call used: %v", e-s)
		}
	}()

	if err := self.checkLookupForWrite(rpcTopicReq.LookupdEpoch); err != nil {
		ret = *err
		return &ret
	}
	coordLog.Infof("got update request for topic : %v on node: %v", rpcTopicReq, self.nsqdCoord.myNode.GetID())
	if rpcTopicReq.Partition < 0 || rpcTopicReq.Name == "" {
		return ErrTopicArgError
	}

	needUpdate, err := self.prepareUpdateTopicInfo(rpcTopicReq)
	if err != nil {
		ret = *err
		return &ret
	}
	if !needUpdate {
		return &ret
	}
	self.nsqdCoord.coordMutex.RLock()
	firstInit := false
	coords, ok := self.nsqdCoord.topicCoords[rpcTopicReq.Name]
	if !ok {
		firstInit = true
	} else {
		_, ok := coords[rpcTopicReq.Partition]
		if !ok {
			firstInit = true
		}
	}
	self.nsqdCoord.coordMutex.RUnlock()
	if firstInit {
		// here we may close or delete local topic if magic is wrong, so we should not check in coordinator lock
		checkErr := self.nsqdCoord.checkLocalTopicMagicCode(&rpcTopicReq.TopicPartitionMetaInfo, true)
		if checkErr != nil {
			ret = *ErrLocalTopicDataCorrupt
			return &ret
		}
	}

	// init should not update topic coordinator topic info, since we need do some check while update topic info
	tpCoord, _, initErr := self.nsqdCoord.initLocalTopicCoord(&rpcTopicReq.TopicPartitionMetaInfo, nil,
		GetTopicPartitionBasePath(self.dataRootPath, rpcTopicReq.Name, rpcTopicReq.Partition),
		false, true,
	)
	if tpCoord == nil {
		ret = *ErrLocalInitTopicCoordFailed
		return &ret
	}
	if initErr != ErrAlreadyExist {
		// init to disable write
		rpcTopicReq.DisableWrite = true
	}
	// should not hold topic coordinator write lock since it need update topic info during cluster message write
	err = self.nsqdCoord.updateTopicInfo(tpCoord, rpcTopicReq.DisableWrite, &rpcTopicReq.TopicPartitionMetaInfo)
	if err != nil {
		ret = *err
		return &ret
	}
	return &ret
}

func (self *NsqdCoordRpcServer) EnableTopicWrite(rpcTopicReq *RpcAdminTopicInfo) *CoordErr {
	s := time.Now().Unix()
	var ret CoordErr
	defer func() {
		coordErrStats.incCoordErr(&ret)
		e := time.Now().Unix()
		if e-s > int64(RPC_TIMEOUT/2) {
			coordLog.Infof("rpc call used: %v", e-s)
		}
	}()

	// set the topic as not writable.
	coordLog.Infof("got enable write for topic : %v", rpcTopicReq)
	if err := self.checkLookupForWrite(rpcTopicReq.LookupdEpoch); err != nil {
		ret = *err
		return &ret
	}
	tp, err := self.nsqdCoord.getTopicCoord(rpcTopicReq.Name, rpcTopicReq.Partition)
	if err != nil {
		ret = *err
		return &ret
	}
	begin := time.Now()
	tp.writeHold.Lock()
	defer tp.writeHold.Unlock()
	if time.Since(begin) > time.Second*3 {
		// timeout for waiting
		coordLog.Infof("timeout while enable write for topic: %v", tp.GetData().topicInfo.GetTopicDesp())
		err = ErrOperationExpired
	} else if tp.IsExiting() {
		coordLog.Infof("exiting while enable write for topic: %v", tp.GetData().topicInfo.GetTopicDesp())
		err = ErrTopicExiting
	} else {
		atomic.StoreInt32(&tp.disableWrite, 0)

		tcData := tp.GetData()
		if tcData.IsMineLeaderSessionReady(self.nsqdCoord.myNode.GetID()) {
			topicData, err := self.nsqdCoord.localNsqd.GetExistingTopic(tcData.topicInfo.Name, tcData.topicInfo.Partition)
			if err != nil {
				coordLog.Infof("no topic on local: %v, %v", tcData.topicInfo.GetTopicDesp(), err)
			} else {
				self.nsqdCoord.switchStateForMaster(tp, topicData, true)
			}
		}
	}

	if err != nil {
		ret = *err
		return &ret
	}
	return &ret
}

func (self *NsqdCoordRpcServer) DisableTopicWrite(rpcTopicReq *RpcAdminTopicInfo) *CoordErr {
	s := time.Now().Unix()
	var ret CoordErr
	defer func() {
		coordErrStats.incCoordErr(&ret)
		e := time.Now().Unix()
		if e-s > int64(RPC_TIMEOUT/2) {
			coordLog.Infof("rpc call used: %v", e-s)
		}
	}()

	coordLog.Infof("got disable write for topic : %v", rpcTopicReq)
	if err := self.checkLookupForWrite(rpcTopicReq.LookupdEpoch); err != nil {
		ret = *err
		return &ret
	}

	tp, err := self.nsqdCoord.getTopicCoord(rpcTopicReq.Name, rpcTopicReq.Partition)
	if err != nil {
		ret = *err
		return &ret
	}
	begin := time.Now()

	tp.writeHold.Lock()
	defer tp.writeHold.Unlock()
	if time.Since(begin) > time.Second*3 {
		// timeout for waiting
		err = ErrOperationExpired
	} else if tp.IsExiting() {
		coordLog.Infof("exiting while disable write for topic: %v", tp.GetData().topicInfo.GetTopicDesp())
		err = ErrTopicExiting
	} else {
		atomic.StoreInt32(&tp.disableWrite, 1)

		tcData := tp.GetData()
		localTopic, localErr := self.nsqdCoord.localNsqd.GetExistingTopic(tcData.topicInfo.Name, tcData.topicInfo.Partition)
		if localErr != nil {
			coordLog.Infof("no topic on local: %v, %v", tcData.topicInfo.GetTopicDesp(), localErr)
		} else {
			// we force sync topic channels while disable write because we may transfer or lose the leader, so
			// try sync channels anyway.
			isMeLeader := tcData.GetLeader() == self.nsqdCoord.GetMyID()
			if isMeLeader {
				self.nsqdCoord.trySyncTopicChannels(tcData, true, true)
			}
			self.nsqdCoord.switchStateForMaster(tp, localTopic, false)
		}
	}
	if err != nil {
		ret = *err
		return &ret
	}
	return &ret
}

func (self *NsqdCoordRpcServer) IsTopicWriteDisabled(rpcTopicReq *RpcAdminTopicInfo) bool {
	tp, err := self.nsqdCoord.getTopicCoord(rpcTopicReq.Name, rpcTopicReq.Partition)
	if err != nil {
		return false
	}
	if tp.IsWriteDisabled() {
		return true
	}
	localTopic, localErr := self.nsqdCoord.localNsqd.GetExistingTopic(rpcTopicReq.Name, rpcTopicReq.Partition)
	if localErr != nil {
		coordLog.Infof("no topic on local: %v, %v", rpcTopicReq.Name, localErr)
		return true
	}
	return localTopic.IsWriteDisabled()
}

func (self *NsqdCoordRpcServer) DeleteNsqdTopic(rpcTopicReq *RpcAdminTopicInfo) *CoordErr {
	var ret CoordErr
	defer coordErrStats.incCoordErr(&ret)
	if err := self.checkLookupForWrite(rpcTopicReq.LookupdEpoch); err != nil {
		ret = *err
		return &ret
	}

	coordLog.Infof("removing the local topic: %v", rpcTopicReq)
	_, err := self.nsqdCoord.removeTopicCoord(rpcTopicReq.Name, rpcTopicReq.Partition, true)
	if err != nil {
		ret = *err
		coordLog.Infof("delete topic %v failed : %v", rpcTopicReq.GetTopicDesp(), err)
	}
	return &ret
}

func (self *NsqdCoordRpcServer) GetTopicStats(topic string) *NodeTopicStats {
	s := time.Now().Unix()
	defer func() {
		e := time.Now().Unix()
		if e-s > int64(RPC_TIMEOUT/2) {
			coordLog.Infof("GetTopicStats rpc call used: %v", e-s)
		}
	}()

	var topicStats []nsqd.TopicStats
	if topic == "" {
		// all topic status
		topicStats = self.nsqdCoord.localNsqd.GetStats(false, true)
	} else {
		topicStats = self.nsqdCoord.localNsqd.GetTopicStatsWithFilter(false, topic, "", true)
	}
	stat := NewNodeTopicStats(self.nsqdCoord.myNode.GetID(), len(topicStats)*2, runtime.NumCPU())
	for _, ts := range topicStats {
		pid, _ := strconv.Atoi(ts.TopicPartition)
		// filter the catchup node
		coordData, coordErr := self.nsqdCoord.getTopicCoordData(ts.TopicName, pid)
		if coordErr != nil {
			continue
		}
		if !coordData.IsMineISR(self.nsqdCoord.myNode.GetID()) {
			continue
		}
		// plus 1 to handle the empty topic/channel
		stat.TopicTotalDataSize[ts.TopicFullName] += (ts.BackendDepth-ts.BackendStart)/1024/1024 + 1
		localTopic, err := self.nsqdCoord.localNsqd.GetExistingTopic(ts.TopicName, pid)
		if err != nil {
			coordLog.Infof("get local topic %v, %v failed: %v", ts.TopicFullName, pid, err)
		} else {
			pubhs := localTopic.GetDetailStats().GetHourlyStats()
			stat.TopicHourlyPubDataList[ts.TopicFullName] = pubhs
		}
		if ts.IsLeader || coordData.GetLeader() == self.nsqdCoord.myNode.GetID() {
			stat.TopicLeaderDataSize[ts.TopicFullName] += (ts.BackendDepth-ts.BackendStart)/1024/1024 + 1
			chList := stat.ChannelList[ts.TopicFullName]
			for _, chStat := range ts.Channels {
				if protocol.IsEphemeral(chStat.ChannelName) {
					continue
				}
				stat.ChannelDepthData[ts.TopicFullName] += chStat.DepthSize/1024/1024 + 1
				chList = append(chList, chStat.ChannelName)
			}
			stat.ChannelList[ts.TopicFullName] = chList
			stat.ChannelMetas[ts.TopicFullName] = localTopic.GetChannelMeta()
			stat.ChannelNum[ts.TopicFullName] = len(chList)

			if topic != "" && localTopic != nil {
				chConsumerList := stat.ChannelOffsets[ts.TopicFullName]
				// consumer offset only need by catchup node,
				// so the topic must not be empty
				channels := localTopic.GetChannelMapCopy()
				for _, ch := range channels {
					var syncOffset WrapChannelConsumerOffset
					syncOffset.Name = ch.GetName()
					syncOffset.Flush = true
					if ch.IsSkipped() {
						continue
					}
					if ch.IsEphemeral() {
						continue
					}
					confirmed := ch.GetConfirmed()
					syncOffset.VOffset = int64(confirmed.Offset())
					syncOffset.VCnt = confirmed.TotalMsgCnt()
					if ch.GetConfirmedIntervalLen() > 100 {
						syncOffset.NeedUpdateConfirmed = false
						syncOffset.ConfirmedInterval = nil
					} else {
						syncOffset.ConfirmedInterval = ch.GetConfirmedInterval()
						syncOffset.NeedUpdateConfirmed = true
					}
					chConsumerList = append(chConsumerList, syncOffset)
				}
				stat.ChannelOffsets[ts.TopicFullName] = chConsumerList
			}
		}
	}
	// the status of specific topic
	return stat
}

type RpcTopicData struct {
	TopicName               string
	TopicPartition          int
	Epoch                   EpochType
	TopicWriteEpoch         EpochType
	TopicLeaderSessionEpoch EpochType
	TopicLeaderSession      string
	TopicLeader             string
}

type RpcChannelState struct {
	RpcTopicData
	Channel        string
	Paused         int
	Skipped        int
	ZanTestSkipped int
}

type RpcChannelOffsetArg struct {
	RpcTopicData
	Channel string
	// position file + file offset
	ChannelOffset ChannelConsumerOffset
}

type RpcChannelListArg struct {
	RpcTopicData
	ChannelList []string
}

type RpcPutMessages struct {
	RpcTopicData
	LogData       CommitLogData
	TopicMessages []*nsqd.Message
	// raw message data will include the size header
	TopicRawMessage []byte
}

type RpcPutMessage struct {
	RpcTopicData
	LogData         CommitLogData
	TopicMessage    *nsqd.Message
	TopicRawMessage []byte
}

type RpcCommitLogReq struct {
	RpcTopicData
	LogOffset        int64
	LogStartIndex    int64
	LogCountNumIndex int64
	UseCountIndex    bool
}

type RpcCommitLogRsp struct {
	LogStartIndex    int64
	LogOffset        int64
	LogData          CommitLogData
	ErrInfo          CoordErr
	LogCountNumIndex int64
	UseCountIndex    bool
}

type RpcPullCommitLogsReq struct {
	RpcTopicData
	StartLogOffset   int64
	LogMaxNum        int
	StartIndexCnt    int64
	LogCountNumIndex int64
	UseCountIndex    bool
}

type RpcPullCommitLogsRsp struct {
	Logs     []CommitLogData
	DataList [][]byte
}

type RpcGetFullSyncInfoReq struct {
	RpcTopicData
}

type RpcGetFullSyncInfoRsp struct {
	FirstLogData CommitLogData
	StartInfo    LogStartInfo
}

type RpcGetBackupedDQReq struct {
	RpcTopicData
}
type RpcGetBackupedDQRsp struct {
	Buffer []byte
}

type RpcTestReq struct {
	Data string
}

type RpcTestRsp struct {
	RspData string
	RetErr  *CoordErr
}

type RpcConfirmedDelayedCursor struct {
	RpcTopicData
	UpdatedChannel string
	KeyList        [][]byte
	ChannelCntList map[string]uint64
	OtherCntList   map[int]uint64
	Timestamp      int64
}

type RpcNodeInfoReq struct {
	NodeID string
}

type RpcNodeInfoRsp struct {
	ID       string
	NodeIP   string
	TcpPort  string
	RpcPort  string
	HttpPort string
}

func (self *NsqdCoordinator) checkWriteForRpcCall(rpcData RpcTopicData) (*TopicCoordinator, *CoordErr) {
	topicCoord, err := self.getTopicCoord(rpcData.TopicName, rpcData.TopicPartition)
	if err != nil || topicCoord == nil {
		coordLog.Infof("rpc call with missing topic :%v", rpcData)
		return nil, err
	}
	tcData := topicCoord.GetData()
	if tcData.GetTopicEpochForWrite() != rpcData.TopicWriteEpoch {
		coordLog.Infof("rpc call with wrong epoch :%v, current: %v", rpcData, tcData.GetTopicEpochForWrite())
		self.requestNotifyNewTopicInfo(rpcData.TopicName, rpcData.TopicPartition)
		coordErrStats.incRpcCheckFailed()
		return nil, ErrEpochMismatch
	}
	if rpcData.TopicLeader != "" && tcData.GetLeader() != rpcData.TopicLeader {
		coordLog.Warningf("rpc call with wrong leader:%v, local: %v", rpcData, tcData.GetLeader())
		self.requestNotifyNewTopicInfo(rpcData.TopicName, rpcData.TopicPartition)
		coordErrStats.incRpcCheckFailed()
		return nil, ErrNotTopicLeader
	}
	if rpcData.TopicLeaderSession != tcData.GetLeaderSession() {
		// maybe running catchup
		coordLog.Warningf("call write with mismatch session: %v, local %v", rpcData, tcData.GetLeaderSession())
	}
	return topicCoord, nil
}

func (self *NsqdCoordRpcServer) UpdateChannelState(state *RpcChannelState) *CoordErr {
	var ret CoordErr
	defer coordErrStats.incCoordErr(&ret)
	tc, err := self.nsqdCoord.checkWriteForRpcCall(state.RpcTopicData)
	if err != nil {
		ret = *err
		return &ret
	}
	// update local channel offset
	err = self.nsqdCoord.updateChannelStateOnSlave(tc.GetData(), state.Channel, state.Paused, state.Skipped, state.ZanTestSkipped)
	if err != nil {
		ret = *err
		return &ret
	}
	return &ret
}

func (self *NsqdCoordRpcServer) UpdateChannelOffset(info *RpcChannelOffsetArg) *CoordErr {
	var ret CoordErr
	defer coordErrStats.incCoordErr(&ret)
	tc, err := self.nsqdCoord.checkWriteForRpcCall(info.RpcTopicData)
	if err != nil {
		ret = *err
		return &ret
	}
	// update local channel offset
	err = self.nsqdCoord.updateChannelOffsetOnSlave(tc.GetData(), info.Channel, info.ChannelOffset)
	if err != nil {
		ret = *err
		return &ret
	}
	return &ret
}

func (self *NsqdCoordRpcServer) UpdateChannelList(info *RpcChannelListArg) *CoordErr {
	var ret CoordErr
	defer coordErrStats.incCoordErr(&ret)
	tc, err := self.nsqdCoord.checkWriteForRpcCall(info.RpcTopicData)
	if err != nil {
		ret = *err
		return &ret
	}
	if tc.IsWriteDisabled() {
		return &ret
	}
	err = self.nsqdCoord.updateChannelListOnSlave(tc.GetData(), info.ChannelList)
	if err != nil {
		ret = *err
		return &ret
	}
	return &ret
}

func (self *NsqdCoordRpcServer) UpdateDelayedQueueState(info *RpcConfirmedDelayedCursor) *CoordErr {
	var ret CoordErr
	defer coordErrStats.incCoordErr(&ret)
	tc, err := self.nsqdCoord.checkWriteForRpcCall(info.RpcTopicData)
	if err != nil {
		ret = *err
		return &ret
	}
	// update local channel offset
	err = self.nsqdCoord.updateDelayedQueueStateOnSlave(tc.GetData(), info.UpdatedChannel,
		info.Timestamp, info.KeyList,
		info.OtherCntList, info.ChannelCntList)
	if err != nil {
		ret = *err
		return &ret
	}
	return &ret
}

func (self *NsqdCoordRpcServer) DeleteChannel(info *RpcChannelOffsetArg) *CoordErr {
	var ret CoordErr
	defer coordErrStats.incCoordErr(&ret)
	tc, err := self.nsqdCoord.checkWriteForRpcCall(info.RpcTopicData)
	if err != nil {
		ret = *err
		return &ret
	}
	// update local channel offset
	err = self.nsqdCoord.deleteChannelOnSlave(tc.GetData(), info.Channel)
	if err != nil {
		ret = *err
		return &ret
	}
	return &ret
}

// receive from leader
func (self *NsqdCoordRpcServer) PutDelayedMessage(info *RpcPutMessage) *CoordErr {
	if self.nsqdCoord.enableBenchCost || coordLog.Level() >= levellogger.LOG_DEBUG {
		s := time.Now()
		defer func() {
			e := time.Now()
			if e.Sub(s) > time.Duration(RPC_TIMEOUT/10) {
				coordLog.Infof("PutDelayedMessage rpc call used: %v, start: %v, end: %v", e.Sub(s), s, e)
			}
		}()
	}

	var ret CoordErr
	defer coordErrStats.incCoordErr(&ret)
	tc, err := self.nsqdCoord.checkWriteForRpcCall(info.RpcTopicData)
	if err != nil {
		ret = *err
		return &ret
	}
	// do local pub message
	if len(info.TopicRawMessage) > 0 {
		err = self.nsqdCoord.putRawDataOnSlave(tc, info.LogData, info.TopicRawMessage, true)
	} else {
		err = self.nsqdCoord.putMessageOnSlave(tc, info.LogData, info.TopicMessage, true)
	}
	if err != nil {
		ret = *err
		return &ret
	}
	return &ret
}

// receive from leader
func (self *NsqdCoordRpcServer) PutMessage(info *RpcPutMessage) *CoordErr {
	if self.nsqdCoord.enableBenchCost || coordLog.Level() >= levellogger.LOG_DEBUG {
		s := time.Now()
		defer func() {
			e := time.Now()
			if e.Sub(s) > time.Duration(RPC_TIMEOUT/10) {
				coordLog.Infof("PutMessage rpc call used: %v, start: %v, end: %v", e.Sub(s), s, e)
			}
		}()
	}

	var ret CoordErr
	defer coordErrStats.incCoordErr(&ret)
	tc, err := self.nsqdCoord.checkWriteForRpcCall(info.RpcTopicData)
	if err != nil {
		ret = *err
		return &ret
	}
	// do local pub message
	if len(info.TopicRawMessage) > 0 {
		err = self.nsqdCoord.putRawDataOnSlave(tc, info.LogData, info.TopicRawMessage, false)
	} else {
		err = self.nsqdCoord.putMessageOnSlave(tc, info.LogData, info.TopicMessage, false)
	}
	if err != nil {
		ret = *err
		return &ret
	}
	return &ret
}

func (self *NsqdCoordRpcServer) PutMessages(info *RpcPutMessages) *CoordErr {
	if coordLog.Level() >= levellogger.LOG_DEBUG {
		s := time.Now().Unix()
		defer func() {
			e := time.Now().Unix()
			if e-s > int64(RPC_TIMEOUT/2) {
				coordLog.Infof("PutMessages rpc call used: %v", e-s)
			}
		}()
	}

	var ret CoordErr
	defer coordErrStats.incCoordErr(&ret)
	tc, err := self.nsqdCoord.checkWriteForRpcCall(info.RpcTopicData)
	if err != nil {
		ret = *err
		return &ret
	}
	// do local pub message
	if len(info.TopicRawMessage) > 0 {
		err = self.nsqdCoord.putRawDataOnSlave(tc, info.LogData, info.TopicRawMessage, false)
	} else {
		err = self.nsqdCoord.putMessagesOnSlave(tc, info.LogData, info.TopicMessages)
	}
	if err != nil {
		ret = *err
		return &ret
	}
	return &ret
}

func (self *NsqdCoordRpcServer) GetLastCommitLogID(req *RpcCommitLogReq) (int64, error) {
	var ret int64
	tc, err := self.nsqdCoord.getTopicCoordData(req.TopicName, req.TopicPartition)
	if err != nil {
		return ret, err.ToErrorType()
	}
	ret = tc.logMgr.GetLastCommitLogID()
	return ret, nil
}

func (self *NsqdCoordRpcServer) GetLastDelayedQueueCommitLogID(req *RpcCommitLogReq) (int64, error) {
	var ret int64
	tc, err := self.nsqdCoord.getTopicCoordData(req.TopicName, req.TopicPartition)
	if err != nil {
		return ret, err.ToErrorType()
	}
	if tc.delayedLogMgr == nil {
		return 0, nil
	}
	ret = tc.delayedLogMgr.GetLastCommitLogID()
	return ret, nil
}

func handleCommitLogError(err error, logMgr *TopicCommitLogMgr, req *RpcCommitLogReq, ret *RpcCommitLogRsp) {
	var err2 error
	var logData *CommitLogData
	ret.LogStartIndex, ret.LogOffset, logData, err2 = logMgr.GetLastCommitLogOffsetV2()
	// if last err is ErrCommitLogEOF or OutOfBound, we need to check the log end again,
	// because the log end maybe changed. and if the second new log end is large than request data,
	// it means the log is changed during this
	// return ErrCommitLogEOF or OutOfBound means the leader has less log data than the others.
	if err2 != nil {
		if err2 == ErrCommitLogEOF {
			if err == ErrCommitLogEOF {
				ret.ErrInfo = *ErrTopicCommitLogEOF
			} else if err == ErrCommitLogLessThanSegmentStart {
				ret.ErrInfo = *ErrTopicCommitLogLessThanSegmentStart
			}
		} else {
			ret.ErrInfo = *NewCoordErr(err.Error(), CoordCommonErr)
			return
		}
	}
	ret.LogCountNumIndex, _ = logMgr.ConvertToCountIndex(ret.LogStartIndex, ret.LogOffset)
	ret.UseCountIndex = true
	if logData != nil {
		ret.LogData = *logData
	}
	if err == ErrCommitLogEOF || err == ErrCommitLogOutofBound {
		if req.UseCountIndex {
			if ret.LogCountNumIndex > req.LogCountNumIndex {
				ret.ErrInfo = *NewCoordErr("commit log changed", CoordCommonErr)
				return
			}
		} else {
			if ret.LogStartIndex > req.LogStartIndex ||
				(ret.LogStartIndex == req.LogStartIndex && ret.LogOffset > req.LogOffset) {
				ret.ErrInfo = *NewCoordErr("commit log changed", CoordCommonErr)
				return
			}
		}
	}
	if err2 == ErrCommitLogEOF && err == ErrCommitLogEOF {
		ret.ErrInfo = *ErrTopicCommitLogEOF
	} else if err == ErrCommitLogOutofBound {
		ret.ErrInfo = *ErrTopicCommitLogOutofBound
	} else if err == ErrCommitLogEOF {
		ret.ErrInfo = *ErrTopicCommitLogEOF
	} else if err == ErrCommitLogLessThanSegmentStart {
		ret.ErrInfo = *ErrTopicCommitLogLessThanSegmentStart
	} else {
		ret.ErrInfo = *NewCoordErr(err.Error(), CoordCommonErr)
	}
}

// return the logdata from offset, if the offset is larger than local,
// then return the last logdata on local.
func (self *NsqdCoordRpcServer) GetCommitLogFromOffset(req *RpcCommitLogReq) *RpcCommitLogRsp {
	return self.getCommitLogFromOffset(req, false)
}

func (self *NsqdCoordRpcServer) getCommitLogFromOffset(req *RpcCommitLogReq,
	fromDelayed bool) *RpcCommitLogRsp {
	var ret RpcCommitLogRsp
	tcData, coorderr := self.nsqdCoord.getTopicCoordData(req.TopicName, req.TopicPartition)
	if coorderr != nil {
		ret.ErrInfo = *coorderr
		return &ret
	}
	logMgr := tcData.logMgr
	if fromDelayed {
		logMgr = tcData.delayedLogMgr
		if logMgr == nil {
			ret.ErrInfo = *ErrTopicMissingDelayedLog
			return &ret
		}
	}
	var err error
	if req.UseCountIndex {
		newFileNum, newOffset, err := logMgr.ConvertToOffsetIndex(req.LogCountNumIndex)
		if err != nil {
			coordLog.Warningf("failed to convert to offset index: %v, err:%v", req.LogCountNumIndex, err)
			handleCommitLogError(err, logMgr, req, &ret)
			return &ret
		}
		req.LogStartIndex = newFileNum
		req.LogOffset = newOffset
	}

	logData, err := logMgr.GetCommitLogFromOffsetV2(req.LogStartIndex, req.LogOffset)
	if err != nil {
		handleCommitLogError(err, logMgr, req, &ret)
		return &ret
	} else {
		ret.LogStartIndex = req.LogStartIndex
		ret.LogOffset = req.LogOffset
		ret.LogData = *logData
		ret.LogCountNumIndex, _ = logMgr.ConvertToCountIndex(ret.LogStartIndex, ret.LogOffset)
	}
	return &ret
}

func (self *NsqdCoordRpcServer) PullCommitLogsAndData(req *RpcPullCommitLogsReq) (*RpcPullCommitLogsRsp, error) {
	return self.nsqdCoord.pullCommitLogsAndData(req, false)
}

func (self *NsqdCoordRpcServer) GetFullSyncInfo(req *RpcGetFullSyncInfoReq) (*RpcGetFullSyncInfoRsp, error) {
	return self.getFullSyncInfo(req, false)
}

func (self *NsqdCoordRpcServer) getFullSyncInfo(req *RpcGetFullSyncInfoReq, fromDelayed bool) (*RpcGetFullSyncInfoRsp, error) {
	var ret RpcGetFullSyncInfoRsp
	tcData, err := self.nsqdCoord.getTopicCoordData(req.TopicName, req.TopicPartition)
	if err != nil {
		return nil, err.ToErrorType()
	}

	logMgr := tcData.logMgr
	if fromDelayed {
		logMgr = tcData.delayedLogMgr
		if logMgr == nil {
			return nil, ErrTopicMissingDelayedLog.ToErrorType()
		}
	}

	var localErr error
	startInfo, firstLog, localErr := logMgr.GetLogStartInfo()
	if localErr != nil {
		if localErr != ErrCommitLogEOF {
			return nil, localErr
		}
	}
	coordLog.Infof("topic %v get log start info : %v", tcData.topicInfo.GetTopicDesp(), startInfo)

	ret.StartInfo = *startInfo
	if firstLog != nil {
		ret.FirstLogData = *firstLog
	} else {
		// we need to get the disk queue end info
		localTopic, err := self.nsqdCoord.localNsqd.GetExistingTopic(req.TopicName, req.TopicPartition)
		if err != nil {
			return nil, err
		}

		if fromDelayed {
			dq := localTopic.GetDelayedQueue()
			if dq == nil {
				coordLog.Infof("topic %v missing local delayed queue", tcData.topicInfo.GetTopicDesp())
				return nil, ErrLocalDelayedQueueMissing.ToErrorType()
			}
			ret.FirstLogData.MsgOffset = dq.TotalDataSize()
			ret.FirstLogData.MsgCnt = int64(dq.TotalMessageCnt())
		} else {
			ret.FirstLogData.MsgOffset = localTopic.TotalDataSize()
			ret.FirstLogData.MsgCnt = int64(localTopic.TotalMessageCnt())
		}
	}
	return &ret, nil
}

func (self *NsqdCoordRpcServer) TriggerLookupChanged() error {
	self.nsqdCoord.localNsqd.TriggerOptsNotification()
	coordLog.Infof("got lookup changed trigger")
	return nil
}

func (self *NsqdCoordRpcServer) GetNodeInfo(req *RpcNodeInfoReq) (*RpcNodeInfoRsp, error) {
	var ret RpcNodeInfoRsp

	if self.nsqdCoord.GetMyID() != req.NodeID {
		return nil, errors.New("node id mismatch")
	}

	ret.ID = req.NodeID
	ret.NodeIP = self.nsqdCoord.myNode.NodeIP
	ret.TcpPort = self.nsqdCoord.myNode.TcpPort
	ret.RpcPort = self.nsqdCoord.myNode.RpcPort
	ret.HttpPort = self.nsqdCoord.myNode.HttpPort

	return &ret, nil
}

func (self *NsqdCoordRpcServer) GetDelayedQueueCommitLogFromOffset(req *RpcCommitLogReq) *RpcCommitLogRsp {
	return self.getCommitLogFromOffset(req, true)
}

func (self *NsqdCoordRpcServer) PullDelayedQueueCommitLogsAndData(req *RpcPullCommitLogsReq) (*RpcPullCommitLogsRsp, error) {
	return self.nsqdCoord.pullCommitLogsAndData(req, true)
}

func (self *NsqdCoordRpcServer) GetDelayedQueueFullSyncInfo(req *RpcGetFullSyncInfoReq) (*RpcGetFullSyncInfoRsp, error) {
	return self.getFullSyncInfo(req, true)
}

func (self *NsqdCoordRpcServer) GetBackupedDelayedQueue(req *RpcGetBackupedDQReq) (*RpcGetBackupedDQRsp, error) {
	topicName := req.TopicName
	var ret RpcGetBackupedDQRsp

	localTopic, err := self.nsqdCoord.localNsqd.GetExistingTopic(req.TopicName, req.TopicPartition)
	if err != nil {
		return &ret, err
	}
	dq := localTopic.GetDelayedQueue()
	if dq == nil {
		coordLog.Infof("topic %v missing local delayed queue", localTopic.GetFullName())
		return &ret, ErrLocalDelayedQueueMissing.ToErrorType()
	}
	buf := bytes.Buffer{}
	_, err = dq.BackupKVStoreTo(&buf)
	if err != nil {
		nsqd.NsqLogger().Logf("failed to backup delayed queue for topic %v: %v", topicName, err)
		return &ret, err
	}
	ret.Buffer = buf.Bytes()
	return &ret, nil
}

func (self *NsqdCoordRpcServer) TestRpcCoordErr(req *RpcTestReq) *CoordErr {
	var ret CoordErr
	ret.ErrMsg = req.Data
	ret.ErrCode = RpcCommonErr
	ret.ErrType = CoordCommonErr
	return &ret
}

func (self *NsqdCoordRpcServer) TestRpcError(req *RpcTestReq) *RpcTestRsp {
	var ret RpcTestRsp
	ret.RspData = req.Data
	ret.RetErr = &CoordErr{
		ErrMsg:  req.Data,
		ErrCode: RpcNoErr,
		ErrType: CoordCommonErr,
	}
	return &ret
}

func (self *NsqdCoordRpcServer) TestRpcTimeout() error {
	time.Sleep(time.Second)
	return nil
}
