package consistence

import (
	"errors"
	"sort"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/youzan/nsq/internal/clusterinfo"
	"github.com/youzan/nsq/internal/protocol"
)

const (
	MAX_PARTITION_NUM  = 255
	MAX_SYNC_EVERY     = 4000
	MAX_RETENTION_DAYS = 60
)

func (nlcoord *NsqLookupCoordinator) GetAllLookupdNodes() ([]NsqLookupdNodeInfo, error) {
	return nlcoord.leadership.GetAllLookupdNodes()
}

func (nlcoord *NsqLookupCoordinator) GetLookupLeader() NsqLookupdNodeInfo {
	nlcoord.leaderMu.RLock()
	defer nlcoord.leaderMu.RUnlock()
	return nlcoord.leaderNode
}

func (nlcoord *NsqLookupCoordinator) GetTopicMetaInfo(topicName string, newest bool) (TopicMetaInfo, error) {
	if !newest {
		meta, ok := nlcoord.leadership.GetTopicMetaInfoTryCacheOnly(topicName)
		if ok {
			return meta, nil
		}
	}
	meta, cached, err := nlcoord.leadership.GetTopicMetaInfoTryCache(topicName)
	if err != nil {
		return TopicMetaInfo{}, err
	}
	if !cached {
		coordLog.Debugf("miss cache read for topic info: %v", topicName)
	}
	return meta, err
}

func (nlcoord *NsqLookupCoordinator) GetTopicsMetaInfoMap(topics []string) (map[string]TopicMetaInfo, error) {
	return nlcoord.leadership.GetTopicsMetaInfoMap(topics)
}

func (nlcoord *NsqLookupCoordinator) GetTopicLeaderNodes(topicName string) (map[string]string, error) {
	meta, cached, err := nlcoord.leadership.GetTopicMetaInfoTryCache(topicName)
	if err != nil {
		coordLog.Debugf("failed to get topic %v meta: %v", topicName, err)
		return nil, err
	}
	if !cached {
		coordLog.Infof("miss cache read for topic info: %v", topicName)
	}
	ret := make(map[string]string)
	var anyErr error
	for i := 0; i < meta.PartitionNum; i++ {
		info, err := nlcoord.leadership.GetTopicInfo(topicName, i)
		if err != nil {
			anyErr = err
			continue
		}
		if len(info.ISR) > info.Replica/2 && !nlcoord.isTopicWriteDisabled(info) {
			ret[strconv.Itoa(info.Partition)] = info.Leader
		}
	}
	if len(ret) == 0 {
		return ret, anyErr
	}
	return ret, nil
}

func (nlcoord *NsqLookupCoordinator) GetTopicLeaderForConsume(topicName string, part int) (string, bool) {
	info, ok := nlcoord.leadership.GetTopicInfoFromCacheOnly(topicName, part)
	if ok {
		return info.Leader, true
	}
	return "", false
}

func (nlcoord *NsqLookupCoordinator) IsMineLeader() bool {
	return nlcoord.GetLookupLeader().GetID() == nlcoord.myNode.GetID()
}

func (nlcoord *NsqLookupCoordinator) IsClusterStable() bool {
	return atomic.LoadInt32(&nlcoord.isClusterUnstable) == 0 &&
		atomic.LoadInt32(&nlcoord.isUpgrading) == 0
}

func (nlcoord *NsqLookupCoordinator) SetTopNBalance(enable bool) error {
	if nlcoord.GetLookupLeader().GetID() != nlcoord.myNode.GetID() {
		coordLog.Infof("not leader while delete topic")
		return ErrNotNsqLookupLeader
	}
	if enable {
		atomic.StoreInt32(&nlcoord.enableTopNBalance, 1)
	} else {
		atomic.StoreInt32(&nlcoord.enableTopNBalance, 0)
	}
	return nil
}

func (nlcoord *NsqLookupCoordinator) SetClusterUpgradeState(upgrading bool) error {
	if nlcoord.GetLookupLeader().GetID() != nlcoord.myNode.GetID() {
		coordLog.Infof("not leader while delete topic")
		return ErrNotNsqLookupLeader
	}

	if upgrading {
		if !atomic.CompareAndSwapInt32(&nlcoord.isUpgrading, 0, 1) {
			coordLog.Infof("the cluster state is already upgrading")
			return nil
		}
		coordLog.Infof("the cluster state has been changed to upgrading")
	} else {
		if !atomic.CompareAndSwapInt32(&nlcoord.isUpgrading, 1, 0) {
			return nil
		}
		coordLog.Infof("the cluster state has been changed to normal")
		topics, err := nlcoord.leadership.ScanTopics()
		if err != nil {
			coordLog.Infof("failed to scan topics: %v", err)
			return err
		}
		for _, topicInfo := range topics {
			retry := 0
			for retry < 3 {
				retry++
				leaderSession, err := nlcoord.leadership.GetTopicLeaderSession(topicInfo.Name, topicInfo.Partition)
				if err != nil {
					coordLog.Infof("failed to get topic %v leader session: %v", topicInfo.GetTopicDesp(), err)
					nlcoord.notifyISRTopicMetaInfo(&topicInfo)
					nlcoord.notifyAcquireTopicLeader(&topicInfo)
					time.Sleep(time.Millisecond * 100)
				} else {
					nlcoord.notifyTopicLeaderSession(&topicInfo, leaderSession, "")
					break
				}
			}
		}
		go nlcoord.triggerCheckTopics("", 0, time.Second)
	}
	return nil
}

func (nlcoord *NsqLookupCoordinator) MoveTopicPartitionDataByManual(topicName string,
	partitionID int, moveLeader bool, fromNode string, toNode string) error {
	coordLog.Infof("try move topic %v-%v from node %v to %v", topicName, partitionID, fromNode, toNode)
	err := nlcoord.dpm.moveTopicPartitionByManual(topicName, partitionID, moveLeader, fromNode, toNode)
	if err != nil {
		coordLog.Infof("failed to move the topic partition: %v", err)
	}
	return err
}

func (nlcoord *NsqLookupCoordinator) GetClusterNodeLoadFactor() (map[string]float64, map[string]float64) {
	currentNodes := nlcoord.getCurrentNodes()
	leaderFactors := make(map[string]float64, len(currentNodes))
	nodeFactors := make(map[string]float64, len(currentNodes))
	for nodeID, nodeInfo := range currentNodes {
		topicStat, err := nlcoord.getNsqdTopicStat(nodeInfo)
		if err != nil {
			coordLog.Infof("failed to get node topic status : %v", nodeID)
			continue
		}
		leaderLF, nodeLF := topicStat.GetNodeLoadFactor()
		leaderFactors[nodeID] = leaderLF
		nodeFactors[nodeID] = nodeLF
	}
	return leaderFactors, nodeFactors
}

func (nlcoord *NsqLookupCoordinator) GetClusterTopNTopics(n int) LFListT {
	currentNodes := nlcoord.getCurrentNodes()
	nodeTopicStats := make([]NodeTopicStats, 0, len(currentNodes))
	for nodeID, nodeInfo := range currentNodes {
		topicStat, err := nlcoord.getNsqdTopicStat(nodeInfo)
		if err != nil {
			coordLog.Infof("failed to get node topic status : %v", nodeID)
			continue
		}
		nodeTopicStats = append(nodeTopicStats, *topicStat)
	}
	topicInfoList, err := nlcoord.leadership.ScanTopics()
	if err != nil {
		coordLog.Infof("scan topics error: %v", err)
		return nil
	}
	return getTopNTopicsStats(nodeTopicStats, topicInfoList, n, false)
}

func (nlcoord *NsqLookupCoordinator) IsTopicLeader(topic string, part int, nid string) bool {
	t, err := nlcoord.leadership.GetTopicInfo(topic, part)
	if err != nil {
		return false
	}
	return t.Leader == nid
}

func (nlcoord *NsqLookupCoordinator) MarkNodeAsRemoving(nid string) error {
	if nlcoord.GetLookupLeader().GetID() != nlcoord.myNode.GetID() {
		coordLog.Infof("not leader while delete topic")
		return ErrNotNsqLookupLeader
	}

	coordLog.Infof("try mark node %v as removed", nid)
	nlcoord.nodesMutex.Lock()
	newRemovingNodes := make(map[string]string)
	if _, ok := nlcoord.removingNodes[nid]; ok {
		coordLog.Infof("already mark as removing")
	} else {
		newRemovingNodes[nid] = "marked"
		for id, removeState := range nlcoord.removingNodes {
			newRemovingNodes[id] = removeState
		}
		nlcoord.removingNodes = newRemovingNodes
	}
	nlcoord.nodesMutex.Unlock()
	return nil
}

func (nlcoord *NsqLookupCoordinator) DeleteTopicForce(topic string, partition string) error {
	if nlcoord.GetLookupLeader().GetID() != nlcoord.myNode.GetID() {
		coordLog.Infof("not leader while delete topic")
		return ErrNotNsqLookupLeader
	}
	begin := time.Now()
	defer atomic.StoreInt32(&nlcoord.interruptChecking, 0)
	for !atomic.CompareAndSwapInt32(&nlcoord.doChecking, 0, 1) {
		coordLog.Infof("waiting check topic finish")
		atomic.StoreInt32(&nlcoord.interruptChecking, 1)
		time.Sleep(time.Millisecond * 200)
		if time.Since(begin) > time.Second*5 {
			return ErrClusterUnstable
		}
	}
	defer atomic.StoreInt32(&nlcoord.doChecking, 0)

	coordLog.Infof("delete topic: %v, with partition: %v", topic, partition)

	if partition == "**" {
		nlcoord.joinStateMutex.Lock()
		state, ok := nlcoord.joinISRState[topic]
		nlcoord.joinStateMutex.Unlock()
		if ok {
			state.Lock()
			if state.waitingJoin {
				state.waitingJoin = false
				state.waitingSession = ""
				if state.doneChan != nil {
					close(state.doneChan)
					state.doneChan = nil
				}
			}
			state.Unlock()
		}
		// delete all
		for pid := 0; pid < MAX_PARTITION_NUM; pid++ {
			nlcoord.deleteTopicPartitionForce(topic, pid)
		}
		nlcoord.leadership.DeleteWholeTopic(topic)
	} else {
		pid, err := strconv.Atoi(partition)
		if err != nil {
			coordLog.Infof("failed to parse the partition id : %v, %v", partition, err)
			return err
		}
		nlcoord.deleteTopicPartitionForce(topic, pid)
	}
	return nil
}

func (nlcoord *NsqLookupCoordinator) DeleteTopic(topic string, partition string) error {
	if nlcoord.GetLookupLeader().GetID() != nlcoord.myNode.GetID() {
		coordLog.Infof("not leader while delete topic")
		return ErrNotNsqLookupLeader
	}

	begin := time.Now()
	defer atomic.StoreInt32(&nlcoord.interruptChecking, 0)
	for !atomic.CompareAndSwapInt32(&nlcoord.doChecking, 0, 1) {
		coordLog.Infof("delete topic %v waiting check topic finish", topic)
		atomic.StoreInt32(&nlcoord.interruptChecking, 1)
		time.Sleep(time.Millisecond * 200)
		if time.Since(begin) > time.Second*5 {
			return ErrClusterUnstable
		}
	}
	defer atomic.StoreInt32(&nlcoord.doChecking, 0)
	// TODO: check partition number for topic, maybe failed to create
	coordLog.Infof("delete topic: %v, with partition: %v", topic, partition)
	if ok, err := nlcoord.leadership.IsExistTopic(topic); !ok {
		coordLog.Infof("no topic : %v", err)
		return ErrKeyNotFound
	}

	if partition == "**" {
		// delete all
		meta, _, err := nlcoord.leadership.GetTopicMetaInfo(topic)
		if err != nil {
			coordLog.Infof("failed to get meta for topic: %v", err)
			meta.PartitionNum = MAX_PARTITION_NUM
		}
		nlcoord.joinStateMutex.Lock()
		state, ok := nlcoord.joinISRState[topic]
		nlcoord.joinStateMutex.Unlock()
		if ok {
			state.Lock()
			if state.waitingJoin {
				state.waitingJoin = false
				state.waitingSession = ""
				if state.doneChan != nil {
					close(state.doneChan)
					state.doneChan = nil
				}
			}
			state.Unlock()
		}

		for pid := 0; pid < meta.PartitionNum; pid++ {
			err := nlcoord.deleteTopicPartition(topic, pid)
			if err != nil {
				coordLog.Infof("failed to delete topic partition %v for topic: %v, err:%v", pid, topic, err)
			}
		}
		err = nlcoord.leadership.DeleteWholeTopic(topic)
		if err != nil {
			coordLog.Infof("failed to delete whole topic: %v : %v", topic, err)
		}
	} else {
		pid, err := strconv.Atoi(partition)
		if err != nil {
			coordLog.Infof("failed to parse the partition id : %v, %v", partition, err)
			return err
		}

		return nlcoord.deleteTopicPartition(topic, pid)
	}
	return nil
}

func (nlcoord *NsqLookupCoordinator) deleteTopicPartitionForce(topic string, pid int) error {
	nlcoord.leadership.DeleteTopic(topic, pid)
	currentNodes := nlcoord.getCurrentNodes()
	var topicInfo TopicPartitionMetaInfo
	topicInfo.Name = topic
	topicInfo.Partition = pid
	for _, node := range currentNodes {
		c, rpcErr := nlcoord.acquireRpcClient(node.ID)
		if rpcErr != nil {
			coordLog.Infof("failed to get rpc client: %v, %v", node.ID, rpcErr)
			continue
		}
		rpcErr = c.DeleteNsqdTopic(nlcoord.GetLookupLeader().Epoch, &topicInfo)
		if rpcErr != nil {
			coordLog.Infof("failed to call rpc : %v, %v", node.ID, rpcErr)
		}
	}
	return nil
}

func (nlcoord *NsqLookupCoordinator) deleteTopicPartition(topic string, pid int) error {
	topicInfo, commonErr := nlcoord.leadership.GetTopicInfo(topic, pid)
	if commonErr != nil {
		coordLog.Infof("failed to get the topic info while delete topic: %v", commonErr)
		return commonErr
	}
	commonErr = nlcoord.leadership.DeleteTopic(topic, pid)
	if commonErr != nil {
		coordLog.Infof("failed to delete the topic info : %v", commonErr)
		return commonErr
	}
	for _, id := range topicInfo.CatchupList {
		c, rpcErr := nlcoord.acquireRpcClient(id)
		if rpcErr != nil {
			coordLog.Infof("failed to get rpc client: %v, %v", id, rpcErr)
			continue
		}
		rpcErr = c.DeleteNsqdTopic(nlcoord.GetLookupLeader().Epoch, topicInfo)
		if rpcErr != nil {
			coordLog.Infof("failed to call rpc : %v, %v", id, rpcErr)
		}
	}
	for _, id := range topicInfo.ISR {
		c, rpcErr := nlcoord.acquireRpcClient(id)
		if rpcErr != nil {
			coordLog.Infof("failed to get rpc client: %v, %v", id, rpcErr)
			continue
		}
		rpcErr = c.DeleteNsqdTopic(nlcoord.GetLookupLeader().Epoch, topicInfo)
		if rpcErr != nil {
			coordLog.Infof("failed to call rpc : %v, %v", id, rpcErr)
		}
	}
	// try remove on other nodes, maybe some left data
	allNodes := nlcoord.getCurrentNodes()
	for _, n := range allNodes {
		c, rpcErr := nlcoord.acquireRpcClient(n.GetID())
		if rpcErr != nil {
			continue
		}
		c.DeleteNsqdTopic(nlcoord.GetLookupLeader().Epoch, topicInfo)
	}

	return nil
}

//return current registered channels slize under target topic
func (nlcoord *NsqLookupCoordinator) GetRegisteredChannel(topic string) ([]string, error) {
	topicMeta, _, err := nlcoord.leadership.GetTopicMetaInfo(topic)
	if err != nil {
		coordLog.Infof("get topic key %v failed :%v", topic, err)
		return nil, err
	}

	var registeredChannels []string
	registeredChannelsMap := make(map[string]bool)
	for i := 0; i < topicMeta.PartitionNum; i++ {
		topicInfo, err := nlcoord.leadership.GetTopicInfo(topic, i)
		if err != nil {
			coordLog.Infof("failed get info for topic : %v-%v, %v", topic, i, err)
			return nil, err
		}
		for _, ch := range topicInfo.Channels {
			registeredChannelsMap[ch] = true
		}
	}
	//convert registered channels map back
	for ch, _ := range registeredChannelsMap {
		channelName := ch
		registeredChannels = append(registeredChannels, channelName)
	}

	return registeredChannels, nil
}

//Update registered channels
func (nlcoord *NsqLookupCoordinator) UpdateRegisteredChannel(topic string, registeredChannels []string) error {
	if nlcoord.GetLookupLeader().GetID() != nlcoord.myNode.GetID() {
		coordLog.Infof("not leader while create topic")
		return ErrNotNsqLookupLeader
	}

	if !protocol.IsValidTopicName(topic) {
		return errors.New("invalid topic name")
	}

	nlcoord.joinStateMutex.Lock()
	state, ok := nlcoord.joinISRState[topic]
	if !ok {
		state = &JoinISRState{}
		nlcoord.joinISRState[topic] = state
	}
	nlcoord.joinStateMutex.Unlock()
	state.Lock()
	defer state.Unlock()
	if state.waitingJoin {
		coordLog.Warningf("topic state is not ready:%v, %v ", topic, state)
		return ErrWaitingJoinISR.ToErrorType()
	}
	//err list
	var errs []error
	if ok, _ := nlcoord.leadership.IsExistTopic(topic); !ok {
		coordLog.Infof("topic not exist %v ", topic)
		return ErrTopicNotCreated
	} else {
		meta, _, err := nlcoord.leadership.GetTopicMetaInfo(topic)
		if err != nil {
			coordLog.Infof("get topic key %v failed :%v", topic, err)
			return err
		}

		//sort register channels
		sort.Strings(registeredChannels)
		for i := 0; i < meta.PartitionNum; i++ {
			topicInfo, err := nlcoord.leadership.GetTopicInfo(topic, i)
			if err != nil {
				coordLog.Infof("failed get info for topic : %v-%v, %v", topic, i, err)
				errs = append(errs, err)
				continue
			}
			if topicInfo.TopicMetaInfo != meta {
				coordLog.Warningf("topic partition meta info %v should match topic meta %v", topicInfo, meta)
			}
			topicReplicaInfo := &topicInfo.TopicPartitionReplicaInfo
			topicReplicaInfo.Channels = registeredChannels
			err = nlcoord.leadership.UpdateTopicNodeInfo(topic, i, topicReplicaInfo, topicReplicaInfo.Epoch)
			if err != nil {
				coordLog.Infof("failed update info for topic : %v-%v, %v", topic, i, err)
				errs = append(errs, err)
				continue
			}
			//override channels with registered ones
			topicInfo.Channels = registeredChannels
			rpcErr := nlcoord.notifyISRTopicMetaInfo(topicInfo)
			if rpcErr != nil {
				coordLog.Warningf("failed notify topic info : %v", rpcErr)
			} else {
				coordLog.Infof("topic %v update successful.", topicInfo)
			}
		}
	}
	if len(errs) > 0 {
		return clusterinfo.ErrList(errs)
	}
	return nil
}

func (nlcoord *NsqLookupCoordinator) ChangeTopicMetaParam(topic string,
	newSyncEvery int, newRetentionDay int, newReplicator int, upgradeExt string, disableChannelAutoCreate string) error {
	if nlcoord.GetLookupLeader().GetID() != nlcoord.myNode.GetID() {
		coordLog.Infof("not leader while create topic")
		return ErrNotNsqLookupLeader
	}

	if !protocol.IsValidTopicName(topic) {
		return errors.New("invalid topic name")
	}

	if newRetentionDay > MAX_RETENTION_DAYS {
		return errors.New("max retention days allowed exceed")
	}
	if newSyncEvery > MAX_SYNC_EVERY {
		return errors.New("max sync every allowed exceed")
	}
	if newReplicator > 5 {
		return errors.New("max replicator allowed exceed")
	}

	nlcoord.joinStateMutex.Lock()
	state, ok := nlcoord.joinISRState[topic]
	if !ok {
		state = &JoinISRState{}
		nlcoord.joinISRState[topic] = state
	}
	nlcoord.joinStateMutex.Unlock()
	state.Lock()
	defer state.Unlock()
	if state.waitingJoin {
		coordLog.Warningf("topic state is not ready:%v, %v ", topic, state)
		return ErrWaitingJoinISR.ToErrorType()
	}
	var meta TopicMetaInfo
	if ok, _ := nlcoord.leadership.IsExistTopic(topic); !ok {
		coordLog.Infof("topic not exist %v ", topic)
		return ErrTopicNotCreated
	} else {
		oldMeta, oldGen, err := nlcoord.leadership.GetTopicMetaInfo(topic)
		if err != nil {
			coordLog.Infof("get topic key %v failed :%v", topic, err)
			return err
		}
		currentNodes := nlcoord.getCurrentNodes()
		meta = oldMeta
		if newSyncEvery >= 0 {
			meta.SyncEvery = newSyncEvery
		}
		if newRetentionDay >= 0 {
			meta.RetentionDay = int32(newRetentionDay)
		}
		if newReplicator > 0 {
			meta.Replica = newReplicator
		}
		// change to ext only, can not change ext to non-ext
		needDisableWrite := false
		if upgradeExt == "true" && !meta.Ext {
			meta.Ext = true
			needDisableWrite = true
		}
		//update channel auto create opt
		if disableChannelAutoCreate == "true" && !meta.DisableChannelAutoCreate {
			meta.DisableChannelAutoCreate = true
		} else if disableChannelAutoCreate == "false" && meta.DisableChannelAutoCreate {
			meta.DisableChannelAutoCreate = false
		}
		if needDisableWrite {
			if !atomic.CompareAndSwapInt32(&nlcoord.isUpgrading, 0, 1) {
				coordLog.Infof("the cluster state is already upgrading")
				return errors.New("the cluster is upgrading.")
			}
			defer atomic.StoreInt32(&nlcoord.isUpgrading, 0)
		}
		err = nlcoord.updateTopicMeta(currentNodes, topic, meta, oldGen)
		if err != nil {
			return err
		}

		for i := 0; i < meta.PartitionNum; i++ {
			topicInfo, err := nlcoord.leadership.GetTopicInfo(topic, i)
			if err != nil {
				coordLog.Infof("failed get info for topic : %v-%v, %v", topic, i, err)
				continue
			}
			if topicInfo.TopicMetaInfo != meta {
				coordLog.Warningf("topic partition meta info %v should match topic meta %v", topicInfo, meta)
			}
			topicReplicaInfo := &topicInfo.TopicPartitionReplicaInfo
			err = nlcoord.leadership.UpdateTopicNodeInfo(topic, i, topicReplicaInfo, topicReplicaInfo.Epoch)
			if err != nil {
				coordLog.Infof("failed update info for topic : %v-%v, %v", topic, i, err)
				continue
			}
			if needDisableWrite {
				nlcoord.notifyLeaderDisableTopicWrite(topicInfo)
				nlcoord.notifyISRDisableTopicWrite(topicInfo)
			}
			rpcErr := nlcoord.notifyTopicMetaInfo(topicInfo)
			if rpcErr != nil {
				coordLog.Warningf("failed notify topic info : %v", rpcErr)
			} else {
				coordLog.Infof("topic %v update successful.", topicInfo)
				if needDisableWrite {
					nlcoord.notifyEnableTopicWrite(topicInfo)
				}
			}
		}

		go nlcoord.triggerCheckTopics("", 0, 0)
	}
	return nil
}

func (nlcoord *NsqLookupCoordinator) updateTopicMeta(currentNodes map[string]NsqdNodeInfo, topic string, meta TopicMetaInfo, oldGen EpochType) error {
	if meta.SyncEvery > MAX_SYNC_EVERY {
		coordLog.Infof("topic %v sync every with too large %v, set to max", topic, meta)
		meta.SyncEvery = MAX_SYNC_EVERY
	}
	coordLog.Infof("update topic: %v, with meta: %v", topic, meta)

	if meta.AllowMulti() {
		if len(currentNodes) < meta.Replica {
			coordLog.Infof("nodes %v is less than replica %v", len(currentNodes), meta)
			return ErrNodeUnavailable.ToErrorType()
		}
	} else {
		if len(currentNodes) < meta.Replica || len(currentNodes) < meta.PartitionNum {
			coordLog.Infof("nodes %v is less than replica or partition %v", len(currentNodes), meta)
			return ErrNodeUnavailable.ToErrorType()
		}
		if len(currentNodes) < meta.Replica*meta.PartitionNum {
			coordLog.Infof("nodes is less than replica*partition")
			return ErrNodeUnavailable.ToErrorType()
		}
	}
	return nlcoord.leadership.UpdateTopicMetaInfo(topic, &meta, oldGen)
}

func (nlcoord *NsqLookupCoordinator) ExpandTopicPartition(topic string, newPartitionNum int) error {
	if nlcoord.GetLookupLeader().GetID() != nlcoord.myNode.GetID() {
		coordLog.Infof("not leader while create topic")
		return ErrNotNsqLookupLeader
	}

	if !protocol.IsValidTopicName(topic) {
		return errors.New("invalid topic name")
	}

	if newPartitionNum >= MAX_PARTITION_NUM {
		return errors.New("max partition allowed exceed")
	}

	coordLog.Infof("expand topic %v partition number to %v", topic, newPartitionNum)
	if !nlcoord.IsClusterStable() {
		return ErrClusterUnstable
	}
	nlcoord.joinStateMutex.Lock()
	state, ok := nlcoord.joinISRState[topic]
	if !ok {
		state = &JoinISRState{}
		nlcoord.joinISRState[topic] = state
	}
	nlcoord.joinStateMutex.Unlock()
	state.Lock()
	defer state.Unlock()
	if state.waitingJoin {
		coordLog.Warningf("topic state is not ready:%v, %v ", topic, state)
		return ErrWaitingJoinISR.ToErrorType()
	}
	var meta TopicMetaInfo
	if ok, _ := nlcoord.leadership.IsExistTopic(topic); !ok {
		coordLog.Infof("topic not exist %v", topic)
		return ErrTopicNotCreated
	} else {
		oldMeta, oldGen, err := nlcoord.leadership.GetTopicMetaInfo(topic)
		if err != nil {
			coordLog.Infof("get topic key %v failed :%v", topic, err)
			return err
		}
		meta = oldMeta
		if newPartitionNum < meta.PartitionNum {
			return errors.New("the partition number can not be reduced")
		}
		currentNodes := nlcoord.getCurrentNodes()
		meta.PartitionNum = newPartitionNum
		err = nlcoord.updateTopicMeta(currentNodes, topic, meta, oldGen)
		if err != nil {
			coordLog.Infof("update topic %v meta failed :%v", topic, err)
			return err
		}
		return nlcoord.checkAndUpdateTopicPartitions(currentNodes, topic, meta)
	}
}

func (nlcoord *NsqLookupCoordinator) CreateTopic(topic string, meta TopicMetaInfo) error {
	if nlcoord.GetLookupLeader().GetID() != nlcoord.myNode.GetID() {
		coordLog.Infof("not leader while create topic")
		return ErrNotNsqLookupLeader
	}

	if !protocol.IsValidTopicName(topic) {
		return errors.New("invalid topic name")
	}

	// TODO: handle default load factor
	if meta.PartitionNum >= MAX_PARTITION_NUM {
		return errors.New("max partition allowed exceed")
	}

	currentNodes := nlcoord.getCurrentNodes()
	if len(currentNodes) < meta.Replica {
		coordLog.Infof("nodes %v is less than replica %v", len(currentNodes), meta)
		return ErrNodeUnavailable.ToErrorType()
	}
	if !meta.AllowMulti() && len(currentNodes) < meta.PartitionNum {
		coordLog.Infof("nodes %v is less than partition %v", len(currentNodes), meta)
		return ErrNodeUnavailable.ToErrorType()
	}
	if !meta.AllowMulti() && len(currentNodes) < meta.Replica*meta.PartitionNum {
		coordLog.Infof("nodes is less than replica*partition")
		return ErrNodeUnavailable.ToErrorType()
	}

	nlcoord.joinStateMutex.Lock()
	state, ok := nlcoord.joinISRState[topic]
	if !ok {
		state = &JoinISRState{}
		nlcoord.joinISRState[topic] = state
	}
	nlcoord.joinStateMutex.Unlock()
	state.Lock()
	defer state.Unlock()
	if state.waitingJoin {
		coordLog.Warningf("topic state is not ready:%v, %v ", topic, state)
		return ErrWaitingJoinISR.ToErrorType()
	}
	if meta.SyncEvery > MAX_SYNC_EVERY {
		coordLog.Infof("topic %v sync every with too large %v, set to max", topic, meta)
		meta.SyncEvery = MAX_SYNC_EVERY
	}

	if ok, _ := nlcoord.leadership.IsExistTopic(topic); !ok {
		meta.MagicCode = time.Now().UnixNano()
		err := nlcoord.leadership.CreateTopic(topic, &meta)
		if err != nil {
			coordLog.Infof("create topic key %v failed :%v", topic, err)
			return err
		}
	} else {
		coordLog.Warningf("topic already exist :%v ", topic)
		// check if meta is the same, if so we re-create again to make sure partitions are all ready
		oldMeta, _, err := nlcoord.leadership.GetTopicMetaInfo(topic)
		if err != nil {
			coordLog.Infof("get topic meta key %v failed :%v", topic, err)
			return err
		}
		meta.MagicCode = oldMeta.MagicCode
		// handle old topic with large sync every param
		if oldMeta.SyncEvery >= MAX_SYNC_EVERY {
			meta.SyncEvery = oldMeta.SyncEvery
		}
		if oldMeta != meta {
			return ErrAlreadyExist
		}
	}
	coordLog.Infof("create topic: %v, with meta: %v", topic, meta)

	return nlcoord.checkAndUpdateTopicPartitions(currentNodes, topic, meta)
}

func (nlcoord *NsqLookupCoordinator) checkAndUpdateTopicPartitions(currentNodes map[string]NsqdNodeInfo,
	topic string, meta TopicMetaInfo) error {
	existPart := make(map[int]*TopicPartitionMetaInfo)
	for i := 0; i < meta.PartitionNum; i++ {
		err := nlcoord.leadership.CreateTopicPartition(topic, i)
		if err != nil {
			coordLog.Warningf("failed to create topic %v-%v: %v", topic, i, err)
			// handle already exist, the partition dir may exist but missing real topic info
			t, err := nlcoord.leadership.GetTopicInfo(topic, i)
			if err != nil {
				coordLog.Warningf("exist topic partition failed to get info: %v", err)
				if err != ErrKeyNotFound {
					return err
				}
			} else {
				coordLog.Infof("create topic partition already exist %v-%v", topic, i)
				existPart[i] = t
			}
		}
	}
	if len(existPart) == meta.PartitionNum {
		coordLog.Infof("topic: %v partitions %v are all ready", topic, existPart)
		nlcoord.triggerCheckTopics("", 0, time.Millisecond*500)
		return nil
	}
	leaders, isrList, err := nlcoord.dpm.allocTopicLeaderAndISR(topic, meta.AllowMulti(), currentNodes, meta.Replica, meta.PartitionNum, existPart)
	if err != nil {
		coordLog.Infof("failed to alloc nodes for topic: %v", err)
		return err
	}
	if len(leaders) != meta.PartitionNum || len(isrList) != meta.PartitionNum {
		return ErrNodeUnavailable.ToErrorType()
	}
	for i := 0; i < meta.PartitionNum; i++ {
		if _, ok := existPart[i]; ok {
			continue
		}
		var tmpTopicReplicaInfo TopicPartitionReplicaInfo
		tmpTopicReplicaInfo.ISR = isrList[i]
		tmpTopicReplicaInfo.Leader = leaders[i]
		tmpTopicReplicaInfo.EpochForWrite = 1

		commonErr := nlcoord.leadership.UpdateTopicNodeInfo(topic, i, &tmpTopicReplicaInfo, tmpTopicReplicaInfo.Epoch)
		if commonErr != nil {
			coordLog.Infof("failed update info for topic : %v-%v, %v", topic, i, commonErr)
			continue
		}
		tmpTopicInfo := TopicPartitionMetaInfo{}
		tmpTopicInfo.Name = topic
		tmpTopicInfo.Partition = i
		tmpTopicInfo.TopicMetaInfo = meta
		tmpTopicInfo.TopicPartitionReplicaInfo = tmpTopicReplicaInfo
		rpcErr := nlcoord.notifyISRTopicMetaInfo(&tmpTopicInfo)
		if rpcErr != nil {
			coordLog.Warningf("failed notify topic info : %v", rpcErr)
		} else {
			coordLog.Infof("topic %v init successful.", tmpTopicInfo)
		}
	}
	nlcoord.triggerCheckTopics("", 0, time.Millisecond*500)
	return nil
}
