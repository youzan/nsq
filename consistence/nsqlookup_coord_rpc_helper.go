package consistence

import (
	"net"
	"strconv"
	"time"
)

// some failed rpc means lost, we should always try to notify to the node when they are available
func (nlcoord *NsqLookupCoordinator) rpcFailRetryFunc(monitorChan chan struct{}) {
	ticker := time.NewTicker(time.Second)
	checkConn := 0
	defer ticker.Stop()
	for {
		select {
		case <-monitorChan:
			return
		case <-ticker.C:
			failList := make(map[string]RpcFailedInfo, 0)
			var currentNodes map[string]NsqdNodeInfo
			nlcoord.failedRpcMutex.Lock()
			for _, v := range nlcoord.failedRpcList {
				failList[v.nodeID+v.topic+strconv.Itoa(v.partition)] = v
			}
			if len(failList) > 0 {
				coordLog.Infof("failed rpc total: %v, %v", len(nlcoord.failedRpcList), len(failList))
				currentNodes, _ = nlcoord.getCurrentNodesWithRemoving()
			}
			nlcoord.failedRpcList = nlcoord.failedRpcList[0:0]
			nlcoord.failedRpcMutex.Unlock()

			checkConn++
			if checkConn%30 == 0 {
				if len(currentNodes) == 0 {
					currentNodes, _ = nlcoord.getCurrentNodesWithRemoving()
				}
				nlcoord.rpcMutex.Lock()
				for nid, c := range nlcoord.nsqdRpcClients {
					_, nodeOK := currentNodes[nid]
					if !nodeOK || (c.c != nil && c.ShouldRemoved()) {
						c.Close()
						delete(nlcoord.nsqdRpcClients, nid)
					}
				}
				nlcoord.rpcMutex.Unlock()
			}

			epoch := nlcoord.GetLookupLeader().Epoch
			for _, info := range failList {
				// check if exiting
				select {
				case <-monitorChan:
					return
				default:
				}
				coordLog.Debugf("retry failed rpc call for topic: %v", info)
				topicInfo, err := nlcoord.leadership.GetTopicInfo(info.topic, info.partition)
				if err != nil {
					if err == ErrKeyNotFound {
						coordLog.Infof("retry cancelled for not exist topic: %v", info)
						continue
					}
					coordLog.Infof("rpc call for topic: %v, failed: %v", info, err)
					nlcoord.addRetryFailedRpc(info.topic, info.partition, info.nodeID)
					continue
				}
				if _, ok := currentNodes[info.nodeID]; !ok {
					coordLog.Infof("retry cancelled since node not exist: %v", info)
					continue
				}
				if FindSlice(topicInfo.ISR, info.nodeID) == -1 && FindSlice(topicInfo.CatchupList, info.nodeID) == -1 {
					continue
				}
				c, rpcErr := nlcoord.acquireRpcClient(info.nodeID)
				if rpcErr != nil {
					coordLog.Infof("rpc call for topic: %v, failed %v", info, rpcErr)
					nlcoord.addRetryFailedRpc(info.topic, info.partition, info.nodeID)
					continue
				}
				rpcErr = c.UpdateTopicInfo(epoch, topicInfo)
				if rpcErr != nil {
					// this error should not retry anymore
					if !rpcErr.IsEqual(ErrTopicCoordExistingAndMismatch) {
						coordLog.Infof("rpc call for topic: %v, failed %v", info, rpcErr)
						nlcoord.addRetryFailedRpc(info.topic, info.partition, info.nodeID)
					}
					continue
				}
				leaderSession, err := nlcoord.leadership.GetTopicLeaderSession(info.topic, info.partition)
				if err != nil {
					if err == ErrKeyNotFound {
						coordLog.Infof("retry cancelled for not exist topic session: %v", info)
						continue
					}
					coordLog.Infof("rpc call for topic: %v, failed: %v", info, err)
					nlcoord.addRetryFailedRpc(info.topic, info.partition, info.nodeID)
					continue
				}
				rpcErr = c.NotifyTopicLeaderSession(epoch, topicInfo, leaderSession, "")
				if rpcErr != nil {
					coordLog.Infof("rpc call for topic: %v, failed: %v", info, rpcErr)
					nlcoord.addRetryFailedRpc(info.topic, info.partition, info.nodeID)
					continue
				}
			}
		}
	}
}

func (nlcoord *NsqLookupCoordinator) doNotifyToNsqdNodes(nodes []string, notifyRpcFunc func(string) *CoordErr) *CoordErr {
	currentNodes, _ := nlcoord.getCurrentNodesWithRemoving()
	var coordErr *CoordErr
	for _, n := range nodes {
		node, ok := currentNodes[n]
		if !ok {
			coordLog.Infof("notify to nsqd node %v failed since node not found", n)
			coordErr = ErrNodeNotFound
			continue
		}
		err := notifyRpcFunc(node.GetID())
		if err != nil {
			coordLog.Infof("notify to nsqd node %v failed: %v", node, err)
			coordErr = err
		}
	}
	return coordErr
}

func (nlcoord *NsqLookupCoordinator) doNotifyToSingleNsqdNode(nodeID string, notifyRpcFunc func(string) *CoordErr) *CoordErr {
	nlcoord.nodesMutex.RLock()
	node, ok := nlcoord.nsqdNodes[nodeID]
	nlcoord.nodesMutex.RUnlock()
	if !ok {
		return ErrNodeNotFound
	}
	err := notifyRpcFunc(node.GetID())
	if err != nil {
		coordLog.Infof("notify to nsqd node %v failed: %v", node, err)
	}
	return err
}

// notify leader first, if failed, left nodes will stop if failStop is true,
// if success then ISR and catchup nodes will be notified
func (nlcoord *NsqLookupCoordinator) doNotifyToTopicLeaderThenOthers(failStop bool, leader string, others []string, notifyRpcFunc func(string) *CoordErr) *CoordErr {
	err := nlcoord.doNotifyToSingleNsqdNode(leader, notifyRpcFunc)
	if err != nil {
		coordLog.Infof("notify to topic leader %v failed: %v", leader, err)
		if failStop {
			return err
		}
	}

	err2 := nlcoord.doNotifyToNsqdNodes(others, notifyRpcFunc)
	if err != nil {
		return err
	}
	return err2
}

func (nlcoord *NsqLookupCoordinator) notifyTopicLeaderSession(topicInfo *TopicPartitionMetaInfo, leaderSession *TopicLeaderSession, joinSession string) *CoordErr {
	others := getOthersExceptLeader(topicInfo)
	coordLog.Infof("notify topic leader session changed: %v, %v, others: %v", topicInfo.GetTopicDesp(), leaderSession.Session, others)
	err := nlcoord.doNotifyToTopicLeaderThenOthers(false, topicInfo.Leader, others, func(nid string) *CoordErr {
		return nlcoord.sendTopicLeaderSessionToNsqd(nlcoord.GetLookupLeader().Epoch, nid, topicInfo, leaderSession, joinSession)
	})
	return err
}

func (nlcoord *NsqLookupCoordinator) notifyAcquireTopicLeader(topicInfo *TopicPartitionMetaInfo) *CoordErr {
	rpcErr := nlcoord.doNotifyToSingleNsqdNode(topicInfo.Leader, func(nid string) *CoordErr {
		return nlcoord.sendAcquireTopicLeaderToNsqd(nlcoord.GetLookupLeader().Epoch, nid, topicInfo)
	})
	if rpcErr != nil {
		coordLog.Infof("notify leader to acquire leader failed: %v", rpcErr)
	}
	return rpcErr
}

func (nlcoord *NsqLookupCoordinator) notifyReleaseTopicLeader(topicInfo *TopicPartitionMetaInfo,
	leaderSessionEpoch EpochType, leaderSession string) *CoordErr {
	rpcErr := nlcoord.doNotifyToSingleNsqdNode(topicInfo.Leader, func(nid string) *CoordErr {
		return nlcoord.sendReleaseTopicLeaderToNsqd(nlcoord.GetLookupLeader().Epoch, nid, topicInfo, leaderSessionEpoch, leaderSession)
	})
	if rpcErr != nil {
		coordLog.Infof("notify leader to acquire leader failed: %v", rpcErr)
	}
	return rpcErr
}

func (nlcoord *NsqLookupCoordinator) notifyISRTopicMetaInfo(topicInfo *TopicPartitionMetaInfo) *CoordErr {
	rpcErr := nlcoord.doNotifyToNsqdNodes(topicInfo.ISR, func(nid string) *CoordErr {
		return nlcoord.sendTopicInfoToNsqd(nlcoord.GetLookupLeader().Epoch, nid, topicInfo)
	})
	if rpcErr != nil {
		coordLog.Infof("notify isr for topic meta info failed: %v", rpcErr)
	}
	return rpcErr
}

func (nlcoord *NsqLookupCoordinator) notifyCatchupTopicMetaInfo(topicInfo *TopicPartitionMetaInfo) *CoordErr {
	rpcErr := nlcoord.doNotifyToNsqdNodes(topicInfo.CatchupList, func(nid string) *CoordErr {
		return nlcoord.sendTopicInfoToNsqd(nlcoord.GetLookupLeader().Epoch, nid, topicInfo)
	})
	if rpcErr != nil {
		coordLog.Infof("notify catchup for topic meta info failed: %v", rpcErr)
	}
	return rpcErr
}

func (nlcoord *NsqLookupCoordinator) notifyTopicMetaInfo(topicInfo *TopicPartitionMetaInfo) *CoordErr {
	others := getOthersExceptLeader(topicInfo)
	coordLog.Infof("notify topic meta info changed: %v", topicInfo)
	if topicInfo.Name == "" {
		coordLog.Infof("==== notify topic name is empty")
	}
	rpcErr := nlcoord.doNotifyToTopicLeaderThenOthers(false, topicInfo.Leader, others, func(nid string) *CoordErr {
		return nlcoord.sendTopicInfoToNsqd(nlcoord.GetLookupLeader().Epoch, nid, topicInfo)
	})
	if rpcErr != nil {
		coordLog.Infof("notify topic meta info failed: %v", rpcErr)
	}
	return rpcErr
}

func (nlcoord *NsqLookupCoordinator) notifyOldNsqdsForTopicMetaInfo(topicInfo *TopicPartitionMetaInfo, oldNodes []string) *CoordErr {
	return nlcoord.doNotifyToNsqdNodes(oldNodes, func(nid string) *CoordErr {
		return nlcoord.sendTopicInfoToNsqd(nlcoord.GetLookupLeader().Epoch, nid, topicInfo)
	})
}

func (nlcoord *NsqLookupCoordinator) addRetryFailedRpc(topic string, partition int, nid string) {
	failed := RpcFailedInfo{
		nodeID:    nid,
		topic:     topic,
		partition: partition,
		failTime:  time.Now(),
	}
	nlcoord.failedRpcMutex.Lock()
	nlcoord.failedRpcList = append(nlcoord.failedRpcList, failed)
	coordLog.Infof("failed rpc added: %v, total: %v", failed, len(nlcoord.failedRpcList))
	nlcoord.failedRpcMutex.Unlock()
}

func (nlcoord *NsqLookupCoordinator) sendTopicLeaderSessionToNsqd(epoch EpochType, nid string, topicInfo *TopicPartitionMetaInfo,
	leaderSession *TopicLeaderSession, joinSession string) *CoordErr {
	c, err := nlcoord.acquireRpcClient(nid)
	if err != nil {
		nlcoord.addRetryFailedRpc(topicInfo.Name, topicInfo.Partition, nid)
		return err
	}
	err = c.NotifyTopicLeaderSession(epoch, topicInfo, leaderSession, joinSession)
	if err != nil {
		nlcoord.addRetryFailedRpc(topicInfo.Name, topicInfo.Partition, nid)
	}
	return err
}

func (nlcoord *NsqLookupCoordinator) sendAcquireTopicLeaderToNsqd(epoch EpochType, nid string,
	topicInfo *TopicPartitionMetaInfo) *CoordErr {
	c, rpcErr := nlcoord.acquireRpcClient(nid)
	if rpcErr != nil {
		return rpcErr
	}
	rpcErr = c.NotifyAcquireTopicLeader(epoch, topicInfo)
	return rpcErr
}

func (nlcoord *NsqLookupCoordinator) sendReleaseTopicLeaderToNsqd(epoch EpochType, nid string,
	topicInfo *TopicPartitionMetaInfo, leaderSessionEpoch EpochType, leaderSession string) *CoordErr {
	c, rpcErr := nlcoord.acquireRpcClient(nid)
	if rpcErr != nil {
		return rpcErr
	}
	rpcErr = c.NotifyReleaseTopicLeader(epoch, topicInfo, leaderSessionEpoch, leaderSession)
	return rpcErr
}

func (nlcoord *NsqLookupCoordinator) sendTopicInfoToNsqd(epoch EpochType, nid string, topicInfo *TopicPartitionMetaInfo) *CoordErr {
	c, rpcErr := nlcoord.acquireRpcClient(nid)
	if rpcErr != nil {
		nlcoord.addRetryFailedRpc(topicInfo.Name, topicInfo.Partition, nid)
		return rpcErr
	}
	rpcErr = c.UpdateTopicInfo(epoch, topicInfo)
	if rpcErr != nil {
		coordLog.Infof("failed to update topic info: %v, %v, %v", topicInfo.GetTopicDesp(), nid, rpcErr)
		nlcoord.addRetryFailedRpc(topicInfo.Name, topicInfo.Partition, nid)
	}
	return rpcErr
}

// each time change leader or isr list, make sure disable write.
// Because we need make sure the new leader and isr is in sync before accepting the
// write request.
func (nlcoord *NsqLookupCoordinator) notifyLeaderDisableTopicWriteFast(topicInfo *TopicPartitionMetaInfo) *CoordErr {
	c, err := nlcoord.acquireRpcClient(topicInfo.Leader)
	if err != nil {
		coordLog.Infof("failed to get rpc client: %v, %v", err, topicInfo.Leader)
		return err
	}
	err = c.DisableTopicWriteFast(nlcoord.GetLookupLeader().Epoch, topicInfo)
	return err
}

func (nlcoord *NsqLookupCoordinator) notifyLeaderDisableTopicWrite(topicInfo *TopicPartitionMetaInfo) *CoordErr {
	c, err := nlcoord.acquireRpcClient(topicInfo.Leader)
	if err != nil {
		coordLog.Infof("failed to get rpc client: %v, %v", err, topicInfo.Leader)
		return err
	}
	err = c.DisableTopicWrite(nlcoord.GetLookupLeader().Epoch, topicInfo)
	return err
}

func (nlcoord *NsqLookupCoordinator) notifyISRDisableTopicWrite(topicInfo *TopicPartitionMetaInfo) (string, *CoordErr) {
	for _, node := range topicInfo.ISR {
		if node == topicInfo.Leader {
			continue
		}
		c, err := nlcoord.acquireRpcClient(node)
		if err != nil {
			return node, err
		}
		err = c.DisableTopicWrite(nlcoord.GetLookupLeader().Epoch, topicInfo)
		if err != nil {
			return node, err
		}
	}
	return "", nil
}

func (nlcoord *NsqLookupCoordinator) notifyEnableTopicWrite(topicInfo *TopicPartitionMetaInfo) *CoordErr {
	for _, node := range topicInfo.ISR {
		if node == topicInfo.Leader {
			continue
		}
		c, err := nlcoord.acquireRpcClient(node)
		if err != nil {
			return err
		}
		err = c.EnableTopicWrite(nlcoord.GetLookupLeader().Epoch, topicInfo)
		if err != nil {
			return err
		}
	}
	c, err := nlcoord.acquireRpcClient(topicInfo.Leader)
	if err != nil {
		return err
	}
	err = c.EnableTopicWrite(nlcoord.GetLookupLeader().Epoch, topicInfo)
	return err
}

func (nlcoord *NsqLookupCoordinator) isTopicWriteDisabled(topicInfo *TopicPartitionMetaInfo) bool {
	c, err := nlcoord.acquireRpcClient(topicInfo.Leader)
	if err != nil {
		return false
	}
	return c.IsTopicWriteDisabled(topicInfo)
}

func (nlcoord *NsqLookupCoordinator) getNsqdTopicStat(node NsqdNodeInfo) (*NodeTopicStats, error) {
	// cache node stats, since it may not change very often, and it may cost if too many topics
	nlcoord.cachedMutex.Lock()
	item, ok := nlcoord.cachedNodeStats[node.GetID()]
	nlcoord.cachedMutex.Unlock()
	if ok && item.stats != nil && time.Since(item.lastTime) < time.Minute {
		return item.stats, nil
	}
	c, err := nlcoord.acquireRpcClient(node.GetID())
	if err != nil {
		return nil, err.ToErrorType()
	}
	stats, serr := c.GetTopicStats("")
	if serr != nil {
		return nil, serr
	}
	nlcoord.cachedMutex.Lock()
	old, ok := nlcoord.cachedNodeStats[node.GetID()]
	if ok {
		old.stats = nil
	}
	item = cachedNodeTopicStats{
		stats:    stats,
		lastTime: time.Now(),
	}
	nlcoord.cachedNodeStats[node.GetID()] = item
	nlcoord.cachedMutex.Unlock()
	return item.stats, nil
}

func (nlcoord *NsqLookupCoordinator) getNsqdLastCommitLogID(nid string, topicInfo *TopicPartitionMetaInfo) (int64, *CoordErr) {
	c, err := nlcoord.acquireRpcClient(nid)
	if err != nil {
		return 0, err
	}
	return c.GetLastCommitLogID(topicInfo)
}

func (nlcoord *NsqLookupCoordinator) acquireRpcClient(nid string) (*NsqdRpcClient, *CoordErr) {
	currentNodes, _ := nlcoord.getCurrentNodesWithRemoving()

	nlcoord.rpcMutex.Lock()
	defer nlcoord.rpcMutex.Unlock()
	c, _ := nlcoord.nsqdRpcClients[nid]
	if c != nil {
		if c.ShouldRemoved() {
			coordLog.Infof("rpc removing removed client: %v", nid)
			c.Close()
			c = nil
			delete(nlcoord.nsqdRpcClients, nid)
		}
	}
	if c == nil {
		n, ok := currentNodes[nid]
		if !ok {
			coordLog.Infof("rpc node not found: %v", nid)
			return nil, ErrNodeNotFound
		}
		var err error
		c, err = NewNsqdRpcClient(net.JoinHostPort(n.NodeIP, n.RpcPort), RPC_TIMEOUT_FOR_LOOKUP)
		if err != nil {
			coordLog.Infof("rpc node %v client init failed : %v", nid, err)
			return nil, &CoordErr{err.Error(), RpcNoErr, CoordNetErr}
		}
		nlcoord.nsqdRpcClients[nid] = c
	}
	return c, nil
}
