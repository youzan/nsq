package consistence

func (ncoord *NsqdCoordinator) requestJoinCatchup(topic string, partition int) *CoordErr {
	coordLog.Infof("try to join catchup for topic: %v-%v", topic, partition)
	c, err := ncoord.getLookupRemoteProxy()
	if err != nil {
		coordLog.Infof("get lookup failed: %v", err)
		return err
	}
	//defer ncoord.putLookupRemoteProxy(c)
	err = c.RequestJoinCatchup(topic, partition, ncoord.myNode.GetID())
	if err != nil {
		coordLog.Infof("request join catchup failed: %v", err)
	}
	return err
}

func (ncoord *NsqdCoordinator) requestCheckTopicConsistence(topic string, partition int) {
	c, err := ncoord.getLookupRemoteProxy()
	if err != nil {
		coordLog.Infof("get lookup failed: %v", err)
		return
	}
	//defer ncoord.putLookupRemoteProxy(c)
	c.RequestCheckTopicConsistence(topic, partition)
}

func (ncoord *NsqdCoordinator) requestNotifyNewTopicInfo(topic string, partition int) {
	c, err := ncoord.getLookupRemoteProxy()
	if err != nil {
		coordLog.Infof("get lookup failed: %v", err)
		return
	}
	//defer ncoord.putLookupRemoteProxy(c)
	c.RequestNotifyNewTopicInfo(topic, partition, ncoord.myNode.GetID())
}

func (ncoord *NsqdCoordinator) requestJoinTopicISR(topicInfo *TopicPartitionMetaInfo) *CoordErr {
	// request change catchup to isr list and wait for nsqlookupd response to temp disable all new write.
	c, err := ncoord.getLookupRemoteProxy()
	if err != nil {
		return err
	}
	//defer ncoord.putLookupRemoteProxy(c)
	err = c.RequestJoinTopicISR(topicInfo.Name, topicInfo.Partition, ncoord.myNode.GetID())
	return err
}

func (ncoord *NsqdCoordinator) notifyReadyForTopicISR(topicInfo *TopicPartitionMetaInfo, leaderSession *TopicLeaderSession, joinSession string) *CoordErr {
	// notify myself is ready for isr list for current session and can accept new write.
	// leader session should contain the (isr list, current leader session, leader epoch), to identify the
	// the different session stage.
	c, err := ncoord.getLookupRemoteProxy()
	if err != nil {
		return err
	}
	//defer ncoord.putLookupRemoteProxy(c)
	return c.ReadyForTopicISR(topicInfo.Name, topicInfo.Partition, ncoord.myNode.GetID(), leaderSession, joinSession)
}

// only move from isr to catchup, if restart, we can catchup directly.
func (ncoord *NsqdCoordinator) requestLeaveFromISR(topic string, partition int) *CoordErr {
	c, err := ncoord.getLookupRemoteProxy()
	if err != nil {
		return err
	}
	//defer ncoord.putLookupRemoteProxy(c)
	return c.RequestLeaveFromISR(topic, partition, ncoord.myNode.GetID())
}

func (ncoord *NsqdCoordinator) requestLeaveFromISRFast(topic string, partition int) *CoordErr {
	c, err := ncoord.getLookupRemoteProxy()
	if err != nil {
		return err
	}
	if realC, ok := c.(*NsqLookupRpcClient); ok {
		return realC.RequestLeaveFromISRFast(topic, partition, ncoord.myNode.GetID())
	}
	return c.RequestLeaveFromISR(topic, partition, ncoord.myNode.GetID())
}

// this should only be called by leader to remove slow node in isr.
// Be careful to avoid removing most of the isr nodes, should only remove while
// only small part of isr is slow.
// TODO: If most of nodes is slow, the leader should check the leader itself and
// maybe giveup the leadership.
func (ncoord *NsqdCoordinator) requestLeaveFromISRByLeader(topic string, partition int, nid string) *CoordErr {
	topicCoord, err := ncoord.getTopicCoordData(topic, partition)
	if err != nil {
		return err
	}
	if topicCoord.GetLeaderSessionID() != ncoord.myNode.GetID() || topicCoord.GetLeader() != ncoord.myNode.GetID() {
		return ErrNotTopicLeader
	}

	// send request with leader session, so lookup can check the valid of session.
	c, err := ncoord.getLookupRemoteProxy()
	if err != nil {
		return err
	}
	//defer ncoord.putLookupRemoteProxy(c)
	return c.RequestLeaveFromISRByLeader(topic, partition, nid, &topicCoord.topicLeaderSession)
}
