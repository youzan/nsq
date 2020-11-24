package consistence

import (
	"fmt"
	"testing"
	"time"

	"github.com/youzan/nsq/internal/test"
)

func TestLookupd(t *testing.T) {
	if testing.Verbose() {
		SetCoordLogger(newTestLogger(t), 4)
	}
	ClusterID := "test-nsq-cluster-unit-test-etcd-leadership"
	NsqdID := "n-1"
	LookupId1 := "l-1"
	LookupId2 := "l-2"

	stop := make(chan struct{})

	nodeMgr, _ := NewNsqdEtcdMgr(testEtcdServers, testEtcdUsername, testEtcdPassword)
	nodeMgr.InitClusterID(ClusterID)
	nodeInfo := &NsqdNodeInfo{
		ID:      NsqdID,
		NodeIP:  "127.0.0.1",
		TcpPort: "2222",
		RpcPort: "2223",
	}
	err := nodeMgr.RegisterNsqd(nodeInfo)
	test.Nil(t, err)
	fmt.Printf("Nsqd Node[%s] register success.\n", nodeInfo.ID)

	lookupdMgr, _ := NewNsqLookupdEtcdMgr(testEtcdServers, testEtcdUsername, testEtcdPassword)
	lookupdMgr.InitClusterID(ClusterID)
	lookupdInfo := &NsqLookupdNodeInfo{
		ID:       LookupId1,
		NodeIP:   "127.0.0.1",
		HttpPort: "8090",
	}
	err = lookupdMgr.Register(lookupdInfo)
	test.Nil(t, err)
	fmt.Printf("Nsqd Lookupd Node[%s] register success.\n", lookupdInfo.ID)

	lookupdMgr2, _ := NewNsqLookupdEtcdMgr(testEtcdServers, testEtcdUsername, testEtcdPassword)
	lookupdMgr2.InitClusterID(ClusterID)
	lookupdInfo2 := &NsqLookupdNodeInfo{
		ID:       LookupId2,
		NodeIP:   "127.0.0.1",
		HttpPort: "8091",
	}
	err = lookupdMgr2.Register(lookupdInfo2)
	test.Nil(t, err)
	fmt.Printf("Nsqd Lookupd Node[%s] register success.\n", lookupdInfo2.ID)

	// get all lookup nodes
	lookupList, err := nodeMgr.GetAllLookupdNodes()
	test.Nil(t, err)
	fmt.Printf("Get all lookup nodes: %v\n", lookupList)
	test.Equal(t, len(lookupList), 2)

	// nsqd watch lookup leader
	lookupLeaderCh := make(chan *NsqLookupdNodeInfo)
	go nodeMgr.WatchLookupdLeader(lookupLeaderCh, stop)
	nodeWatchLookupLeaderStopped := make(chan bool)
	go func() {
		for {
			select {
			case leader, ok := <-lookupLeaderCh:
				if ok {
					fmt.Printf("[nsqd node] watch lookup leader: %v\n", leader)
				} else {
					fmt.Println("[nsqd node] watch lookup leader for loop stop.")
					close(nodeWatchLookupLeaderStopped)
					return
				}
			}
		}
	}()

	// lookup acquire and watch leader
	luLeader1 := make(chan *NsqLookupdNodeInfo)
	lookupdMgr.AcquireAndWatchLeader(luLeader1, stop)
	luAcquireWatchLeaderStopped := make(chan bool)
	go func() {
		for {
			select {
			case leader, ok := <-luLeader1:
				if ok {
					fmt.Printf("[lookup node 1] watch lookup leader: %v\n", leader)
				} else {
					fmt.Println("[lookup node 1] watch lookup leader for loop stop.")
					close(luAcquireWatchLeaderStopped)
					return
				}
			}
		}
	}()

	go func() {
		for {
			_, err := lookupdMgr.ScanTopics()
			test.Nil(t, err)
			time.Sleep(time.Millisecond)
		}
	}()
	// lookup node 1 create topic
	topicName := "ree-topic"
	partition := 0
	// delete topic if exist
	lookupdMgr.DeleteTopic(topicName, partition)

	allTopics, err := lookupdMgr.ScanTopics()
	test.Nil(t, err)
	test.Equal(t, 0, len(allTopics))
	err = lookupdMgr.CreateTopicPartition(topicName, partition)
	test.Nil(t, err)
	fmt.Printf("[lookup node 1] topic[%s] partition[%d] create topic partition success.\n", topicName, partition)

	allTopics, err = lookupdMgr.ScanTopics()
	test.Nil(t, err)
	topicMetainfo := &TopicMetaInfo{
		PartitionNum: 2,
		Replica:      2,
	}
	err = lookupdMgr.CreateTopic(topicName, topicMetainfo)
	fmt.Println("[lookup node 1] create topic metainfo:", err)
	// lookup node 1 update topic info
	topicReplicasInfo := &TopicPartitionReplicaInfo{
		Leader:      "127.0.0.1:2223",
		ISR:         []string{"1111"},
		CatchupList: []string{"2222"},
		Channels:    []string{"3333"},
	}
	allTopics, err = lookupdMgr.ScanTopics()
	test.Nil(t, err)
	err = lookupdMgr.UpdateTopicNodeInfo(topicName, partition, topicReplicasInfo, 0)

	allTopics, err = lookupdMgr.ScanTopics()
	test.Nil(t, err)
	test.Equal(t, 1, len(allTopics))

	// nsqd node 1 get topic info
	topicInfo, err := nodeMgr.GetTopicInfo(topicName, partition)
	test.Nil(t, err)
	fmt.Printf("[nsqd node 1] get topic info: %v\n", topicInfo)

	// nsqd node 1 acquire topic leader
	err = nodeMgr.AcquireTopicLeader(topicName, partition, nodeInfo, 0)
	test.Nil(t, err)

	allTopics, err = lookupdMgr.ScanTopics()
	test.Nil(t, err)
	// lookup node 1 get topic leader session
	topicLeaderS, err := lookupdMgr.GetTopicLeaderSession(topicName, partition)
	test.Nil(t, err)
	fmt.Printf("[lookup node 1] topic[%s] get topic leader session leader: %v\n", topicName, topicLeaderS)
	// check delete topic will trigger scan topic newest data
	partition1 := 1
	// delete topic if exist
	lookupdMgr.DeleteTopic(topicName, partition1)

	err = lookupdMgr.CreateTopicPartition(topicName, partition1)
	test.Nil(t, err)
	fmt.Printf("[lookup node 1] topic[%s] partition[%d] create topic partition success.\n", topicName, partition1)

	allTopics, err = lookupdMgr.ScanTopics()
	test.Nil(t, err)
	// lookup node 1 update topic info
	topicReplicasInfo = &TopicPartitionReplicaInfo{
		Leader:      "127.0.0.1:2223",
		ISR:         []string{"1111"},
		CatchupList: []string{"2222"},
		Channels:    []string{"3333"},
	}
	lookupdMgr.UpdateTopicNodeInfo(topicName, partition1, topicReplicasInfo, 0)

	time.Sleep(time.Second)
	allTopics, err = lookupdMgr.ScanTopics()
	test.Nil(t, err)
	test.Equal(t, 2, len(allTopics))
	lookupdMgr.DeleteTopic(topicName, partition1)
	time.Sleep(time.Second)
	allTopics, err = lookupdMgr.ScanTopics()
	test.Nil(t, err)
	test.Equal(t, 1, len(allTopics))

	go func() {
		<-nodeWatchLookupLeaderStopped
		fmt.Printf("node watch lookup leader loop stopped.\n")
	}()
	go func() {
		<-luAcquireWatchLeaderStopped
		fmt.Printf("lookup acquire and watch leader loop stopped.\n")
	}()

	time.Sleep(3 * time.Second)
	close(stop)

	allTopics, err = lookupdMgr.ScanTopics()
	test.Nil(t, err)
	test.Equal(t, 1, len(allTopics))

	time.Sleep(15 * time.Second)

	err = lookupdMgr2.Unregister(lookupdInfo2)
	test.Nil(t, err)
	err = lookupdMgr.Unregister(lookupdInfo)
	test.Nil(t, err)
	err = nodeMgr.UnregisterNsqd(nodeInfo)
	test.Nil(t, err)
}
