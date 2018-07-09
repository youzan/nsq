package consistence

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/absolute8511/gorpc"
	"github.com/youzan/nsq/internal/test"
	"github.com/youzan/nsq/nsqd"
)

type fakeNsqdLeadership struct {
	sync.Mutex
	clusterID            string
	regData              map[string]*NsqdNodeInfo
	fakeTopicsLeaderData map[string]map[int]*TopicCoordinator
	fakeTopicsInfo       map[string]map[int]*TopicPartitionMetaInfo
}

func NewFakeNSQDLeadership() NSQDLeadership {
	return &fakeNsqdLeadership{
		regData:              make(map[string]*NsqdNodeInfo),
		fakeTopicsLeaderData: make(map[string]map[int]*TopicCoordinator),
		fakeTopicsInfo:       make(map[string]map[int]*TopicPartitionMetaInfo),
	}
}

func (self *fakeNsqdLeadership) InitClusterID(id string) {
	self.clusterID = id
}

func (self *fakeNsqdLeadership) RegisterNsqd(nodeData *NsqdNodeInfo) error {
	self.regData[nodeData.GetID()] = nodeData
	return nil
}

func (self *fakeNsqdLeadership) UnregisterNsqd(nodeData *NsqdNodeInfo) error {
	delete(self.regData, nodeData.GetID())
	coordLog.Infof("fake nsqd unregistered: %v", nodeData)
	return nil
}

func (self *fakeNsqdLeadership) IsNodeTopicLeader(topic string, partition int, nodeData *NsqdNodeInfo) bool {
	self.Lock()
	defer self.Unlock()
	t, ok := self.fakeTopicsLeaderData[topic]
	var tc *TopicCoordinator
	if ok {
		if tc, ok = t[partition]; ok {
			if tc.topicLeaderSession.LeaderNode != nil {
				if tc.topicLeaderSession.LeaderNode.GetID() == nodeData.GetID() {
					return true
				}
			}
		}
	}
	return false
}

func (self *fakeNsqdLeadership) GetAllLookupdNodes() ([]NsqLookupdNodeInfo, error) {
	v := make([]NsqLookupdNodeInfo, 0)
	return v, nil
}

func (self *fakeNsqdLeadership) AcquireTopicLeader(topic string, partition int, nodeData *NsqdNodeInfo, epoch EpochType) error {
	self.Lock()
	defer self.Unlock()
	t, ok := self.fakeTopicsLeaderData[topic]
	var tc *TopicCoordinator
	if ok {
		if tc, ok = t[partition]; ok {
			if tc.topicLeaderSession.LeaderNode != nil {
				if tc.topicLeaderSession.LeaderNode.GetID() == nodeData.GetID() {
					return nil
				}
				return errors.New("topic leader already exist.")
			}
			tc.topicLeaderSession.LeaderNode = nodeData
			tc.topicLeaderSession.LeaderEpoch++
			tc.topicLeaderSession.Session = nodeData.GetID() + strconv.Itoa(int(tc.topicLeaderSession.LeaderEpoch))
			tc.topicInfo.ISR = append(tc.topicInfo.ISR, nodeData.GetID())
			tc.topicInfo.Leader = nodeData.GetID()
			tc.topicInfo.Epoch++
		} else {
			tc = &TopicCoordinator{}
			tc.coordData = &coordData{}
			tc.topicInfo.Name = topic
			tc.topicInfo.Partition = partition
			tc.topicInfo.Leader = nodeData.GetID()
			tc.topicInfo.ISR = append(tc.topicInfo.ISR, nodeData.GetID())
			tc.topicInfo.Epoch++
			tc.topicLeaderSession.LeaderNode = nodeData
			tc.topicLeaderSession.LeaderEpoch++
			tc.topicLeaderSession.Session = nodeData.GetID() + strconv.Itoa(int(tc.topicLeaderSession.LeaderEpoch))
			t[partition] = tc
		}
	} else {
		tmp := make(map[int]*TopicCoordinator)
		tc = &TopicCoordinator{}
		tc.coordData = &coordData{}
		tc.topicInfo.Name = topic
		tc.topicInfo.Partition = partition
		tc.topicInfo.Leader = nodeData.GetID()
		tc.topicInfo.ISR = append(tc.topicInfo.ISR, nodeData.GetID())
		tc.topicInfo.Epoch++
		tc.topicLeaderSession.LeaderNode = nodeData
		tc.topicLeaderSession.LeaderEpoch++
		tc.topicLeaderSession.Session = nodeData.GetID() + strconv.Itoa(int(tc.topicLeaderSession.LeaderEpoch))
		tmp[partition] = tc
		self.fakeTopicsLeaderData[topic] = tmp
	}
	return nil
}

func (self *fakeNsqdLeadership) ReleaseTopicLeader(topic string, partition int, session *TopicLeaderSession) error {
	self.Lock()
	defer self.Unlock()
	t, ok := self.fakeTopicsLeaderData[topic]
	if ok {
		delete(t, partition)
	}
	return nil
}

func (self *fakeNsqdLeadership) WatchLookupdLeader(leader chan *NsqLookupdNodeInfo, stop chan struct{}) error {
	select {
	case <-stop:
		close(leader)
		return nil
	}
}

func (self *fakeNsqdLeadership) UpdateTopics(topic string, info map[int]*TopicPartitionMetaInfo) {
	self.Lock()
	self.fakeTopicsInfo[topic] = info
	self.Unlock()
}

func (self *fakeNsqdLeadership) GetTopicInfo(topic string, partition int) (*TopicPartitionMetaInfo, error) {
	self.Lock()
	self.Unlock()
	t, ok := self.fakeTopicsInfo[topic]
	if ok {
		tp, ok2 := t[partition]
		if ok2 {
			return tp, nil
		}
	}
	return nil, errors.New("topic not exist")
}

func (self *fakeNsqdLeadership) GetTopicLeaderSession(topic string, partition int) (*TopicLeaderSession, error) {
	self.Lock()
	defer self.Unlock()
	s, ok := self.fakeTopicsLeaderData[topic]
	if !ok {
		return nil, ErrLeaderSessionNotExist
	}
	ss, ok := s[partition]
	if !ok {
		return nil, ErrLeaderSessionNotExist
	}
	if ss.topicLeaderSession.LeaderNode == nil || ss.topicLeaderSession.Session == "" {
		return nil, ErrLeaderSessionNotExist
	}
	return &ss.topicLeaderSession, nil
}

func startNsqdCoord(t *testing.T, rpcport string, dataPath string, extraID string, nsqd *nsqd.NSQD, useFake bool) *NsqdCoordinator {
	httpPort := "0"
	if nsqd != nil {
		_, httpPort, _ = net.SplitHostPort(nsqd.GetOpts().HTTPAddress)
		if httpPort == "" {
			httpPort = "0"
		}
	}
	nsqdCoord := NewNsqdCoordinator(TEST_NSQ_CLUSTER_NAME, "127.0.0.1", "0", rpcport, httpPort, extraID, dataPath, nsqd)
	if useFake {
		nsqdCoord.leadership = NewFakeNSQDLeadership()
		nsqdCoord.lookupRemoteCreateFunc = func(addr string, to time.Duration) (INsqlookupRemoteProxy, error) {
			p, err := NewFakeLookupRemoteProxy(addr, to)
			if err == nil {
				p.(*fakeLookupRemoteProxy).t = t
			}
			return p, err
		}
	} else {
		ld, _ := NewNsqdEtcdMgr(testEtcdServers)
		nsqdCoord.SetLeadershipMgr(ld)
		nsqdCoord.leadership.UnregisterNsqd(&nsqdCoord.myNode)
	}
	nsqdCoord.lookupLeader = NsqLookupdNodeInfo{}
	nsqdCoord.lookupLeader.NodeIP = "127.0.0.1"
	nsqdCoord.lookupLeader.RpcPort = "0"
	nsqdCoord.lookupLeader.TcpPort = "0"
	nsqdCoord.lookupLeader.ID = GenNsqLookupNodeID(&nsqdCoord.lookupLeader, "")
	return nsqdCoord
}

func startNsqdCoordWithFakeData(t *testing.T, rpcport string, dataPath string,
	extraID string, nsqd *nsqd.NSQD, fakeLeadership *fakeNsqdLeadership, fakeLookupProxy *fakeLookupRemoteProxy) *NsqdCoordinator {
	nsqdCoord := NewNsqdCoordinator(TEST_NSQ_CLUSTER_NAME, "127.0.0.1", "0", rpcport, "0", extraID, dataPath, nsqd)
	nsqdCoord.leadership = fakeLeadership
	nsqdCoord.lookupRemoteCreateFunc = func(addr string, to time.Duration) (INsqlookupRemoteProxy, error) {
		fakeLookupProxy.t = t
		fakeLookupProxy.fakeNsqdCoords[nsqdCoord.myNode.GetID()] = nsqdCoord
		return fakeLookupProxy, nil
	}
	nsqdCoord.lookupLeader = NsqLookupdNodeInfo{}
	nsqdCoord.lookupLeader.NodeIP = "127.0.0.1"
	nsqdCoord.lookupLeader.RpcPort = "0"
	nsqdCoord.lookupLeader.TcpPort = "0"
	nsqdCoord.lookupLeader.ID = GenNsqLookupNodeID(&nsqdCoord.lookupLeader, "")
	err := nsqdCoord.Start()
	if err != nil {
		panic(err)
	}
	time.Sleep(time.Second)
	return nsqdCoord
}

func TestNsqdRPCClient(t *testing.T) {
	SetCoordLogger(newTestLogger(t), 2)
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)

	nsqdCoord := startNsqdCoord(t, "0", tmpDir, "", nil, true)
	nsqdCoord.Start()
	defer nsqdCoord.Stop()
	time.Sleep(time.Second * 2)
	client, err := NewNsqdRpcClient(nsqdCoord.rpcServer.rpcServer.Listener.ListenAddr().String(), time.Second)
	test.Nil(t, err)
	_, err = client.CallWithRetry("TestRpcCallNotExist", "req")
	test.NotNil(t, err)
	coordErr := client.CallRpcTestCoordErr("coorderr")
	test.NotNil(t, coordErr)
	test.NotEqual(t, coordErr.ErrType, CoordNetErr)
	test.Equal(t, coordErr.ErrMsg, "coorderr")
	test.Equal(t, coordErr.ErrCode, RpcCommonErr)
	test.Equal(t, coordErr.ErrType, CoordCommonErr)

	rsp, rpcErr := client.CallRpcTest("reqdata")
	test.NotNil(t, rpcErr)
	test.Equal(t, rsp, "reqdata")
	test.Equal(t, rpcErr.ErrCode, RpcNoErr)
	test.Equal(t, rpcErr.ErrMsg, "reqdata")
	test.Equal(t, rpcErr.ErrType, CoordCommonErr)
	timeoutErr := client.CallRpcTesttimeout("reqdata")
	test.NotNil(t, timeoutErr)
	test.Equal(t, timeoutErr.(*gorpc.ClientError).Timeout, true)
	time.Sleep(time.Second * 3)
	client.Close()
}
