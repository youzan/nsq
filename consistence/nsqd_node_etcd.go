package consistence

import (
	"encoding/json"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/coreos/etcd/client"
	"golang.org/x/net/context"
)

const (
	ETCD_TTL = 30
)

type MasterChanInfo struct {
	processStopCh chan bool
	stoppedCh     chan bool
}

type NsqdEtcdMgr struct {
	sync.Mutex

	client      *EtcdClient
	clusterID   string
	topicRoot   string
	lookupdRoot string

	nodeKey       string
	nodeValue     string
	refreshStopCh chan bool
}

func NewNsqdEtcdMgr(host, username, pwd string) (*NsqdEtcdMgr, error) {
	client, err := NewEClient(host, username, pwd)
	if err != nil {
		return nil, err
	}
	return &NsqdEtcdMgr{
		client: client,
	}, nil
}

func (nem *NsqdEtcdMgr) InitClusterID(id string) {
	nem.clusterID = id
	nem.topicRoot = nem.createTopicRootPath()
	nem.lookupdRoot = nem.createLookupdRootPath()
}

func (nem *NsqdEtcdMgr) RegisterNsqd(nodeData *NsqdNodeInfo) error {
	value, err := json.Marshal(nodeData)
	if err != nil {
		return err
	}
	if nem.refreshStopCh != nil {
		close(nem.refreshStopCh)
	}

	nem.nodeKey = nem.createNsqdNodePath(nodeData)
	nem.nodeValue = string(value)
	_, err = nem.client.Set(nem.nodeKey, nem.nodeValue, ETCD_TTL)
	if err != nil {
		return err
	}
	coordLog.Infof("registered new node: %v", nodeData)
	nem.refreshStopCh = make(chan bool, 1)
	// start refresh node
	go nem.refresh(nem.refreshStopCh)

	return nil
}

func (nem *NsqdEtcdMgr) refresh(stopChan chan bool) {
	for {
		select {
		case <-stopChan:
			return
		case <-time.After(time.Second * time.Duration(ETCD_TTL/10)):
			_, err := nem.client.SetWithTTL(nem.nodeKey, ETCD_TTL)
			if err != nil {
				coordLog.Errorf("update error: %s", err.Error())
				_, err := nem.client.Set(nem.nodeKey, nem.nodeValue, ETCD_TTL)
				if err != nil {
					coordLog.Errorf("set key error: %s", err.Error())
				}
			}
		}
	}
}

func (nem *NsqdEtcdMgr) UnregisterNsqd(nodeData *NsqdNodeInfo) error {
	nem.Lock()
	defer nem.Unlock()

	// stop refresh
	if nem.refreshStopCh != nil {
		close(nem.refreshStopCh)
		nem.refreshStopCh = nil
	}

	_, err := nem.client.Delete(nem.createNsqdNodePath(nodeData), false)
	if err != nil {
		coordLog.Warningf("cluser[%v] node[%v] unregister failed: %v", nem.clusterID, nodeData, err)
		return err
	}

	coordLog.Infof("cluser[%v] node[%v] unregistered", nem.clusterID, nodeData)

	return nil
}

func (nem *NsqdEtcdMgr) AcquireTopicLeader(topic string, partition int, nodeData *NsqdNodeInfo, epoch EpochType) error {
	topicLeaderSession := &TopicLeaderSession{
		Topic:       topic,
		Partition:   partition,
		LeaderNode:  nodeData,
		Session:     hostname + strconv.FormatInt(time.Now().Unix(), 10),
		LeaderEpoch: epoch,
	}
	valueB, err := json.Marshal(topicLeaderSession)
	if err != nil {
		return err
	}
	topicKey := nem.createTopicLeaderPath(topic, partition)
	rsp, err := nem.client.Get(topicKey, false, false)
	if err != nil {
		if client.IsKeyNotFound(err) {
			coordLog.Infof("try to acquire topic leader session [%s]", topicKey)
			rsp, err = nem.client.Create(topicKey, string(valueB), 0)
			if err != nil {
				coordLog.Infof("acquire topic leader session [%s] failed: %v", topicKey, err)
				return err
			}
			coordLog.Infof("acquire topic leader [%s] success: %v", topicKey, string(valueB))
			return nil
		} else {
			coordLog.Warningf("try to acquire topic %v leader session failed: %v", topicKey, err)
			return err
		}
	}
	if rsp.Node.Value == string(valueB) {
		coordLog.Infof("get topic leader with the same [%s] ", topicKey)
		return nil
	}
	coordLog.Infof("get topic leader [%s] failed, lock exist value[%s]", topicKey, rsp.Node.Value)
	return ErrKeyAlreadyExist
}

func (nem *NsqdEtcdMgr) ReleaseTopicLeader(topic string, partition int, session *TopicLeaderSession) error {
	nem.Lock()
	defer nem.Unlock()

	topicKey := nem.createTopicLeaderPath(topic, partition)
	valueB, err := json.Marshal(session)
	if err != nil {
		return err
	}

	_, err = nem.client.CompareAndDelete(topicKey, string(valueB), 0)
	if err != nil {
		if !client.IsKeyNotFound(err) {
			coordLog.Infof("try release topic leader session [%s] error: %v, orig: %v", topicKey, err, session)
			// since the topic leader session type is changed, we need do the compatible check
			rsp, innErr := nem.client.Get(topicKey, false, false)
			if innErr != nil {
			} else {
				var old TopicLeaderSession
				json.Unmarshal([]byte(rsp.Node.Value), &old)
				if old.IsSame(session) {
					_, err = nem.client.CompareAndDelete(topicKey, rsp.Node.Value, 0)
					if err != nil {
						coordLog.Warningf("release topic leader session [%s] error: %v, orig: %v",
							topicKey, err, old)
					}
				} else {
					coordLog.Warningf("topic leader session [%s] mismatch: %v, orig: %v",
						topicKey, session, old)
				}
			}
		}
	}
	if err == nil {
		coordLog.Infof("try release topic leader session [%s] success: %v", topicKey, session)
	}
	return err
}

func (nem *NsqdEtcdMgr) GetAllLookupdNodes() ([]NsqLookupdNodeInfo, error) {
	rsp, err := nem.client.Get(nem.lookupdRoot, false, false)
	if err != nil {
		if client.IsKeyNotFound(err) {
			return nil, ErrKeyNotFound
		}
		return nil, err
	}
	lookupdNodeList := make([]NsqLookupdNodeInfo, 0)
	for _, node := range rsp.Node.Nodes {
		var nodeInfo NsqLookupdNodeInfo
		if err = json.Unmarshal([]byte(node.Value), &nodeInfo); err != nil {
			continue
		}
		lookupdNodeList = append(lookupdNodeList, nodeInfo)
	}
	return lookupdNodeList, nil
}

func (nem *NsqdEtcdMgr) WatchLookupdLeader(leader chan *NsqLookupdNodeInfo, stop chan struct{}) error {
	key := nem.createLookupdLeaderPath()

	rsp, err := nem.client.Get(key, false, false)
	var initIndex uint64
	if err == nil {
		coordLog.Infof("key: %s value: %s, index: %v", rsp.Node.Key, rsp.Node.Value, rsp.Index)
		var lookupdInfo NsqLookupdNodeInfo
		err = json.Unmarshal([]byte(rsp.Node.Value), &lookupdInfo)
		if err == nil {
			select {
			case leader <- &lookupdInfo:
			case <-stop:
				close(leader)
				return nil
			}
		}
		if rsp.Index > 0 {
			initIndex = rsp.Index - 1
		}
	} else {
		coordLog.Errorf("get error: %s", err.Error())
	}

	watcher := nem.client.Watch(key, initIndex, true)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-stop:
			cancel()
		}
	}()
	isMissing := true
	for {
		rsp, err = watcher.Next(ctx)
		if err != nil {
			if err == context.Canceled {
				coordLog.Infof("watch key[%s] canceled.", key)
				close(leader)
				return nil
			} else {
				coordLog.Errorf("watcher key[%s] error: %s", key, err.Error())
				//rewatch
				if IsEtcdWatchExpired(err) {
					isMissing = true
					rsp, err = nem.client.Get(key, false, true)
					if err != nil {
						time.Sleep(time.Second)
						coordLog.Errorf("rewatch and get key[%s] error: %s", key, err.Error())
						continue
					}
					coordLog.Warningf("rewatch key %v with newest index: %v, new data: %v", key, rsp.Index, rsp.Node.String())
					watcher = nem.client.Watch(key, rsp.Index+1, true)
				} else {
					time.Sleep(5 * time.Second)
					continue
				}
			}
		}
		if rsp == nil {
			continue
		}
		// note: if watch expire we use get to get the newest key value, Action will be "get"
		var lookupdInfo NsqLookupdNodeInfo
		if rsp.Action == "expire" || rsp.Action == "delete" {
			coordLog.Infof("key[%s] action[%s]", key, rsp.Action)
			isMissing = true
		} else if rsp.Action == "create" || rsp.Action == "update" || rsp.Action == "set" {
			err := json.Unmarshal([]byte(rsp.Node.Value), &lookupdInfo)
			if err != nil {
				continue
			}
			if lookupdInfo.NodeIP != "" {
				isMissing = false
			}
		} else {
			if isMissing {
				coordLog.Infof("key[%s] action[%s]", key, rsp.Action)
				if rsp.Node != nil {
					coordLog.Warningf("key %v new data: %v", key, rsp.Node.String())
					err := json.Unmarshal([]byte(rsp.Node.Value), &lookupdInfo)
					if err != nil {
						continue
					}
					if lookupdInfo.NodeIP != "" {
						isMissing = false
					}
				}
			} else {
				continue
			}
		}
		select {
		case leader <- &lookupdInfo:
		case <-stop:
			close(leader)
			return nil
		}
	}
}

func (nem *NsqdEtcdMgr) GetTopicInfo(topic string, partition int) (*TopicPartitionMetaInfo, error) {
	var topicInfo TopicPartitionMetaInfo
	rsp, err := nem.client.Get(nem.createTopicMetaPath(topic), false, false)
	if err != nil {
		if client.IsKeyNotFound(err) {
			return nil, ErrKeyNotFound
		}
		return nil, err
	}
	var mInfo TopicMetaInfo
	err = json.Unmarshal([]byte(rsp.Node.Value), &mInfo)
	if err != nil {
		return nil, err
	}
	topicInfo.TopicMetaInfo = mInfo

	rsp, err = nem.client.Get(nem.createTopicReplicaInfoPath(topic, partition), false, false)
	if err != nil {
		return nil, err
	}
	var rInfo TopicPartitionReplicaInfo
	if err = json.Unmarshal([]byte(rsp.Node.Value), &rInfo); err != nil {
		return nil, err
	}
	rInfo.Epoch = EpochType(rsp.Node.ModifiedIndex)
	topicInfo.TopicPartitionReplicaInfo = rInfo
	topicInfo.Name = topic
	topicInfo.Partition = partition

	return &topicInfo, nil
}

func (nem *NsqdEtcdMgr) GetTopicLeaderSession(topic string, partition int) (*TopicLeaderSession, error) {
	rsp, err := nem.client.Get(nem.createTopicLeaderPath(topic, partition), false, false)
	if err != nil {
		if client.IsKeyNotFound(err) {
			return nil, ErrKeyNotFound
		}
		return nil, err
	}
	var topicLeaderSession TopicLeaderSession
	if err = json.Unmarshal([]byte(rsp.Node.Value), &topicLeaderSession); err != nil {
		return nil, err
	}
	return &topicLeaderSession, nil
}

func (nem *NsqdEtcdMgr) IsTopicRealDeleted(topic string) (bool, error) {
	return true, nil
}

func (nem *NsqdEtcdMgr) createNsqdNodePath(nodeData *NsqdNodeInfo) string {
	return path.Join("/", NSQ_ROOT_DIR, nem.clusterID, NSQ_NODE_DIR, "Node-"+nodeData.ID)
}

func (nem *NsqdEtcdMgr) createTopicRootPath() string {
	return path.Join("/", NSQ_ROOT_DIR, nem.clusterID, NSQ_TOPIC_DIR)
}

func (nem *NsqdEtcdMgr) createTopicMetaPath(topic string) string {
	return path.Join(nem.topicRoot, topic, NSQ_TOPIC_META)
}

func (nem *NsqdEtcdMgr) createTopicReplicaInfoPath(topic string, partition int) string {
	return path.Join(nem.topicRoot, topic, strconv.Itoa(partition), NSQ_TOPIC_REPLICA_INFO)
}

func (nem *NsqdEtcdMgr) createLookupdRootPath() string {
	return path.Join("/", NSQ_ROOT_DIR, nem.clusterID, NSQ_LOOKUPD_DIR, NSQ_LOOKUPD_NODE_DIR)
}

func (nem *NsqdEtcdMgr) createLookupdLeaderPath() string {
	return path.Join("/", NSQ_ROOT_DIR, nem.clusterID, NSQ_LOOKUPD_DIR, NSQ_LOOKUPD_LEADER_SESSION)
}

func (nem *NsqdEtcdMgr) createTopicPartitionPath(topic string, partition int) string {
	return path.Join(nem.topicRoot, topic, strconv.Itoa(partition))
}

func (nem *NsqdEtcdMgr) createTopicLeaderPath(topic string, partition int) string {
	return path.Join(nem.topicRoot, topic, strconv.Itoa(partition), NSQ_TOPIC_LEADER_SESSION)
}
