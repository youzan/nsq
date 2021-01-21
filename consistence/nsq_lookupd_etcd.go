//        file: consistence/nsq_lookupd_etcd.go
// description: opr of nsq lookupd to etcd

//      author: reezhou
//       email: reechou@gmail.com
//   copyright: youzan

package consistence

import (
	"encoding/json"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/client"
	lru "github.com/hashicorp/golang-lru"
	"golang.org/x/net/context"
)

const (
	EVENT_WATCH_TOPIC_L_CREATE = iota
	EVENT_WATCH_TOPIC_L_DELETE
)

type cachedItemWithTs struct {
	value interface{}
	ts    int64
}

func getCacheValue(cache *lru.ARCCache, k string, tn time.Time) (interface{}, bool) {
	cv, ok := cache.Get(k)
	if ok {
		item := cv.(cachedItemWithTs)
		if item.ts > tn.UnixNano() {
			return item.value, true
		}
	}
	return nil, false
}

func putCacheValue(cache *lru.ARCCache, k string, v interface{}, tn time.Time, ttl time.Duration) {
	cache.Add(k, cachedItemWithTs{
		ts:    tn.Add(ttl).UnixNano(),
		value: v,
	})
}

type NsqLookupdEtcdMgr struct {
	tmiMutex sync.RWMutex
	cache    *lru.ARCCache

	client              *EtcdClient
	clusterID           string
	topicRoot           string
	clusterPath         string
	leaderSessionPath   string
	leaderStr           string
	lookupdRootPath     string
	topicMetaInfos      []TopicPartitionMetaInfo
	currentClusterIndex uint64
	topicMetaMap        map[string]TopicMetaInfo
	ifTopicChanged      int32
	ifTopicScanning     int32
	nodeInfo            *NsqLookupdNodeInfo
	nodeKey             string
	nodeValue           string

	refreshStopCh        chan bool
	watchTopicsStopCh    chan struct{}
	watchNsqdNodesStopCh chan bool

	topicReplicasMap    map[string]map[int]TopicPartitionReplicaInfo
	watchedClusterIndex uint64
}

func NewNsqLookupdEtcdMgr(host, username, pwd string) (*NsqLookupdEtcdMgr, error) {
	client, err := NewEClient(host, username, pwd)
	if err != nil {
		return nil, err
	}
	c, _ := lru.NewARC(10000)
	return &NsqLookupdEtcdMgr{
		client:               client,
		ifTopicChanged:       1,
		ifTopicScanning:      0,
		watchTopicsStopCh:    make(chan struct{}, 1),
		watchNsqdNodesStopCh: make(chan bool, 1),
		topicMetaMap:         make(map[string]TopicMetaInfo),
		refreshStopCh:        make(chan bool, 1),
		topicReplicasMap:     make(map[string]map[int]TopicPartitionReplicaInfo),
		cache:                c,
	}, nil
}

func (self *NsqLookupdEtcdMgr) InitClusterID(id string) {
	self.clusterID = id
	self.topicRoot = self.createTopicRootPath()
	self.clusterPath = self.createClusterPath()
	self.leaderSessionPath = self.createLookupdLeaderPath()
	self.lookupdRootPath = self.createLookupdRootPath()
	go self.watchTopics()
}

func (self *NsqLookupdEtcdMgr) Register(value *NsqLookupdNodeInfo) error {
	self.nodeInfo = value
	valueB, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if self.refreshStopCh != nil {
		close(self.refreshStopCh)
	}

	self.leaderStr = string(valueB)
	self.nodeKey = self.createLookupdPath(value)
	self.nodeValue = string(valueB)
	_, err = self.client.Set(self.nodeKey, self.nodeValue, ETCD_TTL)
	if err != nil {
		return err
	}
	self.refreshStopCh = make(chan bool, 1)
	// start to refresh
	go self.refresh(self.refreshStopCh)

	return nil
}

func (self *NsqLookupdEtcdMgr) refresh(stopC <-chan bool) {
	ticker := time.NewTicker(time.Second * time.Duration(ETCD_TTL/10))
	defer ticker.Stop()
	for {
		select {
		case <-stopC:
			return
		case <-ticker.C:
			_, err := self.client.SetWithTTL(self.nodeKey, ETCD_TTL)
			if err != nil {
				coordLog.Errorf("update error: %s", err.Error())
				_, err := self.client.Set(self.nodeKey, self.nodeValue, ETCD_TTL)
				if err != nil {
					coordLog.Errorf("set key error: %s", err.Error())
				}
			}
		}
	}
}

func (self *NsqLookupdEtcdMgr) Unregister(value *NsqLookupdNodeInfo) error {
	// stop to refresh
	if self.refreshStopCh != nil {
		close(self.refreshStopCh)
		self.refreshStopCh = nil
	}

	_, err := self.client.Delete(self.createLookupdPath(value), false)
	if err != nil {
		coordLog.Warningf("cluser[%v] node[%v] unregister failed: %v", self.clusterID, value, err)
		return err
	}

	return nil
}

func (self *NsqLookupdEtcdMgr) Stop() {
	//	self.Unregister()
	coordLog.Infof("lookup etcd leadership is stopping.")
	if self.watchNsqdNodesStopCh != nil {
		close(self.watchNsqdNodesStopCh)
	}
	if self.watchTopicsStopCh != nil {
		close(self.watchTopicsStopCh)
	}
}

func (self *NsqLookupdEtcdMgr) GetClusterEpoch() (EpochType, error) {
	rsp, err := self.client.Get(self.clusterPath, false, false)
	if err != nil {
		if client.IsKeyNotFound(err) {
			return 0, ErrKeyNotFound
		}
		return 0, err
	}

	return EpochType(rsp.Node.ModifiedIndex), nil
}

func (self *NsqLookupdEtcdMgr) GetAllLookupdNodes() ([]NsqLookupdNodeInfo, error) {
	tn := time.Now()
	cv, ok := getCacheValue(self.cache, self.lookupdRootPath, tn)
	if ok {
		return cv.([]NsqLookupdNodeInfo), nil
	}
	rsp, err := self.client.Get(self.lookupdRootPath, false, false)
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
	putCacheValue(self.cache, self.lookupdRootPath, lookupdNodeList, tn, time.Second)
	return lookupdNodeList, nil
}

func (self *NsqLookupdEtcdMgr) AcquireAndWatchLeader(leader chan *NsqLookupdNodeInfo, stop chan struct{}) {
	master := NewMaster(self.client, self.leaderSessionPath, self.leaderStr, ETCD_TTL)
	go self.processMasterEvents(master, leader, stop)
	master.Start()
}

func (self *NsqLookupdEtcdMgr) GetTopicsMetaInfoMap(topics []string) (map[string]TopicMetaInfo, error) {
	topicMetaInfoCache := make(map[string]TopicMetaInfo)
	if atomic.LoadInt32(&self.ifTopicChanged) == 1 {
		//fetch from etcd
		for _, topic := range topics {
			topicMeta, _, err := self.GetTopicMetaInfo(topic)
			if err != nil {
				return nil, err
			}
			topicMetaInfoCache[topic] = topicMeta
		}
	} else {
		self.tmiMutex.RLock()
		defer self.tmiMutex.RUnlock()
		for _, topic := range topics {
			topicMeta, exist := self.topicMetaMap[topic]
			if !exist {
				topicMetaInfoCache[topic] = TopicMetaInfo{}
			} else {
				topicMetaInfoCache[topic] = topicMeta
			}
		}
	}
	return topicMetaInfoCache, nil
}

func (self *NsqLookupdEtcdMgr) processMasterEvents(master Master, leader chan *NsqLookupdNodeInfo, stop chan struct{}) {
	for {
		select {
		case e := <-master.GetEventsChan():
			if e.Type == MASTER_ADD || e.Type == MASTER_MODIFY {
				// Acquired the lock || lock change.
				var lookupdNode NsqLookupdNodeInfo
				if err := json.Unmarshal([]byte(e.Master), &lookupdNode); err != nil {
					leader <- &lookupdNode
					continue
				}
				coordLog.Infof("master event type[%d] lookupdNode[%v].", e.Type, lookupdNode)
				leader <- &lookupdNode
			} else if e.Type == MASTER_DELETE {
				coordLog.Infof("master event delete.")
				// Lost the lock.
				var lookupdNode NsqLookupdNodeInfo
				leader <- &lookupdNode
			} else {
				// TODO: lock error.
				coordLog.Infof("master event unknown: %v.", e)
			}
		case <-stop:
			master.Stop()
			close(leader)
			return
		}
	}
}

func (self *NsqLookupdEtcdMgr) GetNsqdNodes() ([]NsqdNodeInfo, error) {
	nodes, _, err := self.getNsqdNodes(false)
	return nodes, err
}

func (self *NsqLookupdEtcdMgr) WatchNsqdNodes(nsqds chan []NsqdNodeInfo, stop chan struct{}) {
	defer close(nsqds)
	nsqdNodes, _, err := self.getNsqdNodes(false)
	if err == nil {
		select {
		case nsqds <- nsqdNodes:
		case <-stop:
			return
		}
	}

	key := self.createNsqdRootPath()
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-stop:
			coordLog.Infof("watch key[%s] will be stopped by 1.", key)
			cancel()
		case <-self.watchNsqdNodesStopCh:
			coordLog.Infof("watch key[%s] will be stopped by 2.", key)
			cancel()
		}
	}()
	watchWaitAndDo(ctx, self.client, key, true, func(rsp *client.Response) {
		nsqdNodes, _, err := self.getNsqdNodes(true)
		if err != nil {
			coordLog.Errorf("key[%s] getNsqdNodes error: %s", key, err.Error())
			return
		}
		select {
		case nsqds <- nsqdNodes:
		case <-stop:
			return
		}
	}, nil)
}

func (self *NsqLookupdEtcdMgr) getNsqdNodes(upToDate bool) ([]NsqdNodeInfo, uint64, error) {
	var rsp *client.Response
	var err error
	if upToDate {
		rsp, err = self.client.GetNewest(self.createNsqdRootPath(), false, false)
	} else {
		rsp, err = self.client.Get(self.createNsqdRootPath(), false, false)
	}
	if err != nil {
		if client.IsKeyNotFound(err) {
			return nil, 0, ErrKeyNotFound
		}
		return nil, 0, err
	}
	nsqdNodes := make([]NsqdNodeInfo, 0)
	for _, node := range rsp.Node.Nodes {
		if node.Dir {
			continue
		}
		var nodeInfo NsqdNodeInfo
		err := json.Unmarshal([]byte(node.Value), &nodeInfo)
		if err != nil {
			continue
		}
		nsqdNodes = append(nsqdNodes, nodeInfo)
	}
	return nsqdNodes, rsp.Index, err
}

func (self *NsqLookupdEtcdMgr) GetAllTopicMetas() (map[string]TopicMetaInfo, error) {
	self.tmiMutex.RLock()
	topicMetas := self.topicMetaMap
	self.tmiMutex.RUnlock()
	return topicMetas, nil
}

func (self *NsqLookupdEtcdMgr) ScanTopics() ([]TopicPartitionMetaInfo, error) {
	if atomic.LoadInt32(&self.ifTopicChanged) == 1 {
		return self.scanTopics()
	}

	self.tmiMutex.RLock()
	topicMetaInfos := self.topicMetaInfos
	self.tmiMutex.RUnlock()
	return topicMetaInfos, nil
}

func getMaxIndexFromNode(n *client.Node) uint64 {
	if n == nil {
		return 0
	}
	maxI := n.ModifiedIndex
	if n.Dir {
		for _, child := range n.Nodes {
			index := getMaxIndexFromNode(child)
			if index > maxI {
				maxI = index
			}
		}
	}
	return maxI
}

func getMaxIndexFromWatchRsp(resp *client.Response) uint64 {
	if resp == nil {
		return 0
	}
	index := resp.Index
	if resp.Node != nil {
		if nmi := getMaxIndexFromNode(resp.Node); nmi > index {
			index = nmi
		}
	}
	return index
}

func watchWaitAndDo(ctx context.Context, client *EtcdClient,
	key string, recursive bool, callback func(rsp *client.Response),
	watchExpiredCb func()) {
	initIndex := uint64(0)
	rsp, err := client.Get(key, false, recursive)
	if err != nil {
		coordLog.Errorf("get watched key[%s] error: %s", key, err.Error())
	} else {
		if rsp.Index > 0 {
			initIndex = rsp.Index - 1
		}
		callback(rsp)
	}
	watcher := client.Watch(key, initIndex, recursive)
	for {
		// to avoid dead connection issues, we add timeout for watch connection to wake up watch if too long no
		// any event
		rsp, err := watcher.Next(ctx)
		if err != nil {
			if err == context.Canceled {
				coordLog.Infof("watch key[%s] cancelled.", key)
				return
			} else if err == context.DeadlineExceeded {
				coordLog.Debugf("watcher key[%s] timeout: %s", key, err.Error())
				continue
			} else {
				//rewatch
				if IsEtcdWatchExpired(err) {
					coordLog.Debugf("watcher key[%s] error: %s", key, err.Error())
					if watchExpiredCb != nil {
						watchExpiredCb()
					}
					rsp, err = client.Get(key, false, false)
					if err != nil {
						coordLog.Errorf("rewatch and get key[%s] error: %s", key, err.Error())
						time.Sleep(time.Second)
						continue
					}
					// watch for v2 client should not +1 on index, since it is the after index (which will +1 in the method of watch)
					watcher = client.Watch(key, rsp.Index, recursive)
					// watch expired should be treated as changed of node
				} else {
					coordLog.Errorf("watcher key[%s] error: %s", key, err.Error())
					time.Sleep(5 * time.Second)
					continue
				}
			}
		}
		callback(rsp)
	}
}

// watch topics if changed
func (self *NsqLookupdEtcdMgr) watchTopics() {
	atomic.StoreInt32(&self.ifTopicChanged, 1)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-self.watchTopicsStopCh:
			coordLog.Infof("watch key[%s] will be stopped.", self.topicRoot)
			cancel()
		}
	}()
	watchWaitAndDo(ctx, self.client, self.topicRoot, true, func(rsp *client.Response) {
		//since the max modified may become less if some node deleted, we can not determin the current max, so we need
		//  use the cluster index to check if changed to new.
		// note the rsp.index in watch is the cluster-index when the watch begin, so the cluster-index may less than modifiedIndex
		// since it will be increased after watch begin.
		mi := getMaxIndexFromWatchRsp(rsp)
		coordLog.Infof("topic max changed from %v to: %v, cluster-index: %v",
			atomic.LoadUint64(&self.watchedClusterIndex), mi, rsp.Index)
		// cluster index is used to tell the different if the max modify index is equal because of the delete of node
		// Max 1->2->3, this time we have 3(first time) as max modify and then new added become 4 then delete 4, max will be 3(second time)
		// without the cluster index, we can not tell the different from the second time and the first time, so we need this.
		if mi > 0 {
			atomic.StoreUint64(&self.watchedClusterIndex, mi)
		}
		atomic.StoreInt32(&self.ifTopicChanged, 1)
	}, nil)
}

func (self *NsqLookupdEtcdMgr) isCacheNewest() bool {
	if atomic.LoadInt32(&self.ifTopicChanged) == 1 {
		return false
	}
	if atomic.LoadInt32(&self.ifTopicScanning) == 1 {
		return false
	}
	return true
}

func (self *NsqLookupdEtcdMgr) scanTopics() ([]TopicPartitionMetaInfo, error) {
	atomic.StoreInt32(&self.ifTopicScanning, 1)
	defer atomic.StoreInt32(&self.ifTopicScanning, 0)
	atomic.StoreInt32(&self.ifTopicChanged, 0)
	rsp, err := self.client.Get(self.topicRoot, false, true)
	if err != nil {
		atomic.StoreInt32(&self.ifTopicChanged, 1)
		if client.IsKeyNotFound(err) {
			return nil, ErrKeyNotFound
		}
		return nil, err
	}

	self.tmiMutex.Lock()
	// we read the stale data, we can just return the last data
	if rsp.Index <= atomic.LoadUint64(&self.currentClusterIndex) {
		tmi := self.topicMetaInfos
		coordLog.Infof("ignore scan data since %v older then current: %v, max watched: %v",
			rsp.Index,
			atomic.LoadUint64(&self.currentClusterIndex),
			atomic.LoadUint64(&self.watchedClusterIndex),
		)
		if rsp.Index < atomic.LoadUint64(&self.watchedClusterIndex) {
			// we did not read the most newest, so we need scan next time
			atomic.StoreInt32(&self.ifTopicChanged, 1)
		}
		self.tmiMutex.Unlock()
		return tmi, nil
	}
	self.tmiMutex.Unlock()
	topicMetaMap := make(map[string]TopicMetaInfo)
	topicReplicasMap := make(map[string]map[int]TopicPartitionReplicaInfo)
	err = self.processTopicNode(rsp.Node.Nodes, topicMetaMap, topicReplicasMap)
	if err != nil {
		atomic.StoreInt32(&self.ifTopicChanged, 1)
		return nil, err
	}

	topicMetaInfos := make([]TopicPartitionMetaInfo, 0)
	for k, v := range topicReplicasMap {
		topicMeta, ok := topicMetaMap[k]
		if !ok {
			continue
		}
		for k2, v2 := range v {
			partition := k2
			var topicInfo TopicPartitionMetaInfo
			topicInfo.Name = k
			topicInfo.Partition = partition
			topicInfo.TopicMetaInfo = topicMeta
			topicInfo.TopicPartitionReplicaInfo = v2
			topicMetaInfos = append(topicMetaInfos, topicInfo)
		}
	}

	self.tmiMutex.Lock()
	if rsp.Index > atomic.LoadUint64(&self.currentClusterIndex) {
		coordLog.Debugf("scan data %v current: %v, max watched: %v",
			rsp.Index, atomic.LoadUint64(&self.currentClusterIndex),
			atomic.LoadUint64(&self.watchedClusterIndex),
		)
		self.topicMetaInfos = topicMetaInfos
		atomic.StoreUint64(&self.currentClusterIndex, rsp.Index)
		self.topicMetaMap = topicMetaMap
		self.topicReplicasMap = topicReplicasMap
	} else {
		coordLog.Infof("ignore scan data since %v older then current: %v, max watched: %v",
			rsp.Index, atomic.LoadUint64(&self.currentClusterIndex),
			atomic.LoadUint64(&self.watchedClusterIndex),
		)
		topicMetaInfos = self.topicMetaInfos
	}
	// note it is possible rsp.Index may greater than watched, since non-topic root changed will also increase the cluster index
	if rsp.Index < atomic.LoadUint64(&self.watchedClusterIndex) {
		// we did not read the most newest, so we need scan next time
		atomic.StoreInt32(&self.ifTopicChanged, 1)
		coordLog.Infof("scan data %v not the same as the max watched: %v, need scan next time",
			rsp.Index,
			atomic.LoadUint64(&self.watchedClusterIndex),
		)
	}
	self.tmiMutex.Unlock()
	self.cache.Purge()

	return topicMetaInfos, nil
}

func (self *NsqLookupdEtcdMgr) processTopicNode(nodes client.Nodes,
	topicMetaMap map[string]TopicMetaInfo,
	topicReplicasMap map[string]map[int]TopicPartitionReplicaInfo) error {
	for _, node := range nodes {
		if node.Nodes != nil {
			err := self.processTopicNode(node.Nodes, topicMetaMap, topicReplicasMap)
			if err != nil {
				return err
			}
		}
		if node.Dir {
			continue
		}
		_, key := path.Split(node.Key)
		if key == NSQ_TOPIC_REPLICA_INFO {
			var rInfo TopicPartitionReplicaInfo
			if err := json.Unmarshal([]byte(node.Value), &rInfo); err != nil {
				coordLog.Infof("process topic info: %s failed: %v", node.String(), err.Error())
				return err
			}
			rInfo.Epoch = EpochType(node.ModifiedIndex)
			keys := strings.Split(node.Key, "/")
			keyLen := len(keys)
			if keyLen < 3 {
				continue
			}
			topicName := keys[keyLen-3]
			partStr := keys[keyLen-2]
			part, err := strconv.Atoi(partStr)
			if err != nil {
				coordLog.Infof("process topic info: %s failed: %v", node.String(), err.Error())
				continue
			}
			v, ok := topicReplicasMap[topicName]
			if ok {
				v[part] = rInfo
			} else {
				pMap := make(map[int]TopicPartitionReplicaInfo)
				pMap[part] = rInfo
				topicReplicasMap[topicName] = pMap
			}
		} else if key == NSQ_TOPIC_META {
			var mInfo TopicMetaInfo
			if err := json.Unmarshal([]byte(node.Value), &mInfo); err != nil {
				coordLog.Infof("process topic info: %s failed: %v", node.String(), err.Error())
				return err
			}
			keys := strings.Split(node.Key, "/")
			keyLen := len(keys)
			if keyLen < 2 {
				continue
			}
			topicName := keys[keyLen-2]
			topicMetaMap[topicName] = mInfo
		}
	}
	return nil
}

func (self *NsqLookupdEtcdMgr) GetTopicInfoFromCacheOnly(topic string, partition int) (*TopicPartitionMetaInfo, bool) {
	var topicInfo TopicPartitionMetaInfo
	metaInfo, ok := self.GetTopicMetaInfoTryCacheOnly(topic)
	if !ok {
		return nil, false
	}
	topicInfo.TopicMetaInfo = metaInfo
	var rInfo TopicPartitionReplicaInfo
	found := false
	self.tmiMutex.RLock()
	parts, ok := self.topicReplicasMap[topic]
	if ok {
		p, ok := parts[partition]
		if ok {
			rInfo = *(p.Copy())
			found = true
		}
	}
	self.tmiMutex.RUnlock()
	topicInfo.TopicPartitionReplicaInfo = rInfo
	topicInfo.Name = topic
	topicInfo.Partition = partition
	return &topicInfo, found
}

func (self *NsqLookupdEtcdMgr) GetTopicInfo(topic string, partition int) (*TopicPartitionMetaInfo, error) {
	var topicInfo TopicPartitionMetaInfo
	metaInfo, _, err := self.GetTopicMetaInfoTryCache(topic)
	if err != nil {
		return nil, err
	}

	topicInfo.TopicMetaInfo = metaInfo
	var rInfo TopicPartitionReplicaInfo
	found := false
	notInCache := false
	// try get cache first
	if self.isCacheNewest() {
		self.tmiMutex.RLock()
		parts, ok := self.topicReplicasMap[topic]
		if ok {
			p, ok := parts[partition]
			if ok {
				rInfo = *(p.Copy())
				found = true
			} else {
				notInCache = true
			}
		} else {
			notInCache = true
		}
		self.tmiMutex.RUnlock()
	}
	if !found {
		rsp, err := self.client.GetNewest(self.createTopicReplicaInfoPath(topic, partition), false, false)
		if err != nil {
			if client.IsKeyNotFound(err) {
				return nil, ErrKeyNotFound
			}
			return nil, err
		}
		if err = json.Unmarshal([]byte(rsp.Node.Value), &rInfo); err != nil {
			return nil, err
		}
		if notInCache {
			// not in local cached, but in the etcd, something changed
			atomic.StoreInt32(&self.ifTopicChanged, 1)
		}
		rInfo.Epoch = EpochType(rsp.Node.ModifiedIndex)
	}
	topicInfo.TopicPartitionReplicaInfo = rInfo
	topicInfo.Name = topic
	topicInfo.Partition = partition

	return &topicInfo, nil
}

func (self *NsqLookupdEtcdMgr) CreateTopicPartition(topic string, partition int) error {
	_, err := self.client.CreateDir(self.createTopicPartitionPath(topic, partition), 0)
	if err != nil {
		if IsEtcdNotFile(err) {
			return ErrKeyAlreadyExist
		}
		return err
	}
	return nil
}

func (self *NsqLookupdEtcdMgr) CreateTopic(topic string, meta *TopicMetaInfo) error {
	metaValue, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	_, err = self.client.Create(self.createTopicMetaPath(topic), string(metaValue), 0)
	if err != nil {
		if IsEtcdNodeExist(err) {
			return ErrKeyAlreadyExist
		}
		return err
	}

	self.tmiMutex.Lock()
	self.topicMetaMap[topic] = *meta
	self.tmiMutex.Unlock()

	return nil
}

func (self *NsqLookupdEtcdMgr) IsExistTopic(topic string) (bool, error) {
	_, err := self.client.Get(self.createTopicPath(topic), false, false)
	if err != nil {
		if CheckKeyIfExist(err) {
			return false, nil
		} else {
			return false, err
		}
	}
	return true, nil
}

func (self *NsqLookupdEtcdMgr) IsExistTopicPartition(topic string, partitionNum int) (bool, error) {
	_, err := self.client.Get(self.createTopicPartitionPath(topic, partitionNum), false, false)
	if err != nil {
		if CheckKeyIfExist(err) {
			return false, nil
		} else {
			return false, err
		}
	}
	return true, nil
}

func (self *NsqLookupdEtcdMgr) GetTopicMetaInfoTryCacheOnly(topic string) (TopicMetaInfo, bool) {
	var metaInfo TopicMetaInfo
	var ok bool
	self.tmiMutex.RLock()
	metaInfo, ok = self.topicMetaMap[topic]
	self.tmiMutex.RUnlock()
	return metaInfo, ok
}

func (self *NsqLookupdEtcdMgr) GetTopicMetaInfoTryCache(topic string) (TopicMetaInfo, bool, error) {
	var metaInfo TopicMetaInfo
	var ok bool
	noInCache := false
	if self.isCacheNewest() {
		self.tmiMutex.RLock()
		metaInfo, ok = self.topicMetaMap[topic]
		if !ok {
			noInCache = true
		}
		self.tmiMutex.RUnlock()
	}
	if ok {
		return metaInfo, true, nil
	}

	mInfo, _, err := self.GetTopicMetaInfo(topic)
	if err != nil {
		return metaInfo, false, err
	}
	if noInCache {
		atomic.StoreInt32(&self.ifTopicChanged, 1)
	}
	return mInfo, false, nil
}

func (self *NsqLookupdEtcdMgr) GetTopicMetaInfo(topic string) (TopicMetaInfo, EpochType, error) {
	var metaInfo TopicMetaInfo
	rsp, err := self.client.GetNewest(self.createTopicMetaPath(topic), false, false)
	if err != nil {
		if client.IsKeyNotFound(err) {
			return metaInfo, 0, ErrKeyNotFound
		}
		return metaInfo, 0, err
	}
	err = json.Unmarshal([]byte(rsp.Node.Value), &metaInfo)
	if err != nil {
		return metaInfo, 0, err
	}
	epoch := EpochType(rsp.Node.ModifiedIndex)
	return metaInfo, epoch, nil
}

func (self *NsqLookupdEtcdMgr) UpdateTopicMetaInfo(topic string, meta *TopicMetaInfo, oldGen EpochType) error {
	value, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	coordLog.Infof("Update_topic meta info: %s %s %d", topic, string(value), oldGen)

	self.tmiMutex.Lock()
	defer self.tmiMutex.Unlock()
	rsp, err := self.client.CompareAndSwap(self.createTopicMetaPath(topic), string(value), 0, "", uint64(oldGen))
	if err != nil {
		return err
	}
	delete(self.topicMetaMap, topic)
	var newMeta TopicMetaInfo
	err = json.Unmarshal([]byte(rsp.Node.Value), &newMeta)
	if err != nil {
		coordLog.Errorf("unmarshal meta info failed: %v, %v", err, rsp.Node.Value)
		atomic.StoreInt32(&self.ifTopicChanged, 1)
		return err
	}
	self.topicMetaMap[topic] = newMeta
	atomic.StoreInt32(&self.ifTopicChanged, 1)

	return nil
}

func (self *NsqLookupdEtcdMgr) DeleteWholeTopic(topic string) error {
	self.tmiMutex.Lock()
	delete(self.topicMetaMap, topic)
	rsp, err := self.client.Delete(self.createTopicPath(topic), true)
	coordLog.Infof("delete whole topic: %v, %v, %v", topic, err, rsp)
	// TODO: add deleted key in etcd to make sure the deleted topic can be real
	// to avoid all the data is cleared by accident.
	// TODO; should remove deleted key if topic re-created.
	atomic.StoreInt32(&self.ifTopicChanged, 1)
	self.tmiMutex.Unlock()
	return err
}

func (self *NsqLookupdEtcdMgr) DeleteTopic(topic string, partition int) error {
	_, err := self.client.Delete(self.createTopicPartitionPath(topic, partition), true)
	if err != nil {
		if !client.IsKeyNotFound(err) {
			return err
		}
	}
	atomic.StoreInt32(&self.ifTopicChanged, 1)
	return nil
}

func (self *NsqLookupdEtcdMgr) UpdateTopicNodeInfo(topic string, partition int, topicInfo *TopicPartitionReplicaInfo, oldGen EpochType) error {
	value, err := json.Marshal(topicInfo)
	if err != nil {
		return err
	}
	coordLog.Infof("Update_topic info: %s %d %s %d", topic, partition, string(value), oldGen)
	if oldGen == 0 {
		rsp, err := self.client.Create(self.createTopicReplicaInfoPath(topic, partition), string(value), 0)
		if err != nil {
			return err
		}
		topicInfo.Epoch = EpochType(rsp.Node.ModifiedIndex)
		atomic.StoreInt32(&self.ifTopicChanged, 1)
		return nil
	}
	rsp, err := self.client.CompareAndSwap(self.createTopicReplicaInfoPath(topic, partition), string(value), 0, "", uint64(oldGen))
	if err != nil {
		return err
	}
	topicInfo.Epoch = EpochType(rsp.Node.ModifiedIndex)
	atomic.StoreInt32(&self.ifTopicChanged, 1)
	return nil
}

func (self *NsqLookupdEtcdMgr) GetTopicLeaderSession(topic string, partition int) (*TopicLeaderSession, error) {
	etcdKey := self.createTopicLeaderSessionPath(topic, partition)
	tn := time.Now()
	if self.isCacheNewest() {
		cv, ok := getCacheValue(self.cache, etcdKey, tn)
		if ok {
			return cv.(*TopicLeaderSession), nil
		}
	}

	rsp, err := self.client.Get(etcdKey, false, false)
	if err != nil {
		if client.IsKeyNotFound(err) {
			return nil, ErrLeaderSessionNotExist
		}
		return nil, err
	}
	var topicLeaderSession TopicLeaderSession
	if err = json.Unmarshal([]byte(rsp.Node.Value), &topicLeaderSession); err != nil {
		return nil, err
	}
	putCacheValue(self.cache, etcdKey, &topicLeaderSession, tn, time.Minute*5)

	return &topicLeaderSession, nil
}

func (self *NsqLookupdEtcdMgr) ReleaseTopicLeader(topic string, partition int, session *TopicLeaderSession) error {
	topicKey := self.createTopicLeaderSessionPath(topic, partition)
	valueB, err := json.Marshal(session)
	if err != nil {
		return err
	}

	_, err = self.client.CompareAndDelete(topicKey, string(valueB), 0)
	if err != nil {
		coordLog.Errorf("try release topic leader session [%s] error: %v", topicKey, err)
		if !client.IsKeyNotFound(err) {
			coordLog.Errorf("try release topic leader session [%s] error: %v, orig: %v", topicKey, err, session)
			// since the topic leader session type is changed, we need do the compatible check
			rsp, innErr := self.client.Get(topicKey, false, false)
			if innErr != nil {
			} else {
				var old TopicLeaderSession
				json.Unmarshal([]byte(rsp.Node.Value), &old)
				if old.IsSame(session) {
					_, err = self.client.CompareAndDelete(topicKey, rsp.Node.Value, 0)
				} else {
					coordLog.Errorf("leader session mismatch [%s],  %v, orig: %v", topicKey, session.LeaderNode, old.LeaderNode)
				}
			}
		}
	} else {
		coordLog.Infof("try release topic leader session [%s] success: %v", topicKey, session)
	}
	return err
}

func (self *NsqLookupdEtcdMgr) createClusterPath() string {
	return path.Join("/", NSQ_ROOT_DIR, self.clusterID)
}

func (self *NsqLookupdEtcdMgr) createLookupdPath(value *NsqLookupdNodeInfo) string {
	return path.Join("/", NSQ_ROOT_DIR, self.clusterID, NSQ_LOOKUPD_DIR, NSQ_LOOKUPD_NODE_DIR, "Node-"+value.ID)
}

func (self *NsqLookupdEtcdMgr) createLookupdRootPath() string {
	return path.Join("/", NSQ_ROOT_DIR, self.clusterID, NSQ_LOOKUPD_DIR, NSQ_LOOKUPD_NODE_DIR)
}

func (self *NsqLookupdEtcdMgr) createLookupdLeaderPath() string {
	return path.Join("/", NSQ_ROOT_DIR, self.clusterID, NSQ_LOOKUPD_DIR, NSQ_LOOKUPD_LEADER_SESSION)
}

func (self *NsqLookupdEtcdMgr) createNsqdRootPath() string {
	return path.Join("/", NSQ_ROOT_DIR, self.clusterID, NSQ_NODE_DIR)
}

func (self *NsqLookupdEtcdMgr) createTopicRootPath() string {
	return path.Join("/", NSQ_ROOT_DIR, self.clusterID, NSQ_TOPIC_DIR)
}

func (self *NsqLookupdEtcdMgr) createTopicPath(topic string) string {
	return path.Join(self.topicRoot, topic)
}

func (self *NsqLookupdEtcdMgr) createTopicMetaPath(topic string) string {
	return path.Join(self.topicRoot, topic, NSQ_TOPIC_META)
}

func (self *NsqLookupdEtcdMgr) createTopicPartitionPath(topic string, partition int) string {
	return path.Join(self.topicRoot, topic, strconv.Itoa(partition))
}

func (self *NsqLookupdEtcdMgr) createTopicReplicaInfoPath(topic string, partition int) string {
	return path.Join(self.topicRoot, topic, strconv.Itoa(partition), NSQ_TOPIC_REPLICA_INFO)
}

func (self *NsqLookupdEtcdMgr) createTopicLeaderSessionPath(topic string, partition int) string {
	return path.Join(self.topicRoot, topic, strconv.Itoa(partition), NSQ_TOPIC_LEADER_SESSION)
}
