package consistence

import (
	"errors"
	"fmt"
	"math/rand"
	"net"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff"
)

var (
	ErrAlreadyExist         = errors.New("already exist")
	ErrTopicNotCreated      = errors.New("topic is not created")
	ErrWaitingLeaderRelease = errors.New("leader session is still alive")
	ErrNotNsqLookupLeader   = errors.New("Not nsqlookup leader")
	ErrClusterUnstable      = errors.New("the cluster is unstable")
	errLookupExiting        = errors.New("lookup is exiting")

	ErrLeaderNodeLost           = NewCoordErr("leader node is lost", CoordTmpErr)
	ErrNodeNotFound             = NewCoordErr("node not found", CoordCommonErr)
	ErrLeaderElectionFail       = NewCoordErr("Leader election failed.", CoordElectionTmpErr)
	ErrNoLeaderCanBeElected     = NewCoordErr("No leader can be elected", CoordElectionTmpErr)
	ErrNodeUnavailable          = NewCoordErr("No node is available for topic", CoordTmpErr)
	ErrJoinISRInvalid           = NewCoordErr("Join ISR failed", CoordCommonErr)
	ErrJoinISRTimeout           = NewCoordErr("Join ISR timeout", CoordCommonErr)
	ErrWaitingJoinISR           = NewCoordErr("The topic is waiting node to join isr", CoordCommonErr)
	ErrLeaderSessionNotReleased = NewCoordErr("The topic leader session is not released", CoordElectionTmpErr)
	ErrTopicISRCatchupEnough    = NewCoordErr("the topic isr and catchup nodes are enough", CoordTmpErr)
	ErrClusterNodeRemoving      = NewCoordErr("the node is mark as removed", CoordTmpErr)
	ErrTopicNodeConflict        = NewCoordErr("the topic node info is conflicted", CoordElectionErr)
	ErrLeadershipServerUnstable = NewCoordErr("the leadership server is unstable", CoordTmpErr)
)

var (
	waitMigrateInterval          = time.Minute * 10
	waitEmergencyMigrateInterval = time.Second * 10
	waitRemovingNodeInterval     = time.Second * 30
	balanceInterval              = time.Second * 60
	doCheckInterval              = time.Second * 60
)

type JoinISRState struct {
	sync.Mutex
	waitingJoin      bool
	waitingSession   string
	waitingStart     time.Time
	readyNodes       map[string]struct{}
	doneChan         chan struct{}
	isLeadershipWait bool
}

func (s *JoinISRState) String() string {
	return fmt.Sprintf("join %v, session %v, start %s, readys %v, isLeader %v",
		s.waitingJoin, s.waitingSession, s.waitingStart, s.readyNodes, s.isLeadershipWait)
}

type RpcFailedInfo struct {
	nodeID    string
	topic     string
	partition int
	failTime  time.Time
}

func getOthersExceptLeader(topicInfo *TopicPartitionMetaInfo) []string {
	others := make([]string, 0, len(topicInfo.ISR)+len(topicInfo.CatchupList)-1)
	for _, n := range topicInfo.ISR {
		if n == topicInfo.Leader {
			continue
		}
		others = append(others, n)
	}
	others = append(others, topicInfo.CatchupList...)
	return others
}

type TopicNameInfo struct {
	TopicName      string
	TopicPartition int
}

type Options struct {
	BalanceStart int
	BalanceEnd   int
}

// nsqlookup coordinator is used for the topic leader and isr coordinator, all the changes for leader or isr
// will be handled by the nsqlookup coordinator.
// In a cluster, only one nsqlookup coordinator will be the leader which will handle the event for cluster changes.
type NsqLookupCoordinator struct {
	clusterKey         string
	myNode             NsqLookupdNodeInfo
	leaderMu           sync.RWMutex
	leaderNode         NsqLookupdNodeInfo
	leadership         NSQLookupdLeadership
	nodesMutex         sync.RWMutex
	nsqdNodes          map[string]NsqdNodeInfo
	removingNodes      map[string]string
	nodesEpoch         int64
	rpcMutex           sync.RWMutex
	nsqdRpcClients     map[string]*NsqdRpcClient
	checkTopicFailChan chan TopicNameInfo
	stopChan           chan struct{}
	joinStateMutex     sync.Mutex
	joinISRState       map[string]*JoinISRState
	failedRpcMutex     sync.Mutex
	failedRpcList      []RpcFailedInfo
	nsqlookupRpcServer *NsqLookupCoordRpcServer
	wg                 sync.WaitGroup
	nsqdMonitorChan    chan struct{}
	isClusterUnstable  int32
	isUpgrading        int32
	dpm                *DataPlacement
	balanceWaiting     int32
	doChecking         int32
	interruptChecking  int32
	enableTopNBalance  int32
	cachedNodeStats    map[string]cachedNodeTopicStats
	cachedMutex        sync.Mutex
}

func NewNsqLookupCoordinator(cluster string, n *NsqLookupdNodeInfo, opts *Options) *NsqLookupCoordinator {
	coord := &NsqLookupCoordinator{
		clusterKey:         cluster,
		myNode:             *n,
		leadership:         nil,
		nsqdNodes:          make(map[string]NsqdNodeInfo),
		removingNodes:      make(map[string]string),
		nsqdRpcClients:     make(map[string]*NsqdRpcClient),
		checkTopicFailChan: make(chan TopicNameInfo, 3),
		stopChan:           make(chan struct{}),
		joinISRState:       make(map[string]*JoinISRState),
		failedRpcList:      make([]RpcFailedInfo, 0),
		nsqdMonitorChan:    make(chan struct{}),
		cachedNodeStats:    make(map[string]cachedNodeTopicStats),
	}
	if coord.leadership != nil {
		coord.leadership.InitClusterID(coord.clusterKey)
	}
	coord.nsqlookupRpcServer = NewNsqLookupCoordRpcServer(coord)
	coord.dpm = NewDataPlacement(coord)
	if opts != nil {
		coord.dpm.SetBalanceInterval(opts.BalanceStart, opts.BalanceEnd)
	}
	return coord
}

func (nlcoord *NsqLookupCoordinator) SetLeadershipMgr(l NSQLookupdLeadership) {
	nlcoord.leadership = l
	if nlcoord.leadership != nil {
		nlcoord.leadership.InitClusterID(nlcoord.clusterKey)
	}
}

func RetryWithTimeout(fn func() error) error {
	bo := backoff.NewExponentialBackOff()
	bo.MaxElapsedTime = time.Second * 15
	bo.MaxInterval = time.Second * 5
	return backoff.Retry(fn, bo)
}

// init and register to leader server
func (nlcoord *NsqLookupCoordinator) Start() error {
	if nlcoord.leadership != nil {
		err := nlcoord.leadership.Register(&nlcoord.myNode)
		if err != nil {
			coordLog.Warningf("failed to register nsqlookup coordinator: %v", err)
			return err
		}
	}
	nlcoord.wg.Add(1)
	go nlcoord.handleLeadership()
	go nlcoord.nsqlookupRpcServer.start(nlcoord.myNode.NodeIP, nlcoord.myNode.RpcPort)
	nlcoord.notifyNodesLookup()
	return nil
}

func (nlcoord *NsqLookupCoordinator) Stop() {
	close(nlcoord.stopChan)
	nlcoord.leadership.Unregister(&nlcoord.myNode)
	nlcoord.leadership.Stop()
	// TODO: exit should avoid while test.
	nlcoord.nsqlookupRpcServer.stop()
	nlcoord.rpcMutex.RLock()
	for _, c := range nlcoord.nsqdRpcClients {
		c.Close()
	}
	nlcoord.rpcMutex.RUnlock()
	nlcoord.wg.Wait()
	coordLog.Infof("nsqlookup coordinator stopped.")
}

func (nlcoord *NsqLookupCoordinator) notifyNodesLookup() {
	nodes, err := nlcoord.leadership.GetNsqdNodes()
	if err != nil {
		return
	}

	for _, node := range nodes {
		client, err := NewNsqdRpcClient(net.JoinHostPort(node.NodeIP, node.RpcPort), RPC_TIMEOUT_FOR_LOOKUP)
		if err != nil {
			coordLog.Infof("rpc node %v client init failed : %v", node, err)
			continue
		}
		client.TriggerLookupChanged()
		client.Close()
	}
}

func (nlcoord *NsqLookupCoordinator) handleLeadership() {
	defer nlcoord.wg.Done()
	lookupdLeaderChan := make(chan *NsqLookupdNodeInfo)
	if nlcoord.leadership != nil {
		go nlcoord.leadership.AcquireAndWatchLeader(lookupdLeaderChan, nlcoord.stopChan)
	}
	ticker := time.NewTicker(time.Second * 5)
	defer func() {
		ticker.Stop()
		if e := recover(); e != nil {
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			buf = buf[0:n]
			coordLog.Errorf("panic %s:%v", buf, e)
		}

		coordLog.Warningf("leadership watch exit.")
		time.Sleep(time.Second)
		if nlcoord.nsqdMonitorChan != nil {
			close(nlcoord.nsqdMonitorChan)
			nlcoord.nsqdMonitorChan = nil
		}
	}()
	for {
		select {
		case l, ok := <-lookupdLeaderChan:
			if !ok {
				coordLog.Warningf("leader chan closed.")
				return
			}
			if l == nil {
				coordLog.Warningln("leader is lost.")
				continue
			}
			if l.GetID() != nlcoord.GetLookupLeader().GetID() ||
				l.Epoch != nlcoord.GetLookupLeader().Epoch {
				coordLog.Infof("lookup leader changed from %v to %v", nlcoord.GetLookupLeader(), *l)
				nlcoord.leaderMu.Lock()
				nlcoord.leaderNode = *l
				nlcoord.leaderMu.Unlock()
				if nlcoord.GetLookupLeader().GetID() != nlcoord.myNode.GetID() {
					// remove watchers.
					if nlcoord.nsqdMonitorChan != nil {
						close(nlcoord.nsqdMonitorChan)
					}
					nlcoord.nsqdMonitorChan = make(chan struct{})
				}
				nlcoord.notifyLeaderChanged(nlcoord.nsqdMonitorChan)
			}
			if nlcoord.GetLookupLeader().GetID() == "" {
				coordLog.Warningln("leader is missing.")
			}
		case <-ticker.C:
			// reload topics to cache, used for query from client
			_, err := nlcoord.leadership.ScanTopics()
			if err != nil {
				coordLog.Warningf("refresh topics failed: %v", err.Error())
			}
		}
	}
}

func (nlcoord *NsqLookupCoordinator) notifyLeaderChanged(monitorChan chan struct{}) {
	if nlcoord.GetLookupLeader().GetID() != nlcoord.myNode.GetID() {
		coordLog.Infof("I am slave (%v). Leader is: %v", nlcoord.myNode, nlcoord.GetLookupLeader())
		nlcoord.nodesMutex.Lock()
		nlcoord.removingNodes = make(map[string]string)
		nlcoord.nodesMutex.Unlock()
		nlcoord.rpcMutex.Lock()
		for nid, c := range nlcoord.nsqdRpcClients {
			c.Close()
			delete(nlcoord.nsqdRpcClients, nid)
		}
		nlcoord.rpcMutex.Unlock()
		return
	}
	coordLog.Infof("I am master now.")

	// we do not need to watch each topic leader,
	// we can make sure the leader on the alive node is alive.
	// so we bind all the topic leader session on the alive node session
	//nlcoord.wg.Add(1)
	//go func() {
	//	defer nlcoord.wg.Done()
	//	nlcoord.watchTopicLeaderSession(monitorChan)
	//}()

	nlcoord.wg.Add(1)
	go func() {
		defer nlcoord.wg.Done()
		nlcoord.handleNsqdNodes(monitorChan)
	}()
	nlcoord.wg.Add(1)
	go func() {
		defer nlcoord.wg.Done()
		nlcoord.rpcFailRetryFunc(monitorChan)
	}()

	// reload topic information should after watching the nsqd nodes
	if nlcoord.leadership != nil {
		newTopics, err := nlcoord.leadership.ScanTopics()
		if err != nil {
			// may not init any topic yet.
			if err != ErrKeyNotFound {
				coordLog.Infof("load topic info failed: %v", err)
			}
		} else {
			coordLog.Infof("topic loaded : %v", len(newTopics))
			nlcoord.notifyTopicsToAllNsqdForReload(newTopics)
		}
	}

	nlcoord.wg.Add(1)
	go func() {
		defer nlcoord.wg.Done()
		nlcoord.checkTopics(monitorChan)
	}()

	nlcoord.wg.Add(1)
	go func() {
		defer nlcoord.wg.Done()
		nlcoord.dpm.DoBalance(monitorChan)
	}()
	nlcoord.wg.Add(1)
	go func() {
		defer nlcoord.wg.Done()
		nlcoord.handleRemovingNodesLoop(monitorChan)
	}()
}

// for the nsqd node that temporally lost, we need send the related topics to
// it .
func (nlcoord *NsqLookupCoordinator) notifyTopicsToSingleNsqdForReload(topics []TopicPartitionMetaInfo, nodeID string) {
	coordLog.Infof("reload topics for node: %v", nodeID)
	for _, v := range topics {
		select {
		case <-nlcoord.stopChan:
			return
		default:
		}
		if FindSlice(v.ISR, nodeID) != -1 || FindSlice(v.CatchupList, nodeID) != -1 {
			nlcoord.notifySingleNsqdForTopicReload(v, nodeID)
		}
	}
}

func (nlcoord *NsqLookupCoordinator) notifyTopicsToAllNsqdForReload(topics []TopicPartitionMetaInfo) {
	coordLog.Infof("notify all topics %v for all nodes ", len(topics))
	for _, v := range topics {
		select {
		case <-nlcoord.stopChan:
			return
		default:
		}

		nlcoord.notifyAllNsqdsForTopicReload(v)
	}
}

func (nlcoord *NsqLookupCoordinator) getCurrentNodes() map[string]NsqdNodeInfo {
	nlcoord.nodesMutex.RLock()
	currentNodes := nlcoord.nsqdNodes
	if len(nlcoord.removingNodes) > 0 {
		currentNodes = make(map[string]NsqdNodeInfo)
		for nid, n := range nlcoord.nsqdNodes {
			if _, ok := nlcoord.removingNodes[nid]; ok {
				continue
			}
			currentNodes[nid] = n
		}
	}
	nlcoord.nodesMutex.RUnlock()
	return currentNodes
}

func (nlcoord *NsqLookupCoordinator) getCurrentNodesWithRemoving() (map[string]NsqdNodeInfo, int64) {
	nlcoord.nodesMutex.RLock()
	currentNodes := nlcoord.nsqdNodes
	currentNodesEpoch := atomic.LoadInt64(&nlcoord.nodesEpoch)
	nlcoord.nodesMutex.RUnlock()
	return currentNodes, currentNodesEpoch
}

func (nlcoord *NsqLookupCoordinator) getCurrentNodesWithEpoch() (map[string]NsqdNodeInfo, int64) {
	nlcoord.nodesMutex.RLock()
	currentNodes := nlcoord.nsqdNodes
	if len(nlcoord.removingNodes) > 0 {
		currentNodes = make(map[string]NsqdNodeInfo)
		for nid, n := range nlcoord.nsqdNodes {
			if _, ok := nlcoord.removingNodes[nid]; ok {
				continue
			}
			currentNodes[nid] = n
		}
	}
	currentNodesEpoch := atomic.LoadInt64(&nlcoord.nodesEpoch)
	nlcoord.nodesMutex.RUnlock()
	return currentNodes, currentNodesEpoch
}

func (nlcoord *NsqLookupCoordinator) handleNsqdNodes(monitorChan chan struct{}) {
	nsqdNodesChan := make(chan []NsqdNodeInfo)
	if nlcoord.leadership != nil {
		go nlcoord.leadership.WatchNsqdNodes(nsqdNodesChan, monitorChan)
	}
	coordLog.Debugf("start watch the nsqd nodes.")
	defer func() {
		coordLog.Infof("stop watch the nsqd nodes.")
	}()
	for {
		select {
		case nodes, ok := <-nsqdNodesChan:
			if !ok {
				return
			}
			// check if any nsqd node changed.
			coordLog.Debugf("Current nsqd nodes: %v", len(nodes))
			oldNodes := nlcoord.nsqdNodes
			newNodes := make(map[string]NsqdNodeInfo)
			for _, v := range nodes {
				//coordLog.Infof("nsqd node %v : %v", v.GetID(), v)
				newNodes[v.GetID()] = v
			}
			// check if etcd is ok
			_, err := nlcoord.leadership.GetClusterEpoch()
			if err != nil {
				coordLog.Infof("get cluster epoch failed: %v", err)
				continue
			}
			nlcoord.nodesMutex.Lock()
			nlcoord.nsqdNodes = newNodes
			check := false
			for oldID, oldNode := range oldNodes {
				if _, ok := newNodes[oldID]; !ok {
					coordLog.Warningf("nsqd node failed: %v, %v", oldID, oldNode)
					// if node is missing we need check election immediately.
					check = true
				}
			}
			// failed need be protected by lock so we can avoid contention.
			if check {
				atomic.AddInt64(&nlcoord.nodesEpoch, 1)
			}
			nlcoord.nodesMutex.Unlock()

			if nlcoord.leadership == nil {
				continue
			}
			topics, scanErr := nlcoord.leadership.ScanTopics()
			if scanErr != nil {
				coordLog.Infof("scan topics failed: %v", scanErr)
			}
			for newID, newNode := range newNodes {
				if _, ok := oldNodes[newID]; !ok {
					coordLog.Infof("new nsqd node joined: %v, %v", newID, newNode)
					// notify the nsqd node to recheck topic info.(for
					// temp lost)
					if scanErr == nil {
						nlcoord.notifyTopicsToSingleNsqdForReload(topics, newID)
					}
					check = true
				}
			}
			if check {
				atomic.AddInt64(&nlcoord.nodesEpoch, 1)
				atomic.StoreInt32(&nlcoord.isClusterUnstable, 1)
				nlcoord.triggerCheckTopics("", 0, time.Millisecond*10)
			}
		}
	}
}

// this will permanently remove a node from cluster by hand , it will try move all the topics on
// this node to others, make sure we have enough nodes to safely remove a node.
func (nlcoord *NsqLookupCoordinator) handleRemovingNodesLoop(monitorChan chan struct{}) {
	coordLog.Debugf("start handle the removing nsqd nodes.")
	defer func() {
		coordLog.Infof("stop handle the removing nsqd nodes.")
	}()
	ticker := time.NewTicker(waitRemovingNodeInterval)
	nodeTopicStats := make([]NodeTopicStats, 0, 10)
	defer ticker.Stop()
	for {
		select {
		case <-monitorChan:
			return
		case <-ticker.C:
			//
			nlcoord.nodesMutex.RLock()
			removingNodes := make(map[string]string)
			for nid, removeState := range nlcoord.removingNodes {
				removingNodes[nid] = removeState
			}
			nlcoord.nodesMutex.RUnlock()
			// remove state: marked -> pending -> data_transferred -> done
			if len(removingNodes) == 0 {
				continue
			}
			nodeTopicStats = nlcoord.processRemovingNodes(monitorChan, removingNodes, nodeTopicStats)
		}
	}
}

func (nlcoord *NsqLookupCoordinator) processRemovingNodes(monitorChan chan struct{},
	removingNodes map[string]string, nodeTopicStats []NodeTopicStats) []NodeTopicStats {
	anyStateChanged := false
	if !atomic.CompareAndSwapInt32(&nlcoord.balanceWaiting, 0, 1) {
		return nodeTopicStats
	}
	defer atomic.StoreInt32(&nlcoord.balanceWaiting, 0)
	currentNodes := nlcoord.getCurrentNodes()
	allTopics, err := nlcoord.leadership.ScanTopics()
	if err != nil {
		return nodeTopicStats
	}
	nodeTopicStats = nodeTopicStats[:0]
	for nodeID, nodeInfo := range currentNodes {
		topicStat, err := nlcoord.getNsqdTopicStat(nodeInfo)
		if err != nil {
			coordLog.Infof("failed to get node topic status : %v", nodeID)
			continue
		}
		nodeTopicStats = append(nodeTopicStats, *topicStat)
	}
	leaderSort := func(l, r *NodeTopicStats) bool {
		return l.LeaderLessLoader(r)
	}
	By(leaderSort).Sort(nodeTopicStats)

	for nid := range removingNodes {
		anyPending := false
		coordLog.Infof("handle the removing node %v ", nid)
		// only check the topic with one replica left
		// because the doCheckTopics will check the others
		// we add a new replica for the removing node
		moved := 0
		for _, topicInfo := range allTopics {
			if FindSlice(topicInfo.ISR, nid) == -1 {
				if FindSlice(topicInfo.CatchupList, nid) != -1 {
					topicInfo.CatchupList = FilterList(topicInfo.CatchupList, []string{nid})
					nlcoord.notifyOldNsqdsForTopicMetaInfo(&topicInfo, []string{nid})
					err := nlcoord.leadership.UpdateTopicNodeInfo(topicInfo.Name, topicInfo.Partition,
						&topicInfo.TopicPartitionReplicaInfo, topicInfo.Epoch)
					if err != nil {
						anyPending = true
					} else {
						nlcoord.notifyTopicMetaInfo(&topicInfo)
					}
				}
				continue
			}
			if len(topicInfo.ISR) <= topicInfo.Replica {
				anyPending = true
				// find new catchup and wait isr ready
				removingNodes[nid] = "pending"
				err := nlcoord.dpm.addToCatchupAndWaitISRReady(monitorChan, true, nid, topicInfo.Name, topicInfo.Partition,
					nil, getNodeNameList(nodeTopicStats), true)
				if err != nil {
					coordLog.Infof("topic %v data on node %v transferred failed: %v, waiting next time", topicInfo.GetTopicDesp(), nid, err.Error())
					continue
				}
				coordLog.Infof("topic %v data on node %v transferred success", topicInfo.GetTopicDesp(), nid)
				anyStateChanged = true
			} else {
				nlcoord.handleRemoveTopicNodeOrMoveLeader(topicInfo.Leader == nid, topicInfo.Name, topicInfo.Partition, nid)
				anyPending = true
			}
			moved++
			if moved%16 == 0 {
				// recompute the load factor after moved some topics
				nodeTopicStats = nodeTopicStats[:0]
				for nodeID, nodeInfo := range currentNodes {
					topicStat, err := nlcoord.getNsqdTopicStat(nodeInfo)
					if err != nil {
						coordLog.Infof("failed to get node topic status : %v", nodeID)
						continue
					}
					nodeTopicStats = append(nodeTopicStats, *topicStat)
				}
				By(leaderSort).Sort(nodeTopicStats)
			}
		}
		if !anyPending {
			anyStateChanged = true
			coordLog.Infof("node %v data has been transferred, it can be removed from cluster: state: %v", nid, removingNodes[nid])
			if removingNodes[nid] != "data_transferred" && removingNodes[nid] != "done" {
				removingNodes[nid] = "data_transferred"
			} else {
				if removingNodes[nid] == "data_transferred" {
					removingNodes[nid] = "done"
				} else if removingNodes[nid] == "done" {
					nlcoord.nodesMutex.Lock()
					_, ok := nlcoord.nsqdNodes[nid]
					if !ok {
						delete(removingNodes, nid)
						coordLog.Infof("the node %v is removed finally since not alive in cluster", nid)
					}
					nlcoord.nodesMutex.Unlock()
				}
			}
		}
	}

	if anyStateChanged {
		nlcoord.nodesMutex.Lock()
		nlcoord.removingNodes = removingNodes
		nlcoord.nodesMutex.Unlock()
	}
	return nodeTopicStats
}

func (nlcoord *NsqLookupCoordinator) triggerCheckTopicsRandom(topic string, part int, delay time.Duration) {
	d := rand.Int63n(time.Second.Nanoseconds())
	nlcoord.triggerCheckTopics(topic, part, delay+time.Duration(d))
}

func (nlcoord *NsqLookupCoordinator) triggerCheckTopics(topic string, part int, delay time.Duration) {
	time.Sleep(delay)
	ti := time.NewTimer(time.Second)
	defer ti.Stop()

	select {
	case nlcoord.checkTopicFailChan <- TopicNameInfo{topic, part}:
	case <-nlcoord.stopChan:
		return
	case <-ti.C:
		return
	}
}

// check if partition is enough,
// check if replication is enough
// check any unexpected state.
func (nlcoord *NsqLookupCoordinator) checkTopics(monitorChan chan struct{}) {
	ticker := time.NewTicker(doCheckInterval)
	waitingMigrateTopic := make(map[string]map[int]time.Time)
	lostLeaderSessions := make(map[string]bool)
	lastFullCheck := time.Now()
	defer func() {
		ticker.Stop()
		coordLog.Infof("check topics quit.")
	}()

	for {
		select {
		case <-monitorChan:
			return
		case <-ticker.C:
			if nlcoord.leadership == nil {
				continue
			}
			if time.Since(lastFullCheck) < doCheckInterval {
				continue
			}
			nlcoord.doCheckTopics(monitorChan, nil, waitingMigrateTopic, lostLeaderSessions, true)
			lastFullCheck = time.Now()
		case failedInfo := <-nlcoord.checkTopicFailChan:
			if nlcoord.leadership == nil {
				continue
			}
			if failedInfo.TopicName == "" && time.Since(lastFullCheck) < doCheckInterval/10 {
				continue
			}
			nlcoord.doCheckTopics(monitorChan, &failedInfo, waitingMigrateTopic, lostLeaderSessions, failedInfo.TopicName == "")
			if failedInfo.TopicName == "" {
				lastFullCheck = time.Now()
			}
		}
	}
}

func (nlcoord *NsqLookupCoordinator) doCheckTopics(monitorChan chan struct{}, failedInfo *TopicNameInfo,
	waitingMigrateTopic map[string]map[int]time.Time, lostLeaderSessions map[string]bool, fullCheck bool) {

	time.Sleep(time.Millisecond * 10)
	coordLog.Infof("do check topics...")
	defer coordLog.Infof("do check topics done")
	if !atomic.CompareAndSwapInt32(&nlcoord.doChecking, 0, 1) {
		return
	}
	defer atomic.StoreInt32(&nlcoord.doChecking, 0)
	atomic.StoreInt32(&nlcoord.interruptChecking, 0)

	topics := []TopicPartitionMetaInfo{}
	if failedInfo == nil || failedInfo.TopicName == "" || failedInfo.TopicPartition < 0 {
		var commonErr error
		topics, commonErr = nlcoord.leadership.ScanTopics()
		if commonErr != nil {
			if commonErr != ErrKeyNotFound {
				coordLog.Infof("scan topics failed. %v", commonErr)
			}
			return
		}
		topicMetas, _ := nlcoord.leadership.GetAllTopicMetas()
		coordLog.Debugf("scan found topics: %v, %v", topics, topicMetas)
		// check partition number for topic, maybe failed to create
		// some partition when creating topic.
		topicParts := make(map[string]int, len(topicMetas))
		for _, t := range topics {
			parts, ok := topicParts[t.Name]
			if !ok {
				parts = 0
				topicParts[t.Name] = 0
			}
			parts++
			topicParts[t.Name] = parts
		}

		for name, meta := range topicMetas {
			metaNum := meta.PartitionNum
			pnum, _ := topicParts[name]
			if pnum >= metaNum {
				continue
			}
			coordLog.Warningf("topic %v partitions not enough : %v, %v", name, pnum, metaNum)
			nlcoord.CreateTopic(name, meta)
		}
	} else {
		var err error
		coordLog.Infof("check single topic : %v ", failedInfo)
		var t *TopicPartitionMetaInfo
		t, err = nlcoord.leadership.GetTopicInfo(failedInfo.TopicName, failedInfo.TopicPartition)
		if err != nil {
			coordLog.Infof("get topic info failed: %v, %v", failedInfo, err)
			return
		}
		topics = append(topics, *t)
	}

	currentNodes, currentNodesEpoch := nlcoord.getCurrentNodesWithRemoving()
	checkOK := true
	for _, t := range topics {
		if currentNodesEpoch != atomic.LoadInt64(&nlcoord.nodesEpoch) {
			coordLog.Infof("nodes changed while checking topics: %v, %v", currentNodesEpoch, atomic.LoadInt64(&nlcoord.nodesEpoch))
			return
		}
		select {
		case <-monitorChan:
			// exiting
			return
		default:
		}
		if atomic.LoadInt32(&nlcoord.interruptChecking) == 1 {
			return
		}

		if failedInfo != nil && failedInfo.TopicName != "" {
			if t.Name != failedInfo.TopicName {
				continue
			}
		}
		needMigrate := false
		if len(t.ISR) < t.Replica {
			coordLog.Infof("ISR is not enough for topic %v, isr is :%v", t.GetTopicDesp(), t.ISR)
			needMigrate = true
			checkOK = false
		}

		// check if any ISR waiting join the topic, if so
		// we check later.
		nlcoord.joinStateMutex.Lock()
		state, ok := nlcoord.joinISRState[t.GetTopicDesp()]
		nlcoord.joinStateMutex.Unlock()
		if ok && state != nil {
			state.Lock()
			wj := state.waitingJoin
			state.Unlock()
			if wj {
				coordLog.Infof("topic %v is in waiting join state", t.GetTopicDesp())
				checkOK = false
				go nlcoord.triggerCheckTopics(t.Name, t.Partition, time.Second)
				continue
			}
		}

		aliveCount := 0
		failedNodes := make([]string, 0)
		for _, replica := range t.ISR {
			if _, ok := currentNodes[replica]; !ok {
				coordLog.Warningf("topic %v isr node %v is lost.", t.GetTopicDesp(), replica)
				needMigrate = true
				checkOK = false
				if replica != t.Leader {
					// leader fail will be handled while leader election.
					failedNodes = append(failedNodes, replica)
				}
			} else {
				aliveCount++
			}
		}
		if currentNodesEpoch != atomic.LoadInt64(&nlcoord.nodesEpoch) {
			coordLog.Infof("nodes changed while checking topics: %v, %v", currentNodesEpoch, atomic.LoadInt64(&nlcoord.nodesEpoch))
			atomic.StoreInt32(&nlcoord.isClusterUnstable, 1)
			return
		}
		// should copy to avoid reused
		topicInfo := t
		// move the failed node from ISR to catchup
		coordErr := nlcoord.handleRemoveFailedISRNodes(failedNodes, &topicInfo)
		if coordErr != nil {
			go nlcoord.triggerCheckTopics(t.Name, t.Partition, time.Second*2)
			continue
		}

		// if leader of topic is down, we need elect new leader first
		if _, ok := currentNodes[t.Leader]; !ok {
			needMigrate = true
			checkOK = false
			coordLog.Warningf("topic %v leader %v is lost.", t.GetTopicDesp(), t.Leader)
			aliveNodes, aliveEpoch := nlcoord.getCurrentNodesWithEpoch()
			if aliveEpoch != currentNodesEpoch {
				continue
			}
			coordErr := nlcoord.handleTopicLeaderElection(&topicInfo, aliveNodes, aliveEpoch, false)
			if coordErr != nil {
				go nlcoord.triggerCheckTopics(t.Name, t.Partition, time.Second)
				coordLog.Warningf("topic leader election failed: %v", coordErr)
			}
			continue
		}
		// check topic leader session key.
		var leaderSession *TopicLeaderSession
		var err error
		retry := 0
		for retry < 3 {
			retry++
			leaderSession, err = nlcoord.leadership.GetTopicLeaderSession(t.Name, t.Partition)
			if err != nil {
				coordLog.Infof("topic %v leader session failed to get: %v", t.GetTopicDesp(), err)
				// notify the nsqd node to acquire the leader session.
				nlcoord.notifyISRTopicMetaInfo(&topicInfo)
				nlcoord.notifyAcquireTopicLeader(&topicInfo)
				if err == ErrLeaderSessionNotExist {
					break
				}
				time.Sleep(time.Millisecond * 100)
				continue
			} else {
				break
			}
		}
		if leaderSession == nil {
			checkOK = false
			lostLeaderSessions[t.GetTopicDesp()] = true
			continue
		}
		if leaderSession.LeaderNode == nil || leaderSession.Session == "" {
			checkOK = false
			lostLeaderSessions[t.GetTopicDesp()] = true
			coordLog.Infof("topic %v leader session node is missing.", t.GetTopicDesp())
			nlcoord.notifyISRTopicMetaInfo(&topicInfo)
			nlcoord.notifyAcquireTopicLeader(&topicInfo)
			continue
		}
		if leaderSession.LeaderNode.ID != t.Leader {
			checkOK = false
			lostLeaderSessions[t.GetTopicDesp()] = true
			coordLog.Warningf("topic %v leader session %v-%v-%v mismatch: %v", t.GetTopicDesp(),
				leaderSession.LeaderNode, leaderSession.Session, leaderSession.LeaderEpoch, t.Leader)
			tmpTopicInfo := t
			tmpTopicInfo.Leader = leaderSession.LeaderNode.ID
			nlcoord.notifyReleaseTopicLeader(&tmpTopicInfo, leaderSession.LeaderEpoch, leaderSession.Session)
			tmpSession := *leaderSession
			go func() {
				err := nlcoord.waitOldLeaderRelease(&tmpTopicInfo)
				if err != nil {
					coordLog.Warningf("topic %v leader session release failed: %v, force release", tmpTopicInfo.GetTopicDesp(), err.Error())
					err = nlcoord.leadership.ReleaseTopicLeader(tmpTopicInfo.Name, tmpTopicInfo.Partition, &tmpSession)
					if err != nil {
						coordLog.Errorf("release session failed [%s] : %v", tmpTopicInfo.GetTopicDesp(), err)
					}
				}
			}()
			nlcoord.notifyISRTopicMetaInfo(&topicInfo)
			nlcoord.notifyAcquireTopicLeader(&topicInfo)
			continue
		}

		partitions, ok := waitingMigrateTopic[t.Name]
		if !ok {
			partitions = make(map[int]time.Time)
			waitingMigrateTopic[t.Name] = partitions
		}

		if needMigrate {
			for _, replica := range t.CatchupList {
				if _, ok := currentNodes[replica]; ok {
					// alive catchup, just notify node to catchup again
					coordLog.Infof("topic %v has alive catchup node %v, notify catchup now", t.GetTopicDesp(), replica)
					nlcoord.notifyCatchupTopicMetaInfo(&topicInfo)
					break
				}
			}
			if _, ok := partitions[t.Partition]; !ok {
				partitions[t.Partition] = time.Now()
			}

			if atomic.LoadInt32(&nlcoord.isUpgrading) == 1 {
				coordLog.Infof("wait checking topics since the cluster is upgrading")
				continue
			}

			// isr is not enough for replicator, we try migrate some data to new node.
			failedTime := partitions[t.Partition]
			emergency := (aliveCount <= t.Replica/2) && failedTime.Before(time.Now().Add(-1*waitEmergencyMigrateInterval))
			if emergency ||
				failedTime.Before(time.Now().Add(-1*waitMigrateInterval)) {
				coordLog.Infof("begin migrate the topic :%v", t.GetTopicDesp())
				aliveNodes, aliveEpoch := nlcoord.getCurrentNodesWithEpoch()
				if aliveEpoch != currentNodesEpoch {
					go nlcoord.triggerCheckTopics(t.Name, t.Partition, time.Second)
					continue
				}
				nlcoord.handleTopicMigrate(&topicInfo, aliveNodes, aliveEpoch)
				// TODO: migrate may slow, we should keep the migrate failed time to allow
				// next start early. Otherwise we may need wait another more migrate interval
				// add test case for a topic with many ordered partitions
				// delete(partitions, t.Partition)
			} else {
				coordLog.Infof("waiting migrate the topic :%v since time: %v", t.GetTopicDesp(), partitions[t.Partition])
			}
		} else {
			delete(partitions, t.Partition)
		}
		// check if the topic write disabled, and try enable if possible. There is a chance to
		// notify the topic enable write with failure, which may cause the write state is not ok.
		if ok && state != nil {
			state.Lock()
			wj := state.waitingJoin
			state.Unlock()
			if wj {
				coordLog.Infof("topic %v is in waiting join state", t.GetTopicDesp())
				checkOK = false
				go nlcoord.triggerCheckTopics(t.Name, t.Partition, time.Second)
				continue
			}
		}

		if currentNodesEpoch != atomic.LoadInt64(&nlcoord.nodesEpoch) {
			coordLog.Infof("nodes changed while checking topics: %v, %v", currentNodesEpoch, atomic.LoadInt64(&nlcoord.nodesEpoch))
			atomic.StoreInt32(&nlcoord.isClusterUnstable, 1)
			return
		}
		// check if write disabled
		if nlcoord.isTopicWriteDisabled(&topicInfo) {
			coordLog.Infof("the topic write is disabled but not in waiting join state: %v", t)
			checkOK = false
			go nlcoord.revokeEnableTopicWrite(t.Name, t.Partition, true)
		} else {
			if _, ok := lostLeaderSessions[t.GetTopicDesp()]; ok {
				coordLog.Infof("notify %v topic leadership since lost before ", t.GetTopicDesp())
				leaderSession, err := nlcoord.leadership.GetTopicLeaderSession(t.Name, t.Partition)
				if err != nil {
					coordLog.Infof("failed to get topic %v leader session: %v", t.GetTopicDesp(), err)
				} else {
					nlcoord.notifyTopicLeaderSession(&topicInfo, leaderSession, "")
					delete(lostLeaderSessions, t.GetTopicDesp())
				}
			}
			nlcoord.nodesMutex.RLock()
			hasRemovingNode := len(nlcoord.removingNodes) > 0
			nlcoord.nodesMutex.RUnlock()
			if aliveCount > t.Replica && atomic.LoadInt32(&nlcoord.balanceWaiting) == 0 && !hasRemovingNode {
				//remove the unwanted node in isr, it may happen that the nodes in isr is more than the configured replicator
				coordLog.Infof("isr is more than replicator: %v, %v", aliveCount, t.Replica)
				removeNode := nlcoord.dpm.decideUnwantedISRNode(&topicInfo, currentNodes)
				if removeNode != "" {
					failedNodes := make([]string, 0, 1)
					failedNodes = append(failedNodes, removeNode)
					coordErr := nlcoord.handleRemoveISRNodes(failedNodes, &topicInfo, false)
					if coordErr == nil {
						coordLog.Infof("node %v removed by plan from topic : %v", failedNodes, t)
					}
				}
			} else if !hasRemovingNode {
				if !nlcoord.dpm.checkTopicNodeConflict(&topicInfo) {
					coordLog.Warningf("topic %v has conflict nodes %v, removing", topicInfo.GetTopicDesp(), topicInfo.CatchupList)
					// remove conflict catchup
					nlcoord.handleRemoveCatchupNodes(&topicInfo)
				} else {
					// check catchup list to retry
					for _, replica := range topicInfo.CatchupList {
						if _, ok := currentNodes[replica]; ok {
							// alive catchup, just notify node to catchup again
							coordLog.Infof("topic %v has alive catchup node %v, notify catchup now", topicInfo.GetTopicDesp(), replica)
							nlcoord.notifyCatchupTopicMetaInfo(&topicInfo)
							break
						}
					}
				}
			}
		}
	}
	if checkOK {
		if fullCheck {
			atomic.StoreInt32(&nlcoord.isClusterUnstable, 0)
		}
	} else {
		atomic.StoreInt32(&nlcoord.isClusterUnstable, 1)
	}
}

func (nlcoord *NsqLookupCoordinator) handleTopicLeaderElection(topicInfo *TopicPartitionMetaInfo, currentNodes map[string]NsqdNodeInfo,
	currentNodesEpoch int64, isOldLeaderAlive bool) *CoordErr {
	_, leaderSession, state, coordErr := nlcoord.prepareJoinState(topicInfo.Name, topicInfo.Partition, false)
	if coordErr != nil {
		coordLog.Infof("prepare join state failed: %v", coordErr)
		return coordErr
	}
	state.Lock()
	defer state.Unlock()
	if state.waitingJoin {
		coordLog.Warningf("failed because another is waiting join: %v", state)
		return ErrLeavingISRWait
	}
	defer func() {
		go nlcoord.triggerCheckTopics(topicInfo.Name, topicInfo.Partition, time.Second)
	}()

	if currentNodesEpoch != atomic.LoadInt64(&nlcoord.nodesEpoch) {
		return ErrClusterChanged
	}
	if state.doneChan != nil {
		close(state.doneChan)
		state.doneChan = nil
	}
	state.waitingJoin = false
	state.waitingSession = ""

	newTopicInfo, err := nlcoord.leadership.GetTopicInfo(topicInfo.Name, topicInfo.Partition)
	if err != nil {
		return &CoordErr{err.Error(), RpcNoErr, CoordNetErr}
	}
	if topicInfo.Epoch != newTopicInfo.Epoch {
		return ErrClusterChanged
	}

	coordErr = nlcoord.notifyLeaderDisableTopicWriteFast(topicInfo)
	if coordErr != nil {
		coordLog.Infof("disable write failed while elect leader: %v", coordErr)
		// the leader maybe down, so we can ignore this error safely.
	}
	var failedNode string
	failedNode, coordErr = nlcoord.notifyISRDisableTopicWrite(topicInfo)
	if coordErr != nil {
		coordLog.Infof("failed notify %v disable write while election: %v", failedNode, coordErr)
		return coordErr
	}

	if currentNodesEpoch != atomic.LoadInt64(&nlcoord.nodesEpoch) {
		return ErrClusterChanged
	}
	// choose another leader in ISR list, and add new node to ISR
	// list.
	newLeader, newestLogID, coordErr := nlcoord.dpm.chooseNewLeaderFromISR(topicInfo, currentNodes)
	if coordErr != nil {
		return coordErr
	}

	if leaderSession != nil {
		// notify old leader node to release leader
		nlcoord.notifyReleaseTopicLeader(topicInfo, leaderSession.LeaderEpoch, leaderSession.Session)
	}

	err = nlcoord.waitOldLeaderRelease(topicInfo)
	if err != nil {
		coordLog.Infof("Leader is not released: %v", topicInfo)
		return ErrLeaderSessionNotReleased
	}
	// notify new leader to all isr nodes
	coordLog.Infof("topic %v leader election result: %v", topicInfo, newLeader)
	coordErr = nlcoord.makeNewTopicLeaderAcknowledged(topicInfo, newLeader, newestLogID, isOldLeaderAlive)
	if coordErr != nil {
		return coordErr
	}

	return nil
}

func (nlcoord *NsqLookupCoordinator) handleRemoveISRNodes(failedNodes []string, origTopicInfo *TopicPartitionMetaInfo, leaveCatchup bool) *CoordErr {
	topicInfo := origTopicInfo.Copy()
	nlcoord.joinStateMutex.Lock()
	state, ok := nlcoord.joinISRState[topicInfo.GetTopicDesp()]
	if !ok {
		state = &JoinISRState{}
		nlcoord.joinISRState[topicInfo.GetTopicDesp()] = state
	}
	nlcoord.joinStateMutex.Unlock()
	state.Lock()
	defer state.Unlock()

	wj := state.waitingJoin
	if wj {
		coordLog.Infof("isr node is waiting for join session %v, removing should wait.", state.waitingSession)
		return ErrLeavingISRWait
	}

	if len(failedNodes) == 0 {
		return nil
	}
	newISR := FilterList(topicInfo.ISR, failedNodes)
	if len(newISR) == 0 {
		coordLog.Infof("no node left in isr if removing failed")
		return nil
	}
	topicInfo.ISR = newISR
	if len(topicInfo.ISR) <= topicInfo.Replica/2 {
		coordLog.Infof("no enough isr node while removing the failed nodes. %v", topicInfo.ISR)
		if !leaveCatchup {
			return ErrLeavingISRWait
		}
	}
	if leaveCatchup {
		topicInfo.CatchupList = MergeList(topicInfo.CatchupList, failedNodes)
	} else {
		topicInfo.CatchupList = FilterList(topicInfo.CatchupList, failedNodes)
	}
	coordLog.Infof("topic info updated: %v", topicInfo)
	// remove isr node we keep the write epoch unchanged.
	err := nlcoord.leadership.UpdateTopicNodeInfo(topicInfo.Name, topicInfo.Partition, &topicInfo.TopicPartitionReplicaInfo, topicInfo.Epoch)
	if err != nil {
		coordLog.Infof("update topic node isr failed: %v", err.Error())
		return &CoordErr{err.Error(), RpcNoErr, CoordNetErr}
	}
	*origTopicInfo = *topicInfo
	go nlcoord.notifyTopicMetaInfo(topicInfo.Copy())
	return nil
}

func (nlcoord *NsqLookupCoordinator) handleRemoveFailedISRNodes(failedNodes []string, topicInfo *TopicPartitionMetaInfo) *CoordErr {
	return nlcoord.handleRemoveISRNodes(failedNodes, topicInfo, true)
}

func (nlcoord *NsqLookupCoordinator) handleRemoveCatchupNodes(origTopicInfo *TopicPartitionMetaInfo) {
	topicInfo := origTopicInfo.Copy()
	catchupChanged := len(topicInfo.CatchupList) > 0
	if catchupChanged {
		topicInfo.CatchupList = topicInfo.CatchupList[:0]
		err := nlcoord.leadership.UpdateTopicNodeInfo(topicInfo.Name, topicInfo.Partition,
			&topicInfo.TopicPartitionReplicaInfo, topicInfo.Epoch)
		if err != nil {
			coordLog.Infof("update topic node info failed: %v", err.Error())
			return
		}
		*origTopicInfo = *topicInfo
		nlcoord.notifyTopicMetaInfo(topicInfo)
	}
}

func (nlcoord *NsqLookupCoordinator) handleTopicMigrate(origTopicInfo *TopicPartitionMetaInfo,
	currentNodes map[string]NsqdNodeInfo, currentNodesEpoch int64) {
	if currentNodesEpoch != atomic.LoadInt64(&nlcoord.nodesEpoch) {
		return
	}
	topicInfo := origTopicInfo.Copy()
	if _, ok := currentNodes[topicInfo.Leader]; !ok {
		coordLog.Warningf("topic leader node is down: %v", topicInfo)
		return
	}
	isrChanged := false
	for _, replica := range topicInfo.ISR {
		if _, ok := currentNodes[replica]; !ok {
			coordLog.Warningf("topic %v isr node %v is lost.", topicInfo.GetTopicDesp(), replica)
			isrChanged = true
		}
	}
	if isrChanged {
		// will re-check to handle isr node failure
		return
	}

	catchupChanged := false
	aliveCatchup := 0
	for _, n := range topicInfo.CatchupList {
		if _, ok := currentNodes[n]; ok {
			aliveCatchup++
		} else {
			coordLog.Infof("topic %v catchup node %v is lost.", topicInfo.GetTopicDesp(), n)
		}
	}
	topicNsqdNum := len(topicInfo.ISR) + aliveCatchup
	if topicNsqdNum < topicInfo.Replica {
		for i := topicNsqdNum; i < topicInfo.Replica; i++ {
			// should exclude the current isr and catchup node
			n, err := nlcoord.dpm.allocNodeForTopic(topicInfo, currentNodes)
			if err != nil {
				coordLog.Infof("failed to get a new catchup for topic: %v", topicInfo.GetTopicDesp())
			} else {
				topicInfo.CatchupList = append(topicInfo.CatchupList, n.GetID())
				catchupChanged = true
			}
		}
	}
	if catchupChanged {
		if !nlcoord.dpm.checkTopicNodeConflict(topicInfo) {
			coordLog.Warningf("this topic info update is conflict : %v", topicInfo)
			return
		}
		err := nlcoord.leadership.UpdateTopicNodeInfo(topicInfo.Name, topicInfo.Partition,
			&topicInfo.TopicPartitionReplicaInfo, topicInfo.Epoch)
		if err != nil {
			coordLog.Infof("update topic node info failed: %v", err.Error())
			return
		}
		*origTopicInfo = *topicInfo
		nlcoord.notifyTopicMetaInfo(topicInfo)
	} else {
		nlcoord.notifyCatchupTopicMetaInfo(topicInfo)
	}
}

func (nlcoord *NsqLookupCoordinator) addCatchupNode(origTopicInfo *TopicPartitionMetaInfo, nid string) *CoordErr {
	topicInfo := origTopicInfo.Copy()
	catchupChanged := false
	if FindSlice(topicInfo.CatchupList, nid) == -1 {
		topicInfo.CatchupList = append(topicInfo.CatchupList, nid)
		catchupChanged = true
	}
	if catchupChanged {
		if !nlcoord.dpm.checkTopicNodeConflict(topicInfo) {
			coordLog.Warningf("this topic info update is conflict : %v", topicInfo)
			return ErrTopicNodeConflict
		}
		err := nlcoord.leadership.UpdateTopicNodeInfo(topicInfo.Name, topicInfo.Partition,
			&topicInfo.TopicPartitionReplicaInfo, topicInfo.Epoch)
		if err != nil {
			coordLog.Infof("update topic node info failed: %v", err.Error())
			return &CoordErr{err.Error(), RpcNoErr, CoordCommonErr}
		}
		*origTopicInfo = *topicInfo
		nlcoord.notifyTopicMetaInfo(topicInfo)
	} else {
		nlcoord.notifyCatchupTopicMetaInfo(topicInfo)
	}
	return nil
}

// make sure the previous leader is not holding its leader session.
func (nlcoord *NsqLookupCoordinator) waitOldLeaderRelease(topicInfo *TopicPartitionMetaInfo) error {
	err := RetryWithTimeout(func() error {
		s, err := nlcoord.leadership.GetTopicLeaderSession(topicInfo.Name, topicInfo.Partition)
		if err == nil {
			// the leader data is clean, we treat as no leader
			if s.LeaderNode == nil && s.Session == "" {
				coordLog.Infof("leader session is clean: %v", s)
				return nil
			}
			time.Sleep(time.Millisecond * 100)
			aliveNodes := nlcoord.getCurrentNodes()
			if _, ok := aliveNodes[topicInfo.Leader]; !ok {
				coordLog.Warningf("the leader node %v is lost while wait release for topic: %v", topicInfo.Leader, topicInfo.GetTopicDesp())
				if nlcoord.IsMineLeader() {
					err = nlcoord.leadership.ReleaseTopicLeader(topicInfo.Name, topicInfo.Partition, s)
					if err != nil {
						coordLog.Errorf("release session failed [%s] : %v", topicInfo.GetTopicDesp(), err)
					}
				}
			}
			return ErrWaitingLeaderRelease
		}
		if err != nil {
			coordLog.Infof("get leader session error: %v", err)
		}
		if err == ErrLeaderSessionNotExist {
			return nil
		}
		return err
	})
	return err
}

func (nlcoord *NsqLookupCoordinator) makeNewTopicLeaderAcknowledged(topicInfo *TopicPartitionMetaInfo,
	newLeader string, newestLogID int64, isOldLeaderAlive bool) *CoordErr {
	if topicInfo.Leader == newLeader {
		coordLog.Infof("topic new leader is the same with old: %v", topicInfo)
		return ErrLeaderElectionFail
	}
	newTopicInfo := *topicInfo
	newTopicInfo.ISR = make([]string, 0)
	for _, nid := range topicInfo.ISR {
		if nid == topicInfo.Leader {
			continue
		}
		newTopicInfo.ISR = append(newTopicInfo.ISR, nid)
	}
	newTopicInfo.CatchupList = make([]string, 0)
	for _, nid := range topicInfo.CatchupList {
		if nid == topicInfo.Leader {
			continue
		}
		newTopicInfo.CatchupList = append(newTopicInfo.CatchupList, nid)
	}
	if isOldLeaderAlive {
		newTopicInfo.ISR = append(newTopicInfo.ISR, topicInfo.Leader)
	} else {
		newTopicInfo.CatchupList = append(newTopicInfo.CatchupList, topicInfo.Leader)
	}
	newTopicInfo.Leader = newLeader

	rpcErr := nlcoord.notifyLeaderDisableTopicWrite(&newTopicInfo)
	if rpcErr != nil {
		coordLog.Infof("disable write failed while make new leader: %v", rpcErr)
		return rpcErr
	}
	// check the node for topic again to avoid conflict of the topic node
	if !nlcoord.dpm.checkTopicNodeConflict(topicInfo) {
		coordLog.Warningf("this topic info update is conflict : %v", topicInfo)
		return ErrTopicNodeConflict
	}
	newTopicInfo.EpochForWrite++

	err := nlcoord.leadership.UpdateTopicNodeInfo(topicInfo.Name, topicInfo.Partition,
		&newTopicInfo.TopicPartitionReplicaInfo, topicInfo.Epoch)
	if err != nil {
		coordLog.Infof("update topic node info failed: %v", err)
		return &CoordErr{err.Error(), RpcNoErr, CoordCommonErr}
	}
	coordLog.Infof("make new topic leader info : %v", newTopicInfo)
	*topicInfo = newTopicInfo
	nlcoord.notifyTopicMetaInfo(topicInfo)

	var leaderSession *TopicLeaderSession
	retry := 3
	for retry > 0 {
		retry--
		leaderSession, err = nlcoord.leadership.GetTopicLeaderSession(topicInfo.Name, topicInfo.Partition)
		if err != nil || leaderSession.LeaderNode == nil || leaderSession.Session == "" {
			coordLog.Infof("topic leader session still missing")
			currentNodes := nlcoord.getCurrentNodes()
			_, ok := currentNodes[topicInfo.Leader]
			if !ok {
				coordLog.Warningf("leader is lost while waiting acknowledge")
				return ErrLeaderNodeLost
			}
			nlcoord.notifyISRTopicMetaInfo(topicInfo)
			nlcoord.notifyAcquireTopicLeader(topicInfo)
			time.Sleep(time.Millisecond * 100)
		} else {
			coordLog.Infof("topic leader session found: %v", leaderSession)
			nlcoord.notifyTopicLeaderSession(topicInfo, leaderSession, "")
			go nlcoord.revokeEnableTopicWrite(topicInfo.Name, topicInfo.Partition, true)
			return nil
		}
	}

	return ErrLeaderElectionFail
}

func (nlcoord *NsqLookupCoordinator) checkISRLogConsistence(topicInfo *TopicPartitionMetaInfo) ([]string, *CoordErr) {
	wrongNodes := make([]string, 0)
	leaderLogID, err := nlcoord.getNsqdLastCommitLogID(topicInfo.Leader, topicInfo)
	if err != nil {
		coordLog.Infof("failed to get the leader commit log: %v", err)
		return wrongNodes, err
	}

	var coordErr *CoordErr
	for _, nid := range topicInfo.ISR {
		tmp, err := nlcoord.getNsqdLastCommitLogID(nid, topicInfo)
		if err != nil {
			coordLog.Infof("failed to get log id for node: %v", nid)
			coordErr = err
			continue
		}
		if tmp != leaderLogID {
			coordLog.Infof("isr log mismatched with leader %v, %v on node: %v", leaderLogID, tmp, nid)
			wrongNodes = append(wrongNodes, nid)
			coordErr = ErrTopicCommitLogNotConsistent
		}
	}
	return wrongNodes, coordErr
}

// if any failed to enable topic write , we need start a new join isr session to
// make sure all the isr nodes are ready for write
// should disable write before call
func (nlcoord *NsqLookupCoordinator) revokeEnableTopicWrite(topic string, partition int, isLeadershipWait bool) *CoordErr {
	coordLog.Infof("revoke the topic to enable write: %v-%v", topic, partition)
	topicInfo, err := nlcoord.leadership.GetTopicInfo(topic, partition)
	if err != nil {
		coordLog.Infof("get topic info failed : %v", err.Error())
		return &CoordErr{err.Error(), RpcNoErr, CoordElectionErr}
	}
	if len(topicInfo.ISR) <= topicInfo.Replica/2 {
		coordLog.Infof("ignore since not enough isr : %v", topicInfo)
		go nlcoord.notifyCatchupTopicMetaInfo(topicInfo)
		return ErrTopicISRNotEnough
	}
	leaderSession, err := nlcoord.leadership.GetTopicLeaderSession(topic, partition)
	if err != nil {
		coordLog.Infof("failed to get leader session: %v", err)
		return &CoordErr{err.Error(), RpcNoErr, CoordElectionErr}
	}

	coordLog.Infof("revoke begin check: %v-%v", topic, partition)
	nlcoord.joinStateMutex.Lock()
	state, ok := nlcoord.joinISRState[topicInfo.GetTopicDesp()]
	if !ok {
		state = &JoinISRState{}
		nlcoord.joinISRState[topicInfo.GetTopicDesp()] = state
	}
	nlcoord.joinStateMutex.Unlock()
	start := time.Now()
	state.Lock()
	defer state.Unlock()
	if state.waitingJoin {
		coordLog.Warningf("request join isr while is waiting joining: %v", state)
		if isLeadershipWait {
			// maybe the other partition in the same topic while init, need recheck soon
			coordLog.Warningf("interrupt the current join wait since the leader is waiting confirmation")
			go nlcoord.triggerCheckTopics(topicInfo.Name, -1, time.Second*3)
		} else {
			go nlcoord.triggerCheckTopics(topicInfo.Name, topicInfo.Partition, time.Second*3)
			return ErrWaitingJoinISR
		}
	}
	if time.Since(start) > time.Second*10 {
		return ErrOperationExpired
	}
	if state.doneChan != nil {
		close(state.doneChan)
		state.doneChan = nil
	}
	state.waitingJoin = false
	state.waitingSession = ""

	coordLog.Infof("revoke begin disable write first : %v", topicInfo.GetTopicDesp())
	rpcErr := nlcoord.notifyLeaderDisableTopicWrite(topicInfo)
	if rpcErr != nil {
		coordLog.Infof("try disable write for topic %v failed: %v", topicInfo, rpcErr)
		go nlcoord.triggerCheckTopics(topicInfo.Name, topicInfo.Partition, time.Second)
		return rpcErr
	}

	failedNode, rpcErr := nlcoord.notifyISRDisableTopicWrite(topicInfo)
	if rpcErr != nil {
		coordLog.Infof("try disable isr write for topic %v failed: %v, node: %v", topicInfo, rpcErr, failedNode)
		if rpcErr.IsEqual(ErrMissingTopicCoord) {
			failedNodes := make([]string, 0, 1)
			failedNodes = append(failedNodes, failedNode)
			go nlcoord.handleRemoveFailedISRNodes(failedNodes, topicInfo)
		}
		go nlcoord.triggerCheckTopics(topicInfo.Name, topicInfo.Partition, time.Second*3)
		return rpcErr
	}
	state.isLeadershipWait = isLeadershipWait
	nlcoord.initJoinStateAndWait(topicInfo, leaderSession, state)

	return nil
}

func (nlcoord *NsqLookupCoordinator) initJoinStateAndWait(topicInfo *TopicPartitionMetaInfo, leaderSession *TopicLeaderSession, state *JoinISRState) {
	state.waitingJoin = true
	state.waitingStart = time.Now()
	state.waitingSession = topicInfo.Leader + ","
	for _, s := range topicInfo.ISR {
		state.waitingSession += s + ","
	}
	state.waitingSession += strconv.Itoa(int(topicInfo.Epoch)) + "-" + strconv.Itoa(int(leaderSession.LeaderEpoch)) + ","
	state.waitingSession += state.waitingStart.String()

	state.doneChan = make(chan struct{})
	state.readyNodes = make(map[string]struct{})
	state.readyNodes[topicInfo.Leader] = struct{}{}

	coordLog.Infof("topic %v isr waiting session init : %s", topicInfo.GetTopicDesp(), state.String())
	if len(topicInfo.ISR) <= 1 {
		rpcErr := nlcoord.notifyISRTopicMetaInfo(topicInfo)
		state.waitingJoin = false
		state.waitingSession = ""
		if state.doneChan != nil {
			close(state.doneChan)
			state.doneChan = nil
		}

		if rpcErr != nil {
			coordLog.Warningf("failed to notify ISR for topic: %v, %v ", topicInfo.GetTopicDesp(), rpcErr)
			return
		}

		nlcoord.notifyTopicLeaderSession(topicInfo, leaderSession, state.waitingSession)
		if len(topicInfo.ISR) > topicInfo.Replica/2 {
			rpcErr = nlcoord.notifyEnableTopicWrite(topicInfo)
			if rpcErr != nil {
				coordLog.Warningf("failed to enable write for topic: %v, %v ", topicInfo.GetTopicDesp(), rpcErr)
			}
		} else {
			coordLog.Infof("leaving the topic %v without enable write since not enough replicas.", topicInfo)
		}
		coordLog.Infof("isr join state is ready since only leader in isr")
		return
	} else {
		go nlcoord.waitForFinalSyncedISR(*topicInfo, *leaderSession, state, state.doneChan)
	}
	nlcoord.notifyTopicMetaInfo(topicInfo)
	nlcoord.notifyTopicLeaderSession(topicInfo, leaderSession, state.waitingSession)
}

func (nlcoord *NsqLookupCoordinator) notifySingleNsqdForTopicReload(topicInfo TopicPartitionMetaInfo, nodeID string) *CoordErr {
	// TODO: maybe should disable write if reload node is in isr.
	rpcErr := nlcoord.sendTopicInfoToNsqd(nlcoord.GetLookupLeader().Epoch, nodeID, &topicInfo)
	if rpcErr != nil {
		coordLog.Infof("failed to notify topic %v info to %v : %v", topicInfo.GetTopicDesp(), nodeID, rpcErr)
		if rpcErr.IsEqual(ErrTopicCoordStateInvalid) {
			go nlcoord.revokeEnableTopicWrite(topicInfo.Name, topicInfo.Partition, false)
		}
		return rpcErr
	}
	leaderSession, err := nlcoord.leadership.GetTopicLeaderSession(topicInfo.Name, topicInfo.Partition)
	if err != nil {
		coordLog.Infof("get leader session failed: %v", err)
		return &CoordErr{err.Error(), RpcCommonErr, CoordNetErr}
	}
	return nlcoord.sendTopicLeaderSessionToNsqd(nlcoord.GetLookupLeader().Epoch, nodeID, &topicInfo, leaderSession, "")
}

func (nlcoord *NsqLookupCoordinator) notifyAllNsqdsForTopicReload(topicInfo TopicPartitionMetaInfo) *CoordErr {
	rpcErr := nlcoord.notifyISRTopicMetaInfo(&topicInfo)
	if rpcErr != nil {
		coordLog.Infof("failed to notify topic %v info : %v", topicInfo.GetTopicDesp(), rpcErr)
		if rpcErr.IsEqual(ErrTopicCoordStateInvalid) {
			go nlcoord.revokeEnableTopicWrite(topicInfo.Name, topicInfo.Partition, false)
		}
		return rpcErr
	}
	nlcoord.notifyTopicMetaInfo(&topicInfo)
	leaderSession, err := nlcoord.leadership.GetTopicLeaderSession(topicInfo.Name, topicInfo.Partition)
	if err == nil {
		nlcoord.notifyTopicLeaderSession(&topicInfo, leaderSession, "")
	} else {
		coordLog.Infof("get leader session failed: %v", err)
	}
	return nil
}

func (nlcoord *NsqLookupCoordinator) handleRequestJoinCatchup(topic string, partition int, nid string) *CoordErr {
	var topicInfo *TopicPartitionMetaInfo
	var err error
	topicInfo, err = nlcoord.leadership.GetTopicInfo(topic, partition)
	if err != nil {
		coordLog.Infof("failed to get topic info: %v", err)
		return &CoordErr{err.Error(), RpcCommonErr, CoordCommonErr}
	}
	if FindSlice(topicInfo.ISR, nid) != -1 {
		return &CoordErr{"catchup node should not in the isr", RpcCommonErr, CoordCommonErr}
	}
	if len(topicInfo.ISR)+len(topicInfo.CatchupList) > topicInfo.Replica {
		coordLog.Infof("topic(%v) current isr and catchup list: %v, %v", topicInfo.GetTopicDesp(), topicInfo.ISR, topicInfo.CatchupList)
		return ErrTopicISRCatchupEnough
	}
	coordLog.Infof("node %v try join catchup for topic: %v", nid, topicInfo.GetTopicDesp())

	if FindSlice(topicInfo.CatchupList, nid) == -1 {
		topicInfo.CatchupList = append(topicInfo.CatchupList, nid)
		if !nlcoord.dpm.checkTopicNodeConflict(topicInfo) {
			coordLog.Warningf("this topic info update is conflict : %v", topicInfo)
			return ErrTopicNodeConflict
		}
		err = nlcoord.leadership.UpdateTopicNodeInfo(topic, partition, &topicInfo.TopicPartitionReplicaInfo, topicInfo.Epoch)
		if err != nil {
			coordLog.Infof("failed to update catchup list: %v", err)
			return &CoordErr{err.Error(), RpcCommonErr, CoordNetErr}
		}
	}
	go nlcoord.notifyTopicMetaInfo(topicInfo)
	return nil
}

func (nlcoord *NsqLookupCoordinator) prepareJoinState(topic string, partition int, checkLeaderSession bool) (*TopicPartitionMetaInfo, *TopicLeaderSession, *JoinISRState, *CoordErr) {
	topicInfo, err := nlcoord.leadership.GetTopicInfo(topic, partition)
	if err != nil {
		coordLog.Infof("failed to get topic info: %v", err)
		return nil, nil, nil, &CoordErr{err.Error(), RpcCommonErr, CoordCommonErr}
	}
	leaderSession, err := nlcoord.leadership.GetTopicLeaderSession(topic, partition)
	if err != nil {
		coordLog.Infof("failed to get leader session: %v", err)
		if checkLeaderSession {
			return nil, nil, nil, &CoordErr{err.Error(), RpcCommonErr, CoordElectionTmpErr}
		}
	}

	nlcoord.joinStateMutex.Lock()
	state, ok := nlcoord.joinISRState[topicInfo.GetTopicDesp()]
	if !ok {
		state = &JoinISRState{}
		nlcoord.joinISRState[topicInfo.GetTopicDesp()] = state
	}
	nlcoord.joinStateMutex.Unlock()

	return topicInfo, leaderSession, state, nil
}

func (nlcoord *NsqLookupCoordinator) handleRequestJoinISR(topic string, partition int, nodeID string) *CoordErr {
	// 1. got join isr request, check valid, should be in catchup list.
	// 2. notify the topic leader disable write then disable other isr
	// 3. add the node to ISR and remove from CatchupList.
	// 4. insert wait join session, notify all nodes for the new isr
	// 5. wait on the join session until all the new isr is ready (got the ready notify from isr)
	// 6. timeout or done, clear current join session, (only keep isr that got ready notify, shoud be quorum), enable write
	topicInfo, leaderSession, state, coordErr := nlcoord.prepareJoinState(topic, partition, true)
	if coordErr != nil {
		coordLog.Infof("failed to prepare join state: %v", coordErr)
		return coordErr
	}
	if FindSlice(topicInfo.CatchupList, nodeID) == -1 {
		coordLog.Infof("join isr node is not in catchup list.")
		return ErrJoinISRInvalid
	}
	if len(topicInfo.ISR) > topicInfo.Replica {
		coordLog.Infof("topic(%v) current isr list is enough: %v", topicInfo.GetTopicDesp(), topicInfo.ISR)
		return ErrTopicISRCatchupEnough
	}
	nlcoord.nodesMutex.RLock()
	_, ok := nlcoord.removingNodes[nodeID]
	nlcoord.nodesMutex.RUnlock()
	if ok {
		return ErrClusterNodeRemoving
	}

	coordLog.Infof("node %v request join isr for topic %v", nodeID, topicInfo.GetTopicDesp())

	// we go here to allow the rpc call from client can return ok immediately
	go func() {
		start := time.Now()
		state.Lock()
		defer state.Unlock()
		if state.waitingJoin {
			coordLog.Infof("%v failed request join isr because another is joining. :%v", topicInfo.GetTopicDesp(), state)
			nlcoord.triggerCheckTopicsRandom(topic, partition, time.Millisecond*100)
			return
		}
		if time.Since(start) > time.Second*10 {
			coordLog.Warningf("%v failed since waiting too long for lock", topicInfo.GetTopicDesp())
			nlcoord.triggerCheckTopicsRandom(topic, partition, time.Millisecond*100)
			return
		}
		if state.doneChan != nil {
			close(state.doneChan)
			state.doneChan = nil
		}
		state.waitingJoin = false
		state.waitingSession = ""

		rpcErr := nlcoord.notifyLeaderDisableTopicWrite(topicInfo)
		if rpcErr != nil {
			coordLog.Warningf("try disable write for topic %v failed: %v", topicInfo.GetTopicDesp(), rpcErr)
			go nlcoord.triggerCheckTopicsRandom(topicInfo.Name, topicInfo.Partition, time.Second)
			return
		}
		if _, rpcErr = nlcoord.notifyISRDisableTopicWrite(topicInfo); rpcErr != nil {
			coordLog.Infof("try disable isr write for topic %v failed: %v", topicInfo, rpcErr)
			go nlcoord.triggerCheckTopicsRandom(topicInfo.Name, topicInfo.Partition, time.Second*3)
			return
		}

		state.isLeadershipWait = false

		newCatchupList := make([]string, 0)
		for _, nid := range topicInfo.CatchupList {
			if nid == nodeID {
				continue
			}
			newCatchupList = append(newCatchupList, nid)
		}
		topicInfo.CatchupList = newCatchupList
		topicInfo.ISR = append(topicInfo.ISR, nodeID)

		// new node should disable write also.
		if _, rpcErr = nlcoord.notifyISRDisableTopicWrite(topicInfo); rpcErr != nil {
			coordLog.Infof("try disable isr write for topic %v failed: %v", topicInfo, rpcErr)
			go nlcoord.triggerCheckTopicsRandom(topicInfo.Name, topicInfo.Partition, time.Second*3)
			return
		}
		if !nlcoord.dpm.checkTopicNodeConflict(topicInfo) {
			coordLog.Warningf("this topic info update is conflict : %v", topicInfo)
			return
		}

		topicInfo.EpochForWrite++

		err := nlcoord.leadership.UpdateTopicNodeInfo(topicInfo.Name, topicInfo.Partition,
			&topicInfo.TopicPartitionReplicaInfo, topicInfo.Epoch)
		if err != nil {
			coordLog.Infof("move catchup node to isr failed: %v", err)
			// continue here to allow the wait goroutine to handle the timeout
		}
		nlcoord.initJoinStateAndWait(topicInfo, leaderSession, state)
	}()
	return nil
}

func (nlcoord *NsqLookupCoordinator) handleReadyForISR(topic string, partition int, nodeID string,
	leaderSession TopicLeaderSession, joinISRSession string) *CoordErr {
	topicInfo, err := nlcoord.leadership.GetTopicInfo(topic, partition)
	if err != nil {
		coordLog.Infof("get topic info failed : %v", err.Error())
		return &CoordErr{err.Error(), RpcCommonErr, CoordCommonErr}
	}
	if FindSlice(topicInfo.ISR, nodeID) == -1 {
		coordLog.Infof("got ready for isr but not a isr node: %v, isr is: %v", nodeID, topicInfo.ISR)
		return ErrJoinISRInvalid
	}

	// check for state and should lock for the state to prevent others join isr.
	nlcoord.joinStateMutex.Lock()
	state, ok := nlcoord.joinISRState[topicInfo.GetTopicDesp()]
	nlcoord.joinStateMutex.Unlock()
	if !ok || state == nil {
		coordLog.Warningf("failed join isr because the join state is not set: %v", topicInfo.GetTopicDesp())
		return ErrJoinISRInvalid
	}
	// we go here to allow the rpc call from client can return ok immediately
	go func() {
		nlcoord.nodesMutex.RLock()
		hasRemovingNode := len(nlcoord.removingNodes) > 0
		nlcoord.nodesMutex.RUnlock()
		start := time.Now()
		state.Lock()
		defer state.Unlock()
		if !state.waitingJoin || state.waitingSession != joinISRSession {
			go nlcoord.triggerCheckTopicsRandom(topicInfo.Name, topicInfo.Partition, time.Second)
			coordLog.Infof("%v state mismatch: %s, request join session: %v", topicInfo.GetTopicDesp(), state, joinISRSession)
			return
		}

		if time.Since(start) > time.Second*10 {
			coordLog.Warningf("failed since waiting too long for lock")
			return
		}
		coordLog.Infof("topic %v isr node %v ready for state: %v", topicInfo.GetTopicDesp(), nodeID, joinISRSession)
		state.readyNodes[nodeID] = struct{}{}
		for _, n := range topicInfo.ISR {
			if _, ok := state.readyNodes[n]; !ok {
				coordLog.Infof("node %v still waiting ready", n)
				return
			}
		}
		// check the newest log for isr to make sure consistence.
		wrongISR, rpcErr := nlcoord.checkISRLogConsistence(topicInfo)
		if rpcErr != nil || len(wrongISR) > 0 {
			// isr should be removed since not consistence
			coordLog.Infof("the isr nodes: %v not consistence", wrongISR)
			return
		}
		coordLog.Infof("topic %v isr new state is ready for all: %s", topicInfo.GetTopicDesp(), state)
		if len(topicInfo.ISR) > topicInfo.Replica/2 {
			rpcErr = nlcoord.notifyEnableTopicWrite(topicInfo)
			if rpcErr != nil {
				coordLog.Warningf("failed to enable write for topic: %v, %v ", topicInfo.GetTopicDesp(), rpcErr)
			}
			// recheck after all ok to check if any other partitions need enable
			go nlcoord.triggerCheckTopicsRandom(topicInfo.Name, -1, time.Second*3)
		} else {
			coordLog.Infof("leaving the topic %v without enable write since not enough replicas.", topicInfo)
		}
		state.waitingJoin = false
		state.waitingSession = ""
		if state.doneChan != nil {
			close(state.doneChan)
			state.doneChan = nil
		}

		if len(topicInfo.ISR) >= topicInfo.Replica && len(topicInfo.CatchupList) > 0 &&
			atomic.LoadInt32(&nlcoord.balanceWaiting) == 0 && !hasRemovingNode {
			oldCatchupList := topicInfo.CatchupList
			topicInfo.CatchupList = make([]string, 0)
			coordErr := nlcoord.notifyOldNsqdsForTopicMetaInfo(topicInfo, oldCatchupList)
			if coordErr != nil {
				coordLog.Infof("notify removing catchup failed : %v", coordErr)
				// we can ignore the error since the catchup node maybe down, since
				// we have enough isr it is safe to remove catchup without notify to the node
			}
			coordLog.Infof("removing catchup since the isr is enough: %v", oldCatchupList)
			err := nlcoord.leadership.UpdateTopicNodeInfo(topicInfo.Name, topicInfo.Partition,
				&topicInfo.TopicPartitionReplicaInfo, topicInfo.Epoch)
			if err != nil {
				coordLog.Infof("update catchup info failed while removing catchup: %v", err)
			} else {
				nlcoord.notifyTopicMetaInfo(topicInfo)
			}
		}
	}()
	return nil
}

func (nlcoord *NsqLookupCoordinator) resetJoinISRState(topicInfo TopicPartitionMetaInfo, state *JoinISRState, updateISR bool) *CoordErr {
	state.Lock()
	defer state.Unlock()
	if !state.waitingJoin {
		return nil
	}
	state.waitingJoin = false
	state.waitingSession = ""
	if state.doneChan != nil {
		close(state.doneChan)
		state.doneChan = nil
	}
	coordLog.Infof("topic: %v reset waiting join state: %v", topicInfo.GetTopicDesp(), state)
	ready := 0
	for _, n := range topicInfo.ISR {
		if _, ok := state.readyNodes[n]; ok {
			ready++
		}
	}

	if ready <= topicInfo.Replica/2 {
		coordLog.Infof("no enough ready isr while reset wait join: %v, expect: %v, actual: %v", state.waitingSession, topicInfo.ISR, state.readyNodes)
		// even timeout we can not enable this topic since no enough replicas
		// however, we should clear the join state so that we can try join new isr later
		go nlcoord.triggerCheckTopics(topicInfo.Name, topicInfo.Partition, time.Second)
	} else {
		// some of isr failed to ready for the new isr state, we need rollback the new isr with the
		// isr got ready.
		coordLog.Infof("the join state: expect ready isr : %v, actual ready: %v ", topicInfo.ISR, state.readyNodes)
		if updateISR && ready != len(topicInfo.ISR) {
			oldISR := topicInfo.ISR
			newCatchupList := make(map[string]struct{})
			for _, n := range topicInfo.CatchupList {
				newCatchupList[n] = struct{}{}
			}
			topicInfo.ISR = make([]string, 0, len(state.readyNodes))
			for _, n := range oldISR {
				if _, ok := state.readyNodes[n]; ok {
					topicInfo.ISR = append(topicInfo.ISR, n)
				} else {
					newCatchupList[n] = struct{}{}
				}
			}
			topicInfo.CatchupList = make([]string, 0)
			for n := range newCatchupList {
				topicInfo.CatchupList = append(topicInfo.CatchupList, n)
			}

			if !nlcoord.dpm.checkTopicNodeConflict(&topicInfo) {
				coordLog.Warningf("this topic info update is conflict : %v", topicInfo)
				return ErrTopicNodeConflict
			}

			topicInfo.EpochForWrite++
			err := nlcoord.leadership.UpdateTopicNodeInfo(topicInfo.Name, topicInfo.Partition,
				&topicInfo.TopicPartitionReplicaInfo, topicInfo.Epoch)
			if err != nil {
				coordLog.Infof("update topic info failed: %v", err)
				return &CoordErr{err.Error(), RpcNoErr, CoordElectionErr}
			}
			rpcErr := nlcoord.notifyISRTopicMetaInfo(&topicInfo)
			if rpcErr != nil {
				coordLog.Infof("failed to notify new topic info: %v", topicInfo)
				go nlcoord.triggerCheckTopics(topicInfo.Name, topicInfo.Partition, time.Second)
				return rpcErr
			}
		}

		// check the newest log for isr to make sure consistence.
		wrongISR, rpcErr := nlcoord.checkISRLogConsistence(&topicInfo)
		if rpcErr != nil || len(wrongISR) > 0 {
			// isr should be removed since not consistence
			coordLog.Infof("remove the isr node: %v since not consistence", wrongISR)
			go nlcoord.handleRemoveFailedISRNodes(wrongISR, &topicInfo)
			return rpcErr
		}
		rpcErr = nlcoord.notifyEnableTopicWrite(&topicInfo)
		if rpcErr != nil {
			coordLog.Warningf("failed to enable write :%v, %v", topicInfo.GetTopicDesp(), rpcErr)
			go nlcoord.triggerCheckTopics(topicInfo.Name, topicInfo.Partition, time.Second*3)
		}
	}

	return nil
}

func (nlcoord *NsqLookupCoordinator) waitForFinalSyncedISR(topicInfo TopicPartitionMetaInfo, leaderSession TopicLeaderSession, state *JoinISRState, doneChan chan struct{}) {
	ticker := time.NewTicker(time.Second * 5)
	defer ticker.Stop()
	select {
	case <-ticker.C:
		coordLog.Infof("wait timeout for sync isr.")
	case <-doneChan:
		return
	}

	nlcoord.resetJoinISRState(topicInfo, state, true)
}

func (nlcoord *NsqLookupCoordinator) handleLeaveFromISR(topic string, partition int, leader *TopicLeaderSession, nodeID string) *CoordErr {
	topicInfo, err := nlcoord.leadership.GetTopicInfo(topic, partition)
	if err != nil {
		coordLog.Infof("get topic info failed :%v", err)
		return &CoordErr{err.Error(), RpcCommonErr, CoordCommonErr}
	}
	coordLog.Infof("node %v request leave the isr for topic: %v", nodeID, topicInfo.GetTopicDesp())
	if FindSlice(topicInfo.ISR, nodeID) == -1 {
		return nil
	}
	if len(topicInfo.ISR) <= topicInfo.Replica/2 {
		coordLog.Infof("no enough isr node, graceful leaving should wait.")
		go nlcoord.notifyCatchupTopicMetaInfo(topicInfo)
		go nlcoord.triggerCheckTopics(topicInfo.Name, topicInfo.Partition, time.Second)
		return ErrLeavingISRWait
	}

	if topicInfo.Leader == nodeID {
		coordLog.Infof("the leader node %v will leave the isr, prepare transfer leader for topic: %v", nodeID, topicInfo.GetTopicDesp())
		currentNodes, currentNodesEpoch := nlcoord.getCurrentNodesWithEpoch()

		go nlcoord.handleTopicLeaderElection(topicInfo, currentNodes, currentNodesEpoch, false)
		return nil
	}

	if leader != nil {
		newestLeaderSession, err := nlcoord.leadership.GetTopicLeaderSession(topic, partition)
		if err != nil {
			coordLog.Infof("get leader session failed: %v.", err.Error())
			return &CoordErr{err.Error(), RpcCommonErr, CoordCommonErr}
		}
		if !leader.IsSame(newestLeaderSession) {
			return ErrNotTopicLeader
		}
	}

	failedNodes := make([]string, 0, 1)
	failedNodes = append(failedNodes, nodeID)
	coordErr := nlcoord.handleRemoveFailedISRNodes(failedNodes, topicInfo)
	if coordErr == nil {
		coordLog.Infof("node %v removed by plan from topic isr: %v", nodeID, topicInfo)
	}
	return coordErr
}

func (nlcoord *NsqLookupCoordinator) handleRequestCheckTopicConsistence(topic string, partition int) *CoordErr {
	nlcoord.triggerCheckTopics(topic, partition, 0)
	return nil
}

func (nlcoord *NsqLookupCoordinator) handleRequestNewTopicInfo(topic string, partition int, nodeID string) *CoordErr {
	topicInfo, err := nlcoord.leadership.GetTopicInfo(topic, partition)
	if err != nil {
		coordLog.Infof("get topic info failed : %v", err.Error())
		return nil
	}
	nlcoord.sendTopicInfoToNsqd(nlcoord.GetLookupLeader().Epoch, nodeID, topicInfo)
	leaderSession, err := nlcoord.leadership.GetTopicLeaderSession(topicInfo.Name, topicInfo.Partition)
	if err != nil {
		coordLog.Infof("get leader session failed: %v", err)
		return nil
	}
	nlcoord.sendTopicLeaderSessionToNsqd(nlcoord.GetLookupLeader().Epoch, nodeID, topicInfo, leaderSession, "")
	return nil
}

func (nlcoord *NsqLookupCoordinator) handleRemoveTopicNodeOrMoveLeader(isLeader bool, topic string, partition int,
	nodeID string) *CoordErr {
	topicInfo, err := nlcoord.leadership.GetTopicInfo(topic, partition)
	if err != nil {
		coordLog.Infof("get topic info failed :%v", err)
		return &CoordErr{err.Error(), RpcCommonErr, CoordCommonErr}
	}
	if FindSlice(topicInfo.ISR, nodeID) == -1 {
		return nil
	}
	if len(topicInfo.ISR) <= topicInfo.Replica/2 {
		coordLog.Infof("no enough isr node, graceful leaving should wait.")
		go nlcoord.notifyCatchupTopicMetaInfo(topicInfo)
		go nlcoord.triggerCheckTopics(topicInfo.Name, topicInfo.Partition, time.Second)
		return ErrLeavingISRWait
	}

	if topicInfo.Leader == nodeID {
		if !isLeader {
			coordLog.Infof("try move topic %v not as leader, but we are leader topic %v ", topicInfo.GetTopicDesp(), nodeID)
			return nil
		}
		coordLog.Infof("the leader node %v will leave the isr, prepare transfer leader for topic: %v", nodeID, topicInfo.GetTopicDesp())
		currentNodes, currentNodesEpoch := nlcoord.getCurrentNodesWithEpoch()
		coordErr := nlcoord.handleTopicLeaderElection(topicInfo, currentNodes, currentNodesEpoch, true)
		if coordErr != nil {
			return coordErr
		}
		if len(topicInfo.ISR) <= topicInfo.Replica && FindSlice(topicInfo.ISR, nodeID) != -1 {
			return nil
		}
	}
	coordLog.Infof("the topic %v isr node %v leaving ", topicInfo.GetTopicDesp(), nodeID)
	nodeList := make([]string, 1)
	nodeList[0] = nodeID
	coordErr := nlcoord.handleRemoveISRNodes(nodeList, topicInfo, false)
	if coordErr != nil {
		coordLog.Infof("topic %v remove node %v failed: %v", topicInfo.GetTopicDesp(),
			nodeID, coordErr)
		return coordErr
	} else {
		coordLog.Infof("topic %v remove node %v by plan", topicInfo.GetTopicDesp(),
			nodeID)
	}
	return nil
}
