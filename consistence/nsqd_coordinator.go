package consistence

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/youzan/nsq/internal/protocol"
	"github.com/youzan/nsq/nsqd"
)

const (
	MAX_WRITE_RETRY             = 8
	maxWriteWaitTimeout         = time.Second * 5
	MAX_CATCHUP_RETRY           = 5
	MAX_LOG_PULL                = 10000
	MAX_LOG_PULL_BYTES          = 1024 * 1024 * 32
	MAX_CATCHUP_RUNNING         = 8
	API_BACKUP_DELAYED_QUEUE_DB = "/delayqueue/backupto"
)

var (
	MaxRetryWait                = time.Millisecond * 200
	ForceFixLeaderData          = false
	MaxTopicRetentionSizePerDay = int64(1024 * 1024 * 1024 * 16)
	flushTicker                 = time.Second * 2
	sleepMsBetweenLogSyncPull   = int32(0)
)

func ChangeSleepMsBetweenLogSyncPull(ms int) {
	atomic.StoreInt32(&sleepMsBetweenLogSyncPull, int32(ms))
}

var testCatchupPausedPullLogs int32

func GetTopicPartitionFileName(topic string, partition int, suffix string) string {
	var tmpbuf bytes.Buffer
	tmpbuf.WriteString(topic)
	tmpbuf.WriteString("_")
	tmpbuf.WriteString(strconv.Itoa(partition))
	tmpbuf.WriteString(suffix)
	return tmpbuf.String()
}

func GetTopicPartitionBasePath(rootPath string, topic string, partition int) string {
	return filepath.Join(rootPath, topic)
}

type TopicPartitionID struct {
	TopicName      string
	TopicPartition int
}

func (ncoord *TopicPartitionID) String() string {
	return ncoord.TopicName + "-" + strconv.Itoa(ncoord.TopicPartition)
}

type ILocalLogQueue interface {
	IsDataNeedFix() bool
	SetDataFixState(bool)
	ForceFlush()
	ResetBackendEndNoLock(nsqd.BackendOffset, int64) error
	ResetBackendWithQueueStartNoLock(int64, int64) error
	GetDiskQueueSnapshot(checkCommit bool) *nsqd.DiskQueueSnapshot
	TotalMessageCnt() uint64
	TotalDataSize() int64
	TryFixQueueEnd(nsqd.BackendOffset, int64) error
	CheckDiskQueueReadToEndOK(offset int64, seekCnt int64, endOffset nsqd.BackendOffset) error

	PutRawDataOnReplica(rawData []byte, offset nsqd.BackendOffset, checkSize int64, msgNum int32) (nsqd.BackendQueueEnd, error)
	PutMessageOnReplica(msgs *nsqd.Message, offset nsqd.BackendOffset, checkSize int64) (nsqd.BackendQueueEnd, error)
	TryCleanOldData(retentionSize int64, noRealClean bool, maxCleanOffset nsqd.BackendOffset) (nsqd.BackendQueueEnd, error)
}

func getCommitLogAndLocalLogQ(tcData *coordData, localTopic *nsqd.Topic,
	fromDelayedQueue bool) (ILocalLogQueue, *TopicCommitLogMgr) {
	var localLogQ ILocalLogQueue
	localLogQ = localTopic
	logMgr := tcData.logMgr
	if fromDelayedQueue {
		if tcData.topicInfo.OrderedMulti {
			return nil, nil
		}
		dq := localTopic.GetDelayedQueue()
		if dq == nil {
			return nil, nil
		}
		localLogQ = dq
		logMgr = tcData.delayedLogMgr
	}

	return localLogQ, logMgr
}

func getOrCreateCommitLogAndLocalLogQ(tcData *coordData, localTopic *nsqd.Topic,
	fromDelayedQueue bool) (ILocalLogQueue, *TopicCommitLogMgr, *CoordErr) {
	var localLogQ ILocalLogQueue
	localLogQ = localTopic
	logMgr := tcData.logMgr
	if fromDelayedQueue {
		if tcData.topicInfo.OrderedMulti {
			return nil, nil, ErrLocalDelayedQueueMissing
		}
		dq, _ := localTopic.GetOrCreateDelayedQueueNoLock(tcData.delayedLogMgr)
		if dq == nil {
			return nil, nil, ErrLocalDelayedQueueMissing
		}
		localLogQ = dq
		logMgr = tcData.delayedLogMgr
	}

	return localLogQ, logMgr, nil
}

func maybeInitDelayedQ(tcData *coordData, localTopic *nsqd.Topic) error {
	if atomic.LoadInt32(&nsqd.EnableDelayedQueue) != 1 {
		return nil
	}
	if !tcData.topicInfo.OrderedMulti && tcData.delayedLogMgr != nil {
		localTopic.Lock()
		_, err := localTopic.GetOrCreateDelayedQueueNoLock(tcData.delayedLogMgr)
		localTopic.Unlock()
		return err
	}
	return nil
}

type NsqdCoordinator struct {
	clusterKey             string
	leadership             NSQDLeadership
	lookupMutex            sync.Mutex
	lookupLeader           NsqLookupdNodeInfo
	lookupRemoteCreateFunc nsqlookupRemoteProxyCreateFunc
	lookupRemoteClients    map[string]INsqlookupRemoteProxy
	topicCoords            map[string]map[int]*TopicCoordinator
	coordMutex             sync.RWMutex
	myNode                 NsqdNodeInfo
	rpcClientMutex         sync.Mutex
	nsqdRpcClients         sync.Map
	flushNotifyChan        chan TopicPartitionID
	stopChan               chan struct{}
	dataRootPath           string
	localNsqd              *nsqd.NSQD
	rpcServer              *NsqdCoordRpcServer
	grpcServer             *nsqdCoordGRpcServer
	tryCheckUnsynced       chan bool
	wg                     sync.WaitGroup
	enableBenchCost        bool
	stopping               int32
	catchupRunning         int32
}

func NewNsqdCoordinator(cluster, ip, tcpport, rpcport, httpport, extraID string, rootPath string, nsqd *nsqd.NSQD) *NsqdCoordinator {
	nodeInfo := NsqdNodeInfo{
		NodeIP:   ip,
		TcpPort:  tcpport,
		RpcPort:  rpcport,
		HttpPort: httpport,
	}
	nodeInfo.ID = GenNsqdNodeID(&nodeInfo, extraID)
	nsqdCoord := &NsqdCoordinator{
		clusterKey:             cluster,
		leadership:             nil,
		topicCoords:            make(map[string]map[int]*TopicCoordinator),
		myNode:                 nodeInfo,
		flushNotifyChan:        make(chan TopicPartitionID, 2),
		stopChan:               make(chan struct{}),
		dataRootPath:           rootPath,
		localNsqd:              nsqd,
		tryCheckUnsynced:       make(chan bool, 1),
		lookupRemoteCreateFunc: NewNsqLookupRpcClient,
		lookupRemoteClients:    make(map[string]INsqlookupRemoteProxy),
	}

	if nsqdCoord.leadership != nil {
		nsqdCoord.leadership.InitClusterID(nsqdCoord.clusterKey)
	}
	nsqdCoord.rpcServer = NewNsqdCoordRpcServer(nsqdCoord, rootPath)
	nsqdCoord.grpcServer = NewNsqdCoordGRpcServer(nsqdCoord, rootPath)
	return nsqdCoord
}

func (ncoord *NsqdCoordinator) GetMyID() string {
	return ncoord.myNode.GetID()
}

func (ncoord *NsqdCoordinator) SetLeadershipMgr(l NSQDLeadership) {
	ncoord.leadership = l
	if ncoord.leadership != nil {
		ncoord.leadership.InitClusterID(ncoord.clusterKey)
	}
}

func (ncoord *NsqdCoordinator) getExistRpcClient(nid string) (*NsqdRpcClient, bool) {
	v, ok := ncoord.nsqdRpcClients.Load(nid)
	if !ok {
		return nil, false
	}
	c, ok := v.(*NsqdRpcClient)
	if ok && !c.ShouldRemoved() {
		return c, ok
	}
	return nil, false
}

func (ncoord *NsqdCoordinator) cleanRpcClients(all bool) {
	removed := make([]string, 0)
	ncoord.rpcClientMutex.Lock()
	ncoord.nsqdRpcClients.Range(func(key interface{}, value interface{}) bool {
		if all {
			removed = append(removed, key.(string))
			return true
		}
		c, ok := value.(*NsqdRpcClient)
		if ok && c.ShouldRemoved() {
			removed = append(removed, key.(string))
		}
		return true
	})
	for _, nid := range removed {
		v, ok := ncoord.nsqdRpcClients.Load(nid)
		if ok {
			c := v.(*NsqdRpcClient)
			ncoord.nsqdRpcClients.Delete(nid)
			c.Close()
		}
	}
	ncoord.rpcClientMutex.Unlock()
}

func (ncoord *NsqdCoordinator) acquireRpcClient(nid string) (*NsqdRpcClient, *CoordErr) {
	c, ok := ncoord.getExistRpcClient(nid)
	if ok {
		return c, nil
	}
	ncoord.rpcClientMutex.Lock()
	defer ncoord.rpcClientMutex.Unlock()
	v, ok := ncoord.nsqdRpcClients.Load(nid)
	if ok {
		c, ok = v.(*NsqdRpcClient)
		if ok && c.ShouldRemoved() {
			coordLog.Infof("rpc removing removed client: %v", nid)
			ncoord.nsqdRpcClients.Delete(nid)
			c.Close()
			ok = false
		}
	}
	var err error
	if !ok {
		addr := ExtractRpcAddrFromID(nid)
		c, err = NewNsqdRpcClient(addr, RPC_TIMEOUT_SHORT)
		if err != nil {
			return nil, NewCoordErr(err.Error(), CoordNetErr)
		}
		ncoord.nsqdRpcClients.Store(nid, c)
	}
	return c, nil
}

func (ncoord *NsqdCoordinator) Start() error {
	ncoord.wg.Add(1)
	go ncoord.watchNsqLookupd()

	start := time.Now()
	for {
		ncoord.lookupMutex.Lock()
		l := ncoord.lookupLeader
		ncoord.lookupMutex.Unlock()
		if l.GetID() != "" {
			break
		}
		time.Sleep(time.Second)
		coordLog.Infof("waiting for lookupd ...")
		if time.Now().Sub(start) > time.Second*30 {
			panic("no lookupd found while starting nsqd coordinator")
		}
	}

	err := ncoord.loadLocalTopicData()
	if err != nil {
		close(ncoord.stopChan)
		ncoord.rpcServer.stop()
		return err
	}
	realAddr, err := ncoord.rpcServer.start(ncoord.myNode.NodeIP, ncoord.myNode.RpcPort)
	if err != nil {
		return err
	}
	_, realRpcPort, _ := net.SplitHostPort(realAddr)
	ncoord.myNode.RpcPort = realRpcPort
	//port, _ := strconv.Atoi(realRpcPort)
	//grpcPort := strconv.Itoa(port + 1)
	//err = ncoord.grpcServer.start(ncoord.myNode.NodeIP, grpcPort)
	//if err != nil {
	//	coordLog.Warningf("failed to start nsqd grpc server: %v", err)
	//}

	if ncoord.leadership != nil {
		err := ncoord.leadership.RegisterNsqd(&ncoord.myNode)
		if err != nil {
			coordLog.Warningf("failed to register nsqd coordinator: %v", err)
			return err
		}
	}

	ncoord.wg.Add(1)
	go ncoord.checkForUnsyncedTopics()
	ncoord.wg.Add(1)
	go ncoord.periodFlushCommitLogs()
	ncoord.wg.Add(1)
	go ncoord.checkAndCleanOldData()
	return nil
}

func (ncoord *NsqdCoordinator) Stop() {
	if atomic.LoadInt32(&ncoord.stopping) == 1 {
		return
	}
	// give up the leadership on the topic to
	// allow other isr take over to avoid electing.
	ncoord.prepareLeavingCluster()
	close(ncoord.stopChan)
	ncoord.rpcServer.stop()
	ncoord.rpcServer = nil
	//ncoord.grpcServer.stop()
	//ncoord.grpcServer = nil
	ncoord.cleanRpcClients(true)
	ncoord.wg.Wait()

	ncoord.lookupMutex.Lock()
	for _, c := range ncoord.lookupRemoteClients {
		if c != nil {
			c.Close()
		}
	}
	ncoord.lookupMutex.Unlock()
}

func doLogQClean(tcData *coordData, localTopic *nsqd.Topic, retentionSize int64, fromDelayedQueue bool) {
	localLogQ, logMgr := getCommitLogAndLocalLogQ(tcData, localTopic, fromDelayedQueue)
	if localLogQ == nil || logMgr == nil {
		return
	}
	// first clean we just check the clean offset
	// then we clean the commit log to make sure no access from log index
	// after that, we clean the real queue data
	cleanEndInfo, err := localLogQ.TryCleanOldData(retentionSize, true, 0)
	if err != nil {
		coordLog.Infof("failed to get clean end: %v", err)
	}
	if cleanEndInfo != nil {
		coordLog.Infof("topic %v try clean to : %v", tcData.topicInfo.GetTopicDesp(), cleanEndInfo)
		matchIndex, matchOffset, l, err := logMgr.SearchLogDataByMsgOffset(int64(cleanEndInfo.Offset()))
		if err != nil {
			coordLog.Infof("search log failed: %v", err)
			return
		}
		coordLog.Infof("clean commit log at : %v, %v, %v", matchIndex, matchOffset, l)
		if l.MsgOffset > int64(cleanEndInfo.Offset()) {
			coordLog.Infof("search log clean position exceed the clean end, something wrong")
			return
		}
		maxCleanOffset := cleanEndInfo.Offset()
		if l.MsgOffset < int64(cleanEndInfo.Offset()) {
			// the commit log is in the middle of the batch put,
			// it may happen that the batch across the end of the segment of data file,
			// so we should not clean the segment at the middle of the batch.
			maxCleanOffset = nsqd.BackendOffset(l.MsgOffset)
		}
		oldMatchOffset := matchOffset
		matchOffset, err = logMgr.GetMaxAvailableCleanOffset(matchIndex, matchOffset)
		if err != nil {
			coordLog.Infof("clean commit log no more available clean at (%v-%v): %s", matchIndex, matchOffset, err)
			return
		}
		if matchOffset >= oldMatchOffset {
			// should not exceed last check
			matchOffset = oldMatchOffset
		} else {
			// relocate the offset for the topic data
			l, err = logMgr.GetCommitLogFromOffsetV2(matchIndex, matchOffset)
			if err != nil {
				coordLog.Infof("clean commit log failed at (%v-%v): %s", matchIndex, matchOffset, err)
				return
			}
			if l.MsgOffset < int64(maxCleanOffset) {
				maxCleanOffset = nsqd.BackendOffset(l.MsgOffset)
			}
		}
		coordLog.Infof("clean commit log relocated at : %v, %v, %v, %v", matchIndex, matchOffset, maxCleanOffset, l)
		err = logMgr.CleanOldData(matchIndex, matchOffset)
		if err != nil {
			coordLog.Infof("clean commit log err : %v", err)
		} else {
			// TODO: it may happen the disk queue has more old data than commit log,
			// and it may cause the channel cursor old than commit log start but not old than disk queue
			// start. However the slave only sync from commit log start, this may cause
			// the channel cursor can not be set on slave since some old disk queue data not synced.
			retentionSize = 1024 * 512
			_, err := localLogQ.TryCleanOldData(retentionSize, false, maxCleanOffset)
			if err != nil {
				coordLog.Infof("failed to clean disk queue: %v", err)
			}
		}
	}
}

func (ncoord *NsqdCoordinator) GreedyCleanTopicOldData(localTopic *nsqd.Topic) error {
	topicName := localTopic.GetTopicName()
	partition := localTopic.GetTopicPart()
	topicMetaInfoForPartition, err := ncoord.GetTopicInfo(topicName, partition)
	if err != nil {
		coordLog.Errorf("Cleaning: failed to get topic info, %v-%v: %v", topicName, partition, err)
		return nil
	}

	topicMetaInfo := topicMetaInfoForPartition.TopicMetaInfo

	retentionDay := topicMetaInfo.RetentionDay
	if retentionDay == 0 {
		retentionDay = int32(nsqd.DEFAULT_RETENTION_DAYS)
	}
	retentionSize := (MaxTopicRetentionSizePerDay / 16) * int64(retentionDay)

	basePath := GetTopicPartitionBasePath(ncoord.dataRootPath, topicName, partition)
	topicLeaderSession, err := ncoord.leadership.GetTopicLeaderSession(topicName, partition)
	if err != nil {
		coordLog.Errorf("Cleaning: failed to get topic leader info:%v-%v, err:%v", topicName, partition, err)
		return nil
	}

	tc, localTopic, loadErr := ncoord.initLocalTopicCoord(topicMetaInfoForPartition, topicLeaderSession, basePath, true, false)
	if loadErr != nil {
		coordLog.Errorf("Cleaning: topic %v coord init error: %v", topicMetaInfoForPartition.GetTopicDesp(), loadErr.Error())
		return nil
	}
	doLogQClean(tc.GetData(), localTopic, retentionSize, false)
	doLogQClean(tc.GetData(), localTopic, retentionSize, true)
	return nil
}

func (ncoord *NsqdCoordinator) getAllCoords(tmpCoords map[string]map[int]*TopicCoordinator) {
	ncoord.coordMutex.RLock()
	for name, tc := range ncoord.topicCoords {
		coords, ok := tmpCoords[name]
		if !ok {
			coords = make(map[int]*TopicCoordinator)
			tmpCoords[name] = coords
		}
		for pid, tpc := range tc {
			coords[pid] = tpc
		}
	}
	ncoord.coordMutex.RUnlock()
}

func (ncoord *NsqdCoordinator) checkAndCleanOldData() {
	defer ncoord.wg.Done()
	ticker := time.NewTicker(time.Minute * 10)
	defer ticker.Stop()

	for topicName, data := range ncoord.localNsqd.GetLocalCacheTopicMapRef() {
		for partition := range data {
			topicMetaInfoForPartition, err := ncoord.GetTopicInfo(topicName, partition)
			if err != nil {
				coordLog.Errorf("Cleaning: failed to get topic info, %v-%v: %v", topicName, partition, err)
				continue
			}

			basePath := GetTopicPartitionBasePath(ncoord.dataRootPath, topicName, partition)
			topicLeaderSession, err := ncoord.leadership.GetTopicLeaderSession(topicName, partition)
			if err != nil {
				coordLog.Errorf("Cleaning: failed to get topic leader info:%v-%v, err:%v", topicName, partition, err)
				continue
			}

			tc, _, loadErr := ncoord.initLocalTopicCoord(topicMetaInfoForPartition, topicLeaderSession, basePath, true, false)
			if loadErr != nil {
				coordLog.Errorf("Cleaning: topic %v coord init error: %v", topicMetaInfoForPartition.GetTopicDesp(), loadErr.Error())
				continue
			}
			coordLog.Debugf("Load local cache topic from cache file, topic: %v", tc.GetData().topicInfo.GetTopicDesp())
		}
	}

	doCheckAndCleanOld := func(checkRetentionDay bool) {
		tmpCoords := make(map[string]map[int]*TopicCoordinator)
		ncoord.getAllCoords(tmpCoords)
		for _, tc := range tmpCoords {
			for _, tpc := range tc {
				tcData := tpc.GetData()
				localTopic, err := ncoord.localNsqd.GetExistingTopic(tcData.topicInfo.Name, tcData.topicInfo.Partition)
				if err != nil {
					coordLog.Infof("failed to get local topic: %v", tcData.topicInfo.GetTopicDesp())
					continue
				}
				retentionDay := tcData.topicInfo.RetentionDay
				if retentionDay == 0 {
					retentionDay = int32(nsqd.DEFAULT_RETENTION_DAYS)
				}
				retentionSize := MaxTopicRetentionSizePerDay * int64(retentionDay)
				// TODO: check if disk almost full (over 80%), then we do a more greed clean
				if checkRetentionDay {
					retentionSize = 0
				}
				coordLog.Debugf("start clean local topic: %v", tcData.topicInfo.GetTopicDesp())
				doLogQClean(tcData, localTopic, retentionSize, false)
				doLogQClean(tcData, localTopic, retentionSize, true)
			}
		}
	}
	for {
		select {
		case <-ticker.C:
			now := time.Now()
			if now.Hour() > 2 && now.Hour() < 6 {
				coordLog.Infof("check and clean at time: %v", now)
				doCheckAndCleanOld(true)
			}
			doCheckAndCleanOld(false)
		case <-ncoord.stopChan:
			return
		}
	}
}

// since we only commit log in buffer, we need flush period,
// also we will flush while the leader switched.
func (ncoord *NsqdCoordinator) periodFlushCommitLogs() {
	const FLUSH_DISTANCE = 4
	tmpCoords := make(map[string]map[int]*TopicCoordinator)
	syncCounter := 0
	defer ncoord.wg.Done()
	flushTicker := time.NewTicker(flushTicker)
	defer flushTicker.Stop()
	doFlush := func() {
		syncCounter++
		ncoord.getAllCoords(tmpCoords)
		flushAll := syncCounter%30 == 0
		matchCnt := syncCounter % FLUSH_DISTANCE
		// to reduce the io, we just choose parts of topic to flush for each time
		// and sync channels for topic if needed.
		for _, tc := range tmpCoords {
			for pid, tpc := range tc {
				tcData := tpc.GetData()
				if tcData.GetLeader() == ncoord.myNode.GetID() || flushAll {
					localTopic, err := ncoord.localNsqd.GetExistingTopic(tcData.topicInfo.Name, tcData.topicInfo.Partition)
					if err != nil {
						coordLog.Infof("no local topic: %v", tcData.topicInfo.GetTopicDesp())
					} else {
						if flushAll || ((pid+1)%FLUSH_DISTANCE == matchCnt) {
							localTopic.ForceFlush()
							tcData.flushCommitLogs()
						}
					}
				}
				if !tpc.IsExiting() && tcData.GetLeader() == ncoord.myNode.GetID() {
					syncChList := !tpc.IsWriteDisabled() && flushAll
					if ((pid+1)%FLUSH_DISTANCE) == matchCnt || syncChList {
						ncoord.trySyncTopicChannels(tcData, syncChList, false)
					}
				}
				delete(tc, pid)
			}
		}
	}
	for {
		select {
		case <-flushTicker.C:
			doFlush()
		case <-ncoord.stopChan:
			time.Sleep(time.Second)
			doFlush()
			return
		}
	}
}

// get the lookup connection from pool for rpc communicate with nsqlookupd
func (ncoord *NsqdCoordinator) getLookupRemoteProxy() (INsqlookupRemoteProxy, *CoordErr) {
	ncoord.lookupMutex.Lock()
	l := ncoord.lookupLeader
	addr := net.JoinHostPort(l.NodeIP, l.RpcPort)
	c, ok := ncoord.lookupRemoteClients[addr]
	ncoord.lookupMutex.Unlock()
	if l.NodeIP == "" {
		return nil, NewCoordErr("missing lookup leader", CoordNetErr)
	}
	if ok && c != nil {
		return c, nil
	}
	c, err := ncoord.lookupRemoteCreateFunc(addr, RPC_TIMEOUT_FOR_LOOKUP)
	if err == nil {
		ncoord.lookupMutex.Lock()
		ncoord.lookupRemoteClients[addr] = c
		if len(ncoord.lookupRemoteClients) > 3 {
			for k := range ncoord.lookupRemoteClients {
				if k == addr {
					continue
				}
				delete(ncoord.lookupRemoteClients, k)
			}
		}
		ncoord.lookupMutex.Unlock()
		return c, nil
	}
	coordLog.Infof("get lookup remote %v failed: %v", l, err)
	return c, NewCoordErr(err.Error(), CoordNetErr)
}

func (ncoord *NsqdCoordinator) GetCurrentLookupd() NsqLookupdNodeInfo {
	ncoord.lookupMutex.Lock()
	defer ncoord.lookupMutex.Unlock()
	return ncoord.lookupLeader
}

func (ncoord *NsqdCoordinator) GetAllLookupdNodes() ([]NsqLookupdNodeInfo, error) {
	return ncoord.leadership.GetAllLookupdNodes()
}

func (ncoord *NsqdCoordinator) watchNsqLookupd() {
	// watch the leader of nsqlookupd, always check the leader before response
	// to the nsqlookup admin operation.
	nsqlookupLeaderChan := make(chan *NsqLookupdNodeInfo, 1)
	if ncoord.leadership != nil {
		go ncoord.leadership.WatchLookupdLeader(nsqlookupLeaderChan, ncoord.stopChan)
	}
	defer ncoord.wg.Done()
	for {
		select {
		case n, ok := <-nsqlookupLeaderChan:
			if !ok {
				return
			}
			ncoord.lookupMutex.Lock()
			if n.GetID() != ncoord.lookupLeader.GetID() {
				coordLog.Infof("on nsqd: lookup leader changed from %v to %v", ncoord.lookupLeader, *n)
				ncoord.lookupLeader = *n
			}
			ncoord.lookupMutex.Unlock()
		}
	}
}

// check the magic code to avoid we reuse some deleted topic after re-created it.
// should not hold any coordinator lock outside, since we may wait local topic pub loop quit.
// pub loop may be blocked by coordinator lock.
func (ncoord *NsqdCoordinator) checkLocalTopicMagicCode(topicInfo *TopicPartitionMetaInfo, tryFix bool) error {
	removedPath, err := ncoord.localNsqd.CheckMagicCode(topicInfo.Name, topicInfo.Partition, topicInfo.MagicCode, topicInfo.Ext, tryFix)
	if err != nil {
		coordLog.Infof("check magic code error: %v", err)
		return err
	}
	if removedPath != "" {
		basepath := GetTopicPartitionBasePath(ncoord.dataRootPath, topicInfo.Name, topicInfo.Partition)
		tmpLogMgr, err := InitTopicCommitLogMgr(topicInfo.Name, topicInfo.Partition, basepath, 1)
		if err != nil {
			coordLog.Warningf("topic %v failed to init tmp log manager: %v", topicInfo.GetTopicDesp(), err)
		} else {
			tmpLogMgr.MoveTo(removedPath)
			tmpLogMgr.Delete()
		}
	}
	return nil
}

// do not hold any coordinator lock outside, since we need delete local topic which will wait pub loop quit.
// pub loop may hold the coordinator lock.
func (ncoord *NsqdCoordinator) forceCleanTopicData(topicName string, partition int) *CoordErr {
	// check if any data on local and try remove
	ncoord.forceCleanTopicCommitLogs(topicName, partition)
	localErr := ncoord.localNsqd.ForceDeleteTopicData(topicName, partition)
	if localErr != nil {
		if !os.IsNotExist(localErr) {
			coordLog.Infof("delete topic %v-%v local data failed : %v", topicName, partition, localErr)
			return &CoordErr{localErr.Error(), RpcCommonErr, CoordLocalErr}
		}
	}
	return nil
}

func (ncoord *NsqdCoordinator) forceCleanTopicCommitLogs(topicName string, partition int) {
	basepath := GetTopicPartitionBasePath(ncoord.dataRootPath, topicName, partition)
	cleanTopicCommitLogFiles(topicName, partition, basepath)
	coordLog.Infof("topic %v is cleaning topic data: %v", topicName, basepath)
	dqPath := path.Join(basepath, "delayed_queue")
	if _, err := os.Stat(dqPath); err == nil {
		cleanTopicCommitLogFiles(topicName, partition, dqPath)
	}
}

func (ncoord *NsqdCoordinator) initLocalTopicCoord(topicInfo *TopicPartitionMetaInfo,
	topicLeaderSession *TopicLeaderSession,
	basepath string, updateCoordInfo bool, initDisable bool) (*TopicCoordinator, *nsqd.Topic, error) {
	ncoord.coordMutex.Lock()
	defer ncoord.coordMutex.Unlock()
	coords, ok := ncoord.topicCoords[topicInfo.Name]
	if !ok {
		coords = make(map[int]*TopicCoordinator)
		ncoord.topicCoords[topicInfo.Name] = coords
	}
	tc, ok := coords[topicInfo.Partition]
	if ok {
		// already loaded? concurrent?
		return tc, nil, ErrAlreadyExist
	}
	var err error
	topicName := topicInfo.Name
	partition := topicInfo.Partition
	tc, err = NewTopicCoordinatorWithFixMode(topicInfo.Name, topicInfo.Partition, basepath,
		topicInfo.SyncEvery, topicInfo.OrderedMulti, ForceFixLeaderData)
	if err != nil {
		coordLog.Infof("failed to get topic coordinator:%v-%v, err:%v", topicName, partition, err)
		if err == errCommitLogInitIDInvalid {
			// the commit log is old, we should clean it
			ncoord.forceCleanTopicCommitLogs(topicInfo.Name, topicInfo.Partition)
		}
		return nil, nil, err
	}

	if updateCoordInfo {
		tc.topicInfo = *topicInfo
	}
	if initDisable {
		tc.DisableWrite(true)
	}

	tc.writeHold.Lock()
	var coordErr *CoordErr
	topic, coordErr := ncoord.updateLocalTopic(topicInfo, tc.GetData())
	tc.writeHold.Unlock()
	if coordErr != nil {
		coordLog.Errorf("failed to update local topic %v: %v", topicInfo.GetTopicDesp(), coordErr)
		tc.DeleteWithLock(false)
		return nil, nil, coordErr.ToErrorType()
	}
	if topicLeaderSession != nil {
		tc.topicLeaderSession = *topicLeaderSession
	}
	coords[topicInfo.Partition] = tc
	coordLog.Infof("A new topic coord init on the node: %v", topicInfo.GetTopicDesp())
	return tc, topic, nil
}

// check cluster meta info and load topic from disk
func (ncoord *NsqdCoordinator) loadLocalTopicData() error {
	if ncoord.localNsqd == nil {
		return nil
	}
	topicParts := ncoord.localNsqd.GetTopicMapCopy()
	// first we check if all topic info is missing, this can avoid clear all topics if etcd is cleared by accident.
	etcdOK := false
	for _, topic := range topicParts {
		partition := topic.GetTopicPart()
		_, err := ncoord.leadership.GetTopicInfo(topic.GetTopicName(), partition)
		if err == nil {
			// found one success topic means etcd is ok
			etcdOK = true
			break
		}
	}
	if !etcdOK {
		if len(topicParts) > 0 {
			coordLog.Infof("local topic number %v, but no etcd info found", len(topicParts))
		}
		return nil
	}

	totalCnt := len(topicParts)
	loadingCnt := 0
	for _, topic := range topicParts {
		loadingCnt++
		topicName := topic.GetTopicName()
		partition := topic.GetTopicPart()
		if tc, err := ncoord.getTopicCoordData(topicName, partition); err == nil && tc != nil {
			// already loaded
			if tc.topicLeaderSession.LeaderNode == nil || tc.topicLeaderSession.Session == "" {
				if tc.topicInfo.Leader == ncoord.myNode.GetID() {
					err := ncoord.acquireTopicLeader(&tc.topicInfo)
					if err != nil {
						coordLog.Infof("failed to acquire leader : %v", err)
					}
				}
				if FindSlice(tc.topicInfo.ISR, ncoord.myNode.GetID()) != -1 {
					topicLeaderSession, err := ncoord.leadership.GetTopicLeaderSession(topicName, partition)
					if err != nil {
						coordLog.Infof("failed to get topic leader info:%v-%v, err:%v", topicName, partition, err)
					} else {
						coord, err := ncoord.getTopicCoord(topicName, partition)
						if err == nil {
							if topicLeaderSession.LeaderEpoch >= tc.topicLeaderSession.LeaderEpoch {
								coord.dataMutex.Lock()
								newCoordData := coord.coordData.GetCopy()
								newCoordData.topicLeaderSession = *topicLeaderSession
								coord.coordData = newCoordData
								coord.dataMutex.Unlock()
							}
						}
					}
				}
			}
			continue
		}
		coordLog.Infof("loading topic: %v-%v, loaded: %v, total: %v", topicName, partition, loadingCnt, totalCnt)
		if topicName == "" {
			continue
		}
		topicInfo, commonErr := ncoord.leadership.GetTopicInfo(topicName, partition)
		if commonErr != nil {
			coordLog.Infof("failed to get topic info:%v-%v, err:%v", topicName, partition, commonErr)
			if commonErr == ErrKeyNotFound {
				rd, err := ncoord.leadership.IsTopicRealDeleted(topicName)
				if err != nil {
					coordLog.Infof("failed to check the deleted state of topic :%v-%v, err:%v", topicName, partition, err)
					continue
				}
				if rd {
					ncoord.forceCleanTopicData(topicName, partition)
				} else {
					coordLog.Infof("the deleted state of topic not found, can not delete:%v-%v", topicName, partition)
				}
			}
			continue
		}

		if topicInfo.Replica < 1 {
			coordLog.Errorf("topic replica is invalid:%v-%v, err:%v", topicName, partition, topicInfo.Replica)
			continue
		}
		checkErr := ncoord.checkLocalTopicMagicCode(topicInfo, topicInfo.Leader != ncoord.myNode.GetID())
		if checkErr != nil {
			coordLog.Errorf("failed to check topic :%v-%v, err:%v", topicName, partition, checkErr)
			go ncoord.requestLeaveFromISR(topicInfo.Name, topicInfo.Partition)
			continue
		}

		// while load topic from cluster, we need check the current state of replica.
		// If we are one of ISR, that means we still have the newest data for this topic, we just check
		// the data with leader.
		// Otherwise we need catchup from leader.
		// In some case, we still keep leader (maybe only one replica), We need check local data and try to get the leader session again.
		// If we are removed from both ISR and catchups, we can safely remove local topic data
		shouldLoad := FindSlice(topicInfo.ISR, ncoord.myNode.GetID()) != -1 || FindSlice(topicInfo.CatchupList, ncoord.myNode.GetID()) != -1
		// here we will check the last commit log data logid is equal with the disk queue message
		// this can avoid data corrupt, if not equal we need rollback and find backward for the right data.
		// and check the first log commit log is valid on the disk queue, so that we can fix the wrong start of the commit log
		forceFix := false
		canAutoForceFix := false
		if ncoord.GetMyID() != topicInfo.Leader ||
			(topicInfo.Replica <= 1 && len(topicInfo.ISR) <= 1) {
			// we can always fix non-leader since we will catchup new from leader
			// if the replica is 1, we can fix since no other data
			canAutoForceFix = true
		}
		if ForceFixLeaderData || canAutoForceFix {
			forceFix = true
		}
		if shouldLoad {
			basepath := GetTopicPartitionBasePath(ncoord.dataRootPath, topicInfo.Name, topicInfo.Partition)
			topicLeaderSession, err := ncoord.leadership.GetTopicLeaderSession(topicName, partition)
			if err != nil {
				coordLog.Infof("failed to get topic leader info:%v-%v, err:%v", topicName, partition, err)
			}
			_, _, loadErr := ncoord.initLocalTopicCoord(topicInfo, topicLeaderSession, basepath, true, false)
			if loadErr != nil {
				coordLog.Infof("topic %v coord init error: %v", topicInfo.GetTopicDesp(), loadErr.Error())
				continue
			}
		} else {
			coordLog.Infof("topic %v starting as not relevant", topicInfo.GetTopicDesp())
			if len(topicInfo.ISR) >= topicInfo.Replica {
				coordLog.Infof("no need load the local topic since the replica is enough: %v", topicInfo.GetTopicDesp())
				ncoord.forceCleanTopicData(topicInfo.Name, topicInfo.Partition)
			} else if len(topicInfo.ISR)+len(topicInfo.CatchupList) < topicInfo.Replica {
				go ncoord.requestJoinCatchup(topicName, partition)
			} else {
				// never hold any coordinator lock while close or delete local topic
				ncoord.localNsqd.CloseExistingTopic(topicName, partition)
			}
			continue
		}

		tc, err := ncoord.getTopicCoord(topicInfo.Name, topicInfo.Partition)
		if err != nil {
			coordLog.Errorf("no coordinator for topic: %v", topicInfo.GetTopicDesp())
			continue
		}
		dyConf := &nsqd.TopicDynamicConf{SyncEvery: int64(topicInfo.SyncEvery),
			AutoCommit:   0,
			RetentionDay: topicInfo.RetentionDay,
			OrderedMulti: topicInfo.OrderedMulti,
			MultiPart:    topicInfo.MultiPart,
			Ext:          topicInfo.Ext,
		}
		tc.GetData().updateBufferSize(int(dyConf.SyncEvery - 1))
		maybeInitDelayedQ(tc.GetData(), topic)
		topic.SetDynamicInfo(*dyConf, tc.GetData().logMgr)

		// do we need hold lock for local topic? since it will not run concurrently with catchup and will run in single goroutine
		localErr := checkAndFixLocalLogQueueData(tc.GetData(), topic, tc.GetData().logMgr, forceFix)
		if localErr != nil {
			coordLog.Errorf("check local topic %v data need to be fixed:%v", topicInfo.GetTopicDesp(), localErr)
			topic.SetDataFixState(true)
			go ncoord.requestLeaveFromISR(topicInfo.Name, topicInfo.Partition)
		} else if !topicInfo.OrderedMulti {
			delayQ := topic.GetDelayedQueue()
			localErr = checkAndFixLocalLogQueueData(tc.GetData(), delayQ, tc.GetData().delayedLogMgr, forceFix)
			if localErr != nil {
				coordLog.Errorf("check local topic %v delayed queue data need to be fixed:%v", topicInfo.GetTopicDesp(), localErr)
				delayQ.SetDataFixState(true)
				go ncoord.requestLeaveFromISR(topicInfo.Name, topicInfo.Partition)
			}
			if delayQ != nil {
				localErr = delayQ.CheckConsistence()
				if localErr != nil {
					coordLog.Errorf("check local topic %v delayed queue data need to be fixed:%v", topicInfo.GetTopicDesp(), localErr)
					delayQ.SetDataFixState(true)
					go ncoord.requestLeaveFromISR(topicInfo.Name, topicInfo.Partition)
				}
			}
		}
		if topicInfo.Leader == ncoord.myNode.GetID() {
			coordLog.Infof("topic %v starting as leader.", topicInfo.GetTopicDesp())
			tc.DisableWrite(true)
			err := ncoord.acquireTopicLeader(topicInfo)
			if err != nil {
				coordLog.Infof("failed to acquire leader while start as leader: %v", err)
			}
		}
		if FindSlice(topicInfo.ISR, ncoord.myNode.GetID()) != -1 {
			// restart node should rejoin the isr
			coordLog.Infof("topic starting as isr .")
			if len(topicInfo.ISR) > 1 && topicInfo.Leader != ncoord.myNode.GetID() {
				go ncoord.requestLeaveFromISR(topicInfo.Name, topicInfo.Partition)
			}
		} else if FindSlice(topicInfo.CatchupList, ncoord.myNode.GetID()) != -1 {
			coordLog.Infof("topic %v starting as catchup", topicInfo.GetTopicDesp())
			go ncoord.catchupFromLeader(*topicInfo, "")
		}
	}
	return nil
}

func checkAndFixLocalLogQueueEnd(tc *coordData,
	localLogQ ILocalLogQueue, logMgr *TopicCommitLogMgr, tryFixEnd bool, forceFix bool) error {
	if logMgr == nil || localLogQ == nil {
		return nil
	}
	tname := tc.topicInfo.GetTopicDesp()
	logIndex, logOffset, logData, err := logMgr.GetLastCommitLogOffsetV2()
	if err != nil {
		if err != ErrCommitLogEOF {
			coordLog.Errorf("commit log is corrupted: %v", err)
			return err
		}

		coordLog.Infof("no commit last log data : %v", err)
		return nil
	}

	commitEndOffset := nsqd.BackendOffset(logData.MsgOffset + int64(logData.MsgSize))
	commitEndCnt := logData.MsgCnt + int64(logData.MsgNum) - 1
	if localLogQ.TotalDataSize() == int64(commitEndOffset) && localLogQ.TotalMessageCnt() == uint64(commitEndCnt) {
		// need read one data to ensure the data can be read ok (In some case, such as disk full, it may have zero data padding unexpected)
		if localErr := localLogQ.CheckDiskQueueReadToEndOK(logData.MsgOffset, logData.dqSeekCnt(), commitEndOffset); localErr == nil {
			return nil
		} else {
			coordLog.Errorf("topic %v need fix, read end failed: %v , %v:%v", tname, localErr, logIndex, logOffset)
		}
	}
	coordLog.Infof("current topic %v log: %v:%v, %v, diskqueue end: %v",
		tname, logIndex, logOffset, logData, localLogQ.TotalDataSize())

	if !forceFix && !tryFixEnd {
		return nil
	}
	localErr := localLogQ.ResetBackendEndNoLock(commitEndOffset, commitEndCnt)
	if localErr == nil {
		// need read one data to ensure the data can be read ok (In some case, such as disk full, it may have zero data padding unexpected)
		localErr = localLogQ.CheckDiskQueueReadToEndOK(logData.MsgOffset, logData.dqSeekCnt(), commitEndOffset)
		if localErr == nil {
			return nil
		} else {
			coordLog.Errorf("topic %v need fix, read end failed: %v , %v:%v", tname, localErr, logIndex, logOffset)
		}
	}
	coordLog.Errorf("topic %v reset local queue backend failed: %v", tname, localErr)
	if !forceFix {
		return localErr
	}
	realEnd := localLogQ.TotalDataSize()
	if realEnd == 0 {
		localLogQ.TryFixQueueEnd(commitEndOffset, commitEndCnt)
		realEnd = localLogQ.TotalDataSize()
	}
	cntNum, _ := logMgr.ConvertToCountIndex(logIndex, logOffset)
	for {
		cntNum--
		logIndex, logOffset, localErr = logMgr.ConvertToOffsetIndex(cntNum)
		if localErr != nil {
			coordLog.Errorf("topic %v try fix failed: %v , %v", tname, localErr, cntNum)
			break
		}
		logData, localErr = logMgr.GetCommitLogFromOffsetV2(logIndex, logOffset)
		if localErr != nil {
			coordLog.Errorf("topic %v try fix failed: %v , %v:%v", tname, localErr, logIndex, logOffset)
			break
		}
		endOffset := nsqd.BackendOffset(logData.MsgOffset + int64(logData.MsgSize))
		if int64(endOffset) <= realEnd {
			localErr = localLogQ.ResetBackendEndNoLock(endOffset,
				logData.MsgCnt+int64(logData.MsgNum)-1)
			if localErr != nil {
				coordLog.Infof("topic %v reset local queue failed: %v at %v:%v", tname, localErr, logIndex, logOffset)
			} else {
				coordLog.Warningf("topic %v fix local queue to: %v, %v, commit log: %v:%v:%v", tname,
					logData, realEnd, logIndex, logOffset, cntNum)
				_, localErr = logMgr.TruncateToOffsetV2(logIndex, logOffset+int64(GetLogDataSize()))
				if localErr != nil {
					coordLog.Errorf("topic %v reset local queue failed: %v, at %v:%v", tname,
						localErr, logIndex, logOffset)
				} else {
					// need read one data to ensure the data can be read ok (In some case, such as disk full, it may have zero data padding unexpected)
					localErr = localLogQ.CheckDiskQueueReadToEndOK(logData.MsgOffset, logData.dqSeekCnt(), endOffset)
					if localErr == nil {
						return nil
					} else {
						coordLog.Errorf("topic %v need fix, read end failed: %v , %v:%v", tname, localErr, logIndex, logOffset)
					}
				}
			}
		} else {
			localErr = localLogQ.TryFixQueueEnd(endOffset,
				logData.MsgCnt+int64(logData.MsgNum)-1)
			if localErr != nil {
				continue
			}
			localErr = localLogQ.CheckDiskQueueReadToEndOK(logData.MsgOffset, logData.dqSeekCnt(), endOffset)
			if localErr != nil {
				coordLog.Errorf("topic %v try fix end, read failed: %v , %v:%v", tname, localErr, logIndex, logOffset)
				continue
			}
			return nil
		}
	}
	return localErr
}

func checkAndFixLocalLogQueueData(tc *coordData,
	localLogQ ILocalLogQueue, logMgr *TopicCommitLogMgr, forceFix bool) error {
	if logMgr == nil || localLogQ == nil {
		return nil
	}
	logStart, log, err := logMgr.GetLogStartInfo()
	if err != nil {
		if err == ErrCommitLogEOF {
			return nil
		}
		coordLog.Warningf("get log start failed: %v", err)
		return err
	}
	endFixErr := checkAndFixLocalLogQueueEnd(tc, localLogQ, logMgr, true, forceFix)

	snap := localLogQ.GetDiskQueueSnapshot(false)
	for {
		seekCnt := log.dqSeekCnt()
		err = snap.SeekTo(nsqd.BackendOffset(log.MsgOffset), seekCnt)
		if err == nil {
			break
		}
		coordLog.Warningf("topic %v log start %v should be fixed: %v, %v", tc.topicInfo.GetTopicDesp(), logStart, log, err)
		// try fix start
		if err == nsqd.ErrReadQueueAlreadyCleaned {
			start := snap.GetQueueReadStart()
			logStart.SegmentStartOffset = GetNextLogOffset(logStart.SegmentStartOffset)
			if log.MsgOffset+int64(log.MsgSize) < int64(start.Offset()) {
				matchIndex, matchOffset, _, err := logMgr.SearchLogDataByMsgOffset(int64(start.Offset()))
				if err != nil {
					coordLog.Infof("search log failed: %v", err)
				} else if matchIndex > logStart.SegmentStartIndex ||
					(matchIndex == logStart.SegmentStartIndex && matchOffset > logStart.SegmentStartOffset) {
					logStart.SegmentStartIndex = matchIndex
					logStart.SegmentStartOffset = matchOffset
				}
			}
			err = logMgr.CleanOldData(logStart.SegmentStartIndex, logStart.SegmentStartOffset)
			if err != nil {
				// maybe the diskqueue data corrupt, we need sync from leader
				coordLog.Errorf("clean log failed : %v, %v", logStart, err)
				return err
			}
			logStart, log, err = logMgr.GetLogStartInfo()
			if err != nil {
				return err
			}
			coordLog.Warningf("topic %v log start fixed to: %v, %v", tc.topicInfo.GetTopicDesp(), logStart, log)
		} else {
			coordLog.Errorf("read disk failed at log start: %v, %v, %v", logStart, log, err)
			return err
		}
	}
	if endFixErr != nil {
		coordLog.Errorf("check the local log queue end failed %v ", endFixErr)
		return endFixErr
	}
	r := snap.ReadOne()
	if r.Err != nil {
		if r.Err == io.EOF {
			return nil
		}
		coordLog.Errorf("read the start of disk failed %v ", r.Err)
		return r.Err
	}
	return nil
}

// check with current leader whether we have the same logs with leader
func (ncoord *NsqdCoordinator) checkLocalTopicForISR(tc *coordData) *CoordErr {
	if tc.topicInfo.Leader == ncoord.myNode.GetID() {
		// leader should always has the newest local data
		return nil
	}
	logMgr := tc.logMgr
	logid := logMgr.GetLastCommitLogID()
	c, err := ncoord.acquireRpcClient(tc.topicInfo.Leader)
	if err != nil {
		return err
	}
	leaderID, err := c.GetLastCommitLogID(&tc.topicInfo)
	if err != nil {
		return err
	}
	coordLog.Infof("checking if ISR synced, logid leader: %v, myself:%v", leaderID, logid)
	if leaderID > logid {
		coordLog.Infof("this node fall behand, should catchup.")
		return ErrLocalFallBehind
	} else if logid > leaderID {
		coordLog.Infof("this node has more data than leader, should rejoin.")
		return ErrLocalForwardThanLeader
	}
	if atomic.LoadInt32(&nsqd.EnableDelayedQueue) != 1 {
		return nil
	}
	logDelayedMgr := tc.delayedLogMgr
	if tc.topicInfo.OrderedMulti || logDelayedMgr == nil {
		return nil
	}
	logid = logDelayedMgr.GetLastCommitLogID()
	leaderID, err = c.GetLastDelayedQueueCommitLogID(&tc.topicInfo)
	if err != nil {
		return err
	}
	coordLog.Infof("checking if ISR delay queue synced, logid leader: %v, myself:%v", leaderID, logid)
	if leaderID > logid {
		coordLog.Infof("this node fall behand, should catchup.")
		return ErrLocalFallBehind
	} else if logid > leaderID {
		coordLog.Infof("this node has more data than leader, should rejoin.")
		return ErrLocalForwardThanLeader
	}
	return nil
}

func (ncoord *NsqdCoordinator) checkForUnsyncedTopics() {
	ticker := time.NewTicker(time.Minute * 10)
	defer ticker.Stop()
	defer ncoord.wg.Done()
	doWork := func() {
		// check local topic for coordinator
		ncoord.loadLocalTopicData()

		// check coordinator with cluster
		tmpChecks := make(map[string]map[int]bool, len(ncoord.topicCoords))
		ncoord.coordMutex.Lock()
		for topic, info := range ncoord.topicCoords {
			for pid, tc := range info {
				if tc.IsExiting() {
					continue
				}
				if _, ok := tmpChecks[topic]; !ok {
					tmpChecks[topic] = make(map[int]bool)
				}
				tmpChecks[topic][pid] = true
			}
		}
		ncoord.coordMutex.Unlock()
		for topic, info := range tmpChecks {
			for pid := range info {
				topicMeta, err := ncoord.leadership.GetTopicInfo(topic, pid)
				if err != nil {
					continue
				}
				if topicMeta.Replica < 1 {
					continue
				}
				if FindSlice(topicMeta.CatchupList, ncoord.myNode.GetID()) != -1 {
					go ncoord.catchupFromLeader(*topicMeta, "")
				} else if FindSlice(topicMeta.ISR, ncoord.myNode.GetID()) == -1 {
					if len(topicMeta.ISR)+len(topicMeta.CatchupList) >= topicMeta.Replica {
						coordData, err := ncoord.getTopicCoordData(topic, pid)
						canRemove := true
						if err == nil {
							// check whether the topic info is out of date
							isr := coordData.topicInfo.ISR
							catchups := coordData.topicInfo.CatchupList
							if FindSlice(isr, ncoord.myNode.GetID()) != -1 || FindSlice(catchups, ncoord.myNode.GetID()) != -1 {
								canRemove = false
								coordLog.Infof("the topic not relevance to me: %v, but found in topic coordinator %v", topicMeta, coordData.topicInfo)
							}
						}
						if canRemove {
							coordLog.Infof("the topic should be clean since not relevance to me: %v", topicMeta)
							// TODO: we need handle if this topic is init while got update topic info
							ncoord.removeTopicCoord(topicMeta.Name, topicMeta.Partition, true)
						}
						// removed or not we need check the newest topic info from lookup if we do the wrong thing because of the isr changed while removing
						ncoord.requestNotifyNewTopicInfo(topicMeta.Name, topicMeta.Partition)
					}
				}
			}
		}
		// clean unused client connections
		ncoord.cleanRpcClients(false)

		ncoord.lookupMutex.Lock()
		for addr, c := range ncoord.lookupRemoteClients {
			l := ncoord.lookupLeader
			leaderAddr := net.JoinHostPort(l.NodeIP, l.RpcPort)
			if c != nil && addr != leaderAddr {
				c.Close()
				delete(ncoord.lookupRemoteClients, addr)
			}
		}
		ncoord.lookupMutex.Unlock()
	}

	for {
		select {
		case <-ncoord.stopChan:
			return
		case <-ncoord.tryCheckUnsynced:
			doWork()
		case <-ticker.C:
			doWork()
		}
	}
}

// while leader is leaving by ncoord, we can release the leader lock and let other replica became leader
func (ncoord *NsqdCoordinator) releaseTopicLeader(topicInfo *TopicPartitionMetaInfo, session *TopicLeaderSession) *CoordErr {
	if session != nil && session.LeaderNode != nil {
		if ncoord.GetMyID() != session.LeaderNode.GetID() {
			coordLog.Warningf("the leader session should not be released by other node: %v, %v", session.LeaderNode, ncoord.GetMyID())
			return ErrLeaderSessionMismatch
		}
	}
	err := ncoord.leadership.ReleaseTopicLeader(topicInfo.Name, topicInfo.Partition, session)
	if err != nil {
		coordLog.Infof("failed to release leader for topic(%v): %v", topicInfo.Name, err)
		return &CoordErr{err.Error(), RpcErrTopicLeaderChanged, CoordElectionErr}
	}
	return nil
}

// New leader should acquire the leader lock and check if success to avoid multi leader session
func (ncoord *NsqdCoordinator) acquireTopicLeader(topicInfo *TopicPartitionMetaInfo) *CoordErr {
	coordLog.Infof("acquiring leader for topic(%v): %v", topicInfo.Name, ncoord.myNode.GetID())
	// TODO: leader channel should be closed if not success,
	// how to handle acquire twice by the same node?
	err := ncoord.leadership.AcquireTopicLeader(topicInfo.Name, topicInfo.Partition, &ncoord.myNode, topicInfo.Epoch)
	if err != nil {
		coordLog.Infof("failed to acquire leader for topic(%v): %v", topicInfo.Name, err)
		return &CoordErr{err.Error(), RpcNoErr, CoordElectionErr}
	}

	coordLog.Infof("acquiring leader for topic(%v) success", topicInfo.Name)
	return nil
}

func (ncoord *NsqdCoordinator) IsMineConsumeLeaderForTopic(topic string, part int) bool {
	tcData, err := ncoord.getTopicCoordData(topic, part)
	if err != nil {
		return false
	}
	return tcData.GetLeader() == ncoord.myNode.GetID() && tcData.GetLeaderSessionID() == ncoord.myNode.GetID()
}

func (ncoord *NsqdCoordinator) IsMineLeaderForTopic(topic string, part int) bool {
	tcData, err := ncoord.getTopicCoordData(topic, part)
	if err != nil {
		return false
	}
	if IsAllClusterWriteDisabled() {
		return false
	}
	if tcData.topicInfo.OrderedMulti && IsClusterWriteDisabledForOrdered() {
		return false
	}
	return tcData.GetLeader() == ncoord.myNode.GetID() && tcData.GetLeaderSessionID() == ncoord.myNode.GetID()
}

func (ncoord *NsqdCoordinator) SearchLogByMsgID(topic string, part int, msgID int64) (*CommitLogData, int64, int64, error) {
	tcData, err := ncoord.getTopicCoordData(topic, part)
	if err != nil || tcData.logMgr == nil {
		return nil, 0, 0, err.ToErrorType()
	}
	_, _, l, localErr := tcData.logMgr.SearchLogDataByMsgID(msgID)
	if localErr != nil {
		coordLog.Infof("search data failed: %v", localErr)
		return nil, 0, 0, localErr
	}
	realOffset := l.MsgOffset
	curCount := l.dqSeekCnt()
	return l, realOffset, curCount, nil
}

// search commitlog for message given by message disk data offset
func (ncoord *NsqdCoordinator) SearchLogByMsgOffset(topic string, part int, offset int64) (*CommitLogData, int64, int64, error) {
	tcData, err := ncoord.getTopicCoordData(topic, part)
	if err != nil || tcData.logMgr == nil {
		return nil, 0, 0, errors.New(err.String())
	}
	_, _, l, localErr := tcData.logMgr.SearchLogDataByMsgOffset(offset)
	if localErr != nil {
		coordLog.Infof("search data failed: %v", localErr)
		return nil, 0, 0, localErr
	}
	realOffset := l.MsgOffset
	curCount := l.dqSeekCnt()
	if l.MsgOffset < offset {
		t, localErr := ncoord.localNsqd.GetExistingTopic(topic, part)
		if localErr != nil {
			return l, 0, 0, localErr
		}
		snap := t.GetDiskQueueSnapshot(true)

		seekCnt := l.dqSeekCnt()
		localErr = snap.SeekTo(nsqd.BackendOffset(realOffset), seekCnt)
		if localErr != nil {
			coordLog.Infof("seek to disk queue error: %v, %v", localErr, realOffset)
			return l, 0, 0, localErr
		}

		for {
			ret := snap.ReadOne()
			if ret.Err != nil {
				coordLog.Infof("read disk queue error: %v", ret.Err)
				if ret.Err == io.EOF {
					return l, realOffset, curCount, nil
				}
				return l, realOffset, curCount, ret.Err
			} else {
				if int64(ret.Offset) >= offset || curCount > l.MsgCnt+int64(l.MsgNum-1) {
					break
				}
				realOffset = int64(ret.Offset) + int64(ret.MovedSize)
				curCount++
			}
		}
	}
	return l, realOffset, curCount, nil
}

func (ncoord *NsqdCoordinator) SearchLogByMsgCnt(topic string, part int, count int64) (*CommitLogData, int64, int64, error) {
	tcData, err := ncoord.getTopicCoordData(topic, part)
	if err != nil || tcData.logMgr == nil {
		return nil, 0, 0, errors.New(err.String())
	}
	_, _, l, localErr := tcData.logMgr.SearchLogDataByMsgCnt(count)
	if localErr != nil {
		coordLog.Infof("search data failed: %v", localErr)
		return nil, 0, 0, localErr
	}
	realOffset := l.MsgOffset
	curCount := l.dqSeekCnt()
	if l.MsgCnt < count {
		t, localErr := ncoord.localNsqd.GetExistingTopic(topic, part)
		if localErr != nil {
			return l, 0, 0, localErr
		}
		snap := t.GetDiskQueueSnapshot(true)

		seekCnt := l.dqSeekCnt()
		localErr = snap.SeekTo(nsqd.BackendOffset(realOffset), seekCnt)
		if localErr != nil {
			coordLog.Infof("seek to disk queue error: %v, %v", localErr, realOffset)
			return l, 0, 0, localErr
		}

		for {
			ret := snap.ReadOne()
			if ret.Err != nil {
				coordLog.Infof("read disk queue error: %v", ret.Err)
				if ret.Err == io.EOF {
					return l, realOffset, curCount, nil
				}
				return l, realOffset, curCount, ret.Err
			} else {
				if curCount >= count-1 || curCount > l.MsgCnt+int64(l.MsgNum-1) {
					break
				}
				realOffset = int64(ret.Offset) + int64(ret.MovedSize)
				curCount++
			}
		}
	}
	return l, realOffset, curCount, nil
}

type MsgTimestampComparator struct {
	localTopicReader *nsqd.DiskQueueSnapshot
	searchEnd        int64
	searchTs         int64
	ext              bool
}

func (ncoord *MsgTimestampComparator) SearchEndBoundary() int64 {
	return ncoord.searchEnd
}

func (ncoord *MsgTimestampComparator) LessThanLeftBoundary(l *CommitLogData) bool {
	seekCnt := l.dqSeekCnt()
	err := ncoord.localTopicReader.ResetSeekTo(nsqd.BackendOffset(l.MsgOffset), seekCnt)
	if err != nil {
		coordLog.Errorf("seek disk queue failed: %v, %v", l, err)
		return true
	}
	r := ncoord.localTopicReader.ReadOne()
	if r.Err != nil {
		coordLog.Errorf("seek disk queue failed: %v, %v", l, r.Err)
		return true
	}
	msg, err := nsqd.DecodeMessage(r.Data, ncoord.ext)
	if err != nil {
		coordLog.Errorf("failed to decode message - %s - %v", err, r)
		return true
	}
	if ncoord.searchTs < msg.Timestamp {
		return true
	}
	return false
}

func (ncoord *MsgTimestampComparator) GreatThanRightBoundary(l *CommitLogData) bool {
	seekCnt := l.dqSeekCnt()
	// we may read the eof , in this situation we reach the end, so the search should not be great than right boundary
	err := ncoord.localTopicReader.ResetSeekTo(nsqd.BackendOffset(l.MsgOffset+int64(l.MsgSize)), seekCnt)
	if err != nil {
		coordLog.Errorf("seek disk queue failed: %v, %v", l, err)
		return false
	}
	r := ncoord.localTopicReader.ReadOne()
	if r.Err != nil {
		coordLog.Errorf("seek disk queue failed: %v, %v", l, r.Err)
		return false
	}
	msg, err := nsqd.DecodeMessage(r.Data, ncoord.ext)
	if err != nil {
		coordLog.Errorf("failed to decode message - %s - %v", err, r)
		return false
	}
	if ncoord.searchTs > msg.Timestamp {
		return true
	}
	return false
}

// return the searched log data and the exact offset the reader should be reset and the total count before the offset.
func (ncoord *NsqdCoordinator) SearchLogByMsgTimestamp(topic string, part int, ts_sec int64) (*CommitLogData, int64, int64, error) {
	tcData, err := ncoord.getTopicCoordData(topic, part)
	if err != nil || tcData.logMgr == nil {
		return nil, 0, 0, errors.New(err.String())
	}
	t, localErr := ncoord.localNsqd.GetExistingTopic(topic, part)
	if localErr != nil {
		return nil, 0, 0, localErr
	}

	snap := t.GetDiskQueueSnapshot(true)
	comp := &MsgTimestampComparator{
		localTopicReader: snap,
		searchEnd:        tcData.logMgr.GetCurrentStart(),
		searchTs:         ts_sec * 1000 * 1000 * 1000,
		ext:              tcData.topicInfo.Ext,
	}

	if comp.searchTs < int64(0) {
		return nil, 0, 0, fmt.Errorf("Invalid timestamp %v", ts_sec)
	}

	startSearch := time.Now()
	_, _, l, localErr := tcData.logMgr.SearchLogDataByComparator(comp)
	coordLog.Infof("search log cost: %v", time.Since(startSearch))
	if localErr != nil {
		return nil, 0, 0, localErr
	}
	realOffset := l.MsgOffset

	seekCnt := l.dqSeekCnt()
	// check if the message timestamp is fit the require
	localErr = snap.ResetSeekTo(nsqd.BackendOffset(realOffset), seekCnt)
	if localErr != nil {
		coordLog.Infof("seek to disk queue error: %v, %v", localErr, realOffset)
		return l, 0, 0, localErr
	}
	curCount := l.dqSeekCnt()

	for {
		ret := snap.ReadOne()
		if ret.Err != nil {
			coordLog.Infof("read disk queue error: %v", ret.Err)
			if ret.Err == io.EOF {
				return l, realOffset, curCount, nil
			}
			return l, realOffset, curCount, ret.Err
		} else {
			msg, err := nsqd.DecodeMessage(ret.Data, tcData.topicInfo.Ext)
			if err != nil {
				coordLog.Errorf("failed to decode message - %v - %v", err, ret)
				return l, realOffset, curCount, err
			}
			// we allow to read the next message count exceed the current log
			// in case the current log contains only one message (the returned timestamp may be
			// next to the current)
			if msg.Timestamp >= comp.searchTs || curCount > l.MsgCnt+int64(l.MsgNum-1) {
				break
			}
			realOffset = int64(ret.Offset) + int64(ret.MovedSize)
			curCount++
		}
	}
	return l, realOffset, curCount, nil
}

// for isr node to check with leader
// The lookup will wait all isr sync to new leader during the leader switch
func (ncoord *NsqdCoordinator) syncToNewLeader(topicCoord *coordData, joinSession string, mustSynced bool) *CoordErr {
	// If leadership changed, all isr nodes should sync to new leader and check
	// consistent with leader, after all isr nodes notify ready, the leader can
	// accept new write.
	coordLog.Infof("checking sync state with new leader: %v on node: %v", joinSession, ncoord.myNode.GetID())
	err := ncoord.checkLocalTopicForISR(topicCoord)
	if err == ErrLocalFallBehind || err == ErrLocalForwardThanLeader {
		// only sync with leader when write is disabled,
		// otherwise, we may miss to get the un-commit logs during write.
		if joinSession != "" {
			coordLog.Infof("isr begin sync with new leader")
			go ncoord.catchupFromLeader(topicCoord.topicInfo, joinSession)
		} else {
			coordLog.Infof("topic %v isr not synced with new leader ", topicCoord.topicInfo.GetTopicDesp())
			if mustSynced {
				err := ncoord.requestLeaveFromISR(topicCoord.topicInfo.Name, topicCoord.topicInfo.Partition)
				if err != nil {
					coordLog.Infof("request leave isr failed: %v", err)
				} else {
					ncoord.requestJoinCatchup(topicCoord.topicInfo.Name, topicCoord.topicInfo.Partition)
				}
			} else {
				coordLog.Infof("topic %v ignore current isr node out of synced since write is not disabled", topicCoord.topicInfo.GetTopicDesp())
			}
		}
		return err
	} else if err != nil {
		coordLog.Infof("check isr with leader err: %v", err)
		return err
	}
	if joinSession != "" && err == nil {
		rpcErr := ncoord.notifyReadyForTopicISR(&topicCoord.topicInfo, &topicCoord.topicLeaderSession, joinSession)
		if rpcErr != nil {
			coordLog.Infof("notify I am ready for isr failed:%v ", rpcErr)
		}
	}
	return nil
}

// decide where we should pull the commit log from leader, this will read the commit log from end to the start
// to check whether we are the same with leader.
// Catchup will pull logs from the matched newest log offset.
// If we cannot found a matched log, we should do the full sync with leader which we will pull all the logs
// and the log start info from leader.
func (ncoord *NsqdCoordinator) decideCatchupCommitLogInfo(tc *TopicCoordinator,
	topicInfo TopicPartitionMetaInfo, localTopic *nsqd.Topic, c *NsqdRpcClient,
	fromDelayedQueue bool) (int64, int64, bool, *CoordErr) {
	var localLogQ ILocalLogQueue
	localTopic.Lock()
	localLogQ, logMgr, coordErr := getOrCreateCommitLogAndLocalLogQ(tc.GetData(), localTopic, fromDelayedQueue)
	localTopic.Unlock()
	if coordErr != nil {
		if fromDelayedQueue && topicInfo.OrderedMulti {
			return 0, 0, false, nil
		}
		return 0, 0, false, coordErr
	}

	logIndex, offset, _, logErr := logMgr.GetLastCommitLogOffsetV2()
	if logErr != nil && logErr != ErrCommitLogEOF {
		coordLog.Warningf("catching failed since log offset read error: %v", logErr)
		logMgr.Delete()
		logMgr.Reopen()
		tc.Exiting()
		go ncoord.removeTopicCoord(topicInfo.Name, topicInfo.Partition, true)
		return 0, 0, false, ErrLocalTopicDataCorrupt
	}

	retryCnt := 0
	needFullSync := false
	localLogSegStart, _, _ := logMgr.GetLogStartInfo()
	countNumIndex, _ := logMgr.ConvertToCountIndex(logIndex, offset)

	coordLog.Infof("topic %v catchup commit log begin :%v at: %v:%v:%v", topicInfo.GetTopicDesp(),
		localLogSegStart, logIndex, offset, countNumIndex)
	for offset > localLogSegStart.SegmentStartOffset || logIndex > localLogSegStart.SegmentStartIndex {
		// if leader changed we abort and wait next time
		if tc.GetData().GetLeader() != topicInfo.Leader {
			coordLog.Infof("topic leader changed from %v to %v, abort current catchup: %v",
				topicInfo.Leader, tc.GetData().GetLeader(), topicInfo.GetTopicDesp())
			return logIndex, offset, needFullSync, ErrTopicLeaderChanged
		}
		localLogData, localErr := logMgr.GetCommitLogFromOffsetV2(logIndex, offset)
		if localErr != nil {
			offset -= int64(GetLogDataSize())
			if offset < 0 && logIndex > localLogSegStart.SegmentStartIndex {
				logIndex--
				offset, _, localErr = logMgr.GetLastCommitLogDataOnSegment(logIndex)
				if localErr != nil {
					coordLog.Warningf("topic %v read commit log failed: %v:%v, %v", topicInfo.GetTopicDesp(),
						logIndex, offset, localErr)
					return logIndex, offset, needFullSync, &CoordErr{localErr.Error(), RpcNoErr, CoordLocalErr}
				}
			}
			continue
		}

		countNumIndex, _ = logMgr.ConvertToCountIndex(logIndex, offset)
		useCountIndex, leaderCountNumIndex, leaderLogIndex, leaderOffset, leaderLogData, rpcErr := c.GetCommitLogFromOffset(&topicInfo,
			countNumIndex, logIndex, offset, fromDelayedQueue)
		if rpcErr.IsEqual(ErrTopicCommitLogOutofBound) || rpcErr.IsEqual(ErrTopicCommitLogEOF) {
			coordLog.Infof("local commit log is more than leader while catchup: %v:%v:%v vs %v:%v:%v",
				logIndex, offset, countNumIndex, leaderLogIndex, leaderOffset, leaderCountNumIndex)
			// local log is ahead of the leader, must truncate local data.
			// truncate commit log and truncate the data file to last log
			// commit offset.
			if useCountIndex {
				if leaderCountNumIndex > countNumIndex {
					coordLog.Infof("commit log changed while check leader commit log")
					// the leader commit log end changed
					time.Sleep(time.Second)
					continue
				}
				logIndex, offset, localErr = logMgr.ConvertToOffsetIndex(leaderCountNumIndex)
				if localErr != nil {
					coordLog.Infof("convert The leader commit log count index %v failed: %v", leaderCountNumIndex, localErr)
					needFullSync = true
					break
				}
			} else {
				if leaderLogIndex > logIndex ||
					(leaderLogIndex == logIndex && leaderOffset > offset) {
					coordLog.Infof("commit log changed while check leader commit log")
					time.Sleep(time.Second)
					continue
				}
				offset = leaderOffset
				logIndex = leaderLogIndex
			}
		} else if rpcErr.IsEqual(ErrTopicCommitLogLessThanSegmentStart) {
			coordLog.Infof("The leader commit log has been cleaned here, maybe the follower fall behind too much")
			// need a full sync
			needFullSync = true
			break
		} else if rpcErr != nil {
			coordLog.Infof("something wrong while get leader logdata while catchup: %v", rpcErr)
			if strings.Contains(rpcErr.ErrMsg, "unknown service name") {
				return 0, 0, needFullSync, ErrRpcMethodUnknown
			}
			if retryCnt > MAX_CATCHUP_RETRY {
				return logIndex, offset, needFullSync, rpcErr
			}
			retryCnt++
			time.Sleep(time.Second)
		} else {
			if *localLogData == leaderLogData {
				coordLog.Infof("topic %v local commit log match leader %v at: %v", topicInfo.GetTopicDesp(), topicInfo.Leader, leaderLogData)
				break
			}
			coordLog.Infof("topic %v local commit log %v mismatch leader %v with: %v",
				topicInfo.GetTopicDesp(), *localLogData, topicInfo.Leader, leaderLogData)
			offset -= int64(GetLogDataSize())
			if offset < 0 && logIndex > localLogSegStart.SegmentStartIndex {
				logIndex--
				offset, _, localErr = logMgr.GetLastCommitLogDataOnSegment(logIndex)
				if localErr != nil {
					coordLog.Warningf("topic %v read commit log failed: %v, %v", topicInfo.GetTopicDesp(), logIndex, localErr)
					return logIndex, offset, needFullSync, &CoordErr{localErr.Error(), RpcNoErr, CoordLocalErr}
				}
			}
		}
	}
	countNumIndex, _ = logMgr.ConvertToCountIndex(logIndex, offset)
	if fromDelayedQueue {
		coordLog.Infof("topic %v delayed queue commit log match leader %v at: %v:%v:%v", topicInfo.GetTopicDesp(),
			topicInfo.Leader, logIndex, offset, countNumIndex)
	} else {
		coordLog.Infof("topic %v local commit log match leader %v at: %v:%v:%v", topicInfo.GetTopicDesp(),
			topicInfo.Leader, logIndex, offset, countNumIndex)
	}

	compatibleMethod := false
	if offset == localLogSegStart.SegmentStartOffset && logIndex == localLogSegStart.SegmentStartIndex {
		if logIndex == 0 && offset == 0 {
			// for compatible, if all replica start with 0 (no auto clean happen)
			_, _, _, _, _, rpcErr := c.GetCommitLogFromOffset(&topicInfo, 0, 0, 0, fromDelayedQueue)
			coordLog.Infof("catchup start with 0, check return: %v", rpcErr)
			if rpcErr == nil || rpcErr.IsEqual(ErrTopicCommitLogEOF) {
				coordLog.Infof("catchup start with 0, We can do full sync in old way since leader is also start with 0")
				compatibleMethod = true
			} else {
				coordLog.Infof("catchup start with 0, need do full sync in new way: %v", rpcErr)
			}
		}
		needFullSync = true
	}
	// do we need hold lock for local topic?  Maybe no since it will not run concurrently while catchup
	localErr := checkAndFixLocalLogQueueData(tc.GetData(), localLogQ, logMgr, false)
	if localErr != nil {
		coordLog.Errorf("check local topic %v data need to be fixed:%v", topicInfo.GetTopicDesp(), localErr)
		localLogQ.SetDataFixState(true)
	}

	if localLogQ.IsDataNeedFix() {
		needFullSync = true
		compatibleMethod = false
	}

	if !needFullSync {
		lastLog, localErr := logMgr.GetCommitLogFromOffsetV2(logIndex, offset)
		if localErr != nil {
			coordLog.Errorf("failed to get local commit log: %v", localErr)
			return logIndex, offset, needFullSync, &CoordErr{localErr.Error(), RpcNoErr, CoordLocalErr}
		}
		// reset the data file to (lastLog.LogID, lastLog.MsgOffset),
		// and the next message write position should be updated.
		localTopic.Lock()
		localErr = localLogQ.ResetBackendEndNoLock(nsqd.BackendOffset(lastLog.MsgOffset+int64(lastLog.MsgSize)),
			lastLog.MsgCnt+int64(lastLog.MsgNum)-1)
		localTopic.Unlock()
		if localErr != nil {
			localLogQ.SetDataFixState(true)
			coordLog.Errorf("failed to reset local topic %v data: %v", localTopic.GetFullName(), localErr)
			return logIndex, offset, needFullSync, &CoordErr{localErr.Error(), RpcNoErr, CoordLocalErr}
		}
		_, localErr = logMgr.TruncateToOffsetV2(logIndex, offset+int64(GetLogDataSize()))
		if localErr != nil {
			coordLog.Errorf("failed to truncate local commit log to %v:%v: %v", logIndex, offset, localErr)
			return logIndex, offset, needFullSync, &CoordErr{localErr.Error(), RpcNoErr, CoordLocalErr}
		}
		// we can avoid pull log for the first match position
		offset += int64(GetLogDataSize())
		countNumIndex++
		coordLog.Infof("local topic %v only sync log from: %v:%v:%v", localTopic.GetFullName(), logIndex, offset, countNumIndex)
	} else {
		// need sync the commit log segment start info and the topic diskqueue start info
		// we should clean local data, since the local start info maybe different with leader start info
		if fromDelayedQueue {
			coordLog.Infof("local topic %v delayed queue should do full sync", localTopic.GetFullName())
		} else {
			coordLog.Infof("local topic %v should do full sync", localTopic.GetFullName())
			localTopic.PrintCurrentStats()
		}

		if compatibleMethod {
			localTopic.Lock()
			localErr = localLogQ.ResetBackendEndNoLock(nsqd.BackendOffset(0), 0)
			localTopic.Unlock()
			if localErr != nil {
				coordLog.Errorf("failed to reset local topic data: %v", localErr)
				localLogQ.SetDataFixState(true)
				return logIndex, offset, needFullSync, &CoordErr{localErr.Error(), RpcNoErr, CoordLocalErr}
			}
			_, localErr = logMgr.TruncateToOffsetV2(0, 0)
			if localErr != nil {
				if localErr != ErrCommitLogEOF {
					coordLog.Errorf("failed to truncate local commit log to %v:%v: %v", 0, 0, localErr)
					return logIndex, offset, needFullSync, &CoordErr{localErr.Error(), RpcNoErr, CoordLocalErr}
				}
			}
		} else {
			leaderCommitStartInfo, firstLogData, err := c.GetFullSyncInfo(topicInfo.Name,
				topicInfo.Partition, fromDelayedQueue)
			if err != nil {
				coordLog.Infof("failed to get full sync info: %v", err)
				if strings.Contains(err.Error(), "unknown service name") {
					return logIndex, offset, needFullSync, ErrRpcMethodUnknown
				}
				return logIndex, offset, needFullSync, &CoordErr{err.Error(), RpcCommonErr, CoordNetErr}
			}
			coordLog.Infof("topic %v full sync with start: %v, %v", localTopic.GetFullName(), leaderCommitStartInfo, firstLogData)

			localErr = logMgr.ResetLogWithStart(*leaderCommitStartInfo)
			if localErr != nil {
				coordLog.Warningf("reset commit log with start %v failed: %v", leaderCommitStartInfo, localErr)
				return logIndex, offset, needFullSync, &CoordErr{localErr.Error(), RpcNoErr, CoordLocalErr}
			}
			localTopic.Lock()
			// it is possible that the first log start is 0 and this means the leader queue has the full log (no clean happen)
			if firstLogData.MsgCnt == 0 {
				localErr = localLogQ.ResetBackendWithQueueStartNoLock(0, 0)
			} else {
				localErr = localLogQ.ResetBackendWithQueueStartNoLock(firstLogData.MsgOffset, firstLogData.MsgCnt-1)
			}
			localTopic.Unlock()
			if localErr != nil {
				localLogQ.SetDataFixState(true)
				coordLog.Warningf("reset topic %v queue with start %v failed: %v", topicInfo.GetTopicDesp(), firstLogData, localErr)
				return logIndex, offset, needFullSync, &CoordErr{localErr.Error(), RpcNoErr, CoordLocalErr}
			}
			logIndex, offset, localErr = logMgr.ConvertToOffsetIndex(leaderCommitStartInfo.SegmentStartCount)
			if localErr != nil {
				return logIndex, offset, needFullSync, &CoordErr{localErr.Error(), RpcNoErr, CoordLocalErr}
			}
		}
	}
	return logIndex, offset, needFullSync, nil
}

// pull the logs from leader from the given matched offset
func (ncoord *NsqdCoordinator) pullCatchupDataFromLeader(tc *TopicCoordinator,
	topicInfo TopicPartitionMetaInfo, localTopic *nsqd.Topic, logMgr *TopicCommitLogMgr, fromDelayedQueue bool,
	logIndex int64, offset int64) *CoordErr {

	if logMgr == nil {
		return nil
	}
	var delayedQueue *nsqd.DelayQueue
	var localLogQ ILocalLogQueue
	localLogQ = localTopic
	if fromDelayedQueue {
		localTopic.Lock()
		delayedQueue, _ = localTopic.GetOrCreateDelayedQueueNoLock(logMgr)
		localLogQ = delayedQueue
		localTopic.Unlock()
	}
	if fromDelayedQueue && delayedQueue == nil {
		return nil
	}

	synced := false
	retryCnt := 0

	c, coordErr := ncoord.acquireRpcClient(topicInfo.Leader)
	if coordErr != nil {
		coordLog.Warningf("failed to get rpc client while catchup: %v", coordErr)
		return coordErr
	}
	for {
		if tc.GetData().GetLeader() != topicInfo.Leader {
			coordLog.Infof("topic leader changed from %v to %v, abort current catchup: %v", topicInfo.Leader, tc.GetData().GetLeader(), topicInfo.GetTopicDesp())
			return ErrTopicLeaderChanged
		}
		if tc.IsExiting() {
			// the topic coordinator can be notified to close while catchup if other isr node is already joined.
			// mostly happened while restarting some node.
			coordLog.Infof("topic %v is exiting while pulling logs ", topicInfo.GetTopicDesp())
			return ErrTopicExiting
		}
		countNumIndex, localErr := logMgr.ConvertToCountIndex(logIndex, offset)
		if localErr != nil {
			coordLog.Warningf("topic %v error while convert count index:%v, offset: %v:%v", topicInfo.GetTopicDesp(),
				localErr, logIndex, offset)
			return &CoordErr{localErr.Error(), RpcNoErr, CoordLocalErr}
		}
		sleepMs := atomic.LoadInt32(&sleepMsBetweenLogSyncPull)
		if sleepMs > 0 {
			time.Sleep(time.Duration(sleepMs) * time.Millisecond)
		}
		logs, dataList, rpcErr := c.PullCommitLogsAndData(topicInfo.Name, topicInfo.Partition,
			countNumIndex, logIndex, offset, MAX_LOG_PULL, fromDelayedQueue)
		if rpcErr != nil {
			// if not network error, something wrong with commit log file, we need return to abort.
			coordLog.Infof("topic %v error while get logs :%v, offset: %v:%v:%v", topicInfo.GetTopicDesp(),
				rpcErr, logIndex, offset, countNumIndex)
			if strings.Contains(rpcErr.Error(), "unknown service name") {
				return ErrRpcMethodUnknown
			}
			if strings.Contains(rpcErr.Error(), ErrLocalDelayedQueueMissing.ErrMsg) ||
				strings.Contains(rpcErr.Error(), ErrTopicMissingDelayedLog.ErrMsg) {
				return &CoordErr{rpcErr.Error(), RpcCommonErr, CoordNetErr}
			}
			if retryCnt > MAX_CATCHUP_RETRY {
				return &CoordErr{rpcErr.Error(), RpcCommonErr, CoordNetErr}
			}
			retryCnt++
			time.Sleep(time.Second)
			continue
		} else if len(logs) == 0 {
			synced = true
		}
		coordLog.Infof("topic %v pulled logs :%v from offset: %v:%v:%v", topicInfo.GetTopicDesp(),
			len(logs), logIndex, offset, countNumIndex)
		localTopic.Lock()
		hasErr := false
		var lastCommitOffset nsqd.BackendQueueEnd
		// redo logs on the replicas
		for i, l := range logs {
			d := dataList[i]
			// while the topic is upgraded from old version, it may happen in one topic we got the old message
			// and the new extended message. We need handle this while sync leader data.

			var queueEnd nsqd.BackendQueueEnd
			queueEnd, localErr = localLogQ.PutRawDataOnReplica(d,
				nsqd.BackendOffset(l.MsgOffset), int64(l.MsgSize), l.MsgNum)
			if localErr != nil {
				coordLog.Warningf("topic %v Failed to put raw message data on slave: %v, offset: %v, need to be fixed",
					localTopic.GetFullName(), localErr, l.MsgOffset)
				localLogQ.SetDataFixState(true)
				hasErr = true
				break
			}
			lastCommitOffset = queueEnd

			for {
				if atomic.LoadInt32(&testCatchupPausedPullLogs) == 1 {
					time.Sleep(time.Second * 2)
				} else {
					break
				}
			}
			localErr = logMgr.AppendCommitLog(&l, true)
			if localErr != nil {
				coordLog.Errorf("Failed to append local log: %v, need to be fixed ", localErr)
				localLogQ.SetDataFixState(true)
				hasErr = true
				break
			}
		}
		if !hasErr {
			logMgr.FlushCommitLogs()
			if !fromDelayedQueue {
				localTopic.UpdateCommittedOffset(lastCommitOffset)
			}
		}
		localTopic.Unlock()
		if hasErr {
			return &CoordErr{localErr.Error(), RpcNoErr, CoordLocalErr}
		}
		localLogQ.ForceFlush()
		logIndex, offset = logMgr.GetCurrentEnd()
		localLogQ.SetDataFixState(false)
		if synced {
			break
		}
	}
	return nil
}

// delayed queue need be restored if we need full sync from leader
func (ncoord *NsqdCoordinator) pullDelayedQueueFromLeader(tc *TopicCoordinator,
	topicInfo TopicPartitionMetaInfo, localTopic *nsqd.Topic, c *NsqdRpcClient) *CoordErr {
	delayedQueue := localTopic.GetDelayedQueue()
	if delayedQueue == nil {
		return ErrLocalDelayedQueueMissing
	}
	ninfo, err := c.GetNodeInfo(topicInfo.Leader)
	if err != nil {
		if strings.Contains(err.Error(), "unknown service name") {
			return ErrRpcMethodUnknown
		}
		return &CoordErr{err.Error(), RpcCommonErr, CoordNetErr}
	}
	ep := fmt.Sprintf("http://%s%s?topic=%s&partition=%v", net.JoinHostPort(ninfo.NodeIP, ninfo.HttpPort),
		API_BACKUP_DELAYED_QUEUE_DB, localTopic.GetTopicName(),
		localTopic.GetTopicPart())
	// get the boltdb full file
	coordLog.Infof("begin pull topic %v delayed db file ", localTopic.GetFullName())

	rsp, err := http.Get(ep)
	var bodyReader io.Reader
	if err != nil {
		if ninfo.HttpPort == "" || ninfo.HttpPort == "0" {
			coordLog.Warningf("pull topic %v delayed db file from rpc: %v", localTopic.GetFullName(), ninfo)
			// test case use rpc instead
			rsp, rpcerr := c.GetBackupedDelayedQueue(localTopic.GetTopicName(), localTopic.GetTopicPart())
			bodyReader = rsp
			if rpcerr != nil {
				coordLog.Warningf("pull topic %v delayed db file from rpc failed: %v", localTopic.GetFullName(), rpcerr)
				return &CoordErr{err.Error(), RpcNoErr, CoordLocalErr}
			}
			err = nil
		} else {
			coordLog.Warningf("pull topic %v delayed db file failed: %v", localTopic.GetFullName(), err)
			return &CoordErr{err.Error(), RpcNoErr, CoordLocalErr}
		}
	} else {
		defer rsp.Body.Close()
		bodyReader = rsp.Body
	}
	err = delayedQueue.RestoreKVStoreFrom(bodyReader)
	if err != nil {
		coordLog.Warningf("topic %v delayed db file restore failed: %v", localTopic.GetFullName(), err)
		return &CoordErr{err.Error(), RpcNoErr, CoordLocalErr}
	}
	coordLog.Infof("finished pull topic %v delayed db file ", localTopic.GetFullName())
	return nil
}

func (ncoord *NsqdCoordinator) catchupFromLeader(topicInfo TopicPartitionMetaInfo, joinISRSession string) *CoordErr {
	// get local commit log from check point , and pull newer logs from leader
	tc, coordErr := ncoord.getTopicCoord(topicInfo.Name, topicInfo.Partition)
	if coordErr != nil {
		coordLog.Warningf("topic(%v) catching failed since topic coordinator missing: %v", topicInfo.Name, coordErr)
		return ErrMissingTopicCoord
	}
	if !atomic.CompareAndSwapInt32(&tc.catchupRunning, 0, 1) {
		coordLog.Infof("topic(%v) catching already running", topicInfo.Name)
		return ErrTopicCatchupAlreadyRunning
	}
	defer atomic.StoreInt32(&tc.catchupRunning, 0)
	myRunning := atomic.AddInt32(&ncoord.catchupRunning, 1)
	defer atomic.AddInt32(&ncoord.catchupRunning, -1)
	if myRunning > MAX_CATCHUP_RUNNING {
		coordLog.Infof("catching too much running: %v", myRunning)
		return ErrCatchupRunningBusy
	}
	coordLog.Infof("local topic begin catchup : %v, join session: %v", topicInfo.GetTopicDesp(), joinISRSession)

	tc.writeHold.Lock()
	defer tc.writeHold.Unlock()
	if tc.IsExiting() {
		coordLog.Infof("catchup exit since the topic is exited")
		return ErrTopicExitingOnSlave
	}
	c, coordErr := ncoord.acquireRpcClient(topicInfo.Leader)
	if coordErr != nil {
		coordLog.Warningf("failed to get rpc client while catchup: %v", coordErr)
		return coordErr
	}
	localTopic, localErr := ncoord.localNsqd.GetExistingTopic(topicInfo.Name, topicInfo.Partition)
	if localErr != nil {
		coordLog.Errorf("get local topic failed:%v", localErr)
		return ErrLocalMissingTopic
	}

	logIndex, offset, _, coordErr := ncoord.decideCatchupCommitLogInfo(tc, topicInfo, localTopic, c, false)
	if coordErr != nil {
		coordLog.Infof("decide topic %v catchup log failed:%v", topicInfo.GetTopicDesp(), coordErr)
		return coordErr
	}

	dyConf := &nsqd.TopicDynamicConf{SyncEvery: int64(topicInfo.SyncEvery),
		AutoCommit:               0,
		RetentionDay:             topicInfo.RetentionDay,
		OrderedMulti:             topicInfo.OrderedMulti,
		MultiPart:                topicInfo.MultiPart,
		Ext:                      topicInfo.Ext,
		DisableChannelAutoCreate: topicInfo.DisableChannelAutoCreate,
	}
	tc.GetData().updateBufferSize(int(dyConf.SyncEvery - 1))
	localTopic.SetDynamicInfo(*dyConf, tc.GetData().logMgr)

	coordErr = ncoord.pullCatchupDataFromLeader(tc, topicInfo, localTopic, tc.GetData().logMgr, false, logIndex, offset)
	if coordErr != nil {
		coordLog.Infof("pull topic %v catchup data failed:%v", topicInfo.GetTopicDesp(), coordErr)
		//canIgnoreErr := strings.Contains(coordErr.ErrMsg, nsqd.ErrMoveOffsetOverflowed.Error())
		canIgnoreErr := strings.Contains(coordErr.ErrMsg, nsqd.ErrMoveOffsetOverflowed.Error()) ||
			strings.Contains(coordErr.ErrMsg, ErrLocalCommitDataMismatchQueueDataEOF.ErrMsg)
		if joinISRSession == "" && canIgnoreErr {
			// while join session is empty, it may happen the leader is writing while catchup
			// so we may get the unflushed commit log, which will return overflow error.
			// we can ignore this and continue catchup the left data after we disabled write on leader.
			// Also, while the leader is writing, it may happen the commit log is flushed but the disk queue not flushed, it may return EOF while pull data
			// we can ignore this and retry while disable write
			coordErr = nil
		} else {
			return coordErr
		}
	}

	//sync delayed queue, ordered topic has no delayed queue
	if !topicInfo.OrderedMulti {
		var needFullSync bool
		logIndex, offset, needFullSync, coordErr = ncoord.decideCatchupCommitLogInfo(tc, topicInfo, localTopic, c, true)
		if coordErr != nil {
			coordLog.Infof("decide topic %v catchup delayed queue log failed:%v", topicInfo.GetTopicDesp(), coordErr)
			if atomic.LoadInt32(&nsqd.EnableDelayedQueue) == 1 {
				return coordErr
			}
			// ignore error if delayed queue is not enabled
		} else {
			if needFullSync {
				// remove old to avoid old is large
				dq := localTopic.GetDelayedQueue()
				if dq != nil {
					localErr := dq.ReopenWithEmpty()
					if localErr != nil {
						return NewCoordErr(localErr.Error(), CoordLocalErr)
					}
				}
			}
			coordErr = ncoord.pullCatchupDataFromLeader(tc, topicInfo, localTopic, tc.GetData().delayedLogMgr, true, logIndex, offset)
			if coordErr == nil && needFullSync {
				coordErr = ncoord.pullDelayedQueueFromLeader(tc, topicInfo, localTopic, c)
			}
			if coordErr != nil {
				if atomic.LoadInt32(&nsqd.EnableDelayedQueue) == 1 {
					coordLog.Warningf("pull topic %v catchup delayed queue data error:%v", topicInfo.GetTopicDesp(), coordErr)
					return coordErr
				}
				coordLog.Infof("ignore the failed since not enabled, pull topic %v catchup delayed queue data error:%v", topicInfo.GetTopicDesp(), coordErr)
				coordErr = nil
			}
		}
	}
	coordErr = ncoord.syncChannelsFromOther(c, topicInfo, localTopic)
	if coordErr != nil {
		coordLog.Infof("local topic %v sync channels failed: %v while joining isr", topicInfo.GetTopicDesp(), coordErr.String())
		// TODO: if the channel cursor on leader is old than the cleaned queue start, it may failed to
		// sync the channel. In this case we need do full sync from leader to get the old queue files.
		// So we should set fix state to do full sync again.
		return coordErr
	}

	if joinISRSession == "" {
		// notify nsqlookupd coordinator to add myself to isr list.
		// if success, the topic leader will disable new write.
		coordLog.Infof("I am requesting join isr: %v on topic: %v",
			ncoord.myNode.GetID(), topicInfo.Name)
		if !tc.IsExiting() {
			go func() {
				err := ncoord.requestJoinTopicISR(&topicInfo)
				if err != nil {
					coordLog.Infof("request join isr failed: %v", err)
				}
			}()
		}
	} else if joinISRSession != "" {
		// with join isr session, it means the leader write is disabled and waiting the catchup join isr.
		// so we notify leader we are ready join isr safely.
		tc.GetData().flushCommitLogs()
		tc.GetData().switchForMaster(false)

		coordLog.Infof("local topic is ready for isr: %v", topicInfo.GetTopicDesp())
		leaderSession := tc.GetData().topicLeaderSession
		go func() {
			rpcErr := ncoord.notifyReadyForTopicISR(&topicInfo, &leaderSession, joinISRSession)
			if rpcErr != nil {
				coordLog.Infof("notify ready for isr failed: %v", rpcErr)
			} else {
				coordLog.Infof("my node isr synced: %v", topicInfo.GetTopicDesp())
			}
		}()
	}

	coordLog.Infof("local topic catchup done: %v", topicInfo.GetTopicDesp())
	return nil
}

func (ncoord *NsqdCoordinator) syncChannelsFromOther(c *NsqdRpcClient, topicInfo TopicPartitionMetaInfo, localTopic *nsqd.Topic) *CoordErr {
	stat, err := c.GetTopicStats(topicInfo.Name)
	if err != nil {
		coordLog.Infof("topic %v try get stats from other %v failed: %v", topicInfo.GetTopicDesp(), c.remote, err)
		return NewCoordErr(err.Error(), CoordNetErr)
	} else {
		// sync the history stats to make sure balance stats data is ok
		// sync channels from leader
		localTopic.GetDetailStats().ResetHistoryInitPub(localTopic.TotalDataSize())
		localTopic.GetDetailStats().UpdateHistory(stat.TopicHourlyPubDataList[topicInfo.GetTopicDesp()])

		chList, ok := stat.ChannelList[topicInfo.GetTopicDesp()]
		coordLog.Infof("topic %v sync channel list from leader: %v", topicInfo.GetTopicDesp(), chList)
		//TODO: channel confirmed offset must be synced from leader
		// if not, when new topic migrated to this node, channel depth=0 for new channel
		// and then leader changed to this topic, then the channel confirmed offset
		// will be skip to the end on the new topic node.
		if ok && len(chList) > 0 {
			metaMaps := make(map[string]nsqd.ChannelMetaInfo)
			consumerOffsetMap := make(map[string]WrapChannelConsumerOffset)
			if len(stat.ChannelMetas) > 0 {
				chMetas, _ := stat.ChannelMetas[topicInfo.GetTopicDesp()]
				for _, meta := range chMetas {
					metaMaps[meta.Name] = meta
				}
			}
			if len(stat.ChannelOffsets) > 0 {
				chConsumerOffsets, _ := stat.ChannelOffsets[topicInfo.GetTopicDesp()]
				for _, offset := range chConsumerOffsets {
					consumerOffsetMap[offset.Name] = offset
				}
			}

			oldChList := localTopic.GetChannelMapCopy()
			for _, chName := range chList {
				if protocol.IsEphemeral(chName) {
					continue
				}
				ch := localTopic.GetChannel(chName)
				if meta, ok := metaMaps[chName]; ok {
					if meta.Paused {
						ch.Pause()
					} else {
						ch.UnPause()
					}
					if meta.Skipped {
						ch.Skip()
					} else {
						ch.UnSkip()
					}
					if !meta.IsZanTestSkipepd() {
						ch.UnskipZanTest()
					} else {
						ch.SkipZanTest()
					}
				}
				if offset, ok := consumerOffsetMap[chName]; ok {
					offset.AllowBackward = true
					currentEnd := ch.GetChannelEnd()
					if nsqd.BackendOffset(offset.VOffset) > currentEnd.Offset() {
						offset.VOffset = int64(currentEnd.Offset())
						offset.VCnt = currentEnd.TotalMsgCnt()
					}
					if offset.NeedUpdateConfirmed {
						ch.UpdateConfirmedInterval(offset.ConfirmedInterval)
					}
					coordLog.Infof("topic %v local channel(%v) sync confirmed to offset %v, current end: %v",
						topicInfo.GetTopicDesp(), chName, offset, currentEnd)
					err := ch.ConfirmBackendQueueOnSlave(nsqd.BackendOffset(offset.VOffset), offset.VCnt, offset.AllowBackward)
					if err != nil {
						coordLog.Warningf("topic %v update local channel(%v) offset %v failed: %v, current channel end: %v, topic end: %v",
							topicInfo.GetTopicDesp(), chName, offset, err, currentEnd, localTopic.TotalDataSize())
						if err == nsqd.ErrExiting {
							return &CoordErr{err.Error(), RpcNoErr, CoordTmpErr}
						}
						if err == nsqd.ErrReadQueueAlreadyCleaned {
							// mark as fix, and retry catchup as full
							localTopic.SetDataFixState(true)
						}
						return &CoordErr{err.Error(), RpcCommonErr, CoordSlaveErr}
					}
				}
				delete(oldChList, chName)
			}
			if len(oldChList) > 0 {
				coordLog.Infof("topic %v local channel not on leader: %v", topicInfo.GetTopicDesp(), oldChList)
				for chName, _ := range oldChList {
					localTopic.CloseExistingChannel(chName, true)
				}
			}
			err = localTopic.SaveChannelMeta()
			if err != nil {
				return &CoordErr{err.Error(), RpcNoErr, CoordTmpErr}
			}
			localTopic.ForceFlush()
		}
	}
	return nil
}

// Note:
// update epoch should be increased
// if write epoch keep unchanged, the leader should be the same and no new node in isr
// write epoch should be changed only when write is disabled
// Any new ISR or leader change should be allowed only when write is disabled
// any removal of isr or leader session change can be done without disable write
// if I am not in isr anymore, all update should be allowed
func (ncoord *NsqdCoordinator) checkUpdateState(topicCoord *coordData, writeDisabled bool, newData *TopicPartitionMetaInfo) *CoordErr {
	topicDesp := topicCoord.topicInfo.GetTopicDesp()
	if newData.EpochForWrite < topicCoord.topicInfo.EpochForWrite ||
		newData.Epoch < topicCoord.topicInfo.Epoch {
		coordLog.Infof("topic (%v) epoch should be increased: %v, %v", topicDesp,
			newData, topicCoord.topicInfo)
		return ErrEpochLessThanCurrent
	}
	if newData.EpochForWrite == topicCoord.topicInfo.EpochForWrite {
		if newData.Leader != topicCoord.topicInfo.Leader {
			coordLog.Warningf("topic (%v) write epoch should be changed since the leader is changed: %v, %v", topicDesp, newData, topicCoord.topicInfo)
			return ErrTopicCoordStateInvalid
		}
		if len(newData.ISR) > len(topicCoord.topicInfo.ISR) {
			coordLog.Warningf("topic (%v) write epoch should be changed since new node in isr: %v, %v",
				topicDesp, newData, topicCoord.topicInfo)
			return ErrTopicCoordStateInvalid
		}
		for _, nid := range newData.ISR {
			if FindSlice(topicCoord.topicInfo.ISR, nid) == -1 {
				coordLog.Warningf("topic %v write epoch should be changed since new node in isr: %v, %v",
					topicDesp, newData, topicCoord.topicInfo)
				return ErrTopicCoordStateInvalid
			}
		}
	}
	if writeDisabled {
		return nil
	}
	if FindSlice(newData.ISR, ncoord.myNode.GetID()) == -1 {
		return nil
	}
	if newData.EpochForWrite != topicCoord.topicInfo.EpochForWrite {
		return ErrTopicCoordStateInvalid
	}
	return nil
}

// new topic about ISR and catchups and leader info will be notified by nsqlookupd, each replica in topic
// should handle the topic info update based on the state of local node.
func (ncoord *NsqdCoordinator) updateTopicInfo(topicCoord *TopicCoordinator, shouldDisableWrite bool, newTopicInfo *TopicPartitionMetaInfo) *CoordErr {
	if atomic.LoadInt32(&ncoord.stopping) == 1 {
		return ErrClusterChanged
	}
	oldData := topicCoord.GetData()
	if oldData.topicInfo.Name == "" {
		coordLog.Infof("empty topic name not allowed")
		return ErrTopicArgError
	}
	if FindSlice(oldData.topicInfo.ISR, ncoord.myNode.GetID()) == -1 &&
		FindSlice(newTopicInfo.ISR, ncoord.myNode.GetID()) != -1 {
		coordLog.Infof("I am notified to be a new node in ISR: %v", ncoord.myNode.GetID())
		topicCoord.DisableWrite(true)
	}
	disableWrite := topicCoord.IsWriteDisabled()
	topicCoord.dataMutex.Lock()
	// if any of new node in isr or leader is changed, the write disabled should be set first on isr nodes.
	if checkErr := ncoord.checkUpdateState(topicCoord.coordData, disableWrite, newTopicInfo); checkErr != nil {
		coordLog.Warningf("topic (%v) check update failed : %v ",
			topicCoord.topicInfo.GetTopicDesp(), checkErr)
		topicCoord.dataMutex.Unlock()
		return checkErr
	}

	if topicCoord.IsExiting() {
		coordLog.Infof("update the topic info: %v while exiting.", oldData.topicInfo.GetTopicDesp())
		topicCoord.dataMutex.Unlock()
		return nil
	}

	// leader changed, old leader should release the leader lock to allow new leader acquire
	coordLog.Infof("update the topic info: %v", topicCoord.topicInfo.GetTopicDesp())
	if topicCoord.coordData.GetLeader() == ncoord.GetMyID() && newTopicInfo.Leader != ncoord.GetMyID() {
		coordLog.Infof("my leader should release: %v", topicCoord.coordData)
		ncoord.releaseTopicLeader(&topicCoord.coordData.topicInfo, &topicCoord.coordData.topicLeaderSession)
	}
	needAcquireLeaderSession := true
	if topicCoord.IsMineLeaderSessionReady(ncoord.myNode.GetID()) {
		needAcquireLeaderSession = false
		coordLog.Infof("leader keep unchanged: %v", newTopicInfo)
	} else if topicCoord.GetLeader() == ncoord.myNode.GetID() {
		coordLog.Infof("leader session not ready: %v", topicCoord.topicLeaderSession)
	}
	newCoordData := topicCoord.coordData.GetCopy()
	if topicCoord.topicInfo.Epoch != newTopicInfo.Epoch {
		newCoordData.topicInfo = *newTopicInfo
	}
	topicCoord.coordData = newCoordData
	topicCoord.dataMutex.Unlock()

	localTopic, err := ncoord.updateLocalTopic(newTopicInfo, topicCoord.GetData())
	if err != nil {
		coordLog.Warningf("init local topic failed: %v", err)
		ncoord.removeTopicCoord(newTopicInfo.Name, newTopicInfo.Partition, false)
		return err
	}

	if newTopicInfo.Leader == ncoord.myNode.GetID() {
		// not leader before and became new leader
		if oldData.GetLeader() != ncoord.myNode.GetID() {
			coordLog.Infof("I am notified to be leader for the topic.")
			// leader switch need disable write until the lookup notify leader
			// to accept write.
			shouldDisableWrite = true
		}
		if shouldDisableWrite {
			topicCoord.DisableWrite(true)
		}
		if needAcquireLeaderSession {
			go ncoord.acquireTopicLeader(newTopicInfo)
		}
	} else {
		if FindSlice(newTopicInfo.ISR, ncoord.myNode.GetID()) != -1 {
			coordLog.Infof("I am in isr list.")
		} else if FindSlice(newTopicInfo.CatchupList, ncoord.myNode.GetID()) != -1 {
			coordLog.Infof("I am in catchup list.")
			select {
			case ncoord.tryCheckUnsynced <- true:
			default:
			}
		}
	}
	ncoord.switchStateForMaster(topicCoord, localTopic, false)
	return nil
}

func (ncoord *NsqdCoordinator) notifyAcquireTopicLeader(coord *coordData) *CoordErr {
	if atomic.LoadInt32(&ncoord.stopping) == 1 {
		return ErrClusterChanged
	}
	coordLog.Infof("I am notified to acquire topic leader %v.", coord.topicInfo)
	go ncoord.acquireTopicLeader(&coord.topicInfo)
	return nil
}

func (ncoord *NsqdCoordinator) TryFixLocalTopic(topic string, pid int) error {
	topicCoord, err := ncoord.getTopicCoord(topic, pid)
	if err != nil {
		coordLog.Infof("topic coord not found %v-%v.", topic, pid)
		return err.ToErrorType()
	}
	tcData := topicCoord.GetData()
	localTopic, localErr := ncoord.localNsqd.GetExistingTopic(tcData.topicInfo.Name, tcData.topicInfo.Partition)
	if localErr != nil {
		coordLog.Infof("no topic on local: %v, %v", tcData.topicInfo.GetTopicDesp(), err)
		return localErr
	}
	localTopic.Lock()
	localErr = checkAndFixLocalLogQueueData(tcData, localTopic, tcData.logMgr, true)
	if tcData.delayedLogMgr != nil && !tcData.topicInfo.OrderedMulti {
		checkAndFixLocalLogQueueData(tcData, localTopic.GetDelayedQueue(), tcData.delayedLogMgr, true)
	}
	localTopic.Unlock()
	if localErr == nil {
		localTopic.SetDataFixState(false)
	}
	return nil
}

// handle all the things while leader is changed or isr is changed.
func (ncoord *NsqdCoordinator) switchStateForMaster(topicCoord *TopicCoordinator,
	localTopic *nsqd.Topic, lockedAndFixCommitDisk bool) *CoordErr {
	// flush topic data and channel consume data if any cluster topic info changed
	tcData := topicCoord.GetData()
	master := tcData.GetLeader() == ncoord.myNode.GetID()
	// leader changed (maybe down), we make sure out data is flushed to keep data safe
	localTopic.ForceFlush()
	tcData.flushCommitLogs()
	tcData.switchForMaster(master)
	if master {
		isWriteDisabled := topicCoord.IsWriteDisabled()
		localTopic.Lock()
		// lockedAndFixCommitDisk means we need make sure the commit log and disk queue is consistence
		// while enable the new leader (or old leader while leader not changed), we need make sure all data is consistence
		// we can not force fix data while not lockedAndFixCommitDisk because the write lock is not hold without it
		localErr := checkAndFixLocalLogQueueEnd(tcData, localTopic, tcData.logMgr,
			!isWriteDisabled && lockedAndFixCommitDisk, ForceFixLeaderData && lockedAndFixCommitDisk)
		if localErr != nil {
			atomic.StoreInt32(&topicCoord.disableWrite, 1)
			isWriteDisabled = true
			localTopic.SetDataFixState(true)
			localTopic.DisableForSlave(master)
		}
		if tcData.delayedLogMgr != nil && !tcData.topicInfo.OrderedMulti {
			localErr = checkAndFixLocalLogQueueEnd(tcData, localTopic.GetDelayedQueue(), tcData.delayedLogMgr,
				!isWriteDisabled && lockedAndFixCommitDisk, ForceFixLeaderData && lockedAndFixCommitDisk)
			if localErr != nil {
				atomic.StoreInt32(&topicCoord.disableWrite, 1)
				isWriteDisabled = true
				localTopic.GetDelayedQueue().SetDataFixState(true)
				localTopic.DisableForSlave(master)
			}
		}
		localTopic.Unlock()

		if !isWriteDisabled {
			ncoord.trySyncTopicChannels(tcData, false, true)
		}

		coordLog.Infof("current topic %v write state: %v",
			tcData.topicInfo.GetTopicDesp(), isWriteDisabled)
		if !isWriteDisabled && tcData.IsISRReadyForWrite(ncoord.myNode.GetID()) {
			// try fix channel consume count here, since the old version has not count info,
			// we need restore from commit log.
			localTopic.EnableForMaster()
			chs := localTopic.GetChannelMapCopy()
			for _, ch := range chs {
				confirmed := ch.GetConfirmed()
				if confirmed.Offset() > 0 && confirmed.TotalMsgCnt() <= 0 {
					l, offset, cnt, err := ncoord.SearchLogByMsgOffset(
						tcData.topicInfo.Name,
						tcData.topicInfo.Partition,
						int64(confirmed.Offset()))
					if err != nil {
						coordLog.Infof("search msg offset failed: %v", err)
					} else {
						coordLog.Warningf("try fix the channel %v confirmed queue info from %v to %v:%v, commitlog: %v",
							ch.GetName(), confirmed, offset, cnt, l)
						ch.SetConsumeOffset(nsqd.BackendOffset(offset), cnt, true)
					}
				}
			}
		} else {
			localTopic.DisableForSlave(master)
		}
	} else {
		logIndex, logOffset, logData, err := tcData.logMgr.GetLastCommitLogOffsetV2()
		if err != nil {
			if err != ErrCommitLogEOF {
				coordLog.Errorf("commit log is corrupted: %v", err)
			} else {
				coordLog.Infof("no commit last log data : %v", err)
			}
		} else {
			coordLog.Infof("current topic %v log: %v:%v, %v, pid: %v",
				tcData.topicInfo.GetTopicDesp(), logIndex, logOffset, logData, tcData.logMgr.GetLastCommitLogID())
		}
		localTopic.DisableForSlave(master)
	}
	offsetMap := make(map[string]ChannelConsumerOffset)
	tcData.consumeMgr.Lock()
	for chName, offset := range tcData.consumeMgr.channelConsumeOffset {
		offsetMap[chName] = offset
		coordLog.Infof("current channel %v offset: %v", chName, offset)
		delete(tcData.consumeMgr.channelConsumeOffset, chName)
	}
	tcData.consumeMgr.Unlock()
	// since the channel offset is asynced, we need sync here while leader is changed to make
	// sure all the channel state is synced finally.
	// it may not synced for the new create channel while the leader changed.
	for chName, offset := range offsetMap {
		ch, localErr := localTopic.GetExistingChannel(chName)
		if localErr != nil {
			offset.AllowBackward = true
			ch = localTopic.GetChannel(chName)
			coordLog.Infof("slave init the channel : %v, %v, offset: %v",
				tcData.topicInfo.GetTopicDesp(), chName, ch.GetConfirmed())
		}

		currentConfirmed := ch.GetConfirmed()
		if !offset.AllowBackward && (nsqd.BackendOffset(offset.VOffset) <= currentConfirmed.Offset()) {
			continue
		}
		currentEnd := ch.GetChannelEnd()
		if nsqd.BackendOffset(offset.VOffset) > currentEnd.Offset() {
			continue
		}
		err := ch.ConfirmBackendQueueOnSlave(nsqd.BackendOffset(offset.VOffset), offset.VCnt, offset.AllowBackward)
		if err != nil {
			coordLog.Infof("update local channel(%v) offset %v failed: %v, current channel end: %v, topic end: %v",
				chName, offset, err, currentEnd, localTopic.TotalDataSize())
		}
	}

	return nil
}

// the leader session will be updated if leader is lost or new leader is acquired success
func (ncoord *NsqdCoordinator) updateTopicLeaderSession(topicCoord *TopicCoordinator, newLS *TopicLeaderSession, joinSession string) *CoordErr {
	if atomic.LoadInt32(&ncoord.stopping) == 1 {
		return ErrClusterChanged
	}
	topicCoord.dataMutex.Lock()
	if newLS.LeaderEpoch < topicCoord.GetLeaderSessionEpoch() {
		topicCoord.dataMutex.Unlock()
		coordLog.Infof("topic partition leadership epoch error.")
		return ErrEpochLessThanCurrent
	}
	coordLog.Infof("update the topic %v leader session: %v", topicCoord.topicInfo.GetTopicDesp(), newLS)
	if newLS != nil && newLS.LeaderNode != nil && topicCoord.GetLeader() != newLS.LeaderNode.GetID() {
		coordLog.Infof("topic leader info not match leader session: %v", topicCoord.GetLeader())
		topicCoord.dataMutex.Unlock()
		return ErrTopicLeaderSessionInvalid
	}
	newCoordData := topicCoord.coordData.GetCopy()
	if newLS == nil {
		coordLog.Infof("leader session is lost for topic")
		newCoordData.topicLeaderSession = TopicLeaderSession{}
	} else if !topicCoord.topicLeaderSession.IsSame(newLS) {
		newCoordData.topicLeaderSession = *newLS
	}
	topicCoord.coordData = newCoordData
	topicCoord.dataMutex.Unlock()
	tcData := topicCoord.GetData()
	if topicCoord.IsExiting() {
		coordLog.Infof("update the topic info: %v while exiting.", tcData.topicInfo.GetTopicDesp())
		return nil
	}
	// if the write is disabled currently, we should make sure all isr is in synced
	mustSynced := topicCoord.IsWriteDisabled()

	localTopic, err := ncoord.localNsqd.GetExistingTopic(tcData.topicInfo.Name, tcData.topicInfo.Partition)
	if err != nil {
		coordLog.Infof("no topic on local: %v, %v", tcData.topicInfo.GetTopicDesp(), err)
		return ErrLocalMissingTopic
	}
	dyConf := &nsqd.TopicDynamicConf{SyncEvery: int64(tcData.topicInfo.SyncEvery),
		AutoCommit:               0,
		RetentionDay:             tcData.topicInfo.RetentionDay,
		OrderedMulti:             tcData.topicInfo.OrderedMulti,
		MultiPart:                tcData.topicInfo.MultiPart,
		Ext:                      tcData.topicInfo.Ext,
		DisableChannelAutoCreate: tcData.topicInfo.DisableChannelAutoCreate,
	}
	tcData.updateBufferSize(int(dyConf.SyncEvery - 1))
	localTopic.SetDynamicInfo(*dyConf, tcData.logMgr)
	// leader changed (maybe down), we make sure out data is flushed to keep data safe
	ncoord.switchStateForMaster(topicCoord, localTopic, false)

	coordLog.Infof("topic leader session: %v", tcData.topicLeaderSession)
	if tcData.IsMineLeaderSessionReady(ncoord.myNode.GetID()) {
		coordLog.Infof("I become the leader for the topic: %v", tcData.topicInfo.GetTopicDesp())
	} else {
		if newLS == nil || newLS.LeaderNode == nil || newLS.Session == "" {
			coordLog.Infof("topic leader is missing : %v", tcData.topicInfo.GetTopicDesp())
			if tcData.GetLeader() == ncoord.myNode.GetID() {
				go ncoord.acquireTopicLeader(&tcData.topicInfo)
			}
		} else {
			coordLog.Infof("topic %v leader changed to :%v. epoch: %v", tcData.topicInfo.GetTopicDesp(), newLS.LeaderNode.GetID(), newLS.LeaderEpoch)
			// if catching up, pull data from the new leader
			// if isr, make sure sync to the new leader
			if FindSlice(tcData.topicInfo.ISR, ncoord.myNode.GetID()) != -1 {
				coordLog.Infof("I am in isr while update leader session.")
				go ncoord.syncToNewLeader(tcData, joinSession, mustSynced)
			} else if FindSlice(tcData.topicInfo.CatchupList, ncoord.myNode.GetID()) != -1 {
				coordLog.Infof("I am in catchup while update leader session.")
				select {
				case ncoord.tryCheckUnsynced <- true:
				default:
				}
			} else {
				coordLog.Infof("I am not relevant while update leader session.")
			}
		}
	}
	return nil
}

// since only one master is allowed on the same topic, we can get it.
func (ncoord *NsqdCoordinator) GetMasterTopicCoordData(topic string) (int, *coordData, error) {
	ncoord.coordMutex.RLock()
	defer ncoord.coordMutex.RUnlock()
	if v, ok := ncoord.topicCoords[topic]; ok {
		for pid, tc := range v {
			tcData := tc.GetData()
			if tcData.GetLeader() == ncoord.myNode.GetID() {
				return pid, tcData, nil
			}
		}
	}
	return -1, nil, ErrMissingTopicCoord.ToErrorType()
}

func (ncoord *NsqdCoordinator) isMeISR(tinfo *TopicPartitionMetaInfo) bool {
	return FindSlice(tinfo.ISR, ncoord.GetMyID()) != -1
}

func (ncoord *NsqdCoordinator) isMeCatchup(tinfo *TopicPartitionMetaInfo) bool {
	return FindSlice(tinfo.CatchupList, ncoord.GetMyID()) != -1
}

func (ncoord *NsqdCoordinator) GetTopicMetaInfo(topic string) (TopicMetaInfo, error) {
	meta, _, err := ncoord.leadership.GetTopicMetaInfo(topic)
	return meta, err
}

func (ncoord *NsqdCoordinator) GetTopicInfo(topic string, partition int) (*TopicPartitionMetaInfo, error) {
	meta, err := ncoord.leadership.GetTopicInfo(topic, partition)
	return meta, err
}

func (ncoord *NsqdCoordinator) TryCleanUnusedTopicOnLocal(topic string, partition int, dryRun bool) error {
	if ncoord.leadership == nil {
		return nil
	}
	c, _ := ncoord.getTopicCoord(topic, partition)
	if c != nil {
		return nil
	}
	tinfo, err := ncoord.leadership.GetTopicInfo(topic, partition)
	if err != nil {
		if err == ErrKeyNotFound {
			// we continue check if local exist
		} else {
			return err
		}
	} else {
		if ncoord.isMeISR(tinfo) || ncoord.isMeCatchup(tinfo) {
			return nil
		}
	}

	coordLog.Infof("removing topic data: %v-%v", topic, partition)
	if dryRun {
		return nil
	}
	coordErr := ncoord.forceCleanTopicData(topic, partition)
	if coordErr != nil {
		return coordErr.ToErrorType()
	}
	return nil
}

func (ncoord *NsqdCoordinator) getTopicCoordData(topic string, partition int) (*coordData, *CoordErr) {
	c, err := ncoord.getTopicCoord(topic, partition)
	if err != nil {
		return nil, err
	}
	return c.GetData(), nil
}

func (ncoord *NsqdCoordinator) getTopicCoord(topic string, partition int) (*TopicCoordinator, *CoordErr) {
	ncoord.coordMutex.RLock()
	defer ncoord.coordMutex.RUnlock()
	if v, ok := ncoord.topicCoords[topic]; ok {
		if topicCoord, ok := v[partition]; ok {
			return topicCoord, nil
		}
	}
	coordErrStats.incTopicCoordMissingErr()
	return nil, ErrMissingTopicCoord
}

func (ncoord *NsqdCoordinator) removeTopicCoord(topic string, partition int, removeData bool) (*TopicCoordinator, *CoordErr) {
	var topicCoord *TopicCoordinator
	ncoord.coordMutex.Lock()
	if v, ok := ncoord.topicCoords[topic]; ok {
		if tc, ok := v[partition]; ok {
			topicCoord = tc
			delete(v, partition)
		}
	}
	var err *CoordErr
	if topicCoord == nil {
		err = ErrMissingTopicCoord
	} else {
		// should protected by coord lock to avoid recreated topic coord while deleting.
		// Which will cause two topic coord use the commit writer
		coordLog.Infof("removing topic coodinator: %v-%v", topic, partition)
		topicCoord.DeleteWithLock(removeData)
		err = nil
	}
	ncoord.coordMutex.Unlock()
	if removeData {
		coordLog.Infof("removing topic data: %v-%v", topic, partition)
		// check if any data on local and try remove
		ncoord.forceCleanTopicData(topic, partition)
	}
	return topicCoord, err
}

func (ncoord *NsqdCoordinator) SyncTopicChannels(topicName string, part int) error {
	tcData, err := ncoord.getTopicCoordData(topicName, part)
	if err != nil {
		return err.ToErrorType()
	}
	ncoord.trySyncTopicChannelList(tcData, false)
	return nil
}

func (ncoord *NsqdCoordinator) doRpcForNodeList(nodes []string, failBreak bool, rpcFunc func(*NsqdRpcClient) *CoordErr) *CoordErr {
	var lastErr *CoordErr
	myid := ncoord.myNode.GetID()
	for _, nodeID := range nodes {
		if nodeID == myid {
			continue
		}
		c, rpcErr := ncoord.acquireRpcClient(nodeID)
		if rpcErr != nil {
			coordLog.Infof("node %v get rpc client failed %v.", nodeID, rpcErr)
			lastErr = rpcErr
			if failBreak {
				break
			}
			continue
		}
		rpcErr = rpcFunc(c)
		if rpcErr != nil {
			lastErr = rpcErr
			if failBreak {
				break
			}
		}
	}
	return lastErr
}

// sync topic channels state period.
func (ncoord *NsqdCoordinator) trySyncTopicChannelList(tcData *coordData, notifyOnly bool) {
	if tcData.GetLeader() != ncoord.myNode.GetID() {
		return
	}
	localTopic, _ := ncoord.localNsqd.GetExistingTopic(tcData.topicInfo.Name, tcData.topicInfo.Partition)
	if localTopic == nil {
		return
	}
	channels := localTopic.GetChannelMapCopy()
	chNameList := make([]string, 0, len(channels))
	for _, ch := range channels {
		if ch.IsEphemeral() {
			continue
		}
		chNameList = append(chNameList, ch.GetName())
	}
	if !isSameStrList(chNameList, tcData.syncedConsumeMgr.GetSyncedChs()) {
		tcData.syncedConsumeMgr.Clear()
		rpcErr := ncoord.doRpcForNodeList(tcData.topicInfo.ISR, false, func(c *NsqdRpcClient) *CoordErr {
			if notifyOnly {
				c.NotifyChannelList(&tcData.topicLeaderSession, &tcData.topicInfo, chNameList)
			} else {
				return c.UpdateChannelList(&tcData.topicLeaderSession, &tcData.topicInfo, chNameList)
			}
			return nil
		})
		if rpcErr == nil && !notifyOnly {
			tcData.syncedConsumeMgr.UpdateSyncedChs(chNameList)
		}
	}
}

// sync topic channels state period.
func (ncoord *NsqdCoordinator) trySyncTopicChannels(tcData *coordData, syncChannelList bool, notifyOnly bool) {
	localTopic, _ := ncoord.localNsqd.GetExistingTopic(tcData.topicInfo.Name, tcData.topicInfo.Partition)
	if localTopic == nil {
		return
	}
	if syncChannelList {
		ncoord.trySyncTopicChannelList(tcData, notifyOnly)
	}

	lastChangedTs, ok := localTopic.GetDelayedQueueUpdateTs()
	if ok && (lastChangedTs == 0 || tcData.syncedConsumeMgr.GetSyncedDelayedQueueTs() != lastChangedTs) {
		ts, keyList, cntList, channelCntList := localTopic.GetDelayedQueueConsumedState()
		if keyList == nil && cntList == nil && channelCntList == nil {
			// no delayed queue
		} else {
			waitRsp := !notifyOnly
			rpcErr := ncoord.doRpcForNodeList(tcData.topicInfo.ISR, false, func(c *NsqdRpcClient) *CoordErr {
				rpcErr := c.UpdateDelayedQueueState(&tcData.topicLeaderSession, &tcData.topicInfo,
					"", ts, keyList, cntList, channelCntList, waitRsp)
				if rpcErr != nil {
					coordLog.Infof("node %v update delayed queue state %v failed %v.", c.remote,
						tcData.topicInfo.GetTopicDesp(), rpcErr)
				}
				return rpcErr
			})
			if rpcErr == nil && !notifyOnly {
				tcData.syncedConsumeMgr.UpdateSyncedDelayedQueueTs(lastChangedTs)
			}
		}
	}

	channels := localTopic.GetChannelMapCopy()
	var syncOffset ChannelConsumerOffset
	syncOffset.Flush = true
	// always allow backward here since it may happen that slave already created the channel with end
	syncOffset.AllowBackward = true
	for _, ch := range channels {
		// skipped ordered channel will not sync offset,
		// so we need sync here
		if ch.IsOrdered() && !ch.IsSkipped() {
			continue
		}
		if ch.IsEphemeral() {
			continue
		}
		confirmed := ch.GetConfirmed()
		// try fix message count here, since old version has no count info.
		if confirmed.Offset() > 0 && confirmed.TotalMsgCnt() <= 0 {
			l, offset, cnt, err := ncoord.SearchLogByMsgOffset(tcData.topicInfo.Name, tcData.topicInfo.Partition, int64(confirmed.Offset()))
			if err != nil {
				coordLog.Infof("search msg offset failed: %v", err)
			} else {
				coordLog.Infof("try fix the channel %v confirmed queue info from %v to %v:%v, commitlog: %v",
					ch.GetName(), confirmed, offset, cnt, l)
				ch.SetConsumeOffset(nsqd.BackendOffset(offset), cnt, true)
				time.Sleep(time.Millisecond * 10)
				confirmed = ch.GetConfirmed()
			}
		}

		syncOffset.VOffset = int64(confirmed.Offset())
		syncOffset.VCnt = confirmed.TotalMsgCnt()
		if ch.GetConfirmedIntervalLen() > 100 {
			syncOffset.NeedUpdateConfirmed = false
			syncOffset.ConfirmedInterval = nil
		} else {
			syncOffset.ConfirmedInterval = ch.GetConfirmedInterval()
			syncOffset.NeedUpdateConfirmed = true
		}

		cco, ok := tcData.syncedConsumeMgr.Get(ch.GetName())
		if ok && cco.IsSame(&syncOffset) {
			continue
		}
		// Because the new channel on slave will init consume offset to the end of topic,
		// we should update consume offset to both isr and catchup to avoid skip some messages on the new channel on the slave
		// if the slave became the new leader.
		// But the catchup node may fail to connect, so we should avoid block by catchup nodes.
		rpcErr := ncoord.doRpcForNodeList(tcData.topicInfo.ISR, false, func(c *NsqdRpcClient) *CoordErr {
			if notifyOnly {
				c.NotifyUpdateChannelOffset(&tcData.topicLeaderSession, &tcData.topicInfo, ch.GetName(), syncOffset)
			} else {
				rpcErr := c.UpdateChannelOffset(&tcData.topicLeaderSession, &tcData.topicInfo, ch.GetName(), syncOffset)
				if rpcErr != nil {
					coordLog.Debugf("node %v update channel %v offset failed %v.", c.remote, ch.GetName(), rpcErr)
				}
				return rpcErr
			}
			return nil
		})
		if rpcErr == nil && !notifyOnly {
			tcData.syncedConsumeMgr.Update(ch.GetName(), syncOffset)
		}
		ncoord.doRpcForNodeList(tcData.topicInfo.CatchupList, false, func(c *NsqdRpcClient) *CoordErr {
			c.NotifyUpdateChannelOffset(&tcData.topicLeaderSession, &tcData.topicInfo, ch.GetName(), syncOffset)
			return nil
		})
		// only the first channel of topic should flush.
		syncOffset.Flush = false
	}
}

// read topic raw data used for catchup
func (ncoord *NsqdCoordinator) readTopicRawData(topic string, partition int, offsetList []int64,
	sizeList []int32, fromDelayedQueue bool) ([][]byte, *CoordErr) {
	//read directly from local topic data used for pulling data by replicas
	t, err := ncoord.localNsqd.GetExistingTopic(topic, partition)
	if err != nil {
		return nil, ErrLocalMissingTopic
	}
	if t.GetTopicPart() != partition {
		return nil, ErrLocalTopicPartitionMismatch
	}
	dataList := make([][]byte, 0, len(offsetList))
	snap := t.GetDiskQueueSnapshot(true)
	if fromDelayedQueue {
		dq := t.GetDelayedQueue()
		if dq == nil {
			return nil, ErrLocalDelayedQueueMissing
		}
		snap = dq.GetDiskQueueSnapshot(true)
	}
	for i, offset := range offsetList {
		size := sizeList[i]
		err = snap.SeekTo(nsqd.BackendOffset(offset), 0)
		if err != nil {
			coordLog.Infof("read topic %v data at offset %v, size: %v, error: %v", t.GetFullName(), offset, size, err)
			break
		}
		var buf []byte
		buf, err = snap.ReadRaw(size)
		if err != nil {
			coordLog.Infof("read topic data at offset %v, size:%v(actual: %v), error: %v", offset, size, len(buf), err)
			if err == io.EOF {
				if len(dataList) > 0 {
					// we can ignore EOF if we already read some data
					err = nil
				} else {
					return dataList, ErrLocalCommitDataMismatchQueueDataEOF
				}
			}
			break
		}
		dataList = append(dataList, buf)
	}
	if err != nil {
		return dataList, &CoordErr{err.Error(), RpcCommonErr, CoordLocalErr}
	}
	return dataList, nil
}

func (ncoord *NsqdCoordinator) pullCommitLogsAndData(req *RpcPullCommitLogsReq, fromDelayed bool) (*RpcPullCommitLogsRsp, error) {
	var ret RpcPullCommitLogsRsp
	tcData, err := ncoord.getTopicCoordData(req.TopicName, req.TopicPartition)
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
	if req.UseCountIndex {
		newFileNum, newOffset, localErr := logMgr.ConvertToOffsetIndex(req.LogCountNumIndex)
		if localErr != nil {
			coordLog.Warningf("topic %v failed to convert to offset index: %v, err:%v",
				req.TopicName, req.LogCountNumIndex, localErr)
			if localErr != ErrCommitLogEOF {
				return nil, localErr
			}
		}
		req.StartIndexCnt = newFileNum
		req.StartLogOffset = newOffset
	}
	ret.Logs, localErr = logMgr.GetCommitLogsV2(req.StartIndexCnt, req.StartLogOffset, req.LogMaxNum)
	if localErr != nil {
		if localErr != ErrCommitLogEOF {
			return nil, localErr
		}
	}
	offsetList := make([]int64, len(ret.Logs))
	sizeList := make([]int32, len(ret.Logs))
	totalSize := int32(0)
	for i, l := range ret.Logs {
		offsetList[i] = l.MsgOffset
		sizeList[i] = l.MsgSize
		totalSize += l.MsgSize
		// note: this should be large than the max message body size
		if totalSize > MAX_LOG_PULL_BYTES {
			coordLog.Warningf("pulling too much log data at one time: %v, %v", totalSize, i)
			offsetList = offsetList[:i]
			sizeList = sizeList[:i]
			break
		}
	}

	ret.DataList, err = ncoord.readTopicRawData(tcData.topicInfo.Name,
		tcData.topicInfo.Partition, offsetList, sizeList, fromDelayed)
	ret.Logs = ret.Logs[:len(ret.DataList)]
	if err != nil {
		coordLog.Infof("pull log data read failed : %v, %v, %v", err, offsetList, sizeList)
		return nil, err.ToErrorType()
	}
	return &ret, nil
}

// flush cached data to disk. This should be called when topic isr list
// changed or leader changed.
func (ncoord *NsqdCoordinator) notifyFlushData(topic string, partition int) {
	select {
	case ncoord.flushNotifyChan <- TopicPartitionID{topic, partition}:
	default:
	}
}

func (ncoord *NsqdCoordinator) updateLocalTopic(topicInfo *TopicPartitionMetaInfo, tcData *coordData) (*nsqd.Topic, *CoordErr) {
	// check topic exist and prepare on local.
	t := ncoord.localNsqd.GetTopicWithDisabled(topicInfo.Name, topicInfo.Partition, topicInfo.Ext, topicInfo.OrderedMulti, topicInfo.DisableChannelAutoCreate)
	if t == nil {
		return nil, ErrLocalInitTopicFailed
	}
	dyConf := &nsqd.TopicDynamicConf{SyncEvery: int64(topicInfo.SyncEvery),
		AutoCommit:               0,
		RetentionDay:             topicInfo.RetentionDay,
		OrderedMulti:             topicInfo.OrderedMulti,
		MultiPart:                topicInfo.MultiPart,
		Ext:                      topicInfo.Ext,
		DisableChannelAutoCreate: topicInfo.DisableChannelAutoCreate,
	}
	t.SetDynamicInfo(*dyConf, tcData.logMgr)
	//sync channels when disable channel auto create is true
	if topicInfo.DisableChannelAutoCreate {
		newChannels := make(map[string]bool)
		for i := range topicInfo.Channels {
			t.GetChannel(topicInfo.Channels[i])
			newChannels[topicInfo.Channels[i]] = true
		}
		chMeta := t.GetChannelMeta()
		for k, _ := range chMeta {
			if !newChannels[chMeta[k].Name] {
				t.DeleteExistingChannel(chMeta[k].Name)
				coordLog.Infof("ch %v deleted", chMeta[k].Name)
			}
		}
	}

	if t.IsDataNeedFix() {
		t.Lock()
		endFixErr := checkAndFixLocalLogQueueEnd(tcData, t, tcData.logMgr, true, ForceFixLeaderData)
		t.Unlock()
		if endFixErr != nil {
			t.SetDataFixState(true)
		}
	}
	localErr := ncoord.localNsqd.SetTopicMagicCode(t, topicInfo.MagicCode)
	if localErr != nil {
		return t, ErrLocalInitTopicFailed
	}

	tcData.updateBufferSize(int(dyConf.SyncEvery - 1))
	localErr = maybeInitDelayedQ(tcData, t)
	if localErr != nil {
		return t, ErrLocalInitTopicFailed
	}

	return t, nil
}

// before shutdown, we transfer the leader to others to reduce
// the unavailable time.
func (ncoord *NsqdCoordinator) prepareLeavingCluster() {
	coordLog.Infof("I am prepare leaving the cluster.")
	tmpTopicCoords := make(map[string]map[int]*TopicCoordinator, len(ncoord.topicCoords))
	ncoord.getAllCoords(tmpTopicCoords)
	for topicName, topicData := range tmpTopicCoords {
		for pid, tpCoord := range topicData {
			tcData := tpCoord.GetData()
			localTopic, err := ncoord.localNsqd.GetExistingTopic(topicName, pid)
			if err != nil {
				coordLog.Infof("no local topic")
				continue
			}

			if FindSlice(tcData.topicInfo.ISR, ncoord.myNode.GetID()) == -1 {
				tpCoord.Exiting()
				localTopic.PrintCurrentStats()
				localTopic.ForceFlush()
				tcData.flushCommitLogs()
				continue
			}
			if len(tcData.topicInfo.ISR)-1 <= tcData.topicInfo.Replica/2 {
				coordLog.Infof("The isr nodes in topic %v is not enough while leaving: %v",
					tpCoord.topicInfo.GetTopicDesp(), tpCoord.topicInfo.ISR)
			}

			tpCoord.Exiting()
			if tcData.GetLeader() == ncoord.myNode.GetID() {
				ncoord.trySyncTopicChannels(tcData, false, true)
			}
			// TODO: if we release leader first, we can not transfer the leader properly,
			// if we leave isr first, we would get the state that the leader not in isr
			// wait lookup choose new node for isr/leader
			retry := 2
			for retry > 0 {
				retry--
				err := ncoord.requestLeaveFromISRFast(topicName, pid)
				if err == nil {
					break
				}
				if err != nil && err.IsEqual(ErrLeavingISRWait) {
					coordLog.Infof("======= should wait leaving from isr: %v", topicName)
				} else {
					coordLog.Infof("======= topic %v request leave isr failed: %v", topicName, err)
					break
				}
				time.Sleep(time.Millisecond)
			}

			if tcData.IsMineLeaderSessionReady(ncoord.GetMyID()) {
				// leader
				ncoord.leadership.ReleaseTopicLeader(topicName, pid, &tcData.topicLeaderSession)
				coordLog.Infof("The leader for topic %v is transferred.", tcData.topicInfo.GetTopicDesp())
			}

			localTopic.PrintCurrentStats()
			localTopic.ForceFlush()
			tcData.flushCommitLogs()
			localTopic.Close()
		}
	}
	coordLog.Infof("prepare leaving finished.")
	if ncoord.leadership != nil {
		atomic.StoreInt32(&ncoord.stopping, 1)
		ncoord.leadership.UnregisterNsqd(&ncoord.myNode)
	}
}

func (ncoord *NsqdCoordinator) Stats(topic string, part int) *CoordStats {
	s := &CoordStats{}
	if ncoord.rpcServer != nil && ncoord.rpcServer.rpcServer != nil {
		s.RpcStats = ncoord.rpcServer.rpcServer.Stats.Snapshot()
	}
	s.ErrStats = *coordErrStats.GetCopy()
	s.TopicCoordStats = make([]TopicCoordStat, 0)
	if len(topic) == 0 {
		return s
	}
	if part >= 0 {
		tcData, err := ncoord.getTopicCoordData(topic, part)
		if err != nil {
		} else {
			var stat TopicCoordStat
			stat.Name = topic
			stat.Partition = part
			for _, nid := range tcData.topicInfo.ISR {
				stat.ISRStats = append(stat.ISRStats, ISRStat{HostName: "", NodeID: nid})
			}
			for _, nid := range tcData.topicInfo.CatchupList {
				stat.CatchupStats = append(stat.CatchupStats, CatchupStat{HostName: "", NodeID: nid, Progress: 0})
			}
			s.TopicCoordStats = append(s.TopicCoordStats, stat)
		}
	} else {
		ncoord.coordMutex.RLock()
		v, _ := ncoord.topicCoords[topic]
		for _, tc := range v {
			var stat TopicCoordStat
			stat.Name = topic
			stat.Partition = tc.topicInfo.Partition
			for _, nid := range tc.topicInfo.ISR {
				stat.ISRStats = append(stat.ISRStats, ISRStat{HostName: "", NodeID: nid})
			}
			for _, nid := range tc.topicInfo.CatchupList {
				stat.CatchupStats = append(stat.CatchupStats, CatchupStat{HostName: "", NodeID: nid, Progress: 0})
			}

			s.TopicCoordStats = append(s.TopicCoordStats, stat)
		}
		ncoord.coordMutex.RUnlock()
	}
	return s
}
