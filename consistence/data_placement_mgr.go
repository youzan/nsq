package consistence

import (
	"container/heap"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/twmb/murmur3"

	"github.com/youzan/nsq/nsqd"
)

// new topic can have an advice load factor to give the suggestion about the
// future load. While the topic data is small the load compute is not precise,
// so we use the advised load to determine the actual topic load.
// use 1~10 to advise from lowest to highest. The 1 or 10 should be used with much careful.

// TODO: separate the high peak topics to avoid too much high peak on node

// left consume data size level: 0~1GB low, < 5GB medium, < 50GB high , >=50GB very high
// left data size level: <5GB low, <50GB medium , <200GB high, >=200GB very high

// for common topic we do not allow the partition large than nodes,
// To balance the load across nodes, we make the leader across each nodes from
// the idle node to the busy node.
// and to avoid the data migration, we should keep the data as much as possible
// algorithm as below:
// 1. sort node id by the load factor;
// 2. sort the topic partitions
// 3. choose the leader for each partition (ignore the partition already assigned)
// start from the begin index of the current available node array
// 4. choose the followers for each partition,
// start from the begin index of node array and filter out the already assigned node
// for the other partitions.
//  l -> leader, f-> follower
//         nodeA   nodeB   nodeC   nodeD
// p1       l       f        f
// p2               l        f      f
// p3       f                l      f

// after nodeB is down, keep the data no move, and
// choose the node for the missing leader from isr list (filter out the node which
// is assigned to other partition leaders) with the lowest load factor.
// if alive nodes less than partitions we will fail to elect the new leader
// and choose the missing isr with the lowest load node from current alive nodes (filter out the
// node already exist other partitions)
//         nodeA   xxxx   nodeC   nodeD
// p1       l       x        f     x-f
// p2      x-f      x        f     f-l
// p3       f                l      f

var (
	ErrBalanceNodeUnavailable     = errors.New("can not find a node to be balanced")
	ErrNodeIsExcludedForTopicData = errors.New("destination node is excluded for topic")
	ErrClusterBalanceRunning      = errors.New("another balance is running, should wait")
	errMoveTopicWaitTimeout       = errors.New("timeout waiting move topic")
)

var topicTopNLimit = 100
var topNBalanceDiff = 3
var moveWaitTimeout = time.Minute * 10

const (
	RATIO_BETWEEN_LEADER_FOLLOWER = 0.7
	defaultTopicLoadFactor        = 3
	HIGHEST_PUB_QPS_LEVEL         = 100
	HIGHEST_LEFT_CONSUME_MB_SIZE  = 50 * 1024
	HIGHEST_LEFT_DATA_MB_SIZE     = 200 * 1024
	busyTopicLevel                = 15
)

type balanceOpLevel int

const (
	moveAny balanceOpLevel = iota
	moveTryIdle
	moveMinLFOnly
)

// pub qps level : 1~13 low (< 30 KB/sec), 13 ~ 31 medium (<1000 KB/sec), 31 ~ 74 high (<8 MB/sec), >74 very high (>8 MB/sec)
func convertQPSLevel(hourlyPubSize int64) float64 {
	qps := hourlyPubSize / 3600 / 1024
	if qps <= 3 {
		return 1.0 + float64(qps)
	} else if qps <= 30 {
		return 4.0 + float64(qps-3)/3
	} else if qps <= 300 {
		return 13.0 + float64(qps-30)/15
	} else if qps <= 1000 {
		return 31.0 + float64(qps-300)/35
	} else if qps <= 2000 {
		return 51.0 + math.Log2(1.0+float64(qps-1000))
	} else if qps <= 8000 {
		return 61.0 + math.Log2(1.0+float64(qps-2000))
	} else if qps <= 16000 {
		return 74.0 + math.Log2(1.0+float64(qps-8000))
	}
	return HIGHEST_PUB_QPS_LEVEL
}

func splitTopicPartitionID(topicFullName string) (string, int, error) {
	partIndex := strings.LastIndex(topicFullName, "-")
	if partIndex == -1 {
		return "", 0, fmt.Errorf("invalid topic full name: %v", topicFullName)
	}
	topicName := topicFullName[:partIndex]
	partitionID, err := strconv.Atoi(topicFullName[partIndex+1:])
	return topicName, partitionID, err
}

// An IntHeap is a min-heap of ints.
type IntHeap []int

func (h IntHeap) Len() int           { return len(h) }
func (h IntHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h IntHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *IntHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(int))
}

func (h *IntHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type WrapChannelConsumerOffset struct {
	Name string
	ChannelConsumerOffset
}

type cachedNodeTopicStats struct {
	stats    *NodeTopicStats
	lastTime time.Time
}

type NodeTopicStats struct {
	NodeID string
	// the data (MB) need to be consumed on the leader for all channels in the topic.
	ChannelDepthData map[string]int64
	// the data left on disk. unit: MB
	TopicLeaderDataSize map[string]int64
	TopicTotalDataSize  map[string]int64
	NodeCPUs            int

	// the pub stat for past 24hr
	TopicHourlyPubDataList map[string][24]int64
	ChannelNum             map[string]int
	ChannelList            map[string][]string
	ChannelMetas           map[string][]nsqd.ChannelMetaInfo
	ChannelOffsets         map[string][]WrapChannelConsumerOffset
}

func getNodeNameList(nodes []NodeTopicStats) []string {
	nameList := make([]string, 0, len(nodes))
	for _, s := range nodes {
		nameList = append(nameList, s.NodeID)
	}
	return nameList
}

func getTopicInfoMap(topicList []TopicPartitionMetaInfo) map[string]TopicPartitionMetaInfo {
	tinfoMap := make(map[string]TopicPartitionMetaInfo, len(topicList))
	for _, tinfo := range topicList {
		tinfoMap[tinfo.GetTopicDesp()] = tinfo
	}
	return tinfoMap
}

func NewNodeTopicStats(nid string, cap int, cpus int) *NodeTopicStats {
	return &NodeTopicStats{
		NodeID:                 nid,
		ChannelDepthData:       make(map[string]int64, cap),
		TopicLeaderDataSize:    make(map[string]int64, cap),
		TopicTotalDataSize:     make(map[string]int64, cap),
		TopicHourlyPubDataList: make(map[string][24]int64, cap),
		ChannelNum:             make(map[string]int, cap),
		ChannelList:            make(map[string][]string),
		ChannelMetas:           make(map[string][]nsqd.ChannelMetaInfo),
		ChannelOffsets:         make(map[string][]WrapChannelConsumerOffset),
		NodeCPUs:               cpus,
	}
}

// the load factor is something like cpu load factor that
// stand for the busy/idle state for this node.
// the larger means busier.
// 80% recent avg load in 24hr + 10% left need to be consumed + 10% data size left

func (nts *NodeTopicStats) GetNodeLoadFactor() (float64, float64) {
	leaderLF := nts.GetNodeLeaderLoadFactor()
	totalDataSize := int64(0)
	for _, v := range nts.TopicTotalDataSize {
		totalDataSize += v
	}
	totalDataSize += int64(len(nts.TopicTotalDataSize))
	if totalDataSize > HIGHEST_LEFT_DATA_MB_SIZE {
		totalDataSize = HIGHEST_LEFT_DATA_MB_SIZE
	}
	return leaderLF, leaderLF + float64(totalDataSize)/HIGHEST_LEFT_DATA_MB_SIZE*5
}

func (nts *NodeTopicStats) GetNodeLeaderLoadFactor() float64 {
	leftConsumed := int64(0)
	for _, t := range nts.ChannelDepthData {
		leftConsumed += t
	}
	leftConsumed += int64(len(nts.ChannelDepthData))
	if leftConsumed > HIGHEST_LEFT_CONSUME_MB_SIZE {
		leftConsumed = HIGHEST_LEFT_CONSUME_MB_SIZE
	}
	totalLeaderDataSize := int64(0)
	for _, v := range nts.TopicLeaderDataSize {
		totalLeaderDataSize += v
	}
	totalLeaderDataSize += int64(len(nts.TopicLeaderDataSize))
	if totalLeaderDataSize > HIGHEST_LEFT_DATA_MB_SIZE {
		totalLeaderDataSize = HIGHEST_LEFT_DATA_MB_SIZE
	}
	avgWrite := nts.GetNodeAvgWriteLevel()
	if avgWrite > HIGHEST_PUB_QPS_LEVEL {
		avgWrite = HIGHEST_PUB_QPS_LEVEL
	}

	return avgWrite/HIGHEST_PUB_QPS_LEVEL*80.0 + float64(leftConsumed)/HIGHEST_LEFT_CONSUME_MB_SIZE*10.0 + float64(totalLeaderDataSize)/HIGHEST_LEFT_DATA_MB_SIZE*10.0
}

func (nts *NodeTopicStats) GetNodePeakLevelList() []int64 {
	levelList := make([]int64, 8)
	for _, dataList := range nts.TopicHourlyPubDataList {
		peak := int64(10)
		for _, data := range dataList {
			if data > peak {
				peak = data
			}
		}
		index := int(convertQPSLevel(peak))
		if index > HIGHEST_PUB_QPS_LEVEL {
			index = HIGHEST_PUB_QPS_LEVEL
		}
		levelList[index]++
	}
	return levelList
}

func (nts *NodeTopicStats) GetNodeAvgWriteLevel() float64 {
	level := int64(0)
	tmp := make(IntHeap, 0, 24)
	for _, dataList := range nts.TopicHourlyPubDataList {
		sum := int64(10)
		cnt := 0
		tmp = tmp[:0]
		heap.Init(&tmp)
		for _, data := range dataList {
			sum += data
			cnt++
			heap.Push(&tmp, int(data))
		}
		// remove the lowest 1/4 (at midnight all topics are low)
		for i := 0; i < len(dataList)/4; i++ {
			v := heap.Pop(&tmp)
			sum -= int64(v.(int))
			cnt--
		}
		sum = sum / int64(cnt)
		level += sum
	}
	return convertQPSLevel(level)
}

func (nts *NodeTopicStats) GetNodeAvgReadLevel() float64 {
	level := float64(0)
	tmp := make(IntHeap, 0, 24)
	for topicName, dataList := range nts.TopicHourlyPubDataList {
		sum := int64(10)
		cnt := 0
		tmp = tmp[:0]
		heap.Init(&tmp)
		for _, data := range dataList {
			sum += data
			cnt++
			heap.Push(&tmp, int(data))
		}
		for i := 0; i < len(dataList)/4; i++ {
			v := heap.Pop(&tmp)
			sum -= int64(v.(int))
			cnt--
		}

		sum = sum / int64(cnt)
		num, ok := nts.ChannelNum[topicName]
		if ok {
			level += math.Log2(1.0+float64(num)) * float64(sum)
		}
	}
	return convertQPSLevel(int64(level))
}

func (nts *NodeTopicStats) GetTopicLoadFactor(topicFullName string) float64 {
	data := nts.TopicTotalDataSize[topicFullName]
	if data > HIGHEST_LEFT_DATA_MB_SIZE {
		data = HIGHEST_LEFT_DATA_MB_SIZE
	}
	return nts.GetTopicLeaderLoadFactor(topicFullName) + float64(data)/HIGHEST_LEFT_DATA_MB_SIZE*5.0
}

func (nts *NodeTopicStats) GetTopicLeaderLoadFactor(topicFullName string) float64 {
	writeLevel := nts.GetTopicAvgWriteLevel(topicFullName)
	depth := nts.ChannelDepthData[topicFullName]
	if depth > HIGHEST_LEFT_CONSUME_MB_SIZE {
		depth = HIGHEST_LEFT_CONSUME_MB_SIZE
	}
	data := nts.TopicLeaderDataSize[topicFullName]
	if data > HIGHEST_LEFT_DATA_MB_SIZE {
		data = HIGHEST_LEFT_DATA_MB_SIZE
	}
	return writeLevel/HIGHEST_PUB_QPS_LEVEL*80.0 + float64(depth)/HIGHEST_LEFT_CONSUME_MB_SIZE*10.0 + float64(data)/HIGHEST_LEFT_DATA_MB_SIZE*10.0
}

func (nts *NodeTopicStats) GetTopicAvgWriteLevel(topicFullName string) float64 {
	dataList, ok := nts.TopicHourlyPubDataList[topicFullName]
	if ok {
		sum := int64(10)
		cnt := 0
		tmp := make(IntHeap, 0, len(dataList))
		heap.Init(&tmp)
		for _, data := range dataList {
			sum += data
			cnt++
			heap.Push(&tmp, int(data))
		}
		for i := 0; i < len(dataList)/4; i++ {
			v := heap.Pop(&tmp)
			sum -= int64(v.(int))
			cnt--
		}

		sum = sum / int64(cnt)
		return convertQPSLevel(sum)
	}
	return 1.0
}

type loadFactorInfo struct {
	name       string
	topic      string
	loadFactor float64
}

func (lf *loadFactorInfo) GetTopic() string {
	return lf.topic
}

func (lf *loadFactorInfo) GetLF() float64 {
	return lf.loadFactor
}

type LFListT []loadFactorInfo

func (s LFListT) Len() int {
	return len(s)
}
func (s LFListT) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s LFListT) Less(i, j int) bool {
	if math.Abs(s[i].loadFactor-s[j].loadFactor) < 1 {
		return s[i].topic < s[j].topic
	}
	return s[i].loadFactor < s[j].loadFactor
}

func (nts *NodeTopicStats) GetSortedTopicWriteLevel(leaderOnly bool) LFListT {
	topicLFList := make(LFListT, 0)
	for topicName := range nts.TopicHourlyPubDataList {
		d, ok := nts.TopicLeaderDataSize[topicName]
		if leaderOnly {
			if !ok || d <= 0 {
				continue
			}
		}
		lf := 0.0
		if leaderOnly {
			lf = nts.GetTopicLeaderLoadFactor(topicName)
		} else {
			lf = nts.GetTopicLoadFactor(topicName)
		}
		topicLFList = append(topicLFList, loadFactorInfo{
			topic:      topicName,
			loadFactor: lf,
		})
	}
	sort.Sort(topicLFList)
	return topicLFList
}

func (nts *NodeTopicStats) GetMostBusyAndIdleTopicWriteLevel(leaderOnly bool) (string, string, float64, float64) {
	busy := float64(0.0)
	busyTopic := ""
	idle := float64(math.MaxInt32)
	idleTopic := ""
	for topicName := range nts.TopicHourlyPubDataList {
		d, ok := nts.TopicLeaderDataSize[topicName]
		if leaderOnly {
			if !ok || d <= 0 {
				continue
			}
		} else {
			if ok && d > 0 {
				continue
			}
		}

		sum := 0.0
		if leaderOnly {
			sum = nts.GetTopicLeaderLoadFactor(topicName)
		} else {
			sum = nts.GetTopicLoadFactor(topicName)
		}

		if sum > busy {
			busy = sum
			busyTopic = topicName
		}
		if sum < idle {
			idle = sum
			idleTopic = topicName
		}
	}
	return idleTopic, busyTopic, idle, busy
}

func (nts *NodeTopicStats) GetTopicPeakLevel(topic TopicPartitionID) float64 {
	selectedTopic := topic.String()
	dataList, ok := nts.TopicHourlyPubDataList[selectedTopic]
	if ok {
		peak := int64(10)
		for _, data := range dataList {
			if data > peak {
				peak = data
			}
		}
		return convertQPSLevel(peak)
	}
	return 1.0
}

func (nts *NodeTopicStats) LeaderLessLoader(other *NodeTopicStats) bool {
	left := nts.GetNodeLeaderLoadFactor()
	right := other.GetNodeLeaderLoadFactor()
	if math.Abs(left-right) < 0.5 {
		left := float64(len(nts.TopicLeaderDataSize)) + float64(len(nts.TopicTotalDataSize)-len(nts.TopicLeaderDataSize))*RATIO_BETWEEN_LEADER_FOLLOWER
		right := float64(len(other.TopicLeaderDataSize)) + float64(len(other.TopicTotalDataSize)-len(other.TopicLeaderDataSize))*RATIO_BETWEEN_LEADER_FOLLOWER
		return left < right
	}
	if left < right {
		return true
	}

	return false
}

func (nts *NodeTopicStats) SlaveLessLoader(other *NodeTopicStats) bool {
	_, left := nts.GetNodeLoadFactor()
	_, right := other.GetNodeLoadFactor()
	if math.Abs(left-right) < 0.5 {
		left := float64(len(nts.TopicLeaderDataSize)) + float64(len(nts.TopicTotalDataSize)-len(nts.TopicLeaderDataSize))*RATIO_BETWEEN_LEADER_FOLLOWER
		right := float64(len(other.TopicLeaderDataSize)) + float64(len(other.TopicTotalDataSize)-len(other.TopicLeaderDataSize))*RATIO_BETWEEN_LEADER_FOLLOWER
		return left < right
	}
	if left < right {
		return true
	}
	return false
}

type By func(l, r *NodeTopicStats) bool

func (by By) Sort(statList []NodeTopicStats) {
	sorter := &StatsSorter{
		stats: statList,
		by:    by,
	}
	sort.Sort(sorter)
}

type StatsSorter struct {
	stats []NodeTopicStats
	by    By
}

func (s *StatsSorter) Len() int {
	return len(s.stats)
}
func (s *StatsSorter) Swap(i, j int) {
	s.stats[i], s.stats[j] = s.stats[j], s.stats[i]
}
func (s *StatsSorter) Less(i, j int) bool {
	return s.by(&s.stats[i], &s.stats[j])
}

type DataPlacement struct {
	balanceInterval [2]int
	lookupCoord     *NsqLookupCoordinator
}

func NewDataPlacement(coord *NsqLookupCoordinator) *DataPlacement {
	return &DataPlacement{
		lookupCoord:     coord,
		balanceInterval: [2]int{2, 4},
	}
}

func (dpm *DataPlacement) SetBalanceInterval(start int, end int) {
	if start == end && start == 0 {
		return
	}
	dpm.balanceInterval[0] = start
	dpm.balanceInterval[1] = end
}

type nodeLoadInfo struct {
	avgLeaderLoad    float64
	minLeaderLoad    float64
	maxLeaderLoad    float64
	avgNodeLoad      float64
	minNodeLoad      float64
	maxNodeLoad      float64
	validNum         int
	mostLeaderStats  *NodeTopicStats
	mostLeaderNum    int
	leastLeaderStats *NodeTopicStats
	leastLeaderNum   int
}

func newLoadInfo() nodeLoadInfo {
	return nodeLoadInfo{
		avgLeaderLoad:  0.0,
		minLeaderLoad:  float64(math.MaxInt32),
		maxLeaderLoad:  0.0,
		avgNodeLoad:    0.0,
		minNodeLoad:    float64(math.MaxInt32),
		maxNodeLoad:    0.0,
		leastLeaderNum: math.MaxInt32,
	}
}

func (dpm *DataPlacement) getLeaderSortedNodeTopicStats(currentNodes map[string]NsqdNodeInfo, nodeTopicStats []NodeTopicStats) []NodeTopicStats {
	nodeTopicStats = nodeTopicStats[:0]
	for nodeID, nodeInfo := range currentNodes {
		topicStat, err := dpm.lookupCoord.getNsqdTopicStat(nodeInfo)
		if err != nil {
			coordLog.Infof("failed to get node topic status while checking balance: %v", nodeID)
			continue
		}
		nodeTopicStats = append(nodeTopicStats, *topicStat)
	}

	leaderSort := func(l, r *NodeTopicStats) bool {
		return l.LeaderLessLoader(r)
	}
	By(leaderSort).Sort(nodeTopicStats)
	return nodeTopicStats
}

func computeNodeLoadInfo(nodeTopicStats []NodeTopicStats, topicStatsMinMax []*NodeTopicStats) ([]*NodeTopicStats, nodeLoadInfo) {
	nload := newLoadInfo()
	for _, tstat := range nodeTopicStats {
		topicStat := tstat
		nodeID := topicStat.NodeID
		leaderLF, nodeLF := topicStat.GetNodeLoadFactor()
		coordLog.Infof("nsqd node %v load factor is : (%v, %v)", nodeID, leaderLF, nodeLF)
		if leaderLF < nload.minLeaderLoad {
			topicStatsMinMax[0] = &topicStat
			nload.minLeaderLoad = leaderLF
		}
		nload.minNodeLoad = math.Min(nodeLF, nload.minNodeLoad)
		if leaderLF > nload.maxLeaderLoad {
			topicStatsMinMax[1] = &topicStat
			nload.maxLeaderLoad = leaderLF
		}
		nload.maxNodeLoad = math.Max(nodeLF, nload.maxNodeLoad)

		nload.avgLeaderLoad += leaderLF
		nload.avgNodeLoad += nodeLF
		nload.validNum++
		leaderNum := len(topicStat.TopicLeaderDataSize)
		if leaderNum > nload.mostLeaderNum {
			nload.mostLeaderNum = leaderNum
			nload.mostLeaderStats = &topicStat
		}
		if leaderNum < nload.leastLeaderNum {
			nload.leastLeaderNum = leaderNum
			nload.leastLeaderStats = &topicStat
		}
	}
	return topicStatsMinMax, nload
}

func (dpm *DataPlacement) isTopNBalanceEnabled() bool {
	return atomic.LoadInt32(&dpm.lookupCoord.enableTopNBalance) == 1
}

func (dpm *DataPlacement) DoBalance(monitorChan chan struct{}) {
	//check period for the data balance.
	ticker := time.NewTicker(balanceInterval)
	defer func() {
		ticker.Stop()
		coordLog.Infof("balance check exit.")
	}()
	topicStatsMinMax := make([]*NodeTopicStats, 2)
	nodeTopicStats := make([]NodeTopicStats, 0, 10)
	for {
		select {
		case <-monitorChan:
			return
		case <-ticker.C:
			// only balance at given interval
			if time.Now().Hour() > dpm.balanceInterval[1] || time.Now().Hour() < dpm.balanceInterval[0] {
				continue
			}
			if !dpm.lookupCoord.IsMineLeader() {
				coordLog.Infof("not leader while checking balance")
				continue
			}
			if !dpm.lookupCoord.IsClusterStable() {
				coordLog.Infof("no balance since cluster is not stable while checking balance")
				continue
			}

			// if max load is 4 times more than avg load, we need move some
			// leader from max to min load node one by one.
			// if min load is 4 times less than avg load, we can move some
			// leader to this min load node.
			coordLog.Infof("begin checking balance of topic data...")
			moved, _ := dpm.rebalanceMultiPartTopic(monitorChan)
			if moved {
				continue
			}

			currentNodes := dpm.lookupCoord.getCurrentNodes()
			nodeTopicStats = dpm.getLeaderSortedNodeTopicStats(currentNodes, nodeTopicStats)
			topicList, err := dpm.lookupCoord.leadership.ScanTopics()
			if err != nil {
				coordLog.Infof("scan topics error: %v", err)
				continue
			}
			if len(topicList) <= len(currentNodes)*2 {
				coordLog.Infof("the topics less than nodes, no need balance: %v ", len(topicList))
				continue
			}
			var topNTopics LFListT
			if dpm.isTopNBalanceEnabled() {
				topNBalanced := false
				//topNBalanced, _, topNTopics = dpm.rebalanceTopNTopics(monitorChan, nodeTopicStats)
				topNBalanced, _, topNTopics = dpm.rebalanceTopNTopicsByLoad(monitorChan,
					topicList, nodeTopicStats, currentNodes)
				if !topNBalanced {
					continue
				}
			}
			// filter out topN topics
			for _, s := range nodeTopicStats {
				beforeCnt := len(s.TopicTotalDataSize)
				for _, t := range topNTopics {
					topicName := t.topic
					delete(s.ChannelDepthData, topicName)
					delete(s.ChannelList, topicName)
					delete(s.ChannelNum, topicName)
					delete(s.ChannelOffsets, topicName)
					delete(s.TopicHourlyPubDataList, topicName)
					delete(s.TopicLeaderDataSize, topicName)
					delete(s.TopicTotalDataSize, topicName)
				}
				afterCnt := len(s.TopicTotalDataSize)
				coordLog.Infof("node %v filter topn from %v to %v", s.NodeID, beforeCnt, afterCnt)
			}
			var nload nodeLoadInfo
			topicStatsMinMax, nload = computeNodeLoadInfo(nodeTopicStats, topicStatsMinMax)
			avgLeaderLoad := nload.avgLeaderLoad
			minLeaderLoad := nload.minLeaderLoad
			maxLeaderLoad := nload.maxLeaderLoad
			avgNodeLoad := nload.avgNodeLoad
			minNodeLoad := nload.minNodeLoad
			maxNodeLoad := nload.maxNodeLoad
			validNum := nload.validNum
			mostLeaderStats := nload.mostLeaderStats
			mostLeaderNum := nload.mostLeaderNum
			leastLeaderStats := nload.leastLeaderStats
			leastLeaderNum := nload.leastLeaderNum
			if validNum < 2 || topicStatsMinMax[0] == nil || topicStatsMinMax[1] == nil {
				continue
			}

			midLeaderLoad, _ := nodeTopicStats[len(nodeTopicStats)/2].GetNodeLoadFactor()
			nodeTopicStatsSortedSlave := make([]NodeTopicStats, len(nodeTopicStats))
			copy(nodeTopicStatsSortedSlave, nodeTopicStats)
			nodeSort := func(l, r *NodeTopicStats) bool {
				return l.SlaveLessLoader(r)
			}
			By(nodeSort).Sort(nodeTopicStatsSortedSlave)
			_, midNodeLoad := nodeTopicStatsSortedSlave[len(nodeTopicStatsSortedSlave)/2].GetNodeLoadFactor()

			avgLeaderLoad = avgLeaderLoad / float64(validNum)
			avgNodeLoad = avgNodeLoad / float64(validNum)
			coordLog.Infof("min/avg/mid/max leader load %v, %v, %v, %v", minLeaderLoad, avgLeaderLoad, midLeaderLoad, maxLeaderLoad)
			coordLog.Infof("min/avg/mid/max node load %v, %v, %v, %v", minNodeLoad, avgNodeLoad, midNodeLoad, maxNodeLoad)
			if len(topicStatsMinMax[1].TopicHourlyPubDataList) <= 2 {
				coordLog.Infof("the topic number is so less on both nodes, no need balance: %v", topicStatsMinMax[1])
				continue
			}

			moveLeader := false
			avgTopicNum := len(topicList) / len(currentNodes)
			if len(topicStatsMinMax[1].TopicLeaderDataSize) > int(1.2*float64(avgTopicNum)) {
				// too many leader topics on this node, try move leader topic
				coordLog.Infof("move leader topic since leader is more than follower on node")
				moveLeader = true
			} else {
				// too many replica topics on this node, try move replica topic to others
				coordLog.Infof("move follower topic since follower is more than leader on node")
			}
			if minLeaderLoad*4 < avgLeaderLoad {
				// move some topic from the most busy node to the most idle node
				dpm.balanceTopicLeaderBetweenNodes(monitorChan, moveLeader, moveMinLFOnly, minLeaderLoad,
					maxLeaderLoad, topicStatsMinMax, nodeTopicStats)
			} else if avgLeaderLoad < 4 && maxLeaderLoad < 8 {
				// all nodes in the cluster are under low load, no need balance
				continue
			} else if avgLeaderLoad*2 < maxLeaderLoad {
				dpm.balanceTopicLeaderBetweenNodes(monitorChan, moveLeader, moveAny, minLeaderLoad,
					maxLeaderLoad, topicStatsMinMax, nodeTopicStats)
			} else if avgLeaderLoad >= 20 &&
				((minLeaderLoad*1.5 < maxLeaderLoad) || (maxLeaderLoad > avgLeaderLoad*1.3)) {
				dpm.balanceTopicLeaderBetweenNodes(monitorChan, moveLeader,
					moveTryIdle, minLeaderLoad, maxLeaderLoad,
					topicStatsMinMax, nodeTopicStats)
			} else if minNodeLoad*4 < avgNodeLoad {
				topicStatsMinMax[0] = &nodeTopicStatsSortedSlave[0]
				topicStatsMinMax[1] = &nodeTopicStatsSortedSlave[len(nodeTopicStatsSortedSlave)-1]
				moveLeader = len(topicStatsMinMax[1].TopicLeaderDataSize) > len(topicStatsMinMax[1].TopicTotalDataSize)/2
				dpm.balanceTopicLeaderBetweenNodes(monitorChan, moveLeader, moveMinLFOnly, minNodeLoad,
					maxNodeLoad, topicStatsMinMax, nodeTopicStatsSortedSlave)
			} else if avgNodeLoad*2 < maxNodeLoad {
				topicStatsMinMax[0] = &nodeTopicStatsSortedSlave[0]
				topicStatsMinMax[1] = &nodeTopicStatsSortedSlave[len(nodeTopicStatsSortedSlave)-1]
				moveLeader = len(topicStatsMinMax[1].TopicLeaderDataSize) > len(topicStatsMinMax[1].TopicTotalDataSize)/2
				dpm.balanceTopicLeaderBetweenNodes(monitorChan, moveLeader, moveAny, minNodeLoad,
					maxNodeLoad, topicStatsMinMax, nodeTopicStatsSortedSlave)
			} else if avgNodeLoad >= 20 &&
				(minNodeLoad*1.5 < maxNodeLoad || maxNodeLoad > avgNodeLoad*1.3) {
				topicStatsMinMax[0] = &nodeTopicStatsSortedSlave[0]
				topicStatsMinMax[1] = &nodeTopicStatsSortedSlave[len(nodeTopicStatsSortedSlave)-1]
				moveLeader = len(topicStatsMinMax[1].TopicLeaderDataSize) > len(topicStatsMinMax[1].TopicTotalDataSize)/2
				dpm.balanceTopicLeaderBetweenNodes(monitorChan, moveLeader,
					moveTryIdle, minNodeLoad, maxNodeLoad,
					topicStatsMinMax, nodeTopicStatsSortedSlave)
			} else {
				if mostLeaderStats != nil && avgTopicNum > 10 && mostLeaderNum > int(float64(avgTopicNum)*1.3) {
					needMove := checkMoveLotsOfTopics(true, mostLeaderNum, mostLeaderStats,
						avgTopicNum, topicStatsMinMax, midLeaderLoad)
					if needMove {
						topicStatsMinMax[1] = mostLeaderStats
						leaderLF, _ := mostLeaderStats.GetNodeLoadFactor()
						maxLeaderLoad = leaderLF
						dpm.balanceTopicLeaderBetweenNodes(monitorChan, true, moveTryIdle, minLeaderLoad,
							maxLeaderLoad, topicStatsMinMax, nodeTopicStats)
						continue
					}
				}
				// so less leader topics maybe means too much replicas on this node
				if leastLeaderStats != nil && avgTopicNum > 10 && leastLeaderNum < int(float64(avgTopicNum)*0.7) {
					// TODO: check if we can move a follower topic to leader from other nodes
					leaderLF, nodeLF := leastLeaderStats.GetNodeLoadFactor()
					needMove := true
					moveLeader = true
					followerNum := len(leastLeaderStats.TopicTotalDataSize) - leastLeaderNum
					moveToMinLF := moveTryIdle
					if nodeLF > avgNodeLoad || leaderLF > midLeaderLoad {
						// maybe too much topic followers on this node
						if leastLeaderStats.NodeID == topicStatsMinMax[1].NodeID && followerNum > avgTopicNum {
							moveLeader = false
						} else if followerNum > int(float64(avgTopicNum)*1.3) {
							// too much followers
							coordLog.Infof("move follower topic since less leader and much follower on node: %v, %v, avg %v",
								leastLeaderStats.NodeID, followerNum, avgTopicNum)
							moveLeader = false
							topicStatsMinMax[1] = leastLeaderStats
							maxLeaderLoad = nodeLF
						} else {
							needMove = false
						}
					} else if leaderLF < midLeaderLoad {
						topicStatsMinMax[0] = leastLeaderStats
						minLeaderLoad = leaderLF
						moveToMinLF = moveMinLFOnly
						coordLog.Infof("so less topic leader (%v) on idle node: %v, try move some topic leader to this node",
							leastLeaderNum, leastLeaderStats.NodeID)
					} else {
						needMove = false
					}
					if needMove {
						dpm.balanceTopicLeaderBetweenNodes(monitorChan, moveLeader, moveToMinLF, minLeaderLoad,
							maxLeaderLoad, topicStatsMinMax, nodeTopicStats)
						continue
					}
				}
				mostTopicNum := len(nodeTopicStatsSortedSlave[len(nodeTopicStatsSortedSlave)-1].TopicTotalDataSize)
				if mostTopicNum > 10 {
					leastTopicNum := mostTopicNum
					for index, s := range nodeTopicStatsSortedSlave {
						if len(s.TopicTotalDataSize) < leastTopicNum {
							leastTopicNum = len(s.TopicTotalDataSize)
							topicStatsMinMax[0] = &nodeTopicStatsSortedSlave[index]
							_, minNodeLoad = s.GetNodeLoadFactor()
						}
					}
					if float64(mostTopicNum) > float64(leastTopicNum)*1.3 && minNodeLoad < midNodeLoad {
						topicStatsMinMax[1] = &nodeTopicStatsSortedSlave[len(nodeTopicStatsSortedSlave)-1]
						coordLog.Infof("node %v has too much topics: %v, the least has only %v", topicStatsMinMax[1].NodeID, mostTopicNum, leastTopicNum)
						moveLeader = len(topicStatsMinMax[1].TopicLeaderDataSize) > len(topicStatsMinMax[1].TopicTotalDataSize)*2/3
						dpm.balanceTopicLeaderBetweenNodes(monitorChan, moveLeader, moveMinLFOnly, minNodeLoad,
							maxNodeLoad, topicStatsMinMax, nodeTopicStatsSortedSlave)
					}
				}
			}
		}
	}
}

func checkMoveLotsOfTopics(moveLeader bool, moveTopicNum int, moveNodeStats *NodeTopicStats, avgTopicNum int,
	topicStatsMinMax []*NodeTopicStats, midLeaderLoad float64) bool {
	if moveNodeStats.NodeID == topicStatsMinMax[0].NodeID ||
		len(topicStatsMinMax[0].TopicLeaderDataSize) > avgTopicNum {
		return false
	}
	coordLog.Infof("too many topic on node: %v, num: %v", moveNodeStats.NodeID, moveTopicNum)
	leaderLF, _ := moveNodeStats.GetNodeLoadFactor()
	//we should avoid move leader topic if the load is not so much
	if leaderLF < midLeaderLoad && float64(moveTopicNum) < float64(avgTopicNum)*1.5 {
		coordLog.Infof("although many topics , the load is not much: %v", leaderLF)
		return false
	}
	return true
}

func (dpm *DataPlacement) balanceTopicLeaderBetweenNodes(monitorChan chan struct{}, moveLeader bool, moveOp balanceOpLevel,
	minLF float64, maxLF float64, statsMinMax []*NodeTopicStats, sortedNodeTopicStats []NodeTopicStats) {

	if !atomic.CompareAndSwapInt32(&dpm.lookupCoord.balanceWaiting, 0, 1) {
		coordLog.Infof("another balance is running, should wait")
		return
	}
	defer atomic.StoreInt32(&dpm.lookupCoord.balanceWaiting, 0)

	idleTopic, busyTopic, _, busyLevel := statsMinMax[1].GetMostBusyAndIdleTopicWriteLevel(moveLeader)
	if busyTopic == "" && idleTopic == "" {
		coordLog.Infof("no idle or busy topic found")
		return
	}
	// never balance the ordered multi partitions, these partitions should be the same on the all nodes

	moveFromNode := statsMinMax[1].NodeID
	coordLog.Infof("balance topic: %v, %v(%v) from node: %v, move : %v ", idleTopic,
		busyTopic, busyLevel, moveFromNode, moveOp)
	coordLog.Infof("balance topic current max: %v, min: %v ", maxLF, minLF)
	checkMoveOK := false
	topicName := ""
	partitionID := 0
	var err error
	// avoid move the too busy topic to reduce the impaction of the online service.
	// if the busiest topic is not so busy, we try move this topic to avoid move too much idle topics
	if busyTopic != "" && busyLevel < busyTopicLevel && (busyLevel*2 < maxLF-minLF) {
		topicName, partitionID, err = splitTopicPartitionID(busyTopic)
		if err != nil {
			coordLog.Warningf("split topic name and partition %v failed: %v", busyTopic, err)
		} else {
			checkMoveOK = dpm.checkAndPrepareMove(monitorChan, moveFromNode, topicName, partitionID,
				statsMinMax,
				sortedNodeTopicStats, moveOp, moveLeader)
			if checkMoveOK {
				return
			}
		}
	}

	if idleTopic != "" {
		topicName, partitionID, err = splitTopicPartitionID(idleTopic)
		if err != nil {
			coordLog.Warningf("split topic name and partition %v failed: %v", idleTopic, err)
		} else {
			checkMoveOK = dpm.checkAndPrepareMove(monitorChan, moveFromNode, topicName, partitionID,
				statsMinMax,
				sortedNodeTopicStats, moveOp, moveLeader)
			if checkMoveOK {
				return
			}
		}
	}
	// maybe we can move some other topic if both idle/busy is not movable
	sortedTopics := statsMinMax[1].GetSortedTopicWriteLevel(moveLeader)
	coordLog.Infof("check %v for moving , all sorted topic number: %v, %v, %v", moveFromNode, len(sortedTopics),
		len(statsMinMax[1].TopicHourlyPubDataList), len(statsMinMax[1].TopicLeaderDataSize))
	for _, t := range sortedTopics {
		if t.topic == idleTopic || t.topic == busyTopic {
			continue
		}
		if checkMoveOK {
			break
		}
		// do not move the topic with very busy load
		if t.loadFactor > busyTopicLevel || t.loadFactor > maxLF-minLF {
			coordLog.Infof("check topic for moving , all busy : %v, %v", t, sortedTopics)
			break
		}
		topicName, partitionID, err = splitTopicPartitionID(t.topic)
		if err != nil {
			coordLog.Warningf("split topic %v failed: %v", t.topic, err)
		} else {
			coordLog.Infof("check topic %v for moving ", t)
			checkMoveOK = dpm.checkAndPrepareMove(monitorChan, moveFromNode, topicName, partitionID,
				statsMinMax,
				sortedNodeTopicStats, moveOp, moveLeader)
		}
	}
}

func (dpm *DataPlacement) checkAndPrepareMove(monitorChan chan struct{}, fromNode string, topicName string, partitionID int,
	statsMinMax []*NodeTopicStats,
	sortedNodeTopicStats []NodeTopicStats, moveOp balanceOpLevel, moveLeader bool) bool {
	topicInfo, err := dpm.lookupCoord.leadership.GetTopicInfo(topicName, partitionID)
	if err != nil {
		coordLog.Infof("failed to get topic %v info: %v", topicName, err)
		return false
	}
	checkMoveOK := false
	if topicInfo.AllowMulti() {
		coordLog.Debugf("topic %v is configured as multi ordered, no balance", topicName)
		return false
	}
	if moveOp > moveAny {
		leaderNodeLF, _ := statsMinMax[0].GetNodeLoadFactor()
		coordLog.Infof("check the min load node first: %v, %v", statsMinMax[0].NodeID, leaderNodeLF)
		// check first for the specific min load node
		if moveLeader {
			if FindSlice(topicInfo.ISR, statsMinMax[0].NodeID) != -1 {
				checkMoveOK = true
			}
		}
		if !checkMoveOK {
			if leaderNodeLF < sortedNodeTopicStats[len(sortedNodeTopicStats)/2].GetNodeLeaderLoadFactor() {
				nlist := make([]string, 0)
				nlist = append(nlist, statsMinMax[0].NodeID)
				err := dpm.addToCatchupAndWaitISRReady(monitorChan, false, fromNode, topicName, partitionID,
					nlist,
					getNodeNameList(sortedNodeTopicStats), false)
				if err == nil {
					return true
				}
			}
		}
	}
	if moveOp == moveMinLFOnly {
		// no try other node
	} else if moveOp > moveAny || (len(topicInfo.ISR)-1 <= topicInfo.Replica/2) {
		if moveLeader {
			// check if any of current isr nodes is already idle for move
			for _, nid := range topicInfo.ISR {
				if checkMoveOK {
					break
				}
				if nid == fromNode {
					continue
				}
				for index, stat := range sortedNodeTopicStats {
					if index >= len(sortedNodeTopicStats)/3 {
						break
					}
					if stat.NodeID == nid {
						checkMoveOK = true
						break
					}
				}
			}
		}
		if !checkMoveOK {
			// the isr not so idle , we try add a new idle node to the isr.
			// and make sure that node can accept the topic (no other leader/follower for this topic)
			err := dpm.addToCatchupAndWaitISRReady(monitorChan, false, fromNode, topicName, partitionID, nil,
				getNodeNameList(sortedNodeTopicStats), false)
			if err == nil {
				return true
			}
		}
	} else {
		// we are allowed to move to any node and no need to add any node to isr
		checkMoveOK = true
	}

	if checkMoveOK {
		coordErr := dpm.lookupCoord.handleRemoveTopicNodeOrMoveLeader(moveLeader, topicName, partitionID, fromNode)
		if coordErr != nil {
			return false
		}
	}
	return checkMoveOK
}

func (dpm *DataPlacement) moveTopicPartitionByManual(topicName string, partitionID int,
	moveLeader bool, fromNode string, toNode string) error {

	if !atomic.CompareAndSwapInt32(&dpm.lookupCoord.balanceWaiting, 0, 1) {
		coordLog.Infof("another balance is running, should wait")
		return ErrClusterBalanceRunning
	}
	defer atomic.StoreInt32(&dpm.lookupCoord.balanceWaiting, 0)

	if !dpm.lookupCoord.IsClusterStable() {
		return ErrClusterUnstable
	}

	return dpm.tryMoveTopicPartition(dpm.lookupCoord.stopChan, false, topicName, partitionID, moveLeader, fromNode, toNode)
}

func (dpm *DataPlacement) tryMoveTopicPartition(monitorChan chan struct{}, srcNodeRemoving bool, topicName string, partitionID int, moveLeader bool, fromNode string, toNode string) error {
	if fromNode == toNode {
		return errors.New("move to and from node can not be the same")
	}
	topicInfo, err := dpm.lookupCoord.leadership.GetTopicInfo(topicName, partitionID)
	if err != nil {
		coordLog.Infof("failed to get topic info: %v-%v: %v", topicName, partitionID, err)
		return err
	}
	if moveLeader && fromNode != topicInfo.Leader {
		return errors.New("move from not the topic leader")
	}
	if !moveLeader {
		if fromNode == topicInfo.Leader {
			return errors.New("can not move leader without moveleader param")
		}
		if FindSlice(topicInfo.ISR, fromNode) == -1 {
			return errors.New("the move source node has no replica for the topic")
		}
		if FindSlice(topicInfo.ISR, toNode) != -1 {
			return errors.New("the move destination has the topic data already")
		}
	}

	currentNodes := dpm.lookupCoord.getCurrentNodes()
	if _, ok := currentNodes[toNode]; !ok {
		return errors.New("the move destination is not found in cluster")
	}
	if !srcNodeRemoving {
		if _, ok := currentNodes[fromNode]; !ok {
			return errors.New("the move source data is lost in cluster")
		}
	}
	if FindSlice(topicInfo.ISR, toNode) == -1 {
		// add destination to catchup and wait
		if FindSlice(topicInfo.CatchupList, toNode) != -1 {
			// just notify to allow restart and wait ready
			dpm.lookupCoord.notifyCatchupTopicMetaInfo(topicInfo)
		} else {
			excludeNodes, commonErr := dpm.getExcludeNodesForTopic(topicInfo, true)
			if commonErr != nil {
				return ErrLeadershipServerUnstable.ToErrorType()
			}
			if _, ok := excludeNodes[toNode]; ok {
				coordLog.Infof("current node: %v is excluded for topic: %v-%v", toNode, topicName, partitionID)
				return ErrNodeIsExcludedForTopicData
			}
			// TODO: greedy clean topic leader data to speed up catchup
			coordErr := dpm.lookupCoord.addCatchupNode(topicInfo, toNode)
			if coordErr != nil {
				return coordErr.ToErrorType()
			}
		}
		coordLog.Infof("try move topic: %v-%v data from %v to %v", topicName, partitionID, fromNode, toNode)
		waitStart := time.Now()
		for {
			if !dpm.lookupCoord.IsMineLeader() {
				coordLog.Infof("not leader while checking balance")
				return ErrNotNsqLookupLeader
			}
			topicInfo, err = dpm.lookupCoord.leadership.GetTopicInfo(topicName, partitionID)
			if err != nil {
				coordLog.Infof("failed to get topic info: %v-%v: %v", topicName, partitionID, err)
			} else {
				if FindSlice(topicInfo.ISR, toNode) != -1 {
					break
				}
				if FindSlice(topicInfo.CatchupList, toNode) == -1 {
					// maybe changed by others
					coordLog.Infof("topic : %v-%v catchup changed while moving: %v", topicName, partitionID, topicInfo.CatchupList)
					return errors.New("catchup changed while wait moving")
				}
			}
			if time.Since(waitStart) > moveWaitTimeout {
				return errMoveTopicWaitTimeout
			}

			ti := time.NewTimer(time.Second)
			select {
			case <-monitorChan:
				ti.Stop()
				return errLookupExiting
			case <-ti.C:
				coordLog.Infof("node: %v is added for topic: %v-%v catchup, still waiting catchup", toNode, topicName, partitionID)
			}
			ti.Stop()
		}
	}
	if FindSlice(topicInfo.ISR, toNode) != -1 {
		// dest node has been added to isr, so we can remove source node
		coordErr := dpm.lookupCoord.handleRemoveTopicNodeOrMoveLeader(moveLeader, topicName, partitionID, fromNode)
		if coordErr != nil {
			return coordErr.ToErrorType()
		}
	}
	return nil
}

func (dpm *DataPlacement) addToCatchupAndWaitISRReady(monitorChan chan struct{}, srcNodeRemoving bool, fromNode string, topicName string,
	partitionID int, addTryFirstNodes []string, sortedNodes []string, tryAllNodes bool) error {
	currentSelect := 0
	topicInfo, err := dpm.lookupCoord.leadership.GetTopicInfo(topicName, partitionID)
	if err != nil {
		coordLog.Infof("failed to get topic info: %v-%v: %v", topicName, partitionID, err)
		return err
	}
	moveLeader := fromNode == topicInfo.Leader
	if topicInfo.AllowMulti() && len(addTryFirstNodes) == 0 {
		return dpm.addToCatchupAndWaitISRReadyForMultiPartTopic(monitorChan, srcNodeRemoving, fromNode,
			topicInfo, sortedNodes)
	}
	filteredNodes := make([]string, 0)
	for _, nid := range addTryFirstNodes {
		if FindSlice(topicInfo.ISR, nid) != -1 {
		} else {
			filteredNodes = append(filteredNodes, nid)
		}
	}
	if tryAllNodes || len(addTryFirstNodes) == 0 {
		for index, s := range sortedNodes {
			if !tryAllNodes {
				if index >= len(sortedNodes)-2 ||
					index > len(sortedNodes)/2 {
					// never move to the busy nodes
					break
				}
			}
			if FindSlice(topicInfo.ISR, s) != -1 {
				// filter
			} else {
				filteredNodes = append(filteredNodes, s)
			}
		}
	}
	for {
		if currentSelect >= len(filteredNodes) {
			coordLog.Infof("currently no any node can be balanced for topic: %v", topicName)
			return ErrBalanceNodeUnavailable
		}
		toNode := filteredNodes[currentSelect]
		err := dpm.tryMoveTopicPartition(monitorChan, srcNodeRemoving, topicName, partitionID, moveLeader, fromNode, toNode)
		if err == ErrNodeIsExcludedForTopicData {
			currentSelect++
			continue
		}
		return err
	}
	return nil
}

func (dpm *DataPlacement) addToCatchupAndWaitISRReadyForMultiPartTopic(monitorChan chan struct{}, srcNodeRemoving bool,
	fromNode string, topicInfo *TopicPartitionMetaInfo,
	nodeNameList []string) error {
	if len(nodeNameList) == 0 {
		return errors.New("no node")
	}
	moveLeader := fromNode == topicInfo.Leader
	currentSelect := 0
	topicName := topicInfo.Name
	partitionID := topicInfo.Partition
	// we need add new catchup, choose from the the diff between the new isr and old isr
	partitionNodes, err := dpm.getRebalancedMultiTopicPartitionsFromNameList(
		topicInfo.Name,
		topicInfo.PartitionNum,
		topicInfo.Replica, nodeNameList)
	if err != nil {
		return err
	}
	selectedCatchup := make([]string, 0)
	for _, nid := range partitionNodes[topicInfo.Partition] {
		if FindSlice(topicInfo.ISR, nid) != -1 {
			// already isr, ignore add catchup
			continue
		}
		selectedCatchup = append(selectedCatchup, nid)
	}
	for {
		if currentSelect >= len(selectedCatchup) {
			coordLog.Infof("currently no any node %v can be balanced for topic: %v, expect isr: %v, nodes:%v",
				selectedCatchup, topicName, partitionNodes[topicInfo.Partition], nodeNameList)
			return ErrBalanceNodeUnavailable
		}
		toNode := selectedCatchup[currentSelect]
		err := dpm.tryMoveTopicPartition(monitorChan, srcNodeRemoving, topicName, partitionID, moveLeader, fromNode, toNode)
		if err == ErrNodeIsExcludedForTopicData {
			currentSelect++
			continue
		}
		return err
	}
	return nil
}

func (dpm *DataPlacement) getTopNTopics(currentNodes map[string]NsqdNodeInfo) (LFListT, []TopicPartitionMetaInfo, error) {
	topicList, err := dpm.lookupCoord.leadership.ScanTopics()
	if err != nil {
		coordLog.Infof("scan topics error: %v", err)
		return nil, nil, err
	}
	if !dpm.isTopNBalanceEnabled() {
		return nil, topicList, nil
	}
	nodeTopicStats := make([]NodeTopicStats, 0, len(currentNodes))
	nodeTopicStats = dpm.getLeaderSortedNodeTopicStats(currentNodes, nodeTopicStats)
	sortedTopics := getTopNTopicsStats(nodeTopicStats, topicList, topicTopNLimit, true)
	return sortedTopics, topicList, nil
}

func (dpm *DataPlacement) isTopNTopic(topicInfo *TopicPartitionMetaInfo, sortedTopics LFListT) bool {
	if topicInfo.AllowMulti() {
		return false
	}
	for _, t := range sortedTopics {
		if t.topic == topicInfo.GetTopicDesp() {
			return true
		}
	}
	return false
}

func (dpm *DataPlacement) getBalancedTopNTopicISR(topicInfo *TopicPartitionMetaInfo,
	sortedTopics LFListT,
	topicList []TopicPartitionMetaInfo,
	currentNodes map[string]NsqdNodeInfo) ([]string, error) {
	topNISR, _, _, err := dpm.getRebalancedTopNTopic(sortedTopics, topicList, currentNodes)
	if err != nil {
		return nil, err
	}
	isr := topNISR[topicInfo.GetTopicDesp()]
	return isr, nil
}

func (dpm *DataPlacement) getExcludeNodesForTopic(topicInfo *TopicPartitionMetaInfo, checkMulti bool) (map[string]struct{}, error) {
	excludeNodes := make(map[string]struct{})
	excludeNodes[topicInfo.Leader] = struct{}{}
	for _, v := range topicInfo.ISR {
		excludeNodes[v] = struct{}{}
	}
	for _, v := range topicInfo.CatchupList {
		excludeNodes[v] = struct{}{}
	}
	// exclude other partition node with the same topic
	meta, _, err := dpm.lookupCoord.leadership.GetTopicMetaInfo(topicInfo.Name)
	if err != nil {
		coordLog.Infof("failed get the meta info: %v", err)
		return excludeNodes, err
	}
	if checkMulti && meta.AllowMulti() {
		// we allow this topic multi partitions on the same node
		return excludeNodes, nil
	}
	// since one topic have several partitions, here it may not atomic while check all topic partitions
	// we need check isr again while in join session
	num := meta.PartitionNum
	for i := 0; i < num; i++ {
		topicPartInfo, err := dpm.lookupCoord.leadership.GetTopicInfo(topicInfo.Name, i)
		if err != nil {
			if err == ErrKeyNotFound {
				continue
			} else {
				return excludeNodes, err
			}
		}
		excludeNodes[topicPartInfo.Leader] = struct{}{}
		for _, v := range topicPartInfo.ISR {
			excludeNodes[v] = struct{}{}
		}
		for _, v := range topicPartInfo.CatchupList {
			excludeNodes[v] = struct{}{}
		}
	}
	return excludeNodes, nil
}

func (dpm *DataPlacement) getExpectedAllocNodesForTopic(topicInfo *TopicPartitionMetaInfo, currentNodes map[string]NsqdNodeInfo) ([]string, error) {
	if topicInfo.AllowMulti() {
		partitionNodes, err := dpm.getRebalancedMultiTopicPartitions(
			topicInfo.Name,
			topicInfo.PartitionNum,
			topicInfo.Replica, currentNodes)
		if err != nil {
			return nil, err
		}
		return partitionNodes[topicInfo.Partition], nil
	}
	sortedTopN, topicList, err := dpm.getTopNTopics(currentNodes)
	if err != nil {
		return nil, err
	}
	if dpm.isTopNTopic(topicInfo, sortedTopN) {
		expectedISR, err := dpm.getBalancedTopNTopicISR(topicInfo, sortedTopN, topicList, currentNodes)
		if err != nil {
			return nil, err
		}
		return expectedISR, nil
	}
	return nil, nil
}

func getOneFromListAndExclude(expects []string, excludes map[string]struct{}) string {
	for _, n := range expects {
		if _, ok := excludes[n]; ok {
			continue
		}
		return n
	}
	return ""
}

func (dpm *DataPlacement) allocNodeForTopic(topicInfo *TopicPartitionMetaInfo, currentNodes map[string]NsqdNodeInfo) (*NsqdNodeInfo, error) {
	// collect the nsqd data, check if any node has the topic data already.
	var chosenNode NsqdNodeInfo
	var chosenStat *NodeTopicStats

	excludeNodes, commonErr := dpm.getExcludeNodesForTopic(topicInfo, topicInfo.AllowMulti())
	if commonErr != nil {
		return nil, commonErr
	}

	expectedISR, err := dpm.getExpectedAllocNodesForTopic(topicInfo, currentNodes)
	if err != nil {
		return nil, err
	}
	nid := getOneFromListAndExclude(expectedISR, excludeNodes)
	if nid != "" {
		chosenNode = currentNodes[nid]
	} else {
		for nodeID, nodeInfo := range currentNodes {
			if _, ok := excludeNodes[nodeID]; ok {
				continue
			}
			topicStat, err := dpm.lookupCoord.getNsqdTopicStat(nodeInfo)
			if err != nil {
				coordLog.Infof("failed to get topic status for this node: %v", nodeInfo)
				continue
			}
			if chosenNode.ID == "" {
				chosenNode = nodeInfo
				chosenStat = topicStat
				continue
			}
			if topicStat.SlaveLessLoader(chosenStat) {
				chosenNode = nodeInfo
				chosenStat = topicStat
			}
		}
	}
	if chosenNode.ID == "" {
		coordLog.Infof("no more available node for topic: %v, excluding nodes: %v, all nodes: %v", topicInfo.GetTopicDesp(), excludeNodes, currentNodes)
		return nil, ErrBalanceNodeUnavailable
	}
	coordLog.Infof("node %v is alloc for topic: %v", chosenNode, topicInfo.GetTopicDesp())
	return &chosenNode, nil
}

func (dpm *DataPlacement) checkTopicNodeConflict(topicInfo *TopicPartitionMetaInfo) bool {
	existLeaders := make(map[string]struct{})
	existSlaves := make(map[string]struct{})
	if !topicInfo.AllowMulti() {
		for i := 0; i < topicInfo.PartitionNum; i++ {
			if i == topicInfo.Partition {
				continue
			}
			tmpInfo, err := dpm.lookupCoord.leadership.GetTopicInfo(topicInfo.Name, i)
			if err != nil {
				if err != ErrKeyNotFound {
					coordLog.Infof("failed to get topic %v info: %v", topicInfo.GetTopicDesp(), err)
					return false
				} else {
					coordLog.Infof("part of topic %v info not found: %v", topicInfo.Name, i)
					continue
				}
			}
			for _, id := range tmpInfo.ISR {
				if id == tmpInfo.Leader {
					existLeaders[tmpInfo.Leader] = struct{}{}
				} else {
					existSlaves[id] = struct{}{}
				}
			}
			// should check catchup list since it may became isr
			for _, id := range tmpInfo.CatchupList {
				existSlaves[id] = struct{}{}
			}
		}
	}
	// isr should be different
	for _, id := range topicInfo.ISR {
		if _, ok := existLeaders[id]; ok {
			coordLog.Infof("topic %v has conflict leader node: %v, %v", topicInfo.Name, id, existLeaders)
			return false
		}
		if _, ok := existSlaves[id]; ok {
			coordLog.Infof("topic %v has conflict isr nodes: %v, %v", topicInfo.Name, id, existSlaves)
			return false
		}
		// add checked to map to check dup in isr
		existLeaders[id] = struct{}{}
		existSlaves[id] = struct{}{}
	}
	for _, id := range topicInfo.CatchupList {
		if _, ok := existSlaves[id]; ok {
			coordLog.Infof("topic %v has conflict catchup nodes: %v, %v", topicInfo.Name, id, existSlaves)
			return false
		}
		existSlaves[id] = struct{}{}
	}

	return true
}

// init leader node and isr list for the empty topic
func (dpm *DataPlacement) allocTopicLeaderAndISR(topicName string, multi bool, currentNodes map[string]NsqdNodeInfo,
	replica int, partitionNum int, existPart map[int]*TopicPartitionMetaInfo) ([]string, [][]string, error) {

	if multi {
		return dpm.allocMultiPartTopicLeaderAndISR(topicName, currentNodes, replica, partitionNum, existPart)
	}

	if len(currentNodes) < replica || len(currentNodes) < partitionNum {
		coordLog.Infof("nodes %v is less than replica %v or partition %v", len(currentNodes), replica, partitionNum)
		return nil, nil, ErrBalanceNodeUnavailable
	}
	if len(currentNodes) < replica*partitionNum {
		coordLog.Infof("nodes is less than replica*partition")
		return nil, nil, ErrBalanceNodeUnavailable
	}
	coordLog.Infof("alloc current nodes: %v", len(currentNodes))

	existLeaders := make(map[string]struct{})
	existSlaves := make(map[string]struct{})
	for _, topicInfo := range existPart {
		for _, n := range topicInfo.ISR {
			if n == topicInfo.Leader {
				existLeaders[n] = struct{}{}
			} else {
				existSlaves[n] = struct{}{}
			}
		}
	}
	nodeTopicStats := make([]NodeTopicStats, 0, len(currentNodes))
	for _, nodeInfo := range currentNodes {
		stats, err := dpm.lookupCoord.getNsqdTopicStat(nodeInfo)
		if err != nil {
			coordLog.Infof("got topic status for node %v failed: %v", nodeInfo.GetID(), err)
			continue
		}
		nodeTopicStats = append(nodeTopicStats, *stats)
	}
	if len(nodeTopicStats) < partitionNum*replica {
		return nil, nil, ErrBalanceNodeUnavailable
	}
	leaderSort := func(l, r *NodeTopicStats) bool {
		return l.LeaderLessLoader(r)
	}
	By(leaderSort).Sort(nodeTopicStats)
	leaders := make([]string, partitionNum)
	p := 0
	currentSelect := 0
	coordLog.Infof("alloc current exist status: %v, \n %v", existLeaders, existSlaves)
	for p < partitionNum {
		if elem, ok := existPart[p]; ok {
			leaders[p] = elem.Leader
		} else {
			for {
				if currentSelect >= len(nodeTopicStats) {
					coordLog.Infof("not enough nodes for leaders")
					return nil, nil, ErrBalanceNodeUnavailable
				}
				nodeInfo := nodeTopicStats[currentSelect]
				currentSelect++
				if _, ok := existLeaders[nodeInfo.NodeID]; ok {
					coordLog.Infof("ignore for exist other leader(different partition) node: %v", nodeInfo)
					continue
				}
				// TODO: should slave can be used for other leader?
				if _, ok := existSlaves[nodeInfo.NodeID]; ok {
					coordLog.Infof("ignore for exist other slave (different partition) node: %v", nodeInfo)
					continue
				}
				leaders[p] = nodeInfo.NodeID
				existLeaders[nodeInfo.NodeID] = struct{}{}
				break
			}
		}
		p++
	}
	p = 0
	currentSelect = 0
	slaveSort := func(l, r *NodeTopicStats) bool {
		return l.SlaveLessLoader(r)
	}
	By(slaveSort).Sort(nodeTopicStats)

	isrlist := make([][]string, partitionNum)
	for p < partitionNum {
		isr := make([]string, 0, replica)
		isr = append(isr, leaders[p])
		if len(isr) >= replica {
			// already done with replica
		} else if elem, ok := existPart[p]; ok {
			isr = elem.ISR
		} else {
			for {
				if currentSelect >= len(nodeTopicStats) {
					coordLog.Infof("not enough nodes for slaves")
					return nil, nil, ErrBalanceNodeUnavailable
				}
				nodeInfo := nodeTopicStats[currentSelect]
				currentSelect++
				if nodeInfo.NodeID == leaders[p] {
					coordLog.Infof("ignore for leader node: %v", nodeInfo.NodeID)
					continue
				}
				if _, ok := existSlaves[nodeInfo.NodeID]; ok {
					coordLog.Infof("ignore for exist slave node: %v", nodeInfo.NodeID)
					continue
				}
				// TODO: should slave can be used for other leader?
				if _, ok := existLeaders[nodeInfo.NodeID]; ok {
					coordLog.Infof("ignore for exist other leader(different partition) node: %v", nodeInfo.NodeID)
					continue
				}
				existSlaves[nodeInfo.NodeID] = struct{}{}
				isr = append(isr, nodeInfo.NodeID)
				if len(isr) >= replica {
					break
				}
			}
		}
		isrlist[p] = isr
		p++
	}
	coordLog.Infof("topic selected leader: %v, topic selected isr : %v", leaders, isrlist)
	return leaders, isrlist, nil
}

func (dpm *DataPlacement) allocMultiPartTopicLeaderAndISR(topicName string, currentNodes map[string]NsqdNodeInfo,
	replica int, partitionNum int, existPart map[int]*TopicPartitionMetaInfo) ([]string, [][]string, error) {
	leaders := make([]string, partitionNum)
	isrlist := make([][]string, partitionNum)
	partitionNodes, err := dpm.getRebalancedMultiTopicPartitions(
		topicName,
		partitionNum,
		replica, currentNodes)
	if err != nil {
		return nil, nil, err
	}
	for p := 0; p < partitionNum; p++ {
		var isr []string
		if elem, ok := existPart[p]; ok {
			leaders[p] = elem.Leader
			isr = elem.ISR
		} else {
			isr = partitionNodes[p]
			leaders[p] = isr[0]
		}
		isrlist[p] = isr
	}

	coordLog.Infof("selected leader: %v, topic selected isr : %v", leaders, isrlist)
	return leaders, isrlist, nil
}

func (dpm *DataPlacement) prepareCandidateNodesForNewLeader(topicInfo *TopicPartitionMetaInfo,
	currentNodes map[string]NsqdNodeInfo) ([]string, int64) {
	newestReplicas := make([]string, 0)
	newestLogID := int64(0)
	for _, replica := range topicInfo.ISR {
		if _, ok := currentNodes[replica]; !ok {
			coordLog.Infof("ignore failed node %v while choose new leader : %v", replica, topicInfo.GetTopicDesp())
			continue
		}
		if replica == topicInfo.Leader {
			continue
		}
		cid, err := dpm.lookupCoord.getNsqdLastCommitLogID(replica, topicInfo)
		if err != nil {
			coordLog.Infof("failed to get log id on replica: %v, %v", replica, err)
			continue
		}
		if cid > newestLogID {
			newestReplicas = newestReplicas[0:0]
			newestReplicas = append(newestReplicas, replica)
			newestLogID = cid
		} else if cid == newestLogID {
			newestReplicas = append(newestReplicas, replica)
		}
	}
	return newestReplicas, newestLogID
}

func (dpm *DataPlacement) chooseNewLeaderFromISR(topicInfo *TopicPartitionMetaInfo, currentNodes map[string]NsqdNodeInfo) (string, int64, *CoordErr) {
	newestReplicas, newestLogID := dpm.prepareCandidateNodesForNewLeader(topicInfo, currentNodes)
	newLeader := ""
	if len(newestReplicas) == 1 {
		newLeader = newestReplicas[0]
		coordLog.Infof("topic %v new leader %v found with commit id: %v in only one candidate", topicInfo.GetTopicDesp(), newLeader, newestLogID)
		return newLeader, newestLogID, nil
	}
	if topicInfo.AllowMulti() {
		newLeader = dpm.chooseNewLeaderFromISRForMultiPartTopic(topicInfo, currentNodes, newestReplicas)
	} else {
		sortedTopN, topicList, err := dpm.getTopNTopics(currentNodes)
		if err == nil && dpm.isTopNTopic(topicInfo, sortedTopN) {
			newLeader = dpm.chooseNewLeaderFromISRForTopN(topicInfo, sortedTopN, topicList, currentNodes, newestReplicas)
		} else {
			loadFactors := make(map[string]float64, len(newestReplicas))
			for _, replica := range newestReplicas {
				stat, err := dpm.lookupCoord.getNsqdTopicStat(currentNodes[replica])
				if err != nil {
					coordLog.Infof("ignore node %v while choose new leader : %v, %v", replica, topicInfo.GetTopicDesp(), err)
					continue
				}
				loadFactors[replica] = stat.GetNodeLeaderLoadFactor()
			}
			newLeader = dpm.chooseNewLeaderByLeastLoadFactor(loadFactors)
		}
	}
	if newLeader == "" {
		coordLog.Warningf("No leader can be elected. current topic info: %v", topicInfo)
		return "", 0, ErrNoLeaderCanBeElected
	}
	coordLog.Infof("topic %v new leader %v found with commit id: %v from candidate %v", topicInfo.GetTopicDesp(), newLeader, newestLogID, newestReplicas)
	return newLeader, newestLogID, nil
}

// chooseNewLeaderByLeastLoadFactor choose another leader in ISR list, and add new node to ISR
// list.
// select the least load factor node
func (dpm *DataPlacement) chooseNewLeaderByLeastLoadFactor(loadFactors map[string]float64) string {
	if len(loadFactors) == 0 {
		return ""
	}

	var (
		newLeader = ""
		minLF     = float64(math.MaxInt64)
	)

	for replica, lf := range loadFactors {
		coordLog.Infof("node %v load factor is : %v", replica, lf)
		if newLeader == "" || lf < minLF {
			newLeader = replica
			minLF = lf
		}
	}

	return newLeader
}

func (dpm *DataPlacement) chooseNewLeaderFromISRForTopN(topicInfo *TopicPartitionMetaInfo,
	sortedTopics LFListT,
	topicList []TopicPartitionMetaInfo,
	currentNodes map[string]NsqdNodeInfo,
	newestReplicas []string) string {
	newLeader := ""
	if len(newestReplicas) == 0 {
		return newLeader
	}
	expectedISR, err := dpm.getBalancedTopNTopicISR(topicInfo, sortedTopics, topicList, currentNodes)
	if err != nil {
		coordLog.Infof("failed to get balanced partitions for topn topic: %v, %v", topicInfo.GetTopicDesp(), err)
	} else {
		coordLog.Infof("topic %v choose new leader from %v, %v ", topicInfo.GetTopicDesp(), newestReplicas, expectedISR)
		for _, nid := range expectedISR {
			if nid == topicInfo.Leader {
				continue
			}
			for _, validNode := range newestReplicas {
				if nid == validNode {
					newLeader = nid
					break
				}
			}
			if newLeader != "" {
				break
			}
		}
	}
	if newLeader == "" {
		newLeader = newestReplicas[0]
	}
	return newLeader
}

func (dpm *DataPlacement) chooseNewLeaderFromISRForMultiPartTopic(topicInfo *TopicPartitionMetaInfo,
	currentNodes map[string]NsqdNodeInfo,
	newestReplicas []string) string {
	newLeader := ""
	if len(newestReplicas) == 0 {
		return newLeader
	}
	partitionNodes, err := dpm.getRebalancedMultiTopicPartitions(
		topicInfo.Name,
		topicInfo.PartitionNum,
		topicInfo.Replica, currentNodes)
	if err != nil {
		coordLog.Infof("failed to get balanced partitions for ordered topic: %v, %v", topicInfo.GetTopicDesp(), err)
	} else {
		for _, nid := range partitionNodes[topicInfo.Partition] {
			if nid == topicInfo.Leader {
				continue
			}
			for _, validNode := range newestReplicas {
				if nid == validNode {
					newLeader = nid
					break
				}
			}
			if newLeader != "" {
				break
			}
		}
	}
	if newLeader == "" {
		newLeader = newestReplicas[0]
		coordLog.Infof("all the balanced isr is not in the newest log list: %v, %v",
			partitionNodes, newestReplicas)
	}

	return newLeader
}

func getTopNTopicsStats(nodeTopicStats []NodeTopicStats, topicInfoList []TopicPartitionMetaInfo, n int, filterNonmove bool) LFListT {
	// filter out the ordered topics
	tinfoMap := getTopicInfoMap(topicInfoList)
	allTopicLoads := make(LFListT, 0, len(topicInfoList))
	addedNames := make(map[string]string)
	for _, nodeStat := range nodeTopicStats {
		topics := nodeStat.GetSortedTopicWriteLevel(true)
		sort.Sort(sort.Reverse(topics))
		coordLog.Infof("all sorted topics on node %v : %v", nodeStat.NodeID, len(topics))
		added := 0
		for _, tn := range topics {
			tinfo, ok := tinfoMap[tn.topic]
			if !ok {
				continue
			}
			if tinfo.AllowMulti() {
				continue
			}
			if filterNonmove {
				// some topic replica*partition == len(nodes), so we no need balance
				if tinfo.Replica*tinfo.PartitionNum == len(nodeTopicStats) {
					continue
				}
			}
			added++
			v, ok := addedNames[tn.topic]
			if ok {
				coordLog.Infof("dup topic leader %v old: %v", tn, v)
				continue
			}
			allTopicLoads = append(allTopicLoads, tn)
			addedNames[tn.topic] = nodeStat.NodeID
			if added >= n {
				break
			}
		}
	}
	// since we only get leader for each node, the topic-pid should have no duplication
	sort.Sort(sort.Reverse(allTopicLoads))
	if len(allTopicLoads) <= n {
		return allTopicLoads
	}
	return allTopicLoads[:n]
}

func (dpm *DataPlacement) getRebalancedTopNTopic(
	sortedTopicList LFListT, topicInfos []TopicPartitionMetaInfo,
	currentNodes map[string]NsqdNodeInfo) (map[string][]string, []string, int, error) {
	nodeNameList := make(SortableStrings, 0, len(currentNodes))
	for nid := range currentNodes {
		nodeNameList = append(nodeNameList, nid)
	}
	return dpm.getRebalancedTopNTopicFromNameList(sortedTopicList, topicInfos, nodeNameList)
}

func (dpm *DataPlacement) getRebalancedTopNTopicFromNameList(
	sortedTopicList LFListT, topicInfos []TopicPartitionMetaInfo,
	nodeNameList SortableStrings) (map[string][]string, []string, int, error) {
	sort.Sort(nodeNameList)
	topNISRNodes := make(map[string][]string, len(sortedTopicList))
	selectIndex := 0
	topicInfoMap := getTopicInfoMap(topicInfos)

	allTopicNames := make([]string, 0)
	nameMap := make(map[string]bool)
	for i := 0; i < len(sortedTopicList); i++ {
		topicPart := sortedTopicList[i]
		name, _, err := splitTopicPartitionID(topicPart.topic)
		if err != nil {
			coordLog.Infof("topic fullname error %v", topicPart.topic)
			return topNISRNodes, allTopicNames, 0, err
		}
		_, ok := nameMap[name]
		if ok {
			continue
		}
		allTopicNames = append(allTopicNames, name)
		nameMap[name] = true
	}
	isrChanged := 0
	for i := 0; i < len(allTopicNames); i++ {
		hasNew := false
		tname := allTopicNames[i]
		topic0Part := allTopicNames[i] + "-0"
		info, ok := topicInfoMap[topic0Part]
		if !ok {
			coordLog.Infof("no topic info found for %v", topic0Part)
			return topNISRNodes, allTopicNames, isrChanged, ErrBalanceNodeUnavailable
		}
		replica := info.Replica
		if info.PartitionNum*replica > len(nodeNameList) {
			return topNISRNodes, allTopicNames, isrChanged, ErrBalanceNodeUnavailable
		}
		oldISRs := make([][]string, 0)
		for pid := 0; pid < info.PartitionNum; pid++ {
			nlist := make([]string, replica)
			fullname := GetTopicFullName(tname, pid)
			oldInfo := topicInfoMap[fullname]
			oldISRs = append(oldISRs, oldInfo.ISR)
			topNISRNodes[fullname] = nlist
			for k := 0; k < replica; k++ {
				nlist[k] = nodeNameList[(selectIndex+k+pid*replica)%len(nodeNameList)]
			}
			if oldInfo.Leader != nlist[0] {
				hasNew = true
			}
		}
		if !hasNew {
			for pid := 0; pid < info.PartitionNum; pid++ {
				fullname := GetTopicFullName(tname, pid)
				nlist := topNISRNodes[fullname]
				for k := 0; k < replica; k++ {
					if FindSliceList(oldISRs, nlist[k]) == -1 {
						hasNew = true
					}
				}
			}
		}

		if hasNew {
			isrChanged++
		}
		selectIndex++
	}

	return topNISRNodes, allTopicNames, isrChanged, nil
}

func (dpm *DataPlacement) rebalanceTopNTopicsByLoad(monitorChan chan struct{},
	topicList []TopicPartitionMetaInfo,
	nodeTopicStats []NodeTopicStats,
	currentNodes map[string]NsqdNodeInfo) (bool, bool, LFListT) {
	moved := false
	if !atomic.CompareAndSwapInt32(&dpm.lookupCoord.balanceWaiting, 0, 1) {
		coordLog.Infof("another balance is running, should wait")
		return false, moved, nil
	}
	defer atomic.StoreInt32(&dpm.lookupCoord.balanceWaiting, 0)

	sortedTopNTopics := getTopNTopicsStats(nodeTopicStats, topicList, topicTopNLimit, true)
	if len(sortedTopNTopics) < 2 {
		coordLog.Infof("ignore balance topn since not enough topn: %v", len(sortedTopNTopics))
		return true, moved, nil
	}
	topNISRNodes, topicNames, isrChanged, err := dpm.getRebalancedTopNTopic(sortedTopNTopics, topicList, currentNodes)
	if err != nil {
		return false, moved, sortedTopNTopics
	}
	coordLog.Infof("balance topn isr changed : %v", isrChanged)
	if isrChanged <= topNBalanceDiff || len(topicNames) <= topNBalanceDiff {
		return true, moved, sortedTopNTopics
	}
	mostLFTopic := topicNames[0]
	sort.Sort(SortableStrings(topicNames))

	movedCnt := 0
	needBalanceOthers := true
	tinfoMap := getTopicInfoMap(topicList)
	nodeNameList := make([]string, 0)
	for _, n := range currentNodes {
		nodeNameList = append(nodeNameList, n.ID)
	}
	for _, tname := range topicNames {
		select {
		case <-monitorChan:
			return false, moved, sortedTopNTopics
		default:
		}
		if !dpm.lookupCoord.IsClusterStable() || !dpm.lookupCoord.IsMineLeader() || movedCnt > 20 {
			coordLog.Infof("no balance since cluster is not stable or too much moved %v while checking balance", movedCnt)
			return false, moved, sortedTopNTopics
		}
		topicInfo0, ok := tinfoMap[GetTopicFullName(tname, 0)]
		if !ok {
			coordLog.Infof("ignore balance topn since topic info not found: %v", tname)
			continue
		}
		availableNodes := make(map[string]NsqdNodeInfo)
		for k, v := range currentNodes {
			availableNodes[k] = v
		}
		anyFailed := false
		// get all new partitions for this topics
		isrList := make([][]string, topicInfo0.PartitionNum)
		moveToNodes := make([]string, 0, topicInfo0.PartitionNum*topicInfo0.Replica)
		for i := 0; i < topicInfo0.PartitionNum; i++ {
			tfullName := GetTopicFullName(tname, i)
			isr := topNISRNodes[tfullName]
			isrList[i] = isr
			for _, v := range isr {
				delete(availableNodes, v)
				moveToNodes = append(moveToNodes, v)
			}
		}
		if len(isrList) == 0 || len(availableNodes) == 0 {
			continue
		}
		var firstNonISRAvailableNode string
		for n := range availableNodes {
			firstNonISRAvailableNode = n
			break
		}
		oldNodes := make([][]string, topicInfo0.PartitionNum)
		// group old nodes to 3 different group, one for old node but no new
		// one for new node but no old, one for old changed to other new
		// left is old new unchanged isr.
		newNodeForNewISR := make([][]string, topicInfo0.PartitionNum)
		oldNewChangedISR := make([][]string, topicInfo0.PartitionNum)
		oldRemovedISR := make([][]string, topicInfo0.PartitionNum)
		waitingMoveCnt := 0
		for pid := 0; pid < topicInfo0.PartitionNum; pid++ {
			tfullName := GetTopicFullName(tname, pid)
			tinfo, ok := tinfoMap[tfullName]
			if !ok || len(tinfo.ISR) != tinfo.Replica {
				coordLog.Infof("ignore balance topn topic %v since info not stable: %v", tfullName, tinfo)
				anyFailed = true
				break
			}
			oldNodes[pid] = tinfo.ISR
			for _, old := range tinfo.ISR {
				found := false
				for ni, newISR := range isrList {
					if FindSlice(newISR, old) != -1 {
						found = true
						if ni == pid {
							// old partition unchanged on this node, no need move
						} else {
							// the partition on this node is changed, we need replace with other partition
							oldNewChangedISR[pid] = append(oldNewChangedISR[pid], old)
							needBalanceOthers = false
							waitingMoveCnt++
						}
						break
					}
				}
				if !found {
					// the node has no any partitions in new isr
					// we need remove old from them
					oldRemovedISR[pid] = append(oldRemovedISR[pid], old)
					needBalanceOthers = false
					waitingMoveCnt++
				}
			}
		}
		if anyFailed {
			needBalanceOthers = false
			continue
		}

		if waitingMoveCnt == 0 {
			leaderMoved, err := dpm.tryMoveLeaderToExpectedForAllParts(tname, topicInfo0.PartitionNum, isrList)
			if err != nil {
				needBalanceOthers = false
			}
			if leaderMoved {
				moved = true
				needBalanceOthers = false
			}
			continue
		}
		if tname == mostLFTopic {
			// avoid move replica of the most data topic to reduce data transfer
			continue
		}
		hasNewNode := false
		// find all new nodes that not in any old isr
		for pid, nisr := range isrList {
			for _, n := range nisr {
				found := false
				for _, olds := range oldNodes {
					if FindSlice(olds, n) != -1 {
						found = true
						break
					}
				}
				if !found {
					newNodeForNewISR[pid] = append(newNodeForNewISR[pid], n)
					hasNewNode = true
					needBalanceOthers = false
				}
			}
		}
		coordLog.Infof("balance topn topic %v expected isr list : %v, old isr: %v, old need remove: %v, brand new add: %v, partition changed: %v",
			tname, isrList, oldNodes, oldRemovedISR, newNodeForNewISR, oldNewChangedISR)
		if !hasNewNode && firstNonISRAvailableNode == "" {
			// no node available for tmp move
			continue
		}
		// 1. check the nodes that need move, add tmp node for move if no any new node for new isr
		// 2. try move partition changed nodes to new node which has no any old partitions
		// 3. try move partition removed nodes to new node until all new node is done
		// 4. try move partition changed nodes to expect isr if it can until all done
		// 5. try move partition removed nodes to expect isr if it can until all done
		// 6. move tmp node back to expected isr
		// consider situation:
		// n1 n2 n3 n4 n5 n6 n7 n8 n9
		//  0  0  0  1  1  1 (old)
		//     0  0  1  1  1  0
		//  1  0  0     1  1  0
		//  1     0  0  1  1  0
		//  1  1  0  0     1  0
		//  1  1  0  0  0  1 (new)

		//  0  0  0  1  1  1 (old)
		//  0  0  0     1  1  1
		//  0  0  0        1  1  1
		//  0  0  0           1  1  1
		//     0  0  0        1  1  1
		//        0  0  0     1  1  1
		//           0  0  0  1  1  1 (new)

		//  0  0  0  1  1  1  (old)
		//  0     0  1  1  1     0
		//  0     0     1  1  1  0
		//        0  0  1  1  1  0
		//     1  0  0  1     1  0  (new)

		var tmpNonISRMove struct {
			node string
			pid  int
		}
		if !hasNewNode {
			for pid, nlist := range oldNewChangedISR {
				done, movedNode := dpm.tryMoveAnyOldNodesToNewNode(monitorChan, nlist, firstNonISRAvailableNode, tname, pid)
				if done {
					tmpNonISRMove.node = movedNode
					tmpNonISRMove.pid = pid
					break
				}
			}
			if tmpNonISRMove.node == "" {
				coordLog.Infof("balance topn topic %v failed to move any to new node: %v, %v", tname, firstNonISRAvailableNode, oldNewChangedISR)
				continue
			}
		} else {
			for pid, nlist := range newNodeForNewISR {
				for _, n := range nlist {
					moveOlds := oldNewChangedISR[pid]
					done, _ := dpm.tryMoveAnyOldNodesToNewNode(monitorChan, moveOlds, n, tname, pid)
					if done {
						moved = true
						continue
					}
					moveOlds = oldRemovedISR[pid]
					done, _ = dpm.tryMoveAnyOldNodesToNewNode(monitorChan, moveOlds, n, tname, pid)
					if !done {
						// no node can be moved to new
						coordLog.Infof("balance topn topic %v failed to move any to new node: %v, %v",
							tname, n, oldNewChangedISR)
						anyFailed = true
						break
					}
				}
				if anyFailed {
					break
				}
			}
		}

		if anyFailed {
			needBalanceOthers = false
			continue
		}
		loop := 0
		for {
			allMoved := true
			loop++
			anyDone := false
			for pid, nlist := range oldNewChangedISR {
				for i, n := range nlist {
					if n == "" {
						continue
					}
					allMoved = false
					err = dpm.addToCatchupAndWaitISRReady(monitorChan, false, n, tname, pid, isrList[pid], nil, false)
					if err == nil {
						moved = true
						nlist[i] = ""
						anyDone = true
					}
				}
			}
			if allMoved {
				break
			}
			if !anyDone {
				coordLog.Infof("balance topn topic %v can not move any: %v, %v", tname, oldNewChangedISR, isrList)
				anyFailed = true
				break
			}
			if loop > len(moveToNodes) {
				coordLog.Infof("balance topn topic %v too much loop: %v", tname, loop)
				anyFailed = true
				break
			}
		}
		if anyFailed {
			continue
		}
		for pid, nlist := range oldRemovedISR {
			for i, n := range nlist {
				if n == "" {
					continue
				}
				err = dpm.addToCatchupAndWaitISRReady(monitorChan, false, n, tname, pid, isrList[pid], nil, false)
				if err == nil {
					moved = true
					nlist[i] = ""
				} else {
					coordLog.Infof("balance topn topic %v can not move removed node: %v, %v", tname, oldRemovedISR, isrList[pid])
					anyFailed = true
				}
			}
		}
		if anyFailed {
			continue
		}

		if !hasNewNode {
			// move tmp non isr node back to expected isr
			err = dpm.addToCatchupAndWaitISRReady(monitorChan, false, tmpNonISRMove.node,
				tname, tmpNonISRMove.pid, isrList[tmpNonISRMove.pid], nil, false)
			if err != nil {
				coordLog.Infof("balance topn topic %v can not move tmp non isr node backup: %v, %v", tname, tmpNonISRMove, isrList[tmpNonISRMove.pid])
				return false, moved, sortedTopNTopics
			}
		}
		movedCnt++
		leaderMoved, err := dpm.tryMoveLeaderToExpectedForAllParts(tname, topicInfo0.PartitionNum, isrList)
		if err != nil {
			return false, moved, sortedTopNTopics
		}
		if leaderMoved {
			moved = true
			needBalanceOthers = false
		}
	}
	return needBalanceOthers, moved, sortedTopNTopics
}

func (dpm *DataPlacement) tryMoveLeaderToExpectedForAllParts(tname string, pnum int, isrList [][]string) (bool, error) {
	moved := false
	for pid := 0; pid < pnum; pid++ {
		topicInfo, err := dpm.lookupCoord.leadership.GetTopicInfo(tname, pid)
		if err != nil {
			return moved, err
		}
		if (topicInfo.Leader != isrList[pid][0]) &&
			(len(topicInfo.ISR) >= topicInfo.Replica) {
			moved = true
			coordLog.Infof("balance topn topic %v move leader %v to expected isr : %v", tname, topicInfo, isrList[pid])
			dpm.lookupCoord.handleRemoveTopicNodeOrMoveLeader(true,
				topicInfo.Name, topicInfo.Partition, topicInfo.Leader)
			time.Sleep(time.Second)
		}
	}
	return moved, nil
}

func (dpm *DataPlacement) tryMoveAnyOldNodesToNewNode(monitorChan chan struct{}, fromNodeList []string, toNode string, tname string, pid int) (bool, string) {
	done := false
	var movedNode string
	for i, old := range fromNodeList {
		if old == "" {
			continue
		}
		err := dpm.addToCatchupAndWaitISRReady(monitorChan, false, old, tname, pid, []string{toNode}, nil, false)
		if err == nil {
			done = true
			fromNodeList[i] = ""
			movedNode = old
			break
		}
	}
	return done, movedNode
}

func (dpm *DataPlacement) balanceTopicToExpectedISR(monitorChan chan struct{}, topicInfo TopicPartitionMetaInfo, expectedISR []string, nodeNameList []string) (bool, bool, error) {
	if len(expectedISR) == 0 {
		return false, false, nil
	}
	moveNodes := make([]string, 0)
	for _, nid := range topicInfo.ISR {
		found := false
		for _, expectedNode := range expectedISR {
			if nid == expectedNode {
				found = true
				break
			}
		}
		if !found {
			moveNodes = append(moveNodes, nid)
		}
	}
	needMove := false
	moved := false
	if len(moveNodes) > 0 {
		needMove = true
	}
	for _, nid := range moveNodes {
		coordLog.Infof("node %v need move for topic %v since %v not in expected isr list: %v", nid,
			topicInfo.GetTopicDesp(), topicInfo.ISR, expectedISR)
		var err error
		if len(topicInfo.ISR) <= topicInfo.Replica {
			if topicInfo.AllowMulti() {
				err = dpm.addToCatchupAndWaitISRReadyForMultiPartTopic(monitorChan, false, nid, &topicInfo,
					nodeNameList)
			} else {
				err = dpm.addToCatchupAndWaitISRReady(monitorChan, false, nid, topicInfo.Name, topicInfo.Partition,
					expectedISR, nodeNameList, true)
			}
		} else {
			coordErr := dpm.lookupCoord.handleRemoveTopicNodeOrMoveLeader(nid == topicInfo.Leader, topicInfo.Name, topicInfo.Partition, nid)
			if coordErr != nil {
				err = coordErr.ToErrorType()
			}
		}
		if err != nil {
			return needMove, moved, err
		} else {
			moved = true
		}
		newTopicInfo, err := dpm.lookupCoord.leadership.GetTopicInfo(topicInfo.Name, topicInfo.Partition)
		if err != nil {
			return needMove, moved, err
		} else {
			topicInfo = *newTopicInfo
		}
	}
	if (topicInfo.Leader != expectedISR[0]) &&
		(len(topicInfo.ISR) >= topicInfo.Replica) {
		needMove = true
		moved = true
		dpm.lookupCoord.handleRemoveTopicNodeOrMoveLeader(true,
			topicInfo.Name, topicInfo.Partition, topicInfo.Leader)

		time.Sleep(time.Second)
	}
	return needMove, moved, nil
}

func (dpm *DataPlacement) rebalanceMultiPartTopic(monitorChan chan struct{}) (bool, bool) {
	moved := false
	isAllBalanced := false
	if !atomic.CompareAndSwapInt32(&dpm.lookupCoord.balanceWaiting, 0, 1) {
		coordLog.Infof("another balance is running, should wait")
		return moved, isAllBalanced
	}
	defer atomic.StoreInt32(&dpm.lookupCoord.balanceWaiting, 0)

	topicList, err := dpm.lookupCoord.leadership.ScanTopics()
	if err != nil {
		coordLog.Infof("scan topics error: %v", err)
		return moved, isAllBalanced
	}
	nodeNameList := make([]string, 0)
	movedTopic := ""
	isAllBalanced = true
	for _, topicInfo := range topicList {
		if !topicInfo.AllowMulti() {
			continue
		}
		select {
		case <-monitorChan:
			return moved, false
		default:
		}
		if !dpm.lookupCoord.IsClusterStable() {
			return moved, false
		}
		if !dpm.lookupCoord.IsMineLeader() {
			return moved, true
		}
		// balance only one topic once
		if movedTopic != "" && movedTopic != topicInfo.Name {
			continue
		}
		currentNodes := dpm.lookupCoord.getCurrentNodes()
		nodeNameList = nodeNameList[0:0]
		for _, n := range currentNodes {
			nodeNameList = append(nodeNameList, n.ID)
		}

		partitionNodes, err := dpm.getRebalancedMultiTopicPartitions(
			topicInfo.Name,
			topicInfo.PartitionNum,
			topicInfo.Replica, currentNodes)
		if err != nil {
			isAllBalanced = false
			continue
		}

		expectedISR := partitionNodes[topicInfo.Partition]
		needMove, singleMoved, err := dpm.balanceTopicToExpectedISR(monitorChan, topicInfo, expectedISR, nodeNameList)
		if needMove {
			movedTopic = topicInfo.Name
			isAllBalanced = false
		}
		if singleMoved {
			moved = true
		}
		if topicInfo.Leader != expectedISR[0] {
			isAllBalanced = false
		}
		if err != nil {
			return moved, false
		}
	}
	return moved, isAllBalanced
}

type SortableStrings []string

func (s SortableStrings) Less(l, r int) bool {
	return s[l] < s[r]
}
func (s SortableStrings) Len() int {
	return len(s)
}
func (s SortableStrings) Swap(l, r int) {
	s[l], s[r] = s[r], s[l]
}

func (dpm *DataPlacement) getRebalancedMultiTopicPartitions(
	topicName string,
	partitionNum int, replica int,
	currentNodes map[string]NsqdNodeInfo) ([][]string, error) {
	if len(currentNodes) < replica {
		return nil, ErrBalanceNodeUnavailable
	}
	// for ordered topic we have much partitions than nodes,
	// so we need make all the nodes have the almost the same leader partitions,
	// and also we need make all the nodes have the almost the same followers.
	// and to avoid the data migration, we should keep the data as much as possible
	// algorithm as below:
	// 1. sort node id ; 2. sort the topic partitions
	// 3. choose the (leader, follower, follower) for each partition,
	// start from the index of the current node array
	// 4. for next partition, start from the next index of node array.
	//  l -> leader, f-> follower
	//         nodeA   nodeB   nodeC   nodeD
	// p1       l       f        f
	// p2               l        f      f
	// p3       f                l      f
	// p4       f       f               l
	// p5       l       f        f
	// p6               l        f      f
	// p7       f                l      f
	// p8       f       f               l
	// p9       l       f        f
	// p10              l        f      f

	// after nodeB is down, the migration as bellow
	//         nodeA   xxxx   nodeC   nodeD
	// p1       l       x        f     x-f
	// p2      x-f      x       f-l     f
	// p3       f                l      f
	// p4       f       x       x-f     l
	// p5       l       x        f     x-f
	// p6      x-f      x       f-l     f
	// p7       f                l      f
	// p8       f       x       x-f     l
	// p9       l       x        f     x-f
	// p10     x-f      x       f-l     f
	nodeNameList := make(SortableStrings, 0, len(currentNodes))
	for nid := range currentNodes {
		nodeNameList = append(nodeNameList, nid)
	}
	return dpm.getRebalancedMultiTopicPartitionsFromNameList(topicName, partitionNum, replica, nodeNameList)
}
func (dpm *DataPlacement) getRebalancedMultiTopicPartitionsFromNameList(
	topicName string,
	partitionNum int, replica int,
	nodeNameList SortableStrings) ([][]string, error) {
	if len(nodeNameList) < replica {
		return nil, ErrBalanceNodeUnavailable
	}
	sort.Sort(nodeNameList)
	partitionNodes := make([][]string, partitionNum)
	selectIndex := int(murmur3.Sum32([]byte(topicName)))
	if selectIndex < 0 {
		selectIndex = 0
	}
	for i := 0; i < partitionNum; i++ {
		nlist := make([]string, replica)
		partitionNodes[i] = nlist
		for j := 0; j < replica; j++ {
			nlist[j] = nodeNameList[(selectIndex+j)%len(nodeNameList)]
		}
		selectIndex++
	}

	return partitionNodes, nil
}

func (dpm *DataPlacement) decideUnwantedISRNode(topicInfo *TopicPartitionMetaInfo, currentNodes map[string]NsqdNodeInfo) string {
	unwantedNode := ""
	//remove the unwanted node in isr
	if !topicInfo.AllowMulti() {
		maxLF := 0.0
		for _, nodeID := range topicInfo.ISR {
			if nodeID == topicInfo.Leader {
				continue
			}
			n, ok := currentNodes[nodeID]
			if !ok {
				// the isr maybe lost, we should exit without any remove
				return unwantedNode
			}
			stat, err := dpm.lookupCoord.getNsqdTopicStat(n)
			if err != nil {
				continue
			}
			_, nlf := stat.GetNodeLoadFactor()
			if nlf > maxLF {
				maxLF = nlf
				unwantedNode = nodeID
			}
		}
	} else {
		partitionNodes, err := dpm.getRebalancedMultiTopicPartitions(
			topicInfo.Name,
			topicInfo.PartitionNum,
			topicInfo.Replica, currentNodes)
		if err != nil {
			return unwantedNode
		}
		for _, nid := range topicInfo.ISR {
			if nid == topicInfo.Leader {
				continue
			}
			found := false
			for _, validNode := range partitionNodes[topicInfo.Partition] {
				if nid == validNode {
					found = true
					break
				}
			}
			if !found {
				unwantedNode = nid
				if nid != topicInfo.Leader {
					break
				}
			}
		}
	}
	return unwantedNode
}
