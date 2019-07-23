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

	"github.com/spaolacci/murmur3"

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
)

var topicTopNLimit = 50

const (
	RATIO_BETWEEN_LEADER_FOLLOWER = 0.7
	defaultTopicLoadFactor        = 3
	HIGHEST_PUB_QPS_LEVEL         = 100
	HIGHEST_LEFT_CONSUME_MB_SIZE  = 50 * 1024
	HIGHEST_LEFT_DATA_MB_SIZE     = 200 * 1024
	busyTopicLevel                = 13
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

func (self *NodeTopicStats) GetNodeLoadFactor() (float64, float64) {
	leaderLF := self.GetNodeLeaderLoadFactor()
	totalDataSize := int64(0)
	for _, v := range self.TopicTotalDataSize {
		totalDataSize += v
	}
	totalDataSize += int64(len(self.TopicTotalDataSize))
	if totalDataSize > HIGHEST_LEFT_DATA_MB_SIZE {
		totalDataSize = HIGHEST_LEFT_DATA_MB_SIZE
	}
	return leaderLF, leaderLF + float64(totalDataSize)/HIGHEST_LEFT_DATA_MB_SIZE*5
}

func (self *NodeTopicStats) GetNodeLeaderLoadFactor() float64 {
	leftConsumed := int64(0)
	for _, t := range self.ChannelDepthData {
		leftConsumed += t
	}
	leftConsumed += int64(len(self.ChannelDepthData))
	if leftConsumed > HIGHEST_LEFT_CONSUME_MB_SIZE {
		leftConsumed = HIGHEST_LEFT_CONSUME_MB_SIZE
	}
	totalLeaderDataSize := int64(0)
	for _, v := range self.TopicLeaderDataSize {
		totalLeaderDataSize += v
	}
	totalLeaderDataSize += int64(len(self.TopicLeaderDataSize))
	if totalLeaderDataSize > HIGHEST_LEFT_DATA_MB_SIZE {
		totalLeaderDataSize = HIGHEST_LEFT_DATA_MB_SIZE
	}
	avgWrite := self.GetNodeAvgWriteLevel()
	if avgWrite > HIGHEST_PUB_QPS_LEVEL {
		avgWrite = HIGHEST_PUB_QPS_LEVEL
	}

	return avgWrite/HIGHEST_PUB_QPS_LEVEL*80.0 + float64(leftConsumed)/HIGHEST_LEFT_CONSUME_MB_SIZE*10.0 + float64(totalLeaderDataSize)/HIGHEST_LEFT_DATA_MB_SIZE*10.0
}

func (self *NodeTopicStats) GetNodePeakLevelList() []int64 {
	levelList := make([]int64, 8)
	for _, dataList := range self.TopicHourlyPubDataList {
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

func (self *NodeTopicStats) GetNodeAvgWriteLevel() float64 {
	level := int64(0)
	tmp := make(IntHeap, 0, 24)
	for _, dataList := range self.TopicHourlyPubDataList {
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

func (self *NodeTopicStats) GetNodeAvgReadLevel() float64 {
	level := float64(0)
	tmp := make(IntHeap, 0, 24)
	for topicName, dataList := range self.TopicHourlyPubDataList {
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
		num, ok := self.ChannelNum[topicName]
		if ok {
			level += math.Log2(1.0+float64(num)) * float64(sum)
		}
	}
	return convertQPSLevel(int64(level))
}

func (self *NodeTopicStats) GetTopicLoadFactor(topicFullName string) float64 {
	data := self.TopicTotalDataSize[topicFullName]
	if data > HIGHEST_LEFT_DATA_MB_SIZE {
		data = HIGHEST_LEFT_DATA_MB_SIZE
	}
	return self.GetTopicLeaderLoadFactor(topicFullName) + float64(data)/HIGHEST_LEFT_DATA_MB_SIZE*5.0
}

func (self *NodeTopicStats) GetTopicLeaderLoadFactor(topicFullName string) float64 {
	writeLevel := self.GetTopicAvgWriteLevel(topicFullName)
	depth := self.ChannelDepthData[topicFullName]
	if depth > HIGHEST_LEFT_CONSUME_MB_SIZE {
		depth = HIGHEST_LEFT_CONSUME_MB_SIZE
	}
	data := self.TopicLeaderDataSize[topicFullName]
	if data > HIGHEST_LEFT_DATA_MB_SIZE {
		data = HIGHEST_LEFT_DATA_MB_SIZE
	}
	return writeLevel/HIGHEST_PUB_QPS_LEVEL*80.0 + float64(depth)/HIGHEST_LEFT_CONSUME_MB_SIZE*10.0 + float64(data)/HIGHEST_LEFT_DATA_MB_SIZE*10.0
}

func (self *NodeTopicStats) GetTopicAvgWriteLevel(topicFullName string) float64 {
	dataList, ok := self.TopicHourlyPubDataList[topicFullName]
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
	return s[i].loadFactor < s[j].loadFactor
}

func (self *NodeTopicStats) GetSortedTopicWriteLevel(leaderOnly bool) LFListT {
	topicLFList := make(LFListT, 0)
	for topicName := range self.TopicHourlyPubDataList {
		d, ok := self.TopicLeaderDataSize[topicName]
		if leaderOnly {
			if !ok || d <= 0 {
				continue
			}
		} else {
			if ok && d > 0 {
				continue
			}
		}
		lf := 0.0
		if leaderOnly {
			lf = self.GetTopicLeaderLoadFactor(topicName)
		} else {
			lf = self.GetTopicLoadFactor(topicName)
		}
		topicLFList = append(topicLFList, loadFactorInfo{
			topic:      topicName,
			loadFactor: lf,
		})
	}
	sort.Sort(topicLFList)
	return topicLFList
}

func (self *NodeTopicStats) GetMostBusyAndIdleTopicWriteLevel(leaderOnly bool) (string, string, float64, float64) {
	busy := float64(0.0)
	busyTopic := ""
	idle := float64(math.MaxInt32)
	idleTopic := ""
	for topicName := range self.TopicHourlyPubDataList {
		d, ok := self.TopicLeaderDataSize[topicName]
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
			sum = self.GetTopicLeaderLoadFactor(topicName)
		} else {
			sum = self.GetTopicLoadFactor(topicName)
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

func (self *NodeTopicStats) GetTopicPeakLevel(topic TopicPartitionID) float64 {
	selectedTopic := topic.String()
	dataList, ok := self.TopicHourlyPubDataList[selectedTopic]
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

func (self *NodeTopicStats) LeaderLessLoader(other *NodeTopicStats) bool {
	left := self.GetNodeLeaderLoadFactor()
	right := other.GetNodeLeaderLoadFactor()
	if math.Abs(left-right) < 0.5 {
		left := float64(len(self.TopicLeaderDataSize)) + float64(len(self.TopicTotalDataSize)-len(self.TopicLeaderDataSize))*RATIO_BETWEEN_LEADER_FOLLOWER
		right := float64(len(other.TopicLeaderDataSize)) + float64(len(other.TopicTotalDataSize)-len(other.TopicLeaderDataSize))*RATIO_BETWEEN_LEADER_FOLLOWER
		return left < right
	}
	if left < right {
		return true
	}

	return false
}

func (self *NodeTopicStats) SlaveLessLoader(other *NodeTopicStats) bool {
	_, left := self.GetNodeLoadFactor()
	_, right := other.GetNodeLoadFactor()
	if math.Abs(left-right) < 0.5 {
		left := float64(len(self.TopicLeaderDataSize)) + float64(len(self.TopicTotalDataSize)-len(self.TopicLeaderDataSize))*RATIO_BETWEEN_LEADER_FOLLOWER
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

func (dpm *DataPlacement) DoBalance(monitorChan chan struct{}) {
	//check period for the data balance.
	ticker := time.NewTicker(time.Minute)
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
			avgLeaderLoad := 0.0
			minLeaderLoad := float64(math.MaxInt32)
			maxLeaderLoad := 0.0
			avgNodeLoad := 0.0
			minNodeLoad := float64(math.MaxInt32)
			maxNodeLoad := 0.0
			// if max load is 4 times more than avg load, we need move some
			// leader from max to min load node one by one.
			// if min load is 4 times less than avg load, we can move some
			// leader to this min load node.
			coordLog.Infof("begin checking balance of topic data...")
			currentNodes := dpm.lookupCoord.getCurrentNodes()
			nodeTopicStats = nodeTopicStats[:0]
			validNum := 0
			var mostLeaderStats *NodeTopicStats
			mostLeaderNum := 0
			var leastLeaderStats *NodeTopicStats
			leastLeaderNum := math.MaxInt32
			for nodeID, nodeInfo := range currentNodes {
				topicStat, err := dpm.lookupCoord.getNsqdTopicStat(nodeInfo)
				if err != nil {
					coordLog.Infof("failed to get node topic status while checking balance: %v", nodeID)
					continue
				}
				nodeTopicStats = append(nodeTopicStats, *topicStat)
				leaderLF, nodeLF := topicStat.GetNodeLoadFactor()
				coordLog.Infof("nsqd node %v load factor is : (%v, %v)", nodeID, leaderLF, nodeLF)
				if leaderLF < minLeaderLoad {
					topicStatsMinMax[0] = topicStat
					minLeaderLoad = leaderLF
				}
				minNodeLoad = math.Min(nodeLF, minNodeLoad)
				if leaderLF > maxLeaderLoad {
					topicStatsMinMax[1] = topicStat
					maxLeaderLoad = leaderLF
				}
				maxNodeLoad = math.Max(nodeLF, maxNodeLoad)

				avgLeaderLoad += leaderLF
				avgNodeLoad += nodeLF
				validNum++
				leaderNum := len(topicStat.TopicLeaderDataSize)
				if leaderNum > mostLeaderNum {
					mostLeaderNum = leaderNum
					mostLeaderStats = topicStat
				}
				if leaderNum < leastLeaderNum {
					leastLeaderNum = leaderNum
					leastLeaderStats = topicStat
				}
			}
			if validNum < 2 || topicStatsMinMax[0] == nil || topicStatsMinMax[1] == nil {
				continue
			}

			moved, _ := dpm.rebalanceOrderedTopic(monitorChan)
			if moved {
				continue
			}

			leaderSort := func(l, r *NodeTopicStats) bool {
				return l.LeaderLessLoader(r)
			}
			By(leaderSort).Sort(nodeTopicStats)

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
			moved, topNTopics = dpm.rebalanceTopNTopics(monitorChan, nodeTopicStats)
			if moved {
				continue
			}
			// filter out topN topics
			for _, s := range nodeTopicStats {
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
			}
			moveLeader := false
			avgTopicNum := len(topicList) / len(currentNodes)
			if len(topicStatsMinMax[1].TopicLeaderDataSize) > int(1.1*float64(avgTopicNum)) {
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
			} else if avgLeaderLoad < 5 && maxLeaderLoad < 10 {
				// all nodes in the cluster are under low load, no need balance
				continue
			} else if avgLeaderLoad*2 < maxLeaderLoad {
				dpm.balanceTopicLeaderBetweenNodes(monitorChan, moveLeader, moveAny, minLeaderLoad,
					maxLeaderLoad, topicStatsMinMax, nodeTopicStats)
			} else if avgLeaderLoad >= 20 &&
				((minLeaderLoad*2 < maxLeaderLoad) || (maxLeaderLoad > avgLeaderLoad*1.5)) {
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
				(minNodeLoad*2 < maxNodeLoad || maxNodeLoad > avgNodeLoad*1.5) {
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
						} else if followerNum > int(float64(avgTopicNum)*1.5) {
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
					if float64(mostTopicNum) > float64(leastTopicNum)*1.5 && minNodeLoad < midNodeLoad {
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
	if leaderLF < midLeaderLoad && moveTopicNum < avgTopicNum*2 {
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

	coordLog.Infof("balance topic: %v, %v(%v) from node: %v, move : %v ", idleTopic,
		busyTopic, busyLevel, statsMinMax[1].NodeID, moveOp)
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
			coordLog.Warningf("split topic name and partition failed: %v", err)
		} else {
			checkMoveOK = dpm.checkAndPrepareMove(monitorChan, topicName, partitionID,
				statsMinMax,
				sortedNodeTopicStats, moveOp, moveLeader)
		}
	}
	// TODO: handle the node which has more than two busy topics.
	// make sure all nodes have the average busy topics.
	// check top10 topics and make sure all partitions of them is crossed multi different nodes.

	if !checkMoveOK && idleTopic != "" {
		topicName, partitionID, err = splitTopicPartitionID(idleTopic)
		if err != nil {
			coordLog.Warningf("split topic name and partition failed: %v", err)
		} else {
			checkMoveOK = dpm.checkAndPrepareMove(monitorChan, topicName, partitionID,
				statsMinMax,
				sortedNodeTopicStats, moveOp, moveLeader)
		}
	}
	if !checkMoveOK {
		// maybe we can move some other topic if both idle/busy is not movable
		sortedTopics := statsMinMax[1].GetSortedTopicWriteLevel(moveLeader)
		coordLog.Infof("check topic for moving , all sorted topic: %v", sortedTopics)
		for index, t := range sortedTopics {
			if t.topic == idleTopic || t.topic == busyTopic {
				continue
			}
			if index > len(sortedTopics)/2 {
				break
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
				checkMoveOK = dpm.checkAndPrepareMove(monitorChan, topicName, partitionID,
					statsMinMax,
					sortedNodeTopicStats, moveOp, moveLeader)
			}
		}
	}
	if checkMoveOK {
		dpm.lookupCoord.handleMoveTopic(moveLeader, topicName, partitionID, statsMinMax[1].NodeID)
	}
}

func (dpm *DataPlacement) checkAndPrepareMove(monitorChan chan struct{}, topicName string, partitionID int,
	statsMinMax []*NodeTopicStats,
	sortedNodeTopicStats []NodeTopicStats, moveOp balanceOpLevel, moveLeader bool) bool {
	topicInfo, err := dpm.lookupCoord.leadership.GetTopicInfo(topicName, partitionID)
	if err != nil {
		coordLog.Infof("failed to get topic %v info: %v", topicName, err)
		return false
	}
	checkMoveOK := false
	if topicInfo.OrderedMulti {
		coordLog.Infof("topic %v is configured as multi ordered, no balance", topicName)
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
				err := dpm.addToCatchupAndWaitISRReady(monitorChan, topicName, partitionID,
					statsMinMax[0].NodeID,
					sortedNodeTopicStats, false)
				if err != nil {
					checkMoveOK = false
				} else {
					checkMoveOK = true
				}
			}
		}
	}
	if moveOp == moveMinLFOnly {
		return checkMoveOK
	}
	if moveOp > moveAny || (len(topicInfo.ISR)-1 <= topicInfo.Replica/2) {
		if moveLeader {
			// check if any of current isr nodes is already idle for move
			for _, nid := range topicInfo.ISR {
				if checkMoveOK {
					break
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
			err := dpm.addToCatchupAndWaitISRReady(monitorChan, topicName, partitionID, "", sortedNodeTopicStats, false)
			if err != nil {
				checkMoveOK = false
			} else {
				checkMoveOK = true
			}
		}
	} else {
		// we are allowed to move to any node and no need to add any node to isr
		checkMoveOK = true
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

	return dpm.moveTopicPartition(topicName, partitionID, moveLeader, fromNode, toNode)
}

func (dpm *DataPlacement) moveTopicPartition(topicName string, partitionID int,
	moveLeader bool, fromNode string, toNode string) error {
	topicInfo, err := dpm.lookupCoord.leadership.GetTopicInfo(topicName, partitionID)
	if err != nil {
		coordLog.Infof("failed to get topic info: %v-%v: %v", topicName, partitionID, err)
		return err
	}
	coordLog.Infof("try move topic: %v-%v data from %v to %v", topicName, partitionID, fromNode, toNode)

	if fromNode == toNode {
		return errors.New("move to and from node can not be the same")
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

	if !dpm.lookupCoord.IsClusterStable() {
		return ErrClusterUnstable
	}
	currentNodes := dpm.lookupCoord.getCurrentNodes()
	if _, ok := currentNodes[toNode]; !ok {
		return errors.New("the move destination is not found in cluster")
	}
	if _, ok := currentNodes[fromNode]; !ok {
		return errors.New("the move source data is lost in cluster")
	}
	if FindSlice(topicInfo.ISR, toNode) == -1 {
		// add destination to catchup and wait
		if FindSlice(topicInfo.CatchupList, toNode) != -1 {
			// wait ready
		} else {
			excludeNodes, commonErr := dpm.getExcludeNodesForTopic(topicInfo, true)

			if commonErr != nil {
				return ErrLeadershipServerUnstable.ToErrorType()
			}
			if _, ok := excludeNodes[toNode]; ok {
				coordLog.Infof("current node: %v is excluded for topic: %v-%v", toNode, topicName, partitionID)
				return ErrNodeIsExcludedForTopicData
			}
			dpm.lookupCoord.addCatchupNode(topicInfo, toNode)
		}
		waitStart := time.Now()
		for {
			if !dpm.lookupCoord.IsMineLeader() {
				coordLog.Infof("not leader while checking balance")
				return errors.New("lost leader while waiting move done")
			}
			topicInfo, err = dpm.lookupCoord.leadership.GetTopicInfo(topicName, partitionID)
			if err != nil {
				coordLog.Infof("failed to get topic info: %v-%v: %v", topicName, partitionID, err)
			} else {
				if FindSlice(topicInfo.ISR, toNode) != -1 {
					break
				}
			}
			if time.Since(waitStart) > time.Minute*10 {
				return errors.New("timeout waiting move data")
			}

			select {
			case <-dpm.lookupCoord.stopChan:
				return errors.New("quiting")
			case <-time.After(time.Second * 5):
				coordLog.Infof("waiting move data")
			}
		}
	}
	if FindSlice(topicInfo.ISR, toNode) != -1 {
		dpm.lookupCoord.handleMoveTopic(moveLeader, topicName, partitionID, fromNode)
	}
	return nil
}

func (dpm *DataPlacement) addToCatchupAndWaitISRReady(monitorChan chan struct{}, topicName string,
	partitionID int, addNode string, sortedNodeTopicStats []NodeTopicStats, tryAllNodes bool) error {
	if len(sortedNodeTopicStats) == 0 {
		return errors.New("no stats data")
	}
	retry := 0
	currentSelect := 0
	topicInfo, err := dpm.lookupCoord.leadership.GetTopicInfo(topicName, partitionID)
	if err != nil {
		coordLog.Infof("failed to get topic info: %v-%v: %v", topicName, partitionID, err)
		return err
	}
	if topicInfo.OrderedMulti && addNode == "" {
		nodeNameList := make([]string, 0, len(sortedNodeTopicStats))
		for _, s := range sortedNodeTopicStats {
			nodeNameList = append(nodeNameList, s.NodeID)
		}
		return dpm.addToCatchupAndWaitISRReadyForOrderedTopic(monitorChan,
			topicInfo, nodeNameList)
	}
	filteredNodes := make([]string, 0)
	if addNode != "" {
		if FindSlice(topicInfo.ISR, addNode) != -1 {
		} else {
			filteredNodes = append(filteredNodes, addNode)
		}
	} else {
		for index, s := range sortedNodeTopicStats {
			if !tryAllNodes {
				if index >= len(sortedNodeTopicStats)-2 ||
					index > len(sortedNodeTopicStats)/2 {
					// never move to the busy nodes
					break
				}
			}
			if FindSlice(topicInfo.ISR, s.NodeID) != -1 {
				// filter
			} else {
				filteredNodes = append(filteredNodes, s.NodeID)
			}
		}
	}
	for {
		if currentSelect >= len(filteredNodes) {
			coordLog.Infof("currently no any node can be balanced for topic: %v", topicName)
			return ErrBalanceNodeUnavailable
		}
		nid := filteredNodes[currentSelect]
		topicInfo, err := dpm.lookupCoord.leadership.GetTopicInfo(topicName, partitionID)
		if err != nil {
			coordLog.Infof("failed to get topic info: %v-%v: %v", topicName, partitionID, err)
		} else {
			if FindSlice(topicInfo.ISR, nid) != -1 {
				break
			} else if FindSlice(topicInfo.CatchupList, nid) != -1 {
				// wait ready
				select {
				case <-monitorChan:
					return errors.New("quiting")
				case <-time.After(time.Second * 5):
					coordLog.Infof("node: %v is added for topic: %v-%v catchup, still waiting catchup", nid, topicName, partitionID)
				}
				continue
			} else {
				excludeNodes, commonErr := dpm.getExcludeNodesForTopic(topicInfo, false)
				if commonErr != nil {
					return ErrLeadershipServerUnstable.ToErrorType()
				}
				if _, ok := excludeNodes[nid]; ok {
					coordLog.Infof("current node: %v is excluded for topic: %v-%v", nid, topicName, partitionID)
					currentSelect++
					continue
				}
				coordLog.Infof("node: %v is added for topic: %v-%v catchup", nid, topicName, partitionID)
				dpm.lookupCoord.addCatchupNode(topicInfo, nid)
			}
		}
		select {
		case <-monitorChan:
			return errors.New("quiting")
		case <-time.After(time.Second * 5):
		}
		if retry > 5 {
			coordLog.Infof("add catchup and wait timeout : %v", topicName)
			return errors.New("wait timeout")
		}
		retry++
	}
	return nil
}

func (dpm *DataPlacement) addToCatchupAndWaitISRReadyForOrderedTopic(monitorChan chan struct{}, topicInfo *TopicPartitionMetaInfo,
	nodeNameList []string) error {
	retry := 0
	currentSelect := 0
	topicName := topicInfo.Name
	partitionID := topicInfo.Partition
	// since we need add new catchup, we make the replica as replica+1
	partitionNodes, err := dpm.getRebalancedOrderedTopicPartitionsFromNameList(
		topicInfo.Name,
		topicInfo.PartitionNum,
		topicInfo.Replica+1, nodeNameList)
	if err != nil {
		return err.ToErrorType()
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
		nid := selectedCatchup[currentSelect]
		topicInfo, err := dpm.lookupCoord.leadership.GetTopicInfo(topicName, partitionID)
		if err != nil {
			coordLog.Infof("failed to get topic info: %v-%v: %v", topicName, partitionID, err)
		} else {
			if FindSlice(topicInfo.ISR, nid) != -1 {
				break
			} else if FindSlice(topicInfo.CatchupList, nid) != -1 {
				// wait ready
				select {
				case <-monitorChan:
					return errors.New("quiting")
				case <-time.After(time.Second * 5):
					coordLog.Infof("node: %v is added for topic: %v-%v catchup, still waiting catchup", nid, topicName, partitionID)
				}
				continue
			} else {
				excludeNodes, commonErr := dpm.getExcludeNodesForTopic(topicInfo, true)
				if commonErr != nil {
					return ErrLeadershipServerUnstable.ToErrorType()
				}
				if _, ok := excludeNodes[nid]; ok {
					coordLog.Infof("current node: %v is excluded for topic: %v-%v", nid, topicName, partitionID)
					currentSelect++
					continue
				}
				coordLog.Infof("node: %v is added for topic: %v-%v catchup", nid, topicName, partitionID)
				dpm.lookupCoord.addCatchupNode(topicInfo, nid)
			}
		}
		select {
		case <-monitorChan:
			return errors.New("quiting")
		case <-time.After(time.Second * 5):
		}
		if retry > 5 {
			coordLog.Infof("add catchup and wait timeout : %v", topicName)
			return errors.New("wait timeout")
		}
		retry++
	}
	return nil
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
	if checkMulti && meta.OrderedMulti {
		// we allow this topic multi partitions on the same node
		return excludeNodes, nil
	}
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

func (dpm *DataPlacement) allocNodeForTopic(topicInfo *TopicPartitionMetaInfo, currentNodes map[string]NsqdNodeInfo) (*NsqdNodeInfo, *CoordErr) {
	if topicInfo.OrderedMulti {
		return dpm.allocNodeForOrderedTopic(topicInfo, currentNodes)
	}
	// collect the nsqd data, check if any node has the topic data already.
	var chosenNode NsqdNodeInfo
	var chosenStat *NodeTopicStats

	excludeNodes, commonErr := dpm.getExcludeNodesForTopic(topicInfo, false)
	if commonErr != nil {
		return nil, ErrLeadershipServerUnstable
	}

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
	if chosenNode.ID == "" {
		coordLog.Infof("no more available node for topic: %v, excluding nodes: %v, all nodes: %v", topicInfo.GetTopicDesp(), excludeNodes, currentNodes)
		return nil, ErrNodeUnavailable
	}
	coordLog.Infof("node %v is alloc for topic: %v", chosenNode, topicInfo.GetTopicDesp())
	return &chosenNode, nil
}

func (dpm *DataPlacement) allocNodeForOrderedTopic(topicInfo *TopicPartitionMetaInfo, currentNodes map[string]NsqdNodeInfo) (*NsqdNodeInfo, *CoordErr) {
	var chosenNode NsqdNodeInfo

	excludeNodes, commonErr := dpm.getExcludeNodesForTopic(topicInfo, true)
	if commonErr != nil {
		return nil, ErrLeadershipServerUnstable
	}

	partitionNodes, err := dpm.getRebalancedOrderedTopicPartitions(
		topicInfo.Name,
		topicInfo.PartitionNum,
		topicInfo.Replica, currentNodes)
	if err != nil {
		return nil, err
	}
	for _, nodeID := range partitionNodes[topicInfo.Partition] {
		if _, ok := excludeNodes[nodeID]; ok {
			continue
		}
		chosenNode = currentNodes[nodeID]
		break
	}
	if chosenNode.ID == "" {
		coordLog.Infof("no more available node for topic: %v, excluding nodes: %v, all nodes: %v", topicInfo.GetTopicDesp(), excludeNodes, currentNodes)
		return nil, ErrNodeUnavailable
	}
	coordLog.Infof("node %v is alloc for topic: %v", chosenNode, topicInfo.GetTopicDesp())
	return &chosenNode, nil
}

func (dpm *DataPlacement) checkTopicNodeConflict(topicInfo *TopicPartitionMetaInfo) bool {
	existLeaders := make(map[string]struct{})
	existSlaves := make(map[string]struct{})
	if !topicInfo.OrderedMulti {
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
		}
	}
	// isr should be different
	for _, id := range topicInfo.ISR {
		if _, ok := existLeaders[id]; ok {
			return false
		}
		if _, ok := existSlaves[id]; ok {
			return false
		}
		// add checked to map to check dup in isr
		existLeaders[id] = struct{}{}
		existSlaves[id] = struct{}{}
	}

	return true
}

// init leader node and isr list for the empty topic
func (dpm *DataPlacement) allocTopicLeaderAndISR(topicName string, orderedMulti bool, currentNodes map[string]NsqdNodeInfo,
	replica int, partitionNum int, existPart map[int]*TopicPartitionMetaInfo) ([]string, [][]string, *CoordErr) {

	if orderedMulti {
		return dpm.allocOrderedTopicLeaderAndISR(topicName, currentNodes, replica, partitionNum, existPart)
	}

	if len(currentNodes) < replica || len(currentNodes) < partitionNum {
		coordLog.Infof("nodes %v is less than replica %v or partition %v", len(currentNodes), replica, partitionNum)
		return nil, nil, ErrNodeUnavailable
	}
	if len(currentNodes) < replica*partitionNum {
		coordLog.Infof("nodes is less than replica*partition")
		return nil, nil, ErrNodeUnavailable
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
		return nil, nil, ErrNodeUnavailable
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
					return nil, nil, ErrNodeUnavailable
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
					return nil, nil, ErrNodeUnavailable
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

func (dpm *DataPlacement) allocOrderedTopicLeaderAndISR(topicName string, currentNodes map[string]NsqdNodeInfo,
	replica int, partitionNum int, existPart map[int]*TopicPartitionMetaInfo) ([]string, [][]string, *CoordErr) {
	leaders := make([]string, partitionNum)
	isrlist := make([][]string, partitionNum)
	partitionNodes, err := dpm.getRebalancedOrderedTopicPartitions(
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

func (dpm *DataPlacement) chooseNewLeaderFromISR(topicInfo *TopicPartitionMetaInfo, currentNodes map[string]NsqdNodeInfo) (string, int64, *CoordErr) {
	if topicInfo.OrderedMulti {
		return dpm.chooseNewLeaderFromISRForOrderedTopic(topicInfo, currentNodes)
	}
	// choose another leader in ISR list, and add new node to ISR
	// list.
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
	// select the least load factor node
	newLeader := ""
	if len(newestReplicas) == 1 {
		newLeader = newestReplicas[0]
	} else {
		minLF := float64(math.MaxInt64)
		for _, replica := range newestReplicas {
			stat, err := dpm.lookupCoord.getNsqdTopicStat(currentNodes[replica])
			if err != nil {
				coordLog.Infof("ignore node %v while choose new leader : %v", replica, topicInfo.GetTopicDesp())
				continue
			}
			lf := stat.GetNodeLeaderLoadFactor()

			coordLog.Infof("node %v load factor is : %v", replica, lf)
			if newLeader == "" || lf < minLF {
				newLeader = replica
			}
		}
	}
	if newLeader == "" {
		coordLog.Warningf("No leader can be elected. current topic info: %v", topicInfo)
		return "", 0, ErrNoLeaderCanBeElected
	}
	coordLog.Infof("topic %v new leader %v found with commit id: %v", topicInfo.GetTopicDesp(), newLeader, newestLogID)
	return newLeader, newestLogID, nil
}

func (dpm *DataPlacement) chooseNewLeaderFromISRForOrderedTopic(topicInfo *TopicPartitionMetaInfo, currentNodes map[string]NsqdNodeInfo) (string, int64, *CoordErr) {
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
	// select the least load factor node
	newLeader := ""
	if len(newestReplicas) == 1 {
		newLeader = newestReplicas[0]
	} else if len(newestReplicas) > 0 {
		partitionNodes, err := dpm.getRebalancedOrderedTopicPartitions(
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
			if newLeader == "" {
				newLeader = newestReplicas[0]
				coordLog.Infof("all the balanced isr is not in the newest log list: %v, %v",
					partitionNodes[topicInfo.Partition], newestReplicas)
			}
		}
	}
	if newLeader == "" {
		coordLog.Warningf("No leader can be elected. current topic info: %v", topicInfo)
		return "", 0, ErrNoLeaderCanBeElected
	}
	coordLog.Infof("topic %v new leader %v found with commit id: %v", topicInfo.GetTopicDesp(), newLeader, newestLogID)
	return newLeader, newestLogID, nil
}

func getTopNTopicsStats(nodeTopicStats []NodeTopicStats, topicInfoList []TopicPartitionMetaInfo, n int, filterNonmove bool) LFListT {
	// filter out the ordered topics
	tinfoMap := make(map[string]TopicPartitionMetaInfo, len(topicInfoList))
	for _, tinfo := range topicInfoList {
		tinfoMap[tinfo.GetTopicDesp()] = tinfo
	}
	allTopicLoads := make(LFListT, 0, len(topicInfoList))
	for _, nodeStat := range nodeTopicStats {
		topics := nodeStat.GetSortedTopicWriteLevel(true)
		added := 0
		for _, tn := range topics {
			tinfo, ok := tinfoMap[tn.topic]
			if !ok {
				continue
			}
			if tinfo.OrderedMulti {
				continue
			}
			if filterNonmove {
				// some topic replica*partition == len(nodes), so we no need balance
				if tinfo.Replica*tinfo.PartitionNum == len(nodeTopicStats) {
					continue
				}
			}
			added++
			allTopicLoads = append(allTopicLoads, tn)
			if added >= n {
				break
			}
		}
	}
	// since we only get leader for each node, the topic-pid should have no duplication
	sort.Sort(allTopicLoads)
	if len(allTopicLoads) <= n {
		return allTopicLoads
	}
	return allTopicLoads[:n]
}

func (dpm *DataPlacement) rebalanceTopNTopics(monitorChan chan struct{}, nodeTopicStats []NodeTopicStats) (bool, LFListT) {
	anyMoved := false
	if !atomic.CompareAndSwapInt32(&dpm.lookupCoord.balanceWaiting, 0, 1) {
		coordLog.Infof("another balance is running, should wait")
		return anyMoved, nil
	}
	defer atomic.StoreInt32(&dpm.lookupCoord.balanceWaiting, 0)

	topicList, err := dpm.lookupCoord.leadership.ScanTopics()
	if err != nil {
		coordLog.Infof("scan topics error: %v", err)
		return anyMoved, nil
	}
	sortedTopNTopics := getTopNTopicsStats(nodeTopicStats, topicList, topicTopNLimit, true)
	if len(sortedTopNTopics) < 2 {
		return anyMoved, nil
	}

	nodeTopNum := make(map[string]int, len(nodeTopicStats))
	nodeLeaderTopNum := make(map[string]int, len(nodeTopicStats))
	for _, topT := range sortedTopNTopics {
		for _, n := range nodeTopicStats {
			_, ok := n.TopicLeaderDataSize[topT.topic]
			if ok {
				cnt := nodeLeaderTopNum[n.NodeID]
				nodeLeaderTopNum[n.NodeID] = cnt + 1
			}
			_, ok = n.TopicTotalDataSize[topT.topic]
			if ok {
				cnt := nodeTopNum[n.NodeID]
				nodeTopNum[n.NodeID] = cnt + 1
			}
		}
	}
	// move mid load topic first
	waitMoveTopics := make(LFListT, 0, len(sortedTopNTopics))
	waitMoveTopics = append(waitMoveTopics, sortedTopNTopics[len(sortedTopNTopics)/2:]...)
	// no need move the top 1
	for i := len(sortedTopNTopics) / 2; i > 0; i-- {
		waitMoveTopics = append(waitMoveTopics, sortedTopNTopics[i])
	}
	// rebalance all replicas for topics
	anyMoved = dpm.rebalanceTopNTopicsBetweenNodes(false, nodeTopNum, waitMoveTopics)
	if anyMoved {
		return anyMoved, sortedTopNTopics
	}
	// rebalance all leader only if all replicas is balanced
	anyMoved = dpm.rebalanceTopNTopicsBetweenNodes(true, nodeLeaderTopNum, waitMoveTopics)
	return anyMoved, sortedTopNTopics
}

func (dpm *DataPlacement) rebalanceTopNTopicsBetweenNodes(moveLeaderBetweenISR bool, nodeTopNum map[string]int, sortedTopNTopics LFListT) bool {
	// rebalance all replicas for topics
	sum := 0
	for _, c := range nodeTopNum {
		sum += c
	}
	avg := sum / len(nodeTopNum)
	coordLog.Infof("before balance, top n: %v, avg is : %v", nodeTopNum, avg)
	sortedNode := make(LFListT, 0, len(nodeTopNum))
	for n, c := range nodeTopNum {
		if c == avg || c == avg+1 {
			delete(nodeTopNum, n)
			continue
		}
		sortedNode = append(sortedNode, loadFactorInfo{
			name:       n,
			loadFactor: float64(c),
		})
	}
	sort.Sort(sortedNode)
	if len(sortedNode) < 2 {
		return false
	}
	if sortedNode[0].loadFactor-sortedNode[len(sortedNode)-1].loadFactor < 5 {
		return false
	}
	sortedNodeBelowAvg := make(LFListT, 0)
	for _, n := range sortedNode {
		if int(n.loadFactor) >= avg {
			continue
		}
		sortedNodeBelowAvg = append(sortedNodeBelowAvg, n)
	}
	coordLog.Infof("balance need move top n: %v to %v", sortedNode, sortedNodeBelowAvg)
	if len(sortedNodeBelowAvg) == 0 {
		return false
	}
	anyMoved := false
	for _, from := range sortedNode {
		moveToIndex := len(sortedNodeBelowAvg) - 1
		for {
			if int(from.loadFactor) <= avg+1 {
				break
			}
			if moveToIndex < 0 {
				break
			}
			chosen := sortedNodeBelowAvg[moveToIndex]
			// move from n to chosen node
			if int(chosen.loadFactor) >= avg {
				break
			}
			moved := false
			for _, topT := range sortedTopNTopics {
				tn := topT.topic
				topic, pid, _ := splitTopicPartitionID(tn)
				tpInfo, err := dpm.lookupCoord.leadership.GetTopicInfo(topic, pid)
				if err != nil {
					continue
				}
				isLeader := tpInfo.Leader == from.name
				if moveLeaderBetweenISR {
					if !isLeader {
						continue
					}
					if FindSlice(tpInfo.ISR, chosen.name) == -1 {
						continue
					}
				}
				coordLog.Infof("try move topic %v from %v to %v", topT, from.name, chosen.name)
				err = dpm.moveTopicPartition(topic, pid, isLeader, from.name, chosen.name)
				if err != nil {
					continue
				}
				moved = true
			}
			if moved {
				from.loadFactor = from.loadFactor - 1
				chosen.loadFactor = chosen.loadFactor + 1
				sortedNodeBelowAvg[moveToIndex] = chosen
				sort.Sort(sortedNodeBelowAvg)
				anyMoved = true
				moveToIndex = len(sortedNodeBelowAvg) - 1
			} else {
				moveToIndex--
			}
		}
	}
	return anyMoved
}

func (dpm *DataPlacement) rebalanceOrderedTopic(monitorChan chan struct{}) (bool, bool) {
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
		if !topicInfo.OrderedMulti {
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

		partitionNodes, err := dpm.getRebalancedOrderedTopicPartitions(
			topicInfo.Name,
			topicInfo.PartitionNum,
			topicInfo.Replica, currentNodes)
		if err != nil {
			isAllBalanced = false
			continue
		}
		moveNodes := make([]string, 0)
		for _, nid := range topicInfo.ISR {
			found := false
			for _, expectedNode := range partitionNodes[topicInfo.Partition] {
				if nid == expectedNode {
					found = true
					break
				}
			}
			if !found {
				moveNodes = append(moveNodes, nid)
			}
		}
		for _, nid := range moveNodes {
			movedTopic = topicInfo.Name
			coordLog.Infof("node %v need move for topic %v since %v not in expected isr list: %v", nid,
				topicInfo.GetTopicDesp(), topicInfo.ISR, partitionNodes[topicInfo.Partition])
			var err error
			if len(topicInfo.ISR) <= topicInfo.Replica {
				err = dpm.addToCatchupAndWaitISRReadyForOrderedTopic(monitorChan, &topicInfo,
					nodeNameList)
			}
			if err != nil {
				return moved, false
			} else {
				coordErr := dpm.lookupCoord.handleMoveTopic(nid == topicInfo.Leader,
					topicInfo.Name, topicInfo.Partition, nid)
				if coordErr != nil {
					return moved, false
				} else {
					moved = true
				}
			}
			newTopicInfo, err := dpm.lookupCoord.leadership.GetTopicInfo(topicInfo.Name, topicInfo.Partition)
			if err != nil {
				return moved, false
			} else {
				topicInfo = *newTopicInfo
			}
		}
		if (topicInfo.Leader != partitionNodes[topicInfo.Partition][0]) &&
			(len(topicInfo.ISR) >= topicInfo.Replica) {
			moved = true
			dpm.lookupCoord.handleMoveTopic(true,
				topicInfo.Name, topicInfo.Partition, topicInfo.Leader)
			select {
			case <-monitorChan:
			case <-time.After(time.Second):
			}
		}
		if len(moveNodes) > 0 ||
			(topicInfo.Leader != partitionNodes[topicInfo.Partition][0]) {
			isAllBalanced = false
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

func (dpm *DataPlacement) getRebalancedOrderedTopicPartitions(
	topicName string,
	partitionNum int, replica int,
	currentNodes map[string]NsqdNodeInfo) ([][]string, *CoordErr) {
	if len(currentNodes) < replica {
		return nil, ErrNodeUnavailable
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
	return dpm.getRebalancedOrderedTopicPartitionsFromNameList(topicName, partitionNum, replica, nodeNameList)
}
func (dpm *DataPlacement) getRebalancedOrderedTopicPartitionsFromNameList(
	topicName string,
	partitionNum int, replica int,
	nodeNameList SortableStrings) ([][]string, *CoordErr) {
	if len(nodeNameList) < replica {
		return nil, ErrNodeUnavailable
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
	if !topicInfo.OrderedMulti {
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
		partitionNodes, err := dpm.getRebalancedOrderedTopicPartitions(
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
