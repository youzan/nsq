package nsqd

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/youzan/nsq/internal/quantile"
	"github.com/youzan/nsq/internal/util"
)

const (
	maxPubClientStats = 5000
)

type TopicStats struct {
	TopicName                   string           `json:"topic_name"`
	TopicFullName               string           `json:"topic_full_name"`
	TopicPartition              string           `json:"topic_partition"`
	Channels                    []ChannelStats   `json:"channels"`
	ChannelNum                  int64            `json:"channel_num"`
	Depth                       int64            `json:"depth"`
	BackendDepth                int64            `json:"backend_depth"`
	BackendStart                int64            `json:"backend_start"`
	MessageCount                uint64           `json:"message_count"`
	IsLeader                    bool             `json:"is_leader"`
	HourlyPubSize               int64            `json:"hourly_pubsize"`
	Clients                     []ClientPubStats `json:"client_pub_stats"`
	ClientNum                   int64            `json:"client_num"`
	MsgSizeStats                []int64          `json:"msg_size_stats"`
	MsgWriteLatencyStats        []int64          `json:"msg_write_latency_stats"`
	IsMultiOrdered              bool             `json:"is_multi_ordered"`
	IsMultiPart                 bool             `json:"is_multi_part"`
	IsExt                       bool             `json:"is_ext"`
	IsChannelAutoCreateDisabled bool             `json:"is_channel_auto_create_disabled"`
	StatsdName                  string           `json:"statsd_name"`
	PubFailedCnt                int64            `json:"pub_failed_cnt"`

	E2eProcessingLatency *quantile.Result `json:"e2e_processing_latency"`
}

func NewTopicStats(t *Topic, channels []ChannelStats, filterClients bool) TopicStats {
	statsdName := t.GetTopicName()
	if t.IsOrdered() || t.GetDynamicInfo().MultiPart {
		statsdName += "." + strconv.Itoa(t.GetTopicPart())
	}
	var clients []ClientPubStats
	clientNum := t.detailStats.GetPubClientNum()
	if !filterClients {
		clients = t.detailStats.GetPubClientStats()
	}
	return TopicStats{
		TopicName:                   t.GetTopicName(),
		TopicFullName:               t.GetFullName(),
		TopicPartition:              strconv.Itoa(t.GetTopicPart()),
		Channels:                    channels,
		ChannelNum:                  int64(len(channels)),
		Depth:                       t.TotalDataSize(),
		BackendDepth:                t.TotalDataSize(),
		BackendStart:                t.GetQueueReadStart(),
		MessageCount:                t.TotalMessageCnt(),
		IsLeader:                    !t.IsWriteDisabled(),
		Clients:                     clients,
		ClientNum:                   int64(clientNum),
		MsgSizeStats:                t.detailStats.GetMsgSizeStats(),
		MsgWriteLatencyStats:        t.detailStats.GetMsgWriteLatencyStats(),
		IsMultiOrdered:              t.IsOrdered(),
		IsMultiPart:                 t.GetDynamicInfo().MultiPart,
		IsExt:                       t.IsExt(),
		IsChannelAutoCreateDisabled: t.IsChannelAutoCreateDisabled(),
		PubFailedCnt:                t.PubFailed(),
		StatsdName:                  statsdName,

		E2eProcessingLatency: t.AggregateChannelE2eProcessingLatency().Result(),
	}
}

type ChannelStats struct {
	ChannelName string `json:"channel_name"`
	//channel backlogs
	Backlogs int64 `json:"backlogs"`
	// message size need to consume
	Depth          int64  `json:"depth"`
	DepthSize      int64  `json:"depth_size"`
	DepthTimestamp string `json:"depth_ts"`
	DepthTsNano    int64  `json:"depth_ts_nano"`
	BackendDepth   int64  `json:"backend_depth"`
	// total size sub past hour on this channel
	HourlySubSize int64 `json:"hourly_subsize"`
	InFlightCount int   `json:"in_flight_count"`
	DeferredCount int   `json:"deferred_count"`
	// the waiting messages count which is timeouted and peeked from delayed queue
	DeferredFromDelayCount int           `json:"deferred_from_delay_count"`
	MessageCount           uint64        `json:"message_count"`
	RequeueCount           uint64        `json:"requeue_count"`
	TimeoutCount           uint64        `json:"timeout_count"`
	Clients                []ClientStats `json:"clients"`
	ClientNum              int64         `json:"client_num"`
	Paused                 bool          `json:"paused"`
	Skipped                bool          `json:"skipped"`
	ZanTestSkipped         bool          `json:"zan_test_skipped"`

	DelayedQueueCount      uint64 `json:"delayed_queue_count"`
	DelayedQueueRecent     string `json:"delayed_queue_recent"`
	DelayedQueueRecentNano int64  `json:"delayed_queue_recent_nano"`

	E2eProcessingLatency    *quantile.Result `json:"e2e_processing_latency"`
	MsgConsumeLatencyStats  []int64          `json:"msg_consume_latency_stats"`
	MsgDeliveryLatencyStats []int64          `json:"msg_delivery_latency_stats"`
}

func NewChannelStats(c *Channel, clients []ClientStats, clientNum int) ChannelStats {
	c.inFlightMutex.Lock()
	inflightCnt := len(c.inFlightMessages)
	c.inFlightMutex.Unlock()
	recentTs, dqCnt := c.GetDelayedQueueConsumedState()

	return ChannelStats{
		ChannelName:    c.name,
		Backlogs:       c.Backlogs(),
		Depth:          c.Depth(),
		DepthTimestamp: time.Unix(0, c.DepthTimestamp()).String(),
		DepthTsNano:    c.DepthTimestamp(),
		// the message bytes need to be consumed
		DepthSize:     c.DepthSize(),
		BackendDepth:  c.backend.Depth(),
		InFlightCount: inflightCnt,
		// this is total message count need consume.
		// may diff with topic total size since some is in buffer.
		MessageCount:           uint64(c.backend.GetQueueReadEnd().TotalMsgCnt()),
		RequeueCount:           atomic.LoadUint64(&c.requeueCount),
		DeferredCount:          int(atomic.LoadInt64(&c.deferredCount)),
		DeferredFromDelayCount: int(atomic.LoadInt64(&c.deferredFromDelay)),
		TimeoutCount:           atomic.LoadUint64(&c.timeoutCount),
		Clients:                clients,
		ClientNum:              int64(clientNum),
		Paused:                 c.IsPaused(),
		Skipped:                c.IsSkipped(),
		ZanTestSkipped:         c.IsZanTestSkipped(),
		DelayedQueueCount:      dqCnt,
		DelayedQueueRecent:     time.Unix(0, recentTs).String(),
		DelayedQueueRecentNano: recentTs,

		E2eProcessingLatency:    c.e2eProcessingLatencyStream.Result(),
		MsgConsumeLatencyStats:  c.channelStatsInfo.GetChannelLatencyStats(),
		MsgDeliveryLatencyStats: c.channelStatsInfo.GetDeliveryLatencyStats(),
	}
}

type ClientPubStats struct {
	RemoteAddress string `json:"remote_address"`
	UserAgent     string `json:"user_agent"`
	Protocol      string `json:"protocol"`
	PubCount      int64  `json:"pub_count"`
	ErrCount      int64  `json:"err_count"`
	LastPubTs     int64  `json:"last_pub_ts"`
}

func (cps *ClientPubStats) Copy() ClientPubStats {
	s := ClientPubStats{
		RemoteAddress: cps.RemoteAddress,
		UserAgent:     cps.UserAgent,
		Protocol:      cps.Protocol,
		PubCount:      atomic.LoadInt64(&cps.PubCount),
		ErrCount:      atomic.LoadInt64(&cps.ErrCount),
		LastPubTs:     atomic.LoadInt64(&cps.LastPubTs),
	}
	return s
}

func (cps *ClientPubStats) IncrCounter(count int64, hasErr bool) {
	if hasErr {
		atomic.AddInt64(&cps.ErrCount, count)
	} else {
		atomic.AddInt64(&cps.PubCount, count)
	}
}

type ClientStats struct {
	// TODO: deprecated, remove in 1.0
	Name string `json:"name"`

	ClientID        string `json:"client_id"`
	Hostname        string `json:"hostname"`
	Version         string `json:"version"`
	RemoteAddress   string `json:"remote_address"`
	State           int32  `json:"state"`
	ReadyCount      int64  `json:"ready_count"`
	InFlightCount   int64  `json:"in_flight_count"`
	MessageCount    uint64 `json:"message_count"`
	FinishCount     uint64 `json:"finish_count"`
	RequeueCount    uint64 `json:"requeue_count"`
	TimeoutCount    int64  `json:"timeout_count"`
	DeferredCount   int64  `json:"deferred_count"`
	ConnectTime     int64  `json:"connect_ts"`
	SampleRate      int32  `json:"sample_rate"`
	Deflate         bool   `json:"deflate"`
	Snappy          bool   `json:"snappy"`
	UserAgent       string `json:"user_agent"`
	Authed          bool   `json:"authed,omitempty"`
	AuthIdentity    string `json:"auth_identity,omitempty"`
	AuthIdentityURL string `json:"auth_identity_url,omitempty"`
	DesiredTag      string `json:"desired_tag"`

	TLS                           bool   `json:"tls"`
	CipherSuite                   string `json:"tls_cipher_suite"`
	TLSVersion                    string `json:"tls_version"`
	TLSNegotiatedProtocol         string `json:"tls_negotiated_protocol"`
	TLSNegotiatedProtocolIsMutual bool   `json:"tls_negotiated_protocol_is_mutual"`

	OutputBufferSize    int64         `json:"output_buffer_size"`
	OutputBufferTimeout int64         `json:"output_buffer_timeout"`
	MsgTimeout          int64         `json:"msg_timeout"`
	ExtFilter           ExtFilterData `json:"ext_filter,omitempty"`
	LimitedRdy          int32         `json:"limited_rdy"`
}

type Topics []*Topic

func (t Topics) Len() int      { return len(t) }
func (t Topics) Swap(i, j int) { t[i], t[j] = t[j], t[i] }

type TopicsByName struct {
	Topics
}

func (t TopicsByName) Less(i, j int) bool {
	return t.Topics[i].GetFullName() <
		t.Topics[j].GetFullName()
}

type Channels []*Channel

func (c Channels) Len() int      { return len(c) }
func (c Channels) Swap(i, j int) { c[i], c[j] = c[j], c[i] }

type ChannelsByName struct {
	Channels
}

func (c ChannelsByName) Less(i, j int) bool { return c.Channels[i].name < c.Channels[j].name }

func (n *NSQD) GetStats(leaderOnly bool, filterClients bool) []TopicStats {
	n.RLock()
	realTopics := make([]*Topic, 0, len(n.topicMap))
	for _, topicParts := range n.topicMap {
		for _, t := range topicParts {
			if leaderOnly && t.IsWriteDisabled() {
				continue
			}
			realTopics = append(realTopics, t)
		}
	}
	n.RUnlock()

	return n.getTopicStats(realTopics, "", filterClients)
}

func (n *NSQD) getTopicStats(realTopics []*Topic, ch string, filterClients bool) []TopicStats {
	sort.Sort(TopicsByName{realTopics})
	topics := make([]TopicStats, 0, len(realTopics))
	for _, t := range realTopics {
		t.channelLock.RLock()
		realChannels := make([]*Channel, 0, len(t.channelMap))
		for _, c := range t.channelMap {
			realChannels = append(realChannels, c)
		}
		t.channelLock.RUnlock()
		sort.Sort(ChannelsByName{realChannels})
		channels := make([]ChannelStats, 0, len(realChannels))
		for _, c := range realChannels {
			var clients []ClientStats
			c.RLock()
			clientNum := len(c.clients)
			if filterClients {
				clients = nil
			} else {
				if len(ch) == 0 || c.name == ch {
					clients = make([]ClientStats, 0, len(c.clients))
					for _, client := range c.clients {
						clients = append(clients, client.Stats())
					}
				}
			}
			c.RUnlock()
			channels = append(channels, NewChannelStats(c, clients, clientNum))
		}
		topics = append(topics, NewTopicStats(t, channels, filterClients))
	}
	return topics
}

func (n *NSQD) GetTopicStatsWithFilter(leaderOnly bool, topic string, ch string, filterClients bool) []TopicStats {
	n.RLock()
	realTopics := make([]*Topic, 0, len(n.topicMap))
	for name, topicParts := range n.topicMap {
		if name != topic {
			continue
		}
		for _, t := range topicParts {
			if leaderOnly && t.IsWriteDisabled() {
				continue
			}
			realTopics = append(realTopics, t)
		}
	}
	n.RUnlock()
	return n.getTopicStats(realTopics, ch, filterClients)
}

func (n *NSQD) GetTopicStats(leaderOnly bool, topic string) []TopicStats {
	return n.GetTopicStatsWithFilter(leaderOnly, topic, "", false)
}

type DetailStatsInfo struct {
	sync.Mutex
	historyStatsInfo *TopicHistoryStatsInfo
	msgStats         *TopicMsgStatsInfo
	writeErrCnt      int64
	clientPubStats   map[string]*ClientPubStats
}

func NewDetailStatsInfo(topic string, part string, initPubSize int64, historyPath string) *DetailStatsInfo {
	d := &DetailStatsInfo{
		historyStatsInfo: &TopicHistoryStatsInfo{lastHour: int32(time.Now().Hour()),
			lastPubSize: initPubSize},
		msgStats:       &TopicMsgStatsInfo{topicName: topic, topicPart: part},
		clientPubStats: make(map[string]*ClientPubStats),
	}
	d.LoadHistory(historyPath)
	return d
}

type TopicMsgStatsInfo struct {
	topicName string
	topicPart string
	// <100bytes, <1KB, 2KB, 4KB, 8KB, 16KB, 32KB, 64KB, 128KB, 256KB, 512KB, 1MB, 2MB, 4MB
	MsgSizeStats [16]int64
	// <1ms, 2ms, 4ms, 8ms, 16ms, 32ms, 64ms, 128ms, 256ms, 512ms, 1024ms, 2048ms, 4s, 8s
	MsgWriteLatencyStats [16]int64
}

type ChannelStatsInfo struct {
	topicName   string
	topicPart   string
	channelName string
	// 16ms, 32ms, 64ms, 128ms, 256ms, 512ms, 1024ms, 2048ms, 4s, 8s, 16s, above
	MsgConsumeLatencyStats  [12]int64
	MsgDeliveryLatencyStats [12]int64
}

type TopicHistoryStatsInfo struct {
	lastHour      int32
	lastPubSize   int64
	HourlyPubSize [24]int64
}

func (self *ChannelStatsInfo) UpdateChannelStats(latencyInMillSec int64) {
	self.UpdateChannelLatencyStats(latencyInMillSec)
}

func (self *ChannelStatsInfo) GetChannelLatencyStats() []int64 {
	latencyStats := make([]int64, len(self.MsgConsumeLatencyStats))
	for i := range self.MsgConsumeLatencyStats {
		latencyStats[i] = atomic.LoadInt64(&self.MsgConsumeLatencyStats[i])
	}
	return latencyStats
}

//update message consume latency distribution in millisecond
func (self *ChannelStatsInfo) UpdateChannelLatencyStats(latencyInMillSec int64) {
	bucket := 0
	if latencyInMillSec < 16 {
	} else {
		bucket = int(math.Log2(float64(latencyInMillSec/16))) + 1
	}
	if bucket >= len(self.MsgConsumeLatencyStats) {
		bucket = len(self.MsgConsumeLatencyStats) - 1
	}
	atomic.AddInt64(&self.MsgConsumeLatencyStats[bucket], 1)
}

func (self *ChannelStatsInfo) UpdateDelivery2ACKStats(latencyInMillSec int64) {
	self.UpdateDelivery2ACKLatencyStats(latencyInMillSec)
}

func (self *ChannelStatsInfo) GetDeliveryLatencyStats() []int64 {
	latencyStats := make([]int64, len(self.MsgDeliveryLatencyStats))
	for i := range self.MsgDeliveryLatencyStats {
		latencyStats[i] = atomic.LoadInt64(&self.MsgDeliveryLatencyStats[i])
	}
	return latencyStats
}

//update message consume latency distribution in millisecond
func (self *ChannelStatsInfo) UpdateDelivery2ACKLatencyStats(latencyInMillSec int64) {
	bucket := 0
	if latencyInMillSec < 16 {
	} else {
		bucket = int(math.Log2(float64(latencyInMillSec/16))) + 1
	}
	if bucket >= len(self.MsgDeliveryLatencyStats) {
		bucket = len(self.MsgDeliveryLatencyStats) - 1
	}
	atomic.AddInt64(&self.MsgDeliveryLatencyStats[bucket], 1)
	if latencyInMillSec > 8 {
		ChannelConsumeDelivery2AckLatencyMs.With(prometheus.Labels{
			"topic":     self.topicName,
			"partition": self.topicPart,
			"channel":   self.channelName,
		}).Observe(float64(latencyInMillSec))
	}
}

func (self *TopicMsgStatsInfo) UpdateMsgSizeStats(msgSize int64) {
	bucket := 0
	if msgSize < 100 {
	} else if msgSize < 1024 {
		bucket = 1
	} else if msgSize >= 1024 {
		bucket = int(math.Log2(float64(msgSize/1024))) + 2
	}
	if bucket >= len(self.MsgSizeStats) {
		bucket = len(self.MsgSizeStats) - 1
	}
	atomic.AddInt64(&self.MsgSizeStats[bucket], 1)
	if msgSize >= 1024 {
		TopicWriteByteSize.With(prometheus.Labels{
			"topic":     self.topicName,
			"partition": self.topicPart,
		}).Observe(float64(msgSize))
	}
}

func (self *TopicMsgStatsInfo) BatchUpdateMsgLatencyStats(latencyUs int64, num int64) {
	bucket := 0
	if latencyUs < 1000 {
	} else {
		bucket = int(math.Log2(float64(latencyUs/1000))) + 1
		TopicWriteLatencyMs.With(prometheus.Labels{
			"topic":     self.topicName,
			"partition": self.topicPart,
		}).Observe(float64(latencyUs / 1000))
	}
	if bucket >= len(self.MsgWriteLatencyStats) {
		bucket = len(self.MsgWriteLatencyStats) - 1
	}
	atomic.AddInt64(&self.MsgWriteLatencyStats[bucket], num)
}

func (self *TopicMsgStatsInfo) UpdateMsgLatencyStats(latencyUs int64) {
	bucket := 0
	if latencyUs < 1000 {
	} else {
		bucket = int(math.Log2(float64(latencyUs/1000))) + 1
		TopicWriteLatencyMs.With(prometheus.Labels{
			"topic":     self.topicName,
			"partition": self.topicPart,
		}).Observe(float64(latencyUs / 1000))
	}
	if bucket >= len(self.MsgWriteLatencyStats) {
		bucket = len(self.MsgWriteLatencyStats) - 1
	}
	atomic.AddInt64(&self.MsgWriteLatencyStats[bucket], 1)
}

func (self *TopicMsgStatsInfo) UpdateMsgStats(msgSize int64, latency int64) {
	self.UpdateMsgSizeStats(msgSize)
	self.UpdateMsgLatencyStats(latency)
}

// the slave should also update the pub size stat,
// since the slave need sync with leader (which will cost the write performance)
func (self *TopicHistoryStatsInfo) UpdateHourlySize(curPubSize int64) {
	now := int32(time.Now().Hour())
	lastBucket := self.lastHour % 24
	if now != self.lastHour {
		lastBucket = now % 24
		atomic.StoreInt64(&self.HourlyPubSize[lastBucket], 0)
		atomic.StoreInt32(&self.lastHour, now)
	}
	atomic.AddInt64(&self.HourlyPubSize[lastBucket], curPubSize-self.lastPubSize)
	atomic.StoreInt64(&self.lastPubSize, curPubSize)
}

func (self *DetailStatsInfo) ResetHistoryInitPub(msgSize int64) {
	now := int32(time.Now().Hour())
	atomic.StoreInt32(&self.historyStatsInfo.lastHour, now)
	atomic.StoreInt64(&self.historyStatsInfo.lastPubSize, msgSize)
}

func (self *DetailStatsInfo) BatchUpdateTopicLatencyStats(latency int64, num int64) {
	self.msgStats.BatchUpdateMsgLatencyStats(latency, num)
}

func (self *DetailStatsInfo) UpdateTopicMsgStats(msgSize int64, latency int64) {
	if msgSize <= 0 {
		self.msgStats.UpdateMsgLatencyStats(latency)
	} else if latency <= 0 {
		self.msgStats.UpdateMsgSizeStats(msgSize)
	} else {
		self.msgStats.UpdateMsgStats(msgSize, latency)
	}
}

func (self *DetailStatsInfo) InitPubClientStats(remote string, agent string, protocol string) *ClientPubStats {
	self.Lock()
	defer self.Unlock()
	s, ok := self.clientPubStats[remote]
	if ok {
		return s
	}
	// too much clients pub to this topic
	// we just ignore stats
	if len(self.clientPubStats) > maxPubClientStats {
		nsqLog.Debugf("too much pub client : %v", len(self.clientPubStats))
		return nil
	}
	s = &ClientPubStats{
		RemoteAddress: remote,
		UserAgent:     agent,
		Protocol:      protocol,
	}
	// only update ts for new client connection, to avoid too much time.Now() call
	//
	s.LastPubTs = time.Now().Unix()
	self.clientPubStats[remote] = s
	return s
}

func (self *DetailStatsInfo) RemovePubStats(remote string, protocol string) {
	self.Lock()
	delete(self.clientPubStats, remote)
	self.Unlock()
}

func (self *DetailStatsInfo) GetPubClientNum() int {
	self.Lock()
	cnt := len(self.clientPubStats)
	self.Unlock()
	return cnt
}

func (self *DetailStatsInfo) GetPubClientStats() []ClientPubStats {
	self.Lock()
	stats := make([]ClientPubStats, 0, len(self.clientPubStats))
	for _, s := range self.clientPubStats {
		stats = append(stats, s.Copy())
	}
	self.Unlock()
	return stats
}

func (self *DetailStatsInfo) GetHourlyStats() [24]int64 {
	return self.historyStatsInfo.HourlyPubSize
}

func (self *DetailStatsInfo) GetMsgSizeStats() []int64 {
	s := self.msgStats.MsgSizeStats
	return s[:]
}

func (self *DetailStatsInfo) GetMsgWriteLatencyStats() []int64 {
	msgWriteLatencyStats := make([]int64, len(self.msgStats.MsgWriteLatencyStats))
	for i := range self.msgStats.MsgWriteLatencyStats {
		msgWriteLatencyStats[i] = atomic.LoadInt64(&self.msgStats.MsgWriteLatencyStats[i])
	}
	return msgWriteLatencyStats
}

func (self *DetailStatsInfo) UpdateHistory(historyList [24]int64) {
	if len(historyList) != len(self.historyStatsInfo.HourlyPubSize) {
		nsqLog.LogErrorf("failed to update history stats with wrong list size: %v", len(historyList))
		return
	}
	copy(self.historyStatsInfo.HourlyPubSize[:], historyList[:])
}

func (self *DetailStatsInfo) LoadHistory(fileName string) error {
	data, err := ioutil.ReadFile(fileName)
	if err != nil {
		if !os.IsNotExist(err) {
			nsqLog.LogErrorf("failed to read history stats from %s - %s", fileName, err)
		}
		return err
	}
	var historyStat TopicHistoryStatsInfo
	err = json.Unmarshal(data, &historyStat)
	if err != nil {
		nsqLog.Warningf("load history stats failed: %v", err)
		return err
	}
	self.historyStatsInfo.HourlyPubSize = historyStat.HourlyPubSize
	return nil
}

func (self *DetailStatsInfo) SaveHistory(fileName string) error {
	nsqLog.LogDebugf("persisting history stats to %s", fileName)
	data, err := json.Marshal(self.historyStatsInfo)
	if err != nil {
		nsqLog.LogWarningf("failed to save history stats: %v", err)
		return err
	}
	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())
	f, err := os.OpenFile(tmpFileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		nsqLog.LogWarningf("failed to save history stats: %v", err)
		return err
	}
	_, err = f.Write(data)
	if err != nil {
		f.Close()
		nsqLog.LogWarningf("failed to save history stats: %v", err)
		return err
	}
	f.Close()

	err = util.AtomicRename(tmpFileName, fileName)
	if err != nil {
		nsqLog.LogWarningf("failed to save history stats: %v", err)
	}
	return err
}

func (n *NSQD) UpdateTopicHistoryStats() {
	n.RLock()
	realTopics := make([]*Topic, 0, len(n.topicMap))
	for _, topicParts := range n.topicMap {
		for _, t := range topicParts {
			realTopics = append(realTopics, t)
		}
	}
	n.RUnlock()
	for _, t := range realTopics {
		pubSize := t.TotalDataSize()
		t.detailStats.historyStatsInfo.UpdateHourlySize(pubSize)
		t.SaveHistoryStats()
	}

}
