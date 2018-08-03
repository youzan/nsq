package clusterinfo

import (
	"fmt"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"

	"errors"
	"math"
	"sync/atomic"

	"github.com/blang/semver"
	"github.com/youzan/nsq/internal/http_api"
	"github.com/youzan/nsq/internal/stringy"
)

var v1EndpointVersion semver.Version

func init() {
	v1EndpointVersion, _ = semver.Parse("0.2.29-alpha")
}

type PartialErr interface {
	error
	Errors() []error
}

type ErrList []error

func (l ErrList) Error() string {
	var es []string
	for _, e := range l {
		es = append(es, e.Error())
	}
	return strings.Join(es, "\n")
}

func (l ErrList) Errors() []error {
	return l
}

type logger interface {
	Output(maxdepth int, s string) error
}

type ClusterInfo struct {
	log    logger
	client *http_api.Client
}

func New(log logger, client *http_api.Client) *ClusterInfo {
	return &ClusterInfo{
		log:    log,
		client: client,
	}
}

func (c *ClusterInfo) logf(f string, args ...interface{}) {
	if c.log == nil {
		return
	}
	c.log.Output(2, fmt.Sprintf(f, args...))
}

// GetVersion returns a semver.Version object by querying /info
func (c *ClusterInfo) GetVersion(addr string) (semver.Version, error) {
	endpoint := fmt.Sprintf("http://%s/info", addr)
	var resp struct {
		Version string `json:"version"`
	}
	err := c.client.NegotiateV1(endpoint, &resp)
	if err != nil {
		return semver.Version{}, err
	}
	if resp.Version == "" {
		resp.Version = "unknown"
	}
	v, err := semver.Parse(resp.Version)
	if err != nil {
		c.logf("CI: parse version failed %s: %v", resp.Version, err)
	}
	return v, err
}

func (c *ClusterInfo) GetLookupdTopicsMeta(dcLookupdHTTPAddrs map[string][]string, metaInfo bool) ([]*TopicInfo, error) {
	var topics []*TopicInfo
	var lock sync.Mutex
	var wg sync.WaitGroup
	var errs []error
	topicContain := make(map[string]bool)
	type respType struct {
		Topics   []string     `json:"topics"`
		MetaInfo []*TopicInfo `json:"meta_info,omitempty"`
	}
	var cnt int
	for _, lookupdHTTPAddrs := range dcLookupdHTTPAddrs {
		for _, addr := range lookupdHTTPAddrs {
			cnt++
			wg.Add(1)
			go func(addr string) {
				defer wg.Done()
				var endpoint string
				if metaInfo {
					endpoint = fmt.Sprintf("http://%s/topics?metaInfo=true", addr)
				} else {
					endpoint = fmt.Sprintf("http://%s/topics", addr)
				}
				c.logf("CI: querying nsqlookupd %s", endpoint)

				var resp respType
				err := c.client.NegotiateV1(endpoint, &resp)
				if err != nil {
					lock.Lock()
					errs = append(errs, err)
					lock.Unlock()
					return
				}

				lock.Lock()
				defer lock.Unlock()
				if resp.MetaInfo != nil {
					for _, topicMeta := range resp.MetaInfo {
						if !topicContain[topicMeta.TopicName] {
							topics = append(topics, topicMeta)
							topicContain[topicMeta.TopicName] = true
						}
					}
				} else {
					for _, topic := range resp.Topics {
						if !topicContain[topic] {
							topics = append(topics, &TopicInfo{
								TopicName: topic,
							})
							topicContain[topic] = true
						}
					}
				}

			}(addr)
		}
	}
	wg.Wait()

	if len(errs) == cnt {
		return nil, fmt.Errorf("Failed to query any nsqlookupd: %s", ErrList(errs))
	}

	sort.Sort(TopicInfoSortByName(topics))

	if len(errs) > 0 {
		return topics, ErrList(errs)
	}
	return topics, nil
}

// GetLookupdTopics returns a []string containing a union of all the topics
// from all the given nsqlookupd
func (c *ClusterInfo) GetLookupdTopics(lookupdHTTPAddrs []string) ([]string, error) {
	var topics []string
	var lock sync.Mutex
	var wg sync.WaitGroup
	var errs []error

	type respType struct {
		Topics []string `json:"topics"`
	}

	for _, addr := range lookupdHTTPAddrs {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()

			endpoint := fmt.Sprintf("http://%s/topics", addr)
			c.logf("CI: querying nsqlookupd %s", endpoint)

			var resp respType
			err := c.client.NegotiateV1(endpoint, &resp)
			if err != nil {
				lock.Lock()
				errs = append(errs, err)
				lock.Unlock()
				return
			}

			lock.Lock()
			defer lock.Unlock()
			topics = append(topics, resp.Topics...)
		}(addr)
	}
	wg.Wait()

	if len(errs) == len(lookupdHTTPAddrs) {
		return nil, fmt.Errorf("Failed to query any nsqlookupd: %s", ErrList(errs))
	}

	topics = stringy.Uniq(topics)
	sort.Strings(topics)

	if len(errs) > 0 {
		return topics, ErrList(errs)
	}
	return topics, nil
}

// GetLookupdTopicChannels returns a []string containing a union of all the channels
// from all the given lookupd for the given topic
func (c *ClusterInfo) GetLookupdTopicChannels(topic string, dcLookupdHTTPAddrs map[string][]string) ([]string, error) {
	var channels []string
	var lock sync.Mutex
	var wg sync.WaitGroup
	var errs []error

	type respType struct {
		Channels []string `json:"channels"`
	}

	var cnt int
	for _, lookupdHTTPAddrs := range dcLookupdHTTPAddrs {
		for _, addr := range lookupdHTTPAddrs {
			cnt++
			wg.Add(1)
			go func(addr string) {
				defer wg.Done()

				endpoint :=
					fmt.Sprintf("http://%s/channels?topic=%s", addr,
						url.QueryEscape(topic))
				c.logf("CI: querying nsqlookupd %s", endpoint)

				var resp respType
				err := c.client.NegotiateV1(endpoint, &resp)
				if err != nil {
					lock.Lock()
					errs = append(errs, err)
					lock.Unlock()
					return
				}

				lock.Lock()
				defer lock.Unlock()
				channels = append(channels, resp.Channels...)
			}(addr)
		}
	}
	wg.Wait()

	if len(errs) == cnt {
		return nil, fmt.Errorf("Failed to query any nsqlookupd: %s", ErrList(errs))
	}

	channels = stringy.Uniq(channels)
	sort.Strings(channels)

	if len(errs) > 0 {
		return channels, ErrList(errs)
	}
	return channels, nil
}

// GetLookupdProducers returns Producers of all the nsqd connected to the given lookupds
func (c *ClusterInfo) GetLookupdProducers(lookupdHTTPAddrs []string) (Producers, error) {
	var producers []*Producer
	var lock sync.Mutex
	var wg sync.WaitGroup
	var errs []error

	producersByAddr := make(map[string]*Producer)
	maxVersion, _ := semver.Parse("0.0.0")

	type respType struct {
		Producers []*Producer `json:"producers"`
	}

	for _, addr := range lookupdHTTPAddrs {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()

			endpoint := fmt.Sprintf("http://%s/nodes", addr)
			c.logf("CI: querying nsqlookupd %s", endpoint)

			var resp respType
			err := c.client.NegotiateV1(endpoint, &resp)
			if err != nil {
				lock.Lock()
				errs = append(errs, err)
				lock.Unlock()
				return
			}

			lock.Lock()
			defer lock.Unlock()
			for _, producer := range resp.Producers {
				key := producer.TCPAddress()
				p, ok := producersByAddr[key]
				if !ok {
					producersByAddr[key] = producer
					producers = append(producers, producer)
					if maxVersion.LT(producer.VersionObj) {
						maxVersion = producer.VersionObj
					}
					sort.Sort(producer.Topics)
					p = producer
				}
				p.RemoteAddresses = append(p.RemoteAddresses,
					fmt.Sprintf("%s/%s", addr, producer.Address()))
			}
		}(addr)
	}
	wg.Wait()

	if len(errs) == len(lookupdHTTPAddrs) {
		return nil, fmt.Errorf("Failed to query any nsqlookupd: %s", ErrList(errs))
	}

	for _, producer := range producersByAddr {
		if producer.VersionObj.LT(maxVersion) {
			producer.OutOfDate = true
		}
	}
	sort.Sort(ProducersByHost{producers})

	if len(errs) > 0 {
		return producers, ErrList(errs)
	}
	return producers, nil
}

// GetLookupdTopicProducers returns Producers of all the nsqd for a given topic by
// unioning the nodes returned from the given lookupd
func (c *ClusterInfo) GetLookupdTopicProducers(topic string, dcLookupdHTTPAddrs map[string][]string) (Producers, map[string]map[string]Producers, error) {
	var producers Producers
	dcPartitionProducers := make(map[string]map[string]Producers)

	var lock sync.Mutex
	var wg sync.WaitGroup
	var errs []error

	type respType struct {
		Producers          Producers            `json:"producers"`
		PartitionProducers map[string]*Producer `json:"partitions"`
	}

	var cnt int
	for dc, lookupdHTTPAddrs := range dcLookupdHTTPAddrs {
		dcInner := dc
		partitionProducers := make(map[string]Producers)
		dcPartitionProducers[dc] = partitionProducers
		for _, addr := range lookupdHTTPAddrs {
			cnt++
			wg.Add(1)
			go func(addr string) {
				defer wg.Done()

				endpoint := fmt.Sprintf("http://%s/lookup?topic=%s", addr, url.QueryEscape(topic))
				c.logf("CI: querying nsqlookupd %s", endpoint)

				var resp respType
				err := c.client.NegotiateV1(endpoint, &resp)
				if err != nil {
					lock.Lock()
					errs = append(errs, err)
					lock.Unlock()
					return
				}

				//c.logf("CI: querying nsqlookupd return %v, partitions: %v", resp, resp.PartitionProducers)
				lock.Lock()
				defer lock.Unlock()
				for _, p := range resp.Producers {
					version, err := semver.Parse(p.Version)
					if err != nil {
						c.logf("CI: parse version failed %s: %v", p.Version, err)
						version, _ = semver.Parse("0.0.0")
					}
					p.VersionObj = version

					for _, pp := range producers {
						if p.HTTPAddress() == pp.HTTPAddress() {
							goto skip
						}
					}
					p.DC = dcInner;
					producers = append(producers, p)
					skip:
				}
				for pid, p := range resp.PartitionProducers {
					version, err := semver.Parse(p.Version)
					if err != nil {
						c.logf("CI: parse version failed %s: %v", p.Version, err)
						version, _ = semver.Parse("0.0.0")
					}
					p.VersionObj = version

					partproducers := partitionProducers[pid]
					for _, pp := range partproducers {
						if p.HTTPAddress() == pp.HTTPAddress() {
							goto skip2
						}
					}
					p.DC = dcInner
					partproducers = append(partproducers, p)
					partitionProducers[pid] = partproducers
					skip2:
				}
			}(addr)
		}
	}
	wg.Wait()

	if len(errs) == cnt {
		return nil, nil, fmt.Errorf("Failed to query any nsqlookupd: %s", ErrList(errs))
	}
	if len(errs) > 0 {
		return producers, dcPartitionProducers, ErrList(errs)
	}
	return producers, dcPartitionProducers, nil
}

type TopicInfo struct {
	TopicName  string `json:"topic_name"`
	ExtSupport bool   `json:"extend_support"`
	Ordered    bool   `json:"ordered"`
}

type TopicInfoSortByName []*TopicInfo

func (c TopicInfoSortByName) Less(i, j int) bool {
	return c[i].TopicName < c[j].TopicName
}

func (c TopicInfoSortByName) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

func (c TopicInfoSortByName) Len() int {
	return len(c)
}

// GetNSQDTopics returns a []string containing all the topics produced by the given nsqd
func (c *ClusterInfo) GetNSQDTopics(nsqdHTTPAddrs []string) ([]*TopicInfo, error) {
	var topics []*TopicInfo
	var lock sync.Mutex
	var wg sync.WaitGroup
	var errs []error

	type respType struct {
		Topics []struct {
			Name       string `json:"topic_name"`
			Ordered    bool   `json:"is_multi_ordered"`
			ExtSupport bool   `json:"is_ext"`
		} `json:"topics"`
	}

	for _, addr := range nsqdHTTPAddrs {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()

			endpoint := fmt.Sprintf("http://%s/stats?format=json", addr)
			c.logf("CI: querying nsqd %s", endpoint)

			var resp respType
			err := c.client.NegotiateV1(endpoint, &resp)
			if err != nil {
				lock.Lock()
				errs = append(errs, err)
				lock.Unlock()
				return
			}

			lock.Lock()
			defer lock.Unlock()
			for _, topic := range resp.Topics {
				topics = append(topics, &TopicInfo{
					TopicName:  topic.Name,
					Ordered:    topic.Ordered,
					ExtSupport: topic.ExtSupport,
				})
			}
		}(addr)
	}
	wg.Wait()

	if len(errs) == len(nsqdHTTPAddrs) {
		return nil, fmt.Errorf("Failed to query any nsqd: %s", ErrList(errs))
	}

	sort.Sort(TopicInfoSortByName(topics))

	if len(errs) > 0 {
		return topics, ErrList(errs)
	}
	return topics, nil
}

// GetNSQDProducers returns Producers of all the given nsqd
func (c *ClusterInfo) GetNSQDProducers(nsqdHTTPAddrs []string) (Producers, error) {
	var producers Producers
	var lock sync.Mutex
	var wg sync.WaitGroup
	var errs []error

	type infoRespType struct {
		Version          string `json:"version"`
		BroadcastAddress string `json:"broadcast_address"`
		Hostname         string `json:"hostname"`
		HTTPPort         int    `json:"http_port"`
		TCPPort          int    `json:"tcp_port"`
	}

	type statsRespType struct {
		Topics []struct {
			Name string `json:"topic_name"`
		} `json:"topics"`
	}

	for _, addr := range nsqdHTTPAddrs {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()

			endpoint := fmt.Sprintf("http://%s/info", addr)
			c.logf("CI: querying nsqd %s", endpoint)

			var infoResp infoRespType
			err := c.client.NegotiateV1(endpoint, &infoResp)
			if err != nil {
				lock.Lock()
				errs = append(errs, err)
				lock.Unlock()
				return
			}

			endpoint = fmt.Sprintf("http://%s/stats?format=json", addr)
			c.logf("CI: querying nsqd %s", endpoint)

			var statsResp statsRespType
			err = c.client.NegotiateV1(endpoint, &statsResp)
			if err != nil {
				lock.Lock()
				errs = append(errs, err)
				lock.Unlock()
				return
			}

			var producerTopics ProducerTopics
			for _, t := range statsResp.Topics {
				producerTopics = append(producerTopics, ProducerTopic{Topic: t.Name})
			}

			version, err := semver.Parse(infoResp.Version)
			if err != nil {
				c.logf("CI: parse version failed %s: %v", infoResp.Version, err)
				version, _ = semver.Parse("0.0.0")
			}

			lock.Lock()
			defer lock.Unlock()
			producers = append(producers, &Producer{
				Version:          infoResp.Version,
				VersionObj:       version,
				BroadcastAddress: infoResp.BroadcastAddress,
				Hostname:         infoResp.Hostname,
				HTTPPort:         infoResp.HTTPPort,
				TCPPort:          infoResp.TCPPort,
				Topics:           producerTopics,
			})
		}(addr)
	}
	wg.Wait()

	if len(errs) == len(nsqdHTTPAddrs) {
		return nil, fmt.Errorf("Failed to query any nsqd: %s", ErrList(errs))
	}
	if len(errs) > 0 {
		return producers, ErrList(errs)
	}
	return producers, nil
}

// GetNSQDTopicProducers returns Producers containing the addresses of all the nsqd
// that produce the given topic
func (c *ClusterInfo) GetNSQDTopicProducers(topic string, nsqdHTTPAddrs []string) (Producers, error) {
	var producers Producers
	var lock sync.Mutex
	var wg sync.WaitGroup
	var errs []error

	type infoRespType struct {
		Version          string `json:"version"`
		BroadcastAddress string `json:"broadcast_address"`
		Hostname         string `json:"hostname"`
		HTTPPort         int    `json:"http_port"`
		TCPPort          int    `json:"tcp_port"`
	}

	type statsRespType struct {
		Topics []struct {
			Name string `json:"topic_name"`
		} `json:"topics"`
	}

	for _, addr := range nsqdHTTPAddrs {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()

			endpoint := fmt.Sprintf("http://%s/stats?format=json", addr)
			c.logf("CI: querying nsqd %s", endpoint)

			var statsResp statsRespType
			err := c.client.NegotiateV1(endpoint, &statsResp)
			if err != nil {
				lock.Lock()
				errs = append(errs, err)
				lock.Unlock()
				return
			}

			var producerTopics ProducerTopics
			for _, t := range statsResp.Topics {
				producerTopics = append(producerTopics, ProducerTopic{Topic: t.Name})
			}

			for _, t := range statsResp.Topics {
				if t.Name == topic {
					endpoint := fmt.Sprintf("http://%s/info", addr)
					c.logf("CI: querying nsqd %s", endpoint)

					var infoResp infoRespType
					err := c.client.NegotiateV1(endpoint, &infoResp)
					if err != nil {
						lock.Lock()
						errs = append(errs, err)
						lock.Unlock()
						return
					}

					version, err := semver.Parse(infoResp.Version)
					if err != nil {
						c.logf("CI: parse version failed %s: %v", infoResp.Version, err)
						version, _ = semver.Parse("0.0.0")
					}

					// if BroadcastAddress/HTTPPort are missing, use the values from `addr` for
					// backwards compatibility

					if infoResp.BroadcastAddress == "" {
						var p string
						infoResp.BroadcastAddress, p, _ = net.SplitHostPort(addr)
						infoResp.HTTPPort, _ = strconv.Atoi(p)
					}
					if infoResp.Hostname == "" {
						infoResp.Hostname, _, _ = net.SplitHostPort(addr)
					}

					lock.Lock()
					producers = append(producers, &Producer{
						Version:          infoResp.Version,
						VersionObj:       version,
						BroadcastAddress: infoResp.BroadcastAddress,
						Hostname:         infoResp.Hostname,
						HTTPPort:         infoResp.HTTPPort,
						TCPPort:          infoResp.TCPPort,
						Topics:           producerTopics,
					})
					lock.Unlock()

					return
				}
			}
		}(addr)
	}
	wg.Wait()

	if len(errs) == len(nsqdHTTPAddrs) {
		return nil, fmt.Errorf("Failed to query any nsqd: %s", ErrList(errs))
	}
	if len(errs) > 0 {
		return producers, ErrList(errs)
	}
	return producers, nil
}

func (c *ClusterInfo) ListAllLookupdNodes(dcLookupdHTTPAddrs map[string][]string) ([]*LookupdNodes, error) {
	var errs []error
	var dcLookupdNodes []*LookupdNodes
	var cnt int
	for dc, lookupdHTTPAddrs := range dcLookupdHTTPAddrs {
		for _, addr := range lookupdHTTPAddrs {
			cnt++;
			endpoint := fmt.Sprintf("http://%s/listlookup", addr)
			c.logf("CI: querying nsqlookupd %s", endpoint)
			var resp LookupdNodes
			err := c.client.NegotiateV1(endpoint, &resp)
			if err != nil {
				c.logf("CI: querying nsqlookupd %s err: %v", endpoint, err)
				errs = append(errs, err)
				continue
			}
			resp.DC = dc
			dcLookupdNodes = append(dcLookupdNodes, &resp)
			break
		}
	}

	if len(errs) == cnt {
		return nil, fmt.Errorf("Failed to query any nsqlookupd: %s", ErrList(errs))
	}

	return dcLookupdNodes, nil
}

func (c *ClusterInfo) GetNSQDAllMessageHistoryStats(producers Producers) (map[string]map[string]int64, error) {
	var errs []error

	dcNodeHistoryStatsMap := make(map[string]map[string]int64)

	var lock sync.Mutex
	var wg sync.WaitGroup

	for _, p := range producers {
		if _, exist := dcNodeHistoryStatsMap[p.DC]; !exist {
			dcNodeHistoryStatsMap[p.DC] = make(map[string]int64)
		}
		wg.Add(1)
		go func(p *Producer) {
			defer wg.Done()
			addr := p.HTTPAddress()
			endpoint := fmt.Sprintf("http://%s/message/historystats", addr)
			var nodeHistoryStatsResp struct {
				HistoryStats []*NodeHourlyPubsize `json:"node_hourly_pub_size_stats"`
			}
			err := c.client.NegotiateV1(endpoint, &nodeHistoryStatsResp)
			//c.logf("CI: querying nsqd %s resp: %v", endpoint, nodeHistoryStatsResp)
			if err != nil {
				lock.Lock()
				errs = append(errs, err)
				lock.Unlock()
			}
			for _, topicMsgStat := range nodeHistoryStatsResp.HistoryStats {
				lock.Lock()
				_, ok := dcNodeHistoryStatsMap[p.DC][topicMsgStat.TopicName]
				if !ok {
					dcNodeHistoryStatsMap[p.DC][topicMsgStat.TopicName] = topicMsgStat.HourlyPubSize
				} else {
					dcNodeHistoryStatsMap[p.DC][topicMsgStat.TopicName] += topicMsgStat.HourlyPubSize
				}
				lock.Unlock()
			}
		}(p)
	}
	wg.Wait()
	if len(errs) == len(producers) {
		return nil, fmt.Errorf("Failed to query any nsqd for node topic message history: %s", ErrList(errs))
	}

	return dcNodeHistoryStatsMap, nil
}

func (c *ClusterInfo) GetNSQDMessageHistoryStats(nsqdHTTPAddr string, selectedTopic string, par string) ([]int64, error) {
	//aggregate partition dist data from producers
	endpoint := fmt.Sprintf("http://%s/message/historystats?topic=%s&partition=%s", nsqdHTTPAddr, selectedTopic, par)
	var historyStatsResp struct {
		HistoryStat []int64 `json:"hourly_pub_size"`
	}
	err := c.client.NegotiateV1(endpoint, &historyStatsResp)
	if err != nil {
		return nil, err
	}

	c.logf("CI: querying nsqd %s resp: %v", endpoint, historyStatsResp)

	return historyStatsResp.HistoryStat, nil
}

func (c *ClusterInfo) GetNSQDMessageByID(p Producer, selectedTopic string,
	part string, msgID int64) (string, int64, error) {
	if selectedTopic == "" {
		return "", 0, fmt.Errorf("missing topic while get message")
	}
	type msgInfo struct {
		ID        int64  `json:"id"`
		TraceID   uint64 `json:"trace_id"`
		Body      string `json:"body"`
		Timestamp int64  `json:"timestamp"`
		Attempts  uint16 `json:"attempts"`

		Offset        int64 `json:"offset"`
		QueueCntIndex int64 `json:"queue_cnt_index"`
	}

	addr := p.HTTPAddress()
	endpoint := fmt.Sprintf("http://%s/message/get?topic=%s&partition=%s&search_mode=id&search_pos=%d", addr,
		url.QueryEscape(selectedTopic), url.QueryEscape(part), msgID)
	c.logf("CI: querying nsqd %s", endpoint)

	var resp msgInfo
	_, err := c.client.GETV1(endpoint, &resp)
	if err != nil {
		return "", 0, err
	}
	return resp.Body, resp.Offset, nil
}

func (c *ClusterInfo) GetNSQDCoordStats(producers Producers, selectedTopic string, part string) (*CoordStats, error) {
	var lock sync.Mutex
	var wg sync.WaitGroup
	var topicCoordStats CoordStats
	var errs []error

	for _, p := range producers {
		wg.Add(1)
		go func(p *Producer) {
			defer wg.Done()

			addr := p.HTTPAddress()
			endpoint := fmt.Sprintf("http://%s/coordinator/stats?format=json", addr)
			if selectedTopic != "" {
				endpoint = fmt.Sprintf("http://%s/coordinator/stats?format=json&topic=%s", addr, url.QueryEscape(selectedTopic))
			}
			if part != "" {
				endpoint = fmt.Sprintf("http://%s/coordinator/stats?format=json&topic=%s&partition=%s",
					addr, url.QueryEscape(selectedTopic), url.QueryEscape(part))
			}
			c.logf("CI: querying nsqd %s", endpoint)

			var resp CoordStats
			err := c.client.NegotiateV1(endpoint, &resp)
			if err != nil {
				lock.Lock()
				errs = append(errs, err)
				lock.Unlock()
				return
			}

			lock.Lock()
			defer lock.Unlock()
			c.logf("CI: querying nsqd %s resp: %v", endpoint, resp)
			topicCoordStats.RpcStats = resp.RpcStats.Snapshot()
			for _, topicStat := range resp.TopicCoordStats {
				topicStat.DC = p.DC
				topicStat.Node = addr
				if selectedTopic != "" && topicStat.Name != selectedTopic {
					continue
				}
				topicCoordStats.TopicCoordStats = append(topicCoordStats.TopicCoordStats, topicStat)
			}
		}(p)
	}
	wg.Wait()

	if len(errs) == len(producers) {
		return nil, fmt.Errorf("Failed to query any nsqd: %s", ErrList(errs))
	}

	if len(errs) > 0 {
		return &topicCoordStats, ErrList(errs)
	}
	return &topicCoordStats, nil
}

var INDEX = int32(0)

//TODO cluster info from dc
func (c *ClusterInfo) GetClusterInfo(lookupdAdresses []string) (*ClusterNodeInfo, error) {
	INDEX = atomic.AddInt32(&INDEX, 1) & math.MaxInt32
	c.logf("INDEX for picking lookup http address: %d", INDEX)
	lookupdAdress := lookupdAdresses[int(INDEX)%(len(lookupdAdresses))]
	c.logf("lookupd http address %s picked.", lookupdAdress)
	endpoint := fmt.Sprintf("http://%s/cluster/stats", lookupdAdress)

	var resp ClusterNodeInfo
	err := c.client.NegotiateV1(endpoint, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

func (c *ClusterInfo) GetDCClusterInfo(lookupdAdresses map[string][]string) ([]*ClusterNodeInfo, error) {
	var errs []error
	var dcClusterInfo []*ClusterNodeInfo
	for dc, lookupdAddrs := range lookupdAdresses {
		c.logf("lookupd address in dc %v picked. %v", dc, lookupdAddrs)
		clusterInfo, err := c.GetClusterInfo(lookupdAddrs)
		if err != nil {
			errs = append(errs, err)
		}
		if clusterInfo != nil {
			clusterInfo.DC = dc
			dcClusterInfo = append(dcClusterInfo, clusterInfo)
		}
	}
	if len(errs) > 0 {
		return dcClusterInfo, ErrList(errs)
	}
	return dcClusterInfo, nil
}

// GetNSQDStats returns aggregate topic and channel stats from the given Producers
//
// if selectedTopic is empty, this will return stats for *all* topic/channels
// and the ChannelStats dict will be keyed by topic + ':' + channel
func (c *ClusterInfo) GetNSQDStats(producers Producers, selectedTopic string, sortBy string, leaderOnly bool) (map[string]TopicStatsList, map[string]map[string]*ChannelStats, error) {
	var lock sync.Mutex
	var wg sync.WaitGroup
	var errs []error

	dcChannelStatsMap := make(map[string]map[string]*ChannelStats)
	dcTopicStatsList := make(map[string]TopicStatsList)

	type respType struct {
		Topics []*TopicStats `json:"topics"`
	}

	for _, p := range producers {
		pInner := p
		wg.Add(1)
		go func(p *Producer) {
			defer wg.Done()

			addr := p.HTTPAddress()
			endpoint := fmt.Sprintf("http://%s/stats?format=json&leaderOnly=%t&needClients=true", addr, leaderOnly)
			if selectedTopic != "" {
				endpoint = fmt.Sprintf("http://%s/stats?format=json&topic=%s&leaderOnly=%t", addr, selectedTopic, leaderOnly)
			}
			c.logf("CI: querying nsqd %s", endpoint)

			var resp respType
			err := c.client.NegotiateV1(endpoint, &resp)
			if err != nil {
				lock.Lock()
				errs = append(errs, err)
				lock.Unlock()
				return
			}

			lock.Lock()
			defer lock.Unlock()
			for _, topic := range resp.Topics {
				topic.DC = p.DC
				topic.Node = addr
				topic.Hostname = p.Hostname
				topic.MemoryDepth = topic.Depth - topic.BackendDepth
				if selectedTopic != "" && topic.TopicName != selectedTopic {
					continue
				}
				if topic.StatsdName == "" {
					topic.StatsdName = topic.TopicName
				}
				if _, exist := dcTopicStatsList[topic.DC]; !exist {
					dcTopicStatsList[topic.DC] = TopicStatsList{}
				}
				dcTopicStatsList[topic.DC] = append(dcTopicStatsList[topic.DC], topic)

				for _, channel := range topic.Channels {
					channel.DC = p.DC
					channel.Node = addr
					channel.Hostname = p.Hostname
					channel.TopicName = topic.TopicName
					channel.TopicPartition = topic.TopicPartition
					channel.StatsdName = topic.StatsdName
					channel.IsMultiOrdered = topic.IsMultiOrdered
					channel.IsExt = topic.IsExt
					channel.MemoryDepth = channel.Depth - channel.BackendDepth
					key := channel.ChannelName
					if selectedTopic == "" {
						key = fmt.Sprintf("%s:%s", topic.TopicName, channel.ChannelName)
					}
					if len(channel.MsgDeliveryLatencyStats) == 0 {
						channel.MsgDeliveryLatencyStats = make([]int64, 12)
					}
					channelStatsMap, exist := dcChannelStatsMap[p.DC]
					if !exist {
						channelStatsMap = make(map[string]*ChannelStats)
						dcChannelStatsMap[p.DC] = channelStatsMap
					}
					channelStats, ok := channelStatsMap[key]
					if !ok {
						channelStats = &ChannelStats{
							Node:           addr,
							TopicName:      topic.TopicName,
							TopicPartition: topic.TopicPartition,
							StatsdName:     topic.StatsdName,
							ChannelName:    channel.ChannelName,
							IsMultiOrdered: topic.IsMultiOrdered,
							DC:		p.DC,
						}
						channelStatsMap[key] = channelStats
					}
					for _, c := range channel.Clients {
						c.Node = addr
					}
					channelStats.Add(channel)
					topic.TotalChannelDepth += channel.Depth
				}
			}
		}(pInner)
	}
	wg.Wait()

	if len(errs) == len(producers) {
		return nil, nil, fmt.Errorf("Failed to query any nsqd: %s", ErrList(errs))
	}

	for _, topicStatsList := range dcTopicStatsList {
		if sortBy == "partition" {
			sort.Sort(TopicStatsByPartitionAndHost{topicStatsList})
		} else if sortBy == "channel-depth" {
			sort.Sort(TopicStatsByChannelDepth{topicStatsList})
		} else if sortBy == "message-count" {
			sort.Sort(TopicStatsByMessageCount{topicStatsList})
		} else {
			sort.Sort(TopicStatsByPartitionAndHost{topicStatsList})
		}
	}

	if len(errs) > 0 {
		return dcTopicStatsList, dcChannelStatsMap, ErrList(errs)
	}
	return dcTopicStatsList, dcChannelStatsMap, nil
}

// TombstoneNodeForTopic tombstones the given node for the given topic on all the given nsqlookupd
// and deletes the topic from the node
func (c *ClusterInfo) TombstoneNodeForTopic(topic string, node string, dcLookupdHTTPAddrs map[string][]string) error {
	var errs []error
	var lookupdHTTPAddrs []string
	// tombstone the topic on all the lookupds
	qs := fmt.Sprintf("topic=%s&node=%s", url.QueryEscape(topic), url.QueryEscape(node))
	dcLookupdNodes, _ := c.ListAllLookupdNodes(dcLookupdHTTPAddrs)
	for _, lookupdNodes := range dcLookupdNodes {
		for _, node := range lookupdNodes.AllNodes {
			lookupdHTTPAddrs = append(lookupdHTTPAddrs, net.JoinHostPort(node.NodeIP, node.HttpPort))
		}
	}
	err := c.versionPivotNSQLookupd(lookupdHTTPAddrs, "tombstone_topic_producer", "topic/tombstone", qs)
	if err != nil {
		pe, ok := err.(PartialErr)
		if !ok {
			return err
		}
		errs = append(errs, pe.Errors()...)
	}

	if len(errs) > 0 {
		return ErrList(errs)
	}
	return nil
}

func (c *ClusterInfo) CreateTopicChannelAfterTopicCreation(topicName string, channelName string, dcLookupdHTTPAddrs map[string][]string, partitionNum int) error {
	var errs []error

	//fetch nsqd from leader only
	dcLookupdNodes, err := c.ListAllLookupdNodes(dcLookupdHTTPAddrs)
	if err != nil {
		c.logf("failed to list lookupd nodes while create topic: %v", err)
		return err
	}

	dcLookupdLeader := make(map[string][]string)
	for _, lookupdNodes := range dcLookupdNodes {
		if _, exist :=  dcLookupdLeader[lookupdNodes.DC]; !exist {
			dcLookupdLeader[lookupdNodes.DC] = make([]string, 0)
		}
		leaderAddr := dcLookupdLeader[lookupdNodes.DC]
		leaderAddr = append(leaderAddr, net.JoinHostPort(lookupdNodes.LeaderNode.NodeIP, lookupdNodes.LeaderNode.HttpPort))
	}


	producers, dcPartitionProducers, err := c.GetTopicProducers(topicName, dcLookupdLeader, nil)
	if err != nil {
		pe, ok := err.(PartialErr)
		if !ok {
			return err
		}
		errs = append(errs, pe.Errors()...)
	}

	if len(producers) == 0 && len(dcPartitionProducers) == 0 {
		c.logf(fmt.Sprintf("Producer:%d, DCPartitionProducers:%d", len(producers), len(dcPartitionProducers)))
		text := fmt.Sprintf("no producer or partition producer found for Topic:%s, Channel:%s", topicName, channelName)
		return errors.New(text)
	}
	if len(producers) > 0 && len(dcPartitionProducers) == 0 {
		qs := fmt.Sprintf("topic=%s&channel=%s", url.QueryEscape(topicName), url.QueryEscape(channelName))
		err = c.versionPivotProducers(producers, "create_channel", "channel/create", qs)
		if err != nil {
			pe, ok := err.(PartialErr)
			if !ok {
				return err
			}
			errs = append(errs, pe.Errors()...)
		}
	} else {
		for dc, partitionProducers := range dcPartitionProducers {
			if len(partitionProducers) < partitionNum {
				text := fmt.Sprintf("dc: %v, partition number: %v returned from leader lookup is less than expected partition number: %v", dc, len(partitionProducers), partitionNum)
				return errors.New(text)
			}
			for pid, pp := range partitionProducers {
				qs := fmt.Sprintf("topic=%s&channel=%s&partition=%s", url.QueryEscape(topicName), url.QueryEscape(channelName), pid)
				err = c.versionPivotProducers(pp, "create_channel", "channel/create", qs)
				if err != nil {
					pe, ok := err.(PartialErr)
					if !ok {
						return err
					}
					errs = append(errs, pe.Errors()...)
				}
			}
		}
	}
	if len(errs) > 0 {
		return ErrList(errs)
	}
	return nil
}

func (c *ClusterInfo) CreateTopicChannel(topicName string, channelName string, dcLookupdHTTPAddrs map[string][]string) error {
	var errs []error

	producers, dcPartitionProducers, err := c.GetTopicProducers(topicName, dcLookupdHTTPAddrs, nil)
	if err != nil {
		pe, ok := err.(PartialErr)
		if !ok {
			return err
		}
		errs = append(errs, pe.Errors()...)
	}

	if len(dcPartitionProducers) == 0 {
		qs := fmt.Sprintf("topic=%s&channel=%s", url.QueryEscape(topicName), url.QueryEscape(channelName))
		err = c.versionPivotProducers(producers, "create_channel", "channel/create", qs)
		if err != nil {
			pe, ok := err.(PartialErr)
			if !ok {
				return err
			}
			errs = append(errs, pe.Errors()...)
		}
	} else {
		for _, partitionProducers := range dcPartitionProducers {
			for pid, pp := range partitionProducers {
				qs := fmt.Sprintf("topic=%s&channel=%s&partition=%s", url.QueryEscape(topicName), url.QueryEscape(channelName), pid)
				err = c.versionPivotProducers(pp, "create_channel", "channel/create", qs)
				if err != nil {
					pe, ok := err.(PartialErr)
					if !ok {
						return err
					}
					errs = append(errs, pe.Errors()...)
				}
			}
		}
	}
	if len(errs) > 0 {
		return ErrList(errs)
	}
	return nil
}

func (c *ClusterInfo) CreateTopic(topicName string, partitionNum int, replica int, syncDisk int,
	retentionDays string, orderedmulti string, ext string, dcLookupdHTTPAddrs map[string][]string) error {
	var errs []error

	// TODO: found the master lookup node first
	// create the topic on all the nsqlookupd
	qs := fmt.Sprintf("topic=%s&partition_num=%d&replicator=%d&syncdisk=%d&retention=%s&orderedmulti=%s&extend=%s",
		url.QueryEscape(topicName), partitionNum, replica, syncDisk, retentionDays, orderedmulti, ext)
	dcLookupdNodes, err := c.ListAllLookupdNodes(dcLookupdHTTPAddrs)
	if err != nil {
		c.logf("failed to list lookupd nodes while create topic: %v", err)
		return err
	}
	leaderAddr := make([]string, 0)
	for _, lookupdNodes := range dcLookupdNodes {
		leaderAddr = append(leaderAddr, net.JoinHostPort(lookupdNodes.LeaderNode.NodeIP, lookupdNodes.LeaderNode.HttpPort))
	}
	err = c.versionPivotNSQLookupd(leaderAddr, "create_topic", "topic/create", qs)
	if err != nil {
		pe, ok := err.(PartialErr)
		if !ok {
			return err
		}
		errs = append(errs, pe.Errors()...)
	}

	if len(errs) > 0 {
		return ErrList(errs)
	}
	return nil
}

// this will delete all partitions of topic on all nsqd node.
func (c *ClusterInfo) DeleteTopic(topicName string, dcLookupdHTTPAddrs map[string][]string, nsqdHTTPAddrs []string) error {
	var errs []error

	dcLookupdNodes, err := c.ListAllLookupdNodes(dcLookupdHTTPAddrs)
	if err != nil {
		c.logf("failed to list lookupd nodes while delete topic: %v", err)
		return err
	}
	leaderAddr := make([]string, 0)
	for _, lookupdNodes := range dcLookupdNodes {
		leaderAddr = append(leaderAddr, net.JoinHostPort(lookupdNodes.LeaderNode.NodeIP, lookupdNodes.LeaderNode.HttpPort))
	}
	qs := fmt.Sprintf("topic=%s&partition=**", url.QueryEscape(topicName))
	// remove the topic from all the nsqlookupd
	err = c.versionPivotNSQLookupd(leaderAddr, "delete_topic", "topic/delete", qs)
	if err != nil {
		pe, ok := err.(PartialErr)
		if !ok {
			return err
		}
		errs = append(errs, pe.Errors()...)
	}

	if len(errs) > 0 {
		return ErrList(errs)
	}
	return nil
}

func (c *ClusterInfo) DeleteChannel(topicName string, channelName string, dcLookupdHTTPAddrs map[string][]string, nsqdHTTPAddrs []string) error {
	var errs []error
	retry := true
	producers, dcPartitionProducers, err := c.GetTopicProducers(topicName, dcLookupdHTTPAddrs, nsqdHTTPAddrs)
	if err != nil {
		pe, ok := err.(PartialErr)
		if !ok {
			return err
		}
		errs = append(errs, pe.Errors()...)
	}

channelDelete:
	if len(dcPartitionProducers) == 0 {
		qs := fmt.Sprintf("topic=%s&channel=%s", url.QueryEscape(topicName), url.QueryEscape(channelName))
		// remove the channel from all the nsqd that produce this topic
		err = c.versionPivotProducers(producers, "delete_channel", "channel/delete", qs)
		if err != nil {
			pe, ok := err.(PartialErr)
			if !ok {
				return err
			}
			errs = append(errs, pe.Errors()...)
		}
	} else {
		//TODO: concurrent delete
		for _, partitionProducers := range dcPartitionProducers {
			for pid, pp := range partitionProducers {
				qs := fmt.Sprintf("topic=%s&channel=%s&partition=%s", url.QueryEscape(topicName), url.QueryEscape(channelName), pid)
				// remove the channel from all the nsqd that produce this topic
				err = c.versionPivotProducers(pp, "delete_channel", "channel/delete", qs)
				if err != nil {
					pe, ok := err.(PartialErr)
					if !ok {
						return err
					}
					errs = append(errs, pe.Errors()...)
				}
			}
		}
	}

	_, dcAllChannelStats, err := c.GetNSQDStats(producers, topicName, "partition", true)
	if err != nil {
		pe, ok := err.(PartialErr)
		if !ok {
			return err
		}
		errs = append(errs, pe.Errors()...)
	}

	for dc, allChannelStats := range dcAllChannelStats {
		if _, exist := allChannelStats[channelName]; exist {
			c.logf("channel %v are not completely deleted", channelName)
			if retry {
				//do delete again
				retry = false
				goto channelDelete
			} else {
				c.logf("dc: %v , fail to delete channel %v completely", dc, channelName)
			}
		} else {
			c.logf("dc: %v, channel %v deleted", dc, channelName)
		}
	}


	if len(errs) > 0 {
		return ErrList(errs)
	}
	return nil
}

func (c *ClusterInfo) PauseTopic(topicName string, dcLookupdHTTPAddrs map[string][]string, nsqdHTTPAddrs []string) error {
	qs := fmt.Sprintf("topic=%s", url.QueryEscape(topicName))
	return c.actionHelper(topicName, dcLookupdHTTPAddrs, nsqdHTTPAddrs, "pause_topic", "topic/pause", qs)
}

func (c *ClusterInfo) UnPauseTopic(topicName string, dcLookupdHTTPAddrs map[string][]string, nsqdHTTPAddrs []string) error {
	qs := fmt.Sprintf("topic=%s", url.QueryEscape(topicName))
	return c.actionHelper(topicName, dcLookupdHTTPAddrs, nsqdHTTPAddrs, "unpause_topic", "topic/unpause", qs)
}

func (c *ClusterInfo) PauseChannel(topicName string, channelName string, dcLookupdHTTPAddrs map[string][]string, nsqdHTTPAddrs []string) error {
	qs := fmt.Sprintf("topic=%s&channel=%s", url.QueryEscape(topicName), url.QueryEscape(channelName))
	return c.actionHelper(topicName, dcLookupdHTTPAddrs, nsqdHTTPAddrs, "pause_channel", "channel/pause", qs)
}

func (c *ClusterInfo) UnPauseChannel(topicName string, channelName string, dcLookupdHTTPAddrs map[string][]string, nsqdHTTPAddrs []string) error {
	qs := fmt.Sprintf("topic=%s&channel=%s", url.QueryEscape(topicName), url.QueryEscape(channelName))
	return c.actionHelper(topicName, dcLookupdHTTPAddrs, nsqdHTTPAddrs, "unpause_channel", "channel/unpause", qs)
}

func (c *ClusterInfo) SkipChannel(topicName string, channelName string, dcLookupdHTTPAddrs map[string][]string, nsqdHTTPAddrs []string) error {
	qs := fmt.Sprintf("topic=%s&channel=%s", url.QueryEscape(topicName), url.QueryEscape(channelName))
	return c.actionHelper(topicName, dcLookupdHTTPAddrs, nsqdHTTPAddrs, "skip_channel", "channel/skip", qs)
}

func (c *ClusterInfo) UnSkipChannel(topicName string, channelName string, dcLookupdHTTPAddrs map[string][]string, nsqdHTTPAddrs []string) error {
	qs := fmt.Sprintf("topic=%s&channel=%s", url.QueryEscape(topicName), url.QueryEscape(channelName))
	return c.actionHelper(topicName, dcLookupdHTTPAddrs, nsqdHTTPAddrs, "unskip_channel", "channel/unskip", qs)
}

func (c *ClusterInfo) EmptyTopic(topicName string, dcLookupdHTTPAddrs map[string][]string, nsqdHTTPAddrs []string) error {
	qs := fmt.Sprintf("topic=%s", url.QueryEscape(topicName))
	return c.actionHelper(topicName, dcLookupdHTTPAddrs, nsqdHTTPAddrs, "empty_topic", "topic/empty", qs)
}

func (c *ClusterInfo) EmptyChannel(topicName string, channelName string, dcLookupdHTTPAddrs map[string][]string, nsqdHTTPAddrs []string) error {
	qs := fmt.Sprintf("topic=%s&channel=%s", url.QueryEscape(topicName), url.QueryEscape(channelName))
	return c.actionHelper(topicName, dcLookupdHTTPAddrs, nsqdHTTPAddrs, "empty_channel", "channel/empty", qs)
}

func (c *ClusterInfo) ResetChannel(topicName string, channelName string, dcLookupdHTTPAddrs map[string][]string, resetBy string) error {
	qs := fmt.Sprintf("topic=%s&channel=%s", url.QueryEscape(topicName), url.QueryEscape(channelName))
	return c.actionHelperWithContent(topicName, dcLookupdHTTPAddrs, nil, "", "channel/setoffset", qs, resetBy)
}

func (c *ClusterInfo) actionHelperWithContent(topicName string, dcLookupdHTTPAddrs map[string][]string, nsqdHTTPAddrs []string, deprecatedURI string, v1URI string, qs string, content string) error {
	var errs []error

	_, dcPartitionProducers, err := c.GetTopicProducers(topicName, dcLookupdHTTPAddrs, nsqdHTTPAddrs)
	if err != nil {
		pe, ok := err.(PartialErr)
		if !ok {
			return err
		}
		errs = append(errs, pe.Errors()...)
	}
	for dc, partitionProducers := range dcPartitionProducers {
		c.logf("CI: got %v partition producers for topic %v in dc %v", len(partitionProducers), topicName, dc)
		if len(partitionProducers) > 0 {
			for pid, pp := range partitionProducers {
				qsPart := qs + "&partition=" + pid
				err = c.versionPivotProducersWithContent(pp, deprecatedURI, v1URI, qsPart, content)
				if err != nil {
					pe, ok := err.(PartialErr)
					if !ok {
						return err
					}
					errs = append(errs, pe.Errors()...)
				}
			}
		}
	}
	if len(errs) > 0 {
		return ErrList(errs)
	}
	return nil
}

func (c *ClusterInfo) actionHelper(topicName string, dcLookupdHTTPAddrs map[string][]string, nsqdHTTPAddrs []string, deprecatedURI string, v1URI string, qs string) error {
	var errs []error

	producers, dcPartitionProducers, err := c.GetTopicProducers(topicName, dcLookupdHTTPAddrs, nsqdHTTPAddrs)
	if err != nil {
		pe, ok := err.(PartialErr)
		if !ok {
			return err
		}
		errs = append(errs, pe.Errors()...)
	}

	c.logf("CI: got %v producer nodes %v partition producers for topic %v", len(producers), len(dcPartitionProducers), topicName)
	if len(dcPartitionProducers) == 0 {
		err = c.versionPivotProducers(producers, deprecatedURI, v1URI, qs)
		if err != nil {
			pe, ok := err.(PartialErr)
			if !ok {
				return err
			}
			errs = append(errs, pe.Errors()...)
		}
	} else {
		for dc, partitionProducers := range dcPartitionProducers {
			c.logf("CI: got %v partition producers for topic %v in dc %v", len(producers), len(dcPartitionProducers), topicName, dc)
			for pid, pp := range partitionProducers {
				qsPart := qs + "&partition=" + pid
				err = c.versionPivotProducers(pp, deprecatedURI, v1URI, qsPart)
				if err != nil {
					pe, ok := err.(PartialErr)
					if !ok {
						return err
					}
					errs = append(errs, pe.Errors()...)
				}
			}
		}
	}

	if len(errs) > 0 {
		return ErrList(errs)
	}
	return nil
}

func (c *ClusterInfo) GetDCProducers(dcLookupdHTTPAddrs map[string][]string, nsqdHTTPAddrs []string) (Producers, error) {
	var errs []error
	var dcProducers Producers
	if len(dcLookupdHTTPAddrs) != 0 {
		for dc, lookupdHTTPAddrs := range dcLookupdHTTPAddrs {
			producers, err := c.GetLookupdProducers(lookupdHTTPAddrs)
			if err != nil {
				pe, ok := err.(PartialErr)
				if !ok {
					return nil, err
				}
				errs = append(errs, pe.Errors()...)
			}
			for _, p := range producers {
				p.DC = dc
				dcProducers = append(dcProducers, p)
			}
		}
		if len(errs) > 0 {
			return dcProducers, ErrList(errs)
		} else {
			return dcProducers, nil
		}
	}
	return c.GetNSQDProducers(nsqdHTTPAddrs)
}

func (c *ClusterInfo) GetProducers(lookupdHTTPAddrs []string, nsqdHTTPAddrs []string) (Producers, error) {
	if len(lookupdHTTPAddrs) != 0 {
		return c.GetLookupdProducers(lookupdHTTPAddrs)
	}
	return c.GetNSQDProducers(nsqdHTTPAddrs)
}

func (c *ClusterInfo) GetTopicProducers(topicName string, dcLookupdHTTPAddrs map[string][]string, nsqdHTTPAddrs []string) (Producers, map[string]map[string]Producers, error) {
	if len(dcLookupdHTTPAddrs) != 0 {
		p, dpp, err := c.GetLookupdTopicProducers(topicName, dcLookupdHTTPAddrs)
		return p, dpp, err
	}
	p, err := c.GetNSQDTopicProducers(topicName, nsqdHTTPAddrs)
	return p, nil, err
}

func (c *ClusterInfo) versionPivotNSQLookupd(addrs []string, deprecatedURI string, v1URI string, qs string) error {
	var errs []error

	for _, addr := range addrs {
		nodeVer, _ := c.GetVersion(addr)

		uri := deprecatedURI
		if nodeVer.NE(semver.Version{}) && nodeVer.GTE(v1EndpointVersion) {
			uri = v1URI
		}

		endpoint := fmt.Sprintf("http://%s/%s?%s", addr, uri, qs)
		c.logf("CI: querying nsqlookupd %s", endpoint)
		_, err := c.client.POSTV1(endpoint)
		if err != nil {
			errs = append(errs, err)
			continue
		}
	}

	if len(errs) > 0 {
		return ErrList(errs)
	}
	return nil
}

func (c *ClusterInfo) versionPivotProducers(pl Producers, deprecatedURI string, v1URI string, qs string) error {
	var errs []error

	for _, p := range pl {
		uri := deprecatedURI
		if p.VersionObj.NE(semver.Version{}) && p.VersionObj.GTE(v1EndpointVersion) {
			uri = v1URI
		}

		endpoint := fmt.Sprintf("http://%s/%s?%s", p.HTTPAddress(), uri, qs)
		c.logf("CI: querying nsqd %s", endpoint)
		_, err := c.client.POSTV1(endpoint)
		if err != nil {
			errs = append(errs, err)
			continue
		}
	}

	if len(errs) > 0 {
		return ErrList(errs)
	}
	return nil
}

func (c *ClusterInfo) versionPivotProducersWithContent(pl Producers, deprecatedURI string, v1URI string, qs string, content string) error {
	var errs []error

	for _, p := range pl {
		uri := deprecatedURI
		if p.VersionObj.NE(semver.Version{}) && p.VersionObj.GTE(v1EndpointVersion) {
			uri = v1URI
		}

		endpoint := fmt.Sprintf("http://%s/%s?%s", p.HTTPAddress(), uri, qs)
		c.logf("CI: querying nsqd %s", endpoint)
		_, err := c.client.POSTV1WithContent(endpoint, content)
		if err != nil {
			errs = append(errs, err)
			continue
		}
	}

	if len(errs) > 0 {
		return ErrList(errs)
	}
	return nil
}
