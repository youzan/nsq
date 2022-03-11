package nsqdserver

import (
	"fmt"
	"math"
	"runtime"
	"sort"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/youzan/nsq/internal/protocol"
	"github.com/youzan/nsq/internal/statsd"
	"github.com/youzan/nsq/nsqd"
)

type Uint64Slice []uint64

func (s Uint64Slice) Len() int {
	return len(s)
}

func (s Uint64Slice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s Uint64Slice) Less(i, j int) bool {
	return s[i] < s[j]
}

func (n *NsqdServer) historySaveLoop() {
	opts := n.ctx.getOpts()
	ticker := time.NewTicker(opts.StatsdInterval * 10)
	defer ticker.Stop()
	for {
		select {
		case <-n.exitChan:
			return
		case <-ticker.C:
			n.ctx.nsqd.UpdateTopicHistoryStats()
		}
	}
}

func (n *NsqdServer) statsdLoop() {
	var lastMemStats runtime.MemStats
	var lastStats []nsqd.TopicStats
	opts := n.ctx.getOpts()
	ticker := time.NewTicker(opts.StatsdInterval)
	for {
		select {
		case <-n.exitChan:
			goto exit
		case <-ticker.C:
			stats := n.ctx.nsqd.GetStats(false, true)
			var client *statsd.Client
			if opts.StatsdAddress != "" {
				nc := statsd.NewClient(opts.StatsdAddress, opts.StatsdPrefix)
				err := nc.CreateSocket(opts.StatsdProtocol)
				if err != nil {
					nsqd.NsqLogger().Logf("failed to create %v socket to statsd(%s), err: %s", opts.StatsdProtocol, nc, err.Error())
					// we continue here to allow update the stats for prometheus without pushing the old statsd
				} else {
					client = nc
				}
				nsqd.NsqLogger().LogDebugf("STATSD: pushing stats to %s, using prefix: %v", nc, opts.StatsdPrefix)
			}

			// since the topic may migrate to other nodes, we should always reset to keep only needed labels
			nsqd.TopicQueueMsgEnd.Reset()
			nsqd.TopicPubClientCnt.Reset()
			nsqd.ChannelBacklog.Reset()
			nsqd.ChannelDepth.Reset()
			nsqd.ChannelDepthSize.Reset()
			nsqd.ChannelDepthTs.Reset()
			nsqd.ChannelClientCnt.Reset()
			nsqd.ChannelDelayedQueueCnt.Reset()
			nsqd.ChannelDelayedQueueTs.Reset()
			nsqd.ChannelInflightCnt.Reset()
			for _, topic := range stats {
				nsqd.TopicQueueMsgEnd.With(prometheus.Labels{
					"topic":     topic.TopicName,
					"partition": topic.TopicPartition,
				}).Set(float64(topic.MessageCount))
				clientNum := len(topic.Clients)
				if clientNum == 0 {
					clientNum = int(topic.ClientNum)
				}
				nsqd.TopicPubClientCnt.With(prometheus.Labels{
					"topic":     topic.TopicName,
					"partition": topic.TopicPartition,
				}).Set(float64(clientNum))
				// try to find the topic in the last collection
				lastTopic := nsqd.TopicStats{}
				for _, checkTopic := range lastStats {
					if topic.StatsdName == checkTopic.StatsdName {
						lastTopic = checkTopic
						break
					}
				}
				statdName := topic.StatsdName

				for _, channel := range topic.Channels {
					// ephemeral may be too much, so we just ignore report ephemeral channel stats to remote
					if protocol.IsEphemeral(channel.ChannelName) {
						continue
					}
					nsqd.ChannelBacklog.With(prometheus.Labels{
						"topic":     topic.TopicName,
						"partition": topic.TopicPartition,
						"channel":   channel.ChannelName,
					}).Set(float64(channel.Backlogs))
					nsqd.ChannelDepth.With(prometheus.Labels{
						"topic":     topic.TopicName,
						"partition": topic.TopicPartition,
						"channel":   channel.ChannelName,
					}).Set(float64(channel.Depth))
					nsqd.ChannelDepthSize.With(prometheus.Labels{
						"topic":     topic.TopicName,
						"partition": topic.TopicPartition,
						"channel":   channel.ChannelName,
					}).Set(float64(channel.DepthSize))
					nsqd.ChannelDepthTs.With(prometheus.Labels{
						"topic":     topic.TopicName,
						"partition": topic.TopicPartition,
						"channel":   channel.ChannelName,
					}).Set(float64(channel.DepthTsNano))
					nsqd.ChannelClientCnt.With(prometheus.Labels{
						"topic":     topic.TopicName,
						"partition": topic.TopicPartition,
						"channel":   channel.ChannelName,
					}).Set(float64(channel.ClientNum))
					nsqd.ChannelDelayedQueueCnt.With(prometheus.Labels{
						"topic":     topic.TopicName,
						"partition": topic.TopicPartition,
						"channel":   channel.ChannelName,
					}).Set(float64(channel.DelayedQueueCount))
					nsqd.ChannelDelayedQueueTs.With(prometheus.Labels{
						"topic":     topic.TopicName,
						"partition": topic.TopicPartition,
						"channel":   channel.ChannelName,
					}).Set(float64(channel.DelayedQueueRecentNano))
					nsqd.ChannelInflightCnt.With(prometheus.Labels{
						"topic":     topic.TopicName,
						"partition": topic.TopicPartition,
						"channel":   channel.ChannelName,
					}).Set(float64(channel.InFlightCount))
					if client == nil {
						continue
					}
					// try to find the channel in the last collection
					lastChannel := nsqd.ChannelStats{}
					for _, checkChannel := range lastTopic.Channels {
						if channel.ChannelName == checkChannel.ChannelName {
							lastChannel = checkChannel
							break
						}
					}
					diff := channel.MessageCount - lastChannel.MessageCount
					if (topic.IsMultiOrdered || topic.IsMultiPart) && !topic.IsLeader {
						diff = 0
					}

					stat := fmt.Sprintf("topic.%s.channel.%s.message_count", statdName, channel.ChannelName)
					client.Incr(stat, int64(diff))

					var cnt int64
					cnt = channel.Depth
					if (topic.IsMultiOrdered || topic.IsMultiPart) && !topic.IsLeader {
						cnt = 0
					}
					stat = fmt.Sprintf("topic.%s.channel.%s.depth", statdName, channel.ChannelName)
					client.Gauge(stat, cnt)

					cnt = channel.BackendDepth
					if (topic.IsMultiOrdered || topic.IsMultiPart) && !topic.IsLeader {
						cnt = 0
					}
					stat = fmt.Sprintf("topic.%s.channel.%s.backend_depth", statdName, channel.ChannelName)
					client.Gauge(stat, cnt)

					stat = fmt.Sprintf("topic.%s.channel.%s.in_flight_count", statdName, channel.ChannelName)
					client.Gauge(stat, int64(channel.InFlightCount))

					stat = fmt.Sprintf("topic.%s.channel.%s.deferred_count", statdName, channel.ChannelName)
					client.Gauge(stat, int64(channel.DeferredCount))

					diff = channel.RequeueCount - lastChannel.RequeueCount
					stat = fmt.Sprintf("topic.%s.channel.%s.requeue_count", statdName, channel.ChannelName)
					client.Incr(stat, int64(diff))

					diff = channel.TimeoutCount - lastChannel.TimeoutCount
					stat = fmt.Sprintf("topic.%s.channel.%s.timeout_count", statdName, channel.ChannelName)
					client.Incr(stat, int64(diff))

					// 16ms, 32ms, 64ms, 128ms, 256ms, 512ms, 1024ms, 2048ms, 4s, 8s, 16s, above
					old500ms := int64(0)
					for i := 6; i < len(lastChannel.MsgConsumeLatencyStats); i++ {
						old500ms += lastChannel.MsgConsumeLatencyStats[i]
					}
					old1s := int64(0)
					if len(lastChannel.MsgConsumeLatencyStats) > 6 {
						old1s = old500ms - lastChannel.MsgConsumeLatencyStats[6]
					}
					new500ms := int64(0)
					for i := 6; i < len(channel.MsgConsumeLatencyStats); i++ {
						new500ms += channel.MsgConsumeLatencyStats[i]
					}
					new1s := int64(0)
					if len(channel.MsgConsumeLatencyStats) > 6 {
						new1s = new500ms - channel.MsgConsumeLatencyStats[6]
					}

					diff = uint64(new500ms - old500ms)
					stat = fmt.Sprintf("topic.%s.channel.%s.consume_above500ms_count", statdName, channel.ChannelName)
					client.Incr(stat, int64(diff))

					diff = uint64(new1s - old1s)
					stat = fmt.Sprintf("topic.%s.channel.%s.consume_above1s_count", statdName, channel.ChannelName)
					client.Incr(stat, int64(diff))

					old500ms = int64(0)
					for i := 6; i < len(lastChannel.MsgDeliveryLatencyStats); i++ {
						old500ms += lastChannel.MsgDeliveryLatencyStats[i]
					}
					old1s = int64(0)
					if len(lastChannel.MsgDeliveryLatencyStats) > 6 {
						old1s = old500ms - lastChannel.MsgDeliveryLatencyStats[6]
					}
					new500ms = int64(0)
					for i := 6; i < len(channel.MsgDeliveryLatencyStats); i++ {
						new500ms += channel.MsgDeliveryLatencyStats[i]
					}
					new1s = int64(0)
					if len(channel.MsgDeliveryLatencyStats) > 6 {
						new1s = new500ms - channel.MsgDeliveryLatencyStats[6]
					}

					diff = uint64(new500ms - old500ms)
					stat = fmt.Sprintf("topic.%s.channel.%s.delivery2ack_above500ms_count", statdName, channel.ChannelName)
					client.Incr(stat, int64(diff))

					diff = uint64(new1s - old1s)
					stat = fmt.Sprintf("topic.%s.channel.%s.delivery2ack_above1s_count", statdName, channel.ChannelName)
					client.Incr(stat, int64(diff))

					stat = fmt.Sprintf("topic.%s.channel.%s.clients", statdName, channel.ChannelName)
					client.Gauge(stat, int64(channel.ClientNum))

					for _, item := range channel.E2eProcessingLatency.Percentiles {
						stat = fmt.Sprintf("topic.%s.channel.%s.e2e_processing_latency_%.0f", statdName, channel.ChannelName, item["quantile"]*100.0)
						client.Gauge(stat, int64(item["value"]))
					}
				}
				if client == nil {
					continue
				}
				diff := topic.MessageCount - lastTopic.MessageCount
				if (topic.IsMultiOrdered || topic.IsMultiPart) && !topic.IsLeader {
					diff = 0
				}
				stat := fmt.Sprintf("topic.%s.message_count", statdName)
				err := client.Incr(stat, int64(diff))
				if err != nil {
					nsqd.NsqLogger().Logf("STATSD: pushing stats failed: %v", err)
				}

				for _, item := range topic.E2eProcessingLatency.Percentiles {
					stat = fmt.Sprintf("topic.%s.e2e_processing_latency_%.0f", statdName, item["quantile"]*100.0)
					// We can cast the value to int64 since a value of 1 is the
					// minimum resolution we will have, so there is no loss of
					// accuracy
					client.Gauge(stat, int64(item["value"]))
				}
			}
			lastStats = stats
			if client == nil {
				continue
			}

			if opts.StatsdMemStats {
				var memStats runtime.MemStats
				runtime.ReadMemStats(&memStats)

				// sort the GC pause array
				length := len(memStats.PauseNs)
				if int(memStats.NumGC) < length {
					length = int(memStats.NumGC)
				}
				gcPauses := make(Uint64Slice, length)
				copy(gcPauses, memStats.PauseNs[:length])
				sort.Sort(gcPauses)

				client.Gauge("mem.heap_objects", int64(memStats.HeapObjects))
				client.Gauge("mem.heap_idle_bytes", int64(memStats.HeapIdle))
				client.Gauge("mem.heap_in_use_bytes", int64(memStats.HeapInuse))
				client.Gauge("mem.heap_released_bytes", int64(memStats.HeapReleased))
				client.Gauge("mem.gc_pause_usec_100", int64(percentile(100.0, gcPauses, len(gcPauses))/1000))
				client.Gauge("mem.gc_pause_usec_99", int64(percentile(99.0, gcPauses, len(gcPauses))/1000))
				client.Gauge("mem.gc_pause_usec_95", int64(percentile(95.0, gcPauses, len(gcPauses))/1000))
				client.Gauge("mem.next_gc_bytes", int64(memStats.NextGC))
				err := client.Incr("mem.gc_runs", int64(memStats.NumGC-lastMemStats.NumGC))
				if err != nil {
					nsqd.NsqLogger().Logf("STATSD: pushing stats failed: %v", err)
				}

				lastMemStats = memStats
			}

			client.Close()
		}
	}

exit:
	ticker.Stop()
}

func percentile(perc float64, arr []uint64, length int) uint64 {
	if length == 0 {
		return 0
	}
	indexOfPerc := int(math.Floor(((perc / 100.0) * float64(length)) + 0.5))
	if indexOfPerc >= length {
		indexOfPerc = length - 1
	}
	return arr[indexOfPerc]
}
