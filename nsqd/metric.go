package nsqd

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	ServerPubFailedCnt = promauto.NewCounter(prometheus.CounterOpts{
		Name: "server_pub_failed_cnt",
		Help: "the pub failed counter for server error",
	})
	// metric for topic
	TopicPubFailedCnt = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "topic_pub_failed_cnt",
		Help: "the pub failed counter for the topic pub error",
	}, []string{"topic", "partition"})
	TopicPubTotalCnt = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "topic_pub_total_cnt",
		Help: "the pub total counter for the client pub",
	}, []string{"topic", "partition"})
	TopicQueueMsgEnd = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "topic_queue_msg_end",
		Help: "the end message count of the topic disk queue",
	}, []string{"topic", "partition"})
	TopicWriteLatencyMs = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "topic_write_latency_ms",
		Help:    "topic write latency ms",
		Buckets: prometheus.ExponentialBuckets(1, 2, 14),
	}, []string{"topic", "partition"})
	TopicWriteByteSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "topic_write_byte_size",
		Help:    "topic write message byte size",
		Buckets: prometheus.ExponentialBuckets(128, 2, 14),
	}, []string{"topic", "partition"})
	TopicPubClientCnt = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "topic_pub_client_cnt",
		Help: "the current producer connections for topic",
	}, []string{"topic", "partition"})
	TopicPubRefusedByLimitedCnt = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "topic_pub_refused_by_limited_cnt",
		Help: "topic pub refused by the waiting limit cnt",
	}, []string{"topic"})

	// metric for channel
	ChannelBacklog = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "channel_backlog",
		Help: "channel message backlog not ack",
	}, []string{"topic", "partition", "channel"})
	ChannelDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "channel_depth",
		Help: "channel depth for message waiting consumed cnt",
	}, []string{"topic", "partition", "channel"})
	ChannelDepthSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "channel_depth_size",
		Help: "channel depth size from the earliest message consumed cursor",
	}, []string{"topic", "partition", "channel"})
	ChannelDepthTs = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "channel_depth_ts",
		Help: "channel depth for message recent consumed timestamp",
	}, []string{"topic", "partition", "channel"})
	ChannelRequeuedCnt = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "channel_requeued_cnt",
		Help: "total requeued cnt for channel consumer",
	}, []string{"topic", "partition", "channel"})
	ChannelRetryToomuchCnt = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "channel_retry_toomuch_cnt",
		Help: "total retry too much cnt for channel consumer",
	}, []string{"topic", "channel"})
	ChannelAutoAckCnt = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "channel_auto_ack_cnt",
		Help: "total auto ack cnt for channel consumer",
	}, []string{"topic", "channel", "reason"})
	ChannelTimeoutCnt = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "channel_timeout_cnt",
		Help: "total timeout cnt for channel consumer",
	}, []string{"topic", "partition", "channel"})
	ChannelClientCnt = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "channel_client_cnt",
		Help: "channel current consumer connections",
	}, []string{"topic", "partition", "channel"})
	ChannelDelayedQueueCnt = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "channel_delayed_queue_cnt",
		Help: "channel delayed queue cnt which need to be consumed",
	}, []string{"topic", "partition", "channel"})
	ChannelDelayedQueueTs = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "channel_delayed_queue_ts",
		Help: "channel delayed queue the recent timestamps which need to be consumed",
	}, []string{"topic", "partition", "channel"})
	ChannelConsumeDelivery2AckLatencyMs = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "channel_consume_delivery2ack_latency_ms",
		Help:    "the latency ms cost between delivery to client and ack from client",
		Buckets: prometheus.ExponentialBuckets(8, 2, 12),
	}, []string{"topic", "partition", "channel"})
	ChannelRateLimitCnt = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "channel_ratelimit_cnt",
		Help: "total ratelimit cnt for channel consumer",
	}, []string{"topic", "channel"})
	ChannelInflightCnt = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "channel_inflight_cnt",
		Help: "channel inflight message cnt",
	}, []string{"topic", "partition", "channel"})
)
