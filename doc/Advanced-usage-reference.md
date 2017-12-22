# 新版高级功能指南

## 顺序topic的使用指南
严格顺序消费需要生产方和消费方配合才能达到完全的顺序, 生产方需要指定一个顺序的字段, 比如订单id, 所有同一个订单的消息都必须单节点单线程按照顺序写入nsq, 消费业务则指定顺序消费即可, 可以多节点消费, 由服务端控制顺序推送.

### 顺序topic创建
首先在nsqadmin界面创建一个顺序topic, 具体分区数根据业务数据量决定, 对于一个每天200w消息量的topic, 建议10个分区左右

![topic create](resources/ordered_topic_create.png)

### 消息生产 (Golang, Java)
Golang示例

```Go
    topics := ["test_topic"]
    lookupAddress := "127.0.0.1:4161"
    config := nsq.NewConfig()
    // 启用顺序生产
	config.EnableOrdered = true
	// 默认pub返回的id, offset, rawSize数据无效, 当需要跟踪或者调试的时候, 可以开启
	// trace, 这时id, offset, rawSize会返回服务端写入的队列位置便于跟踪. 注意不要一直开启
	// 影响写入性能.
	// config.EnableTrace = true

    // 顺序生产的分区hash算法, 针对pub传入的sharding key做hash分区, 保证同样的订单id落到同一个分区保证顺序,
    // 并且保证不同的订单id, 均匀的分散到多个分区保证不同订单id的并发能力
	config.Hasher = murmur3.New32()
	pubMgr, err := nsq.NewTopicProducerMgr(topics, config)
	if err != nil {
		log.Printf("init error : %v", err)
		return
	}
	pubMgr.SetLogger(log.New(os.Stderr, "", log.LstdFlags), nsq.LogLevelInfo)
	err = pubMgr.ConnectToNSQLookupd(lookupAddress)
	if err != nil {
		log.Printf("lookup connect error : %v", err)
		return
	}
	var id nsq.NewMessageID
	var offset uint64
	var rawSize uint32
    // 生产顺序消息, orderID就是传入需要保证顺序的订单id, 注意必须保证相同的orderID不会产生并发写入, 否则生产的消息可能乱序, 此处可以用锁保护, 或者相同的订单id消息使用固定的线程.
	id, offset, rawSize, err = pubMgr.PublishOrdered(topic, orderID, msg)
```

### 消息消费

```Go
type consumeHandler struct {
	topic           string
}

func (c *consumeHandler) HandleMessage(message *nsq.Message) error {
	return nil
}

func main() {
    topic := "test_topic"
    channel := "order_sub"
    lookupAddress := "127.0.0.1:4161"
    config := nsq.NewConfig()
    // 启用顺序消费
    config.EnableOrdered = true
	config.Hasher = murmur3.New32()
	consumer, err := nsq.NewConsumer(topic, channel, config)
	if err != nil {
		panic(err.Error())
	}
    consumer.SetLogger(log.New(os.Stderr, "", log.LstdFlags), nsq.LogLevelInfo)
    // 注册并发消费处理函数
	consumer.AddConcurrentHandlers(&consumeHandler{topic}, 5)
	done := make(chan struct{})
	go func() {
        time.Sleep(time.Minute)
        // 消费完成后, 优雅停止消费
		consumer.Stop()
		<-consumer.StopChan
		close(done)
    }()
    // 连接lookup地址开始消费
	consumer.ConnectToNSQLookupd(lookupAddr)
    // 等待消费完成或者消费被停止
    <-done
}
```

## 消息跟踪id使用

## 延时消息使用指南
延时消息目前只支持消费延时, 只能在消费端控制延时的消息. 注意使用场景是少量需要重试的消息, 比如异常需要重试, 或者消费的下游业务系统超时, 或者需要一直重试直到成功的情况(比如信用卡还款这种). 消费业务方可以根据合适的延时策略做一些个性化的延时需求. NSQ的延时场景设计成非精确的, 因此只能保证尽量按照指定的延时投递, 可能会有误差, 特别是服务端较忙, 或者消费有积压的情况. 注意顺序消费的topic不允许使用延时消费功能.

示例
```Go
type consumeHandler struct {
	topic           string
}

func (c *consumeHandler) HandleMessage(message *nsq.Message) error {
	now := time.Now().Unix()
	// 消息产生的时间
	produceTime := message.Timestamp
	var delayTs int64
	// 此处解析消息体, 获取消费时间数据, 判断是否延时, 如果需要延时计算延时时间, 计算结果存储到delayTS
	// ....
	//
	//
	if delayTs > now {
		// 消息延时时间还未到, 继续发送延时请求. 注意服务端可能推送的时间不准确, 如果未到时间, 需要下次收到时再次进行延时
		message.DisableAutoResponse()
		message.RequeueWithoutBackoff(time.Duration(delayTs-now) * time.Second)
	} else {
		log.Printf("message is processed: (id %v), at time : %v", message.ID, now)
		// 延时时间已到, 此处处理消息消费的业务逻辑
	}
	return nil
}

func main() {
    topic := "test_topic"
    channel := "sub"
    lookupAddress := "127.0.0.1:4161"
	config := nsq.NewConfig()
	// 配置最大允许的延时时间, 需小于服务端配置的最大值
	config.MaxRequeueDelay = time.Minute * 60
	consumer, err := nsq.NewConsumer(topic, channel, config)
	if err != nil {
		panic(err.Error())
	}
    consumer.SetLogger(log.New(os.Stderr, "", log.LstdFlags), nsq.LogLevelInfo)
    // 注册并发消费处理函数
	consumer.AddConcurrentHandlers(&consumeHandler{topic}, 5)
	done := make(chan struct{})
	go func() {
        time.Sleep(time.Minute)
        // 消费完成后, 优雅停止消费
		consumer.Stop()
		<-consumer.StopChan
		close(done)
    }()
    // 连接lookup地址开始消费
	consumer.ConnectToNSQLookupd(lookupAddr)
    // 等待消费完成或者消费被停止
    <-done
}

```

## 扩展消息的使用指南

### 扩展topic创建
### 扩展字段类型
### 扩展字段写入
### 消费消息的扩展字段

## 消费过滤示例

## 分区个数创建的建议
非顺序的topic, 由于支持同一个partition进行多个并发消费, 因此无需过多的partitions, 只需保证写入性能满足需求即可, 另外为了保持和原版nsq兼容, 每个节点只能有一个分区, 因此分区数*副本数不能大于节点总数. 非顺序的topic可以动态扩建分区不影响业务使用. 建议普通topic使用 2分区2副本, 业务数据很多, 但是不怎么重要的, 比如log数据, 可以使用4分区1副本, 对于数据要求很高的, 可以使用2分区3副本(需要6台机器集群).

对于顺序的topic, 分区数取决于消费能力, 如果需要更高的并发度, 可以优化消费业务的消费能力, 如果消费能力已经无法优化, 则需要更多的分区数提高并发能力. 大部分情况下16个分区可以满足大部分需求, 如果吞吐量很大, 还需要实际计算消费业务的延迟来决定. 由于顺序消费topic在动态扩建topic时会导致无序消息, 因此需要规划一个较长时间的潜在能力.
## 一些消费配置的建议

ready:
max_inflight:
max_attempt:
msg_timeout:
并发工作线程数:

其他值可以保持默认值

## 常见错误信息排查
