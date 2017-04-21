# Extension to Building Client Libraries

This article is a extend of &lt;building client libraries&gt; in nsq.io, to provide some guidelines in building nsq client for redesigned nsq . The nsq client offers user ability to publish/consume message to nsq cluster,  it supports new features in redesigned nsq like HA, access conctrol, partition based ordered consumption as well. It is compatible with SPEC of client lib in nsqd.io and also extends to address features new in redesigned nsq.  Besides basic consume/publish abilities, following features provided by the nsq client, in order to collaborate with new features in redesigned nsq:

* nsqd node discovery via launching request to nsqlookupd. 
* write/read acess control for produce/consume message. 
* partition based message consume/produce in order.
* publish retry policy

## Client workflow

The nsq client work in following flow: user start nsq client, as producer or consumer, lookupServ process lookup provided topics for nsqd nodes, and client connects to them, if there is any. After connection established, for producer, Pub command is sent and  Sub for consumer. lookupServ invoke lookup at interval and producer/consumer update connections to nsqd accordlly. Connections to nsqd disconnected upon closure of nsq client.

Fig1. nsq client work flow

![nsq-client-workflow](resources/nsq-client-overview/nsq-client-wf.png)

## Access control

Access control serves within nsq lookup http service in redesigned nsq, to provide access control. It allow nsqlookupd to mark nsqd node as writable and readable.  Access controls applies to scenario when there is a nsqd node need retire, access control allows nsq client to connect to that nsqd which is readable for consuming but producing, then node could retire after messages in stack are consumed. The nsq client requests lookup service with access query to support that.

In redesigned nsq, producers also uses nsqlookupd to discovery nsqd nodes which it can produce messages to. When lookup request arrives in nsqlookupd, access control parameter tells nsqlookupd the role of nsq client \(producer or consumer\), and nsqlooupd return nsqd nodes for write/read base on that parameter. nsqd nodes which are writable return for nsq client producer and readable nodes return for read.

> \#lookup request with w\(ritable\) parameter, which has extra access parameter to indicate the role of nsq client
>
> \#following is a lookup query from producer
>
> [http://{nsqlookupd\_host}:{port}/lookup?topic={topic}&access=w](http://{nsqlookupd_host}:{port}/lookup?topic={topic}&access=w)
>
> \#another lookup query from consumer
>
> [http://{nsqlookupd\_host}:{port}/lookup?topic={topic}&access=r](http://{nsqlookupd_host}:{port}/lookup?topic={topic}&access=r)

## NSQd discovery & Load balance

Since in redesigned nsq, producer also consumes lookup services in nsqlookupd, and partition is introduced in redesigned nsq, producer may have more than one node to write. Prodcuers in nsq client could consume lookup services to fetch nsqd partitions which it could write to.

Besides the load baclance from redesigned nsq server, nsq client could also balance data flow to writable nodes. Take nsq java client 2.2 as example, round robin  policy applies for publish, messages are send to all writable partitions evenly. Round robin also applies in publish retry. for one topic which has moren than one partition nsqd node, if Pub to first failed with exception which nsq client should retry, nsq client will retry with next nsqd partition node. As to "exception which nsq client shoudl retry",  examples are connection problem or timeout, if Pub fail with exception like MESSAEG\_INVALID or_ \_E_\_\_INVALID\_TOPIC, nsq client should exit publish process, without retry.

## Connection Handling Cont.

As redesigned nsqd handles new commands like SubOrdered, after IDENTIFY sent,  sending SubOrdered is an option here.  If target topic for subscribing is configured for order subscribe, while sub command is sent E\_SUB\_ORDER\_IS\_MUST error responses and sub scribe process should fail.

## Publish Retry

nsq client SDK may support retry publishing message when an attemp to send message fail, however, there is some error type republish is not recommand. Below is a list

> ```
> E_BAD_TOPIC
> E_TOPIC_NOT_EXIST
> E_FAILED_ON_NOT_LEADER
> E_FAILED_ON_NOT_WRITABLE
> ```

## Partition based ordered consume/produce

> Note: Ordered consume is supportted in nsq java client 2.3.

Redesigned nsq has partition based ordered consumed, which make sure message in a particular partition is consumed in order. The nsq client send a new **SubOrdered** command to nsqd. After tcp conenction established, rdy for that conenction is fixed as 1 and requeue message will be put at the top of message queue. For ordered publish to one specified partition, nsq client accepts one sharding key to map it to one partition. A potential sharding key would be: order id or product related id. Message has same sharding id goes into same partition, if one partition is not available upon publish ordered, nsq client shoudl return with publish failure. Extra lookup query parameter **metainfo** intrduced for producer to allow it know total partition number for current topic. meta info param does not contains in lookup query from consumer in order to improve unnecessary cost.

> \#lookup request with meta info param, originate form producer
>
> [http://{nsqlookupd\_host}:{port}/lookup?topic={topic}&access=w&metainfo=true](http://{nsqlookupd_host}:{port}/lookup?topic={topic}&access=w&metainfo=true)
