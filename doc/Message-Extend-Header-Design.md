# Message custom extend header design

## Background

Currently, we dispatch channel message to the client by select on the same go chan which result in the average dispatch across all the clients under the same channel.

We want to add a new dispatch policy which can dispatch by the tag in the message. For a client that identify its desired_tag = tagA, we try dispatch messages with tagA to this client only. If no any matching client, we dispatch the message to the client without any desired_tag.

First, we need support add extend data in the message. We allow new created topic can support extend feature.
The topic with extend feature can pub the message with tag as the extend data. The channel consumer can use the identify command to set the desired_tag as dispatch policy. While delivering the server should dispatch the messages based on the tag in the message and the desired_tag in the consumer.

We found it may be a common demand to have a custom header for each message in future, so we make it a common feature and the tag is just one of usage.

## Extend PUB protocol

For tcp , we introduced a new command

```
PUB_EXT <topic_name> <topic_partition>\n
[ 4-byte size in bytes ][2-byte header length][json header data][body binary data]
 
<topic_name> - a valid string
json header example: {"##client_dispatch_tag":"test_tag","custom_header1":"test_header","custom_h2":"test"}
valid header name is [0-9a-zA-Z_-#]+, valid header value is valid json string
```

Just like the Http Header, we use key-value as different cumstom message header. For the header name begin with `##` we reserved it as internal use. (For example, we use `##client_dispatch_tag` as internal client dispatch policy, and use `##trace_id` as internal message tracer)

Success response will be different based on the internal header `##trace_id`
```
// if ##trace_id is empty in json header
OK

// if  ##trace_id is string of number
OK(2-bytes)+[8-byte internal id]+[8-byte trace id from client]+[8-byte internal disk queue offset]+[4 bytes internal disk queue data size]
```
The error response will be same with the old PUB command.

HTTP extend api
```
curl -X POST -d "<message>" "http://127.0.0.1:4151/pub_ext?topic=name&partition=1&ext={'k1':'v1'}"
  
http header with X-NSQEXT-XXX:value will also be added to the extend json and named as xxx:value, and the header key xxx will be converted to low-case

```
Success Response for http will be 
```
// if ##trace_id is empty
"OK"
 
// if ##trace_id is string of numbers
{ 
  "status":"OK",
  "id":123,
  "trace_id":"123456789",
  "queue_offset":100000,
  "rawsize":400
}
```

## Extend Message format on disk
In order to make it compatible while upgrading the topic to the extend topic, we reused some bits on the old attempts field since currently it is never used all the 2-bytes. The high 4 bits in attempts will indicate whether the message has extend or not. If the message is extend, we need encode/decode the version of extend, the length of extend and the real extend data before the body.

```
[x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x]...[x][x][x][x]...
|       (int64)        ||    ||      (binary string )                        || ||    ||   ext string   ||  (binary)
|       8-byte         ||    ||                 16-byte                      || ||    ||    extN-byte   ||   N-byte
---------------------------------------------------------------------------------------------------------------------------------...
  nanosecond timestamp    ^^                   message ID                      ^    ^      extend data              message body
                       (uint16)                                            uint8,  uint16
                        2-byte                                             1-byte, 2-byte
                       attempts                                     ver of extend, length of extend

```

## Handle the subscribe command
The server will handle the unexpected subscribe params based on the topic extend feature.
- if topic is extend, the subscribe client must identify with  `"extend_support":true`
- if topic is not extend, but the client identified with `extend_support":true`, return error
- if topic is not extend, but the client identified with `"desired_tag":"tagA"`, just ignore the desired_tag field and subscribe as normal client.

## Consume on the Extend topic
add a new identify 
```
"extend_support":true
```
Consumer must identify itself with this if consuming on the extend topic, and the sdk should decode the message header.

## Extend Message format received by client
The message format is almost same with the extend message format in disk (above), except the attempts is not including the indicator on the high 4-bits. The attempts send to client is same with old message.

## Subscribe with dispatch tag
While subscribe the client can identify with a `"desired_tag":"tagA"` to indicate this client should receive the message with `tagA` extend firstly.

## lookup response
The meta info responsed from lookup api in the nsqlookupd will add new json field if this topic is extend.
```
...
"meta":{       
    "partition_num":4,
    "replica":1,
    "extend_support": true
 },
 ...
```

## create or upgrade a topic with extend support 
Send http api to nsqlookupd to create topic with ext
```
curl -X POST "http://127.0.0.1:4161/topic/creat?topic=xxx&partition_num=2&replicator=2&extend=true"
```
If you want upgrade the old topic to new topic with ext supported, use the below api
```
curl -X POST "http://127.0.0.1:4161/topic/meta/update?topic=replay_rpc_copy&upgradeext=true"
```

