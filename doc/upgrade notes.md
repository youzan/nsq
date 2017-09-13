# 升级事项

## upgrade to 1.5.7+
由于1.5.7版本开始引入了新的延时队列, 从1.5.7以下版本升级到1.5.7及更高版本需要参照以下步骤
修改nsqd配置文件的如下两个参数
```
## max allowed delayed req time
max_req_timeout = "48h"
## req threshold for delayed queue
req_to_end_threshold = "15m"
```

升级过程中, 部分延迟消费的请求可能会失败, 但是不影响最终消费. 全部升级完毕后, 同时发送API给所有nsqd节点, 启用延时队列新特性.

```
curl -X PUT "http://127.0.0.1:4151/delayqueue/enable?enable=true"
```