module github.com/youzan/nsq

go 1.13

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/StackExchange/wmi v0.0.0-20210224194228-fe8f1750fd46 // indirect
	github.com/Workiva/go-datastructures v1.0.50
	github.com/absolute8511/bolt v1.5.2
	github.com/absolute8511/glog v0.4.1
	github.com/absolute8511/gorpc v0.0.0-20161203145636-60ee7d4359cb
	github.com/absolute8511/goskiplist v0.0.0-20170727031420-3ba6f667c3df
	github.com/bitly/go-hostpool v0.0.0-20171023180738-a3a6125de932
	github.com/bitly/go-simplejson v0.5.0
	github.com/bitly/timer_metrics v0.0.0-20170606164300-b1c65ca7ae62
	github.com/blang/semver v3.5.1+incompatible
	github.com/bmizerany/assert v0.0.0-20160611221934-b7ed37b82869 // indirect
	github.com/bmizerany/perks v0.0.0-20141205001514-d9a9656a3a4b
	github.com/cenkalti/backoff v2.1.0+incompatible
	github.com/certifi/gocertifi v0.0.0-20200211180108-c7c1fbc02894 // indirect
	github.com/cockroachdb/pebble v0.0.0-20210322142411-65860c8c27ac
	github.com/coreos/etcd v3.3.13+incompatible
	github.com/getsentry/raven-go v0.2.0 // indirect
	github.com/go-ole/go-ole v1.2.5 // indirect
	github.com/gobwas/glob v0.2.3
	github.com/gogo/protobuf v1.3.1
	github.com/golang/protobuf v1.4.2
	github.com/golang/snappy v0.0.2-0.20190904063534-ff6b7dc882cf
	github.com/gorilla/sessions v1.1.3
	github.com/hashicorp/golang-lru v0.5.3
	github.com/json-iterator/go v1.1.10
	github.com/judwhite/go-svc v1.0.0
	github.com/julienschmidt/httprouter v1.2.0
	github.com/kr/pretty v0.2.0 // indirect
	github.com/mreiferson/go-options v0.0.0-20161229190002-77551d20752b
	github.com/myesui/uuid v1.0.0 // indirect
	github.com/prometheus/client_golang v1.7.1
	github.com/shirou/gopsutil v3.21.2+incompatible
	github.com/spaolacci/murmur3 v1.1.0
	github.com/stretchr/testify v1.6.1
	github.com/tidwall/gjson v1.1.3
	github.com/tidwall/match v1.0.1 // indirect
	github.com/twinj/uuid v1.0.0
	github.com/twmb/murmur3 v1.1.5
	github.com/valyala/fastjson v1.6.1
	github.com/viki-org/dnscache v0.0.0-20130720023526-c70c1f23c5d8
	github.com/youzan/go-nsq v1.7.2-HA
	golang.org/x/net v0.0.0-20200520004742-59133d7f0dd7
	golang.org/x/sync v0.0.0-20201020160332-67f06af15bc9
	golang.org/x/time v0.0.0-20200630173020-3af7569d3a1e
	google.golang.org/grpc v1.29.1
	gopkg.in/stretchr/testify.v1 v1.2.2 // indirect
	gopkg.in/yaml.v2 v2.3.0
)

replace github.com/ugorji/go => github.com/ugorji/go/codec v1.1.7
