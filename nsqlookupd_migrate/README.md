#nsqlookupd migrate proxy
Migrate proxy to help migrate from old nsq to youzan nsq.
 
Migrate works as proxy of nsqlookupd of nsq and youzan nsq. It delegates lookup requests to nsqlookupd in old and new, 
migrate response and return to nsq client, according to migrate status read from memory.

Migrate status defines as follow:
<pre>const (
     	M_OFF = iota    //0. migrate not start
     	M_CSR           //1. return old&new lookup response for consumer, and old lookup response for producer
     	M_CSR_PDR       //2. return old&nsq lookup response for consumer, and new lookup response for producer
     	M_FIN           //3. return new lookup response for consumer&producer
     )</pre>
     
> Note: migrate from youzan nsq to another does not support, since partition info in two youzan nsq cold not mix.

> As to implementation of topic switches, current version shipped with topic switches in memory, switches could be updated via
POST '$host:$port/switch' with body {"$topic":[0-3]}, and default switches for all topic is 0(M_OFF) 

##Deploy
start with config file:
<pre>nsqlookupd_migrate -config=conf/nsqlookup_migrate.conf</pre>

##Config
properties defined in nsqlookup migrate:
<pre>
origin-lookupd-http = "http://origin.nsqlookupd:4161"
target-lookupd-http = "http://target.nsqlookupd:4161"
env = "qa"
log-level = 2
log-dir = "/data/logs/nsqlookup_migrate"
migrate-key = "origin.to.target"
</pre>