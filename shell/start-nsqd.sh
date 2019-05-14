#!/usr/bin/env bash
## unique identifier (int) for this worker (will default to a hash of hostname)
# worker_id = 41000

# the etcd cluster ip list
cluster_leadership_addresses=$1
if [ -z $cluster_leadership_addresses ]; then
    echo "cluster_leadership_addresses should be given" 1>&2
    exit 1
fi
# cluster id to separate different cluster
cluster_id=$2
if [ -z $cluster_id ]; then
   echo "cluster_id should be given" 1>&2
   exit 1
fi

# the dir of log
log_dir=$3
if [ -z $log_dir ]; then
    log_dir="/data/logs/nsqlookupd"
fi

## <addr>:<port> to listen on for TCP clients
tcp_address=$4
if [ -z $tcp_address ]; then
    tcp_address="0.0.0.0:4150"
fi

## <addr>:<port> to listen on for HTTP clients
## the port 4161 be used by tengine for proxy
http_address=$5
if [ -z $http_address ]; then
    http_address="0.0.0.0:4151"
fi
## rpc port used for cluster communication
rpc_port=$6
if [ -z $rpc_port ]; then
    rpc_port="4250"
fi
## address that will be registered with lookupd (defaults to the OS hostname)
#broadcast_address = ""
broadcast_interface=$7
if [ -z $broadcast_interface ]; then
    broadcast_interface="eth0"
fi


## keep alive heartbeat interval between the nsqd node with the nsqlookupd
lookup_ping_interval="5s"

## path to store disk-backed messages
data_path="/data/nsqd"

#remote_tracer = "flume-qa.s.qima-inc.com:5140"
## default retention days to keep the topic consumed data
retention_days=1
## retention size bytes for one day at most
retention_size_per_day=100000000

## number of messages to keep in memory (per topic/channel)
mem_queue_size=0

## number of bytes per diskqueue file before rolling
max_bytes_per_file=12428800

## number of messages per diskqueue fsync
sync_every=2500

## duration of time per diskqueue fsync (time.Duration)
sync_timeout="1s"


## duration to wait before auto-requeing a message
msg_timeout="60s"

## maximum duration before a message will timeout
max_msg_timeout="15m"

## maximum size of a single message in bytes
max_msg_size=1024768

## maximum requeuing timeout for a message
max_req_timeout="48h"

## maximum finished count with unordered
max_confirm_win=1000

## maximum size of a single command body
max_body_size=5123840


## maximum client configurable duration of time between client heartbeats
max_heartbeat_interval="60s"

## maximum RDY count for a client
max_rdy_count=2500

## maximum client configurable size (in bytes) for a client output buffer
max_output_buffer_size=65536

## maximum client configurable duration of time between flushing to a client (time.Duration)
max_output_buffer_timeout="1s"



## path to certificate file
tls_cert=""

## path to private key file
tls_key=""

## require client TLS upgrades
tls_required=false

## minimum TLS version ("ssl3.0", "tls1.0," "tls1.1", "tls1.2")
tls_min_version=""

## enable deflate feature negotiation (client compression)
deflate=true

## max deflate compression level a client can negotiate (> values == > nsqd CPU usage)
max_deflate_level=6

## enable snappy feature negotiation (client compression)
snappy=true

## bigger level means more details
log_level=2


req_to_end_threshold="10m"

allow_ext_compatible=true
allow_sub_ext_compatible=true

queue_scan_interval="100ms"
queue_scan_selection_count=100
queue_scan_worker_pool_max=8
queue_scan_dirty_percent=0.1

start_as_fix_mode=true

allow_zan_test_skip=true
default_commit_buf=100
max_commit_buf=400

max_channel_delayed_qnum=8000

./nsqd -cluster-leadership-addresses=$cluster_leadership_addresses -cluster-id=$cluster_id -log-dir=$log_dir -tcp-address=$tcp_address -http-address=$http_address -rpc-port=$rpc_port -broadcast-interface=$broadcast_interface -log-level=$log_level -lookup-ping-interval=$lookup_ping_interval -data-path=$data_path -retention-days=$retention_days -retention-size-per-day=$retention_size_per_day -mem-queue-size=$mem_queue_size -max-bytes-per-file=$max_bytes_per_file -sync-every=$sync_every -sync-timeout=$sync_timeout -msg-timeout=$msg_timeout -max-msg-timeout=$max_msg_timeout -max-msg-size=$max_msg_size -max-req-timeout=$max_req_timeout -max-confirm-win=$max_confirm_win -max-body-size=$max_body_size -max-heartbeat-interval=$max_heartbeat_interval -max-rdy-count=$max_rdy_count -max-output-buffer-size=$max_output_buffer_size -max-output-buffer-timeout=$max_output_buffer_timeout -tls-cert=$tls_cert -tls-key=$tls_key -tls-required=$tls_required -tls-min-version=$tls_min_version -deflate=$deflate -max-deflate-level=$max_deflate_level -snappy=$snappy -req-to-end-threshold=$req_to_end_threshold -allow-ext-compatible=$allow_ext_compatible -allow-sub-ext-compatible=$allow_sub_ext_compatible -queue-scan-interval=$queue_scan_interval -queue-scan-selection-count=$queue_scan_selection_count -queue-scan-worker-pool-max=$queue_scan_worker_pool_max -queue-scan-dirty-percent=$queue_scan_dirty_percent -start-as-fix-mode=$start_as_fix_mode -allow-zan-test-skip=$allow_zan_test_skip -default-commit-buf=$default_commit_buf -max-commit-buf=$max_commit_buf -max-channel-delayed-qnum=$max_channel_delayed_qnum