#!/usr/bin/env bash


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
    tcp_address="0.0.0.0:4160"
fi

## <addr>:<port> to listen on for HTTP clients
## the port 4161 be used by tengine for proxy
http_address=$5
if [ -z $http_address ]; then
    http_address="0.0.0.0:4161"
fi
## rpc port used for cluster communication
rpc_port=$6
if [ -z $rpc_port ]; then
    rpc_port="4260"
fi

broadcast_interface=$7
if [ -z $broadcast_interface ]; then
    broadcast_interface="eth0"
fi



## enable verbose logging
verbose=false



## duration of time a producer will remain in the active list since its last ping
inactive_producer_timeout="300s"

## duration of time a producer will remain tombstoned if registration remains
tombstone_lifetime="45m"

log_level=2

# 为了兼容老版本nsqd的管理, 心跳超时需要大于30s
nsqd_ping_timeout="35s"

./nsqlookupd -cluster-leadership-addresses=$cluster_leadership_addresses -cluster-id=$cluster_id -log-dir=$log_dir -tcp-address=$tcp_address -http-address=$http_address -log-level=$log_level -rpc-port=$rpc_port -verbose=$verbose -broadcast-interface=$broadcast_interface -inactive-producer-timeout=$inactive_producer_timeout -tombstone-lifetime=$tombstone_lifetime -nsqd-ping-timeout=$nsqd_ping_timeout

