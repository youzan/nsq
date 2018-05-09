#!/bin/bash

# http://blog.scphillips.com/posts/2013/07/getting-a-python-script-to-run-in-the-background-as-a-service-on-boot/
# https://stackoverflow.com/questions/8251933/how-can-i-log-the-stdout-of-a-process-started-by-start-stop-daemon

MAINDIR=$PWD/..
BLDDIR=$MAINDIR/build

status_nsq() {
    start-stop-daemon --status --pidfile /tmp/$1.pid
    print_status $1 $?
}

print_status() {
    case $2 in
	0)
	    echo "$1: Program is running."
	    ;;
        1)
	    echo "$1: Program is not running and the pid file exists."
	    ;;
	3)
	    echo "$1: Program is not running."
	    ;;
	4)
	    echo "$1: Program is not running."
	    ;;
	*)
	    echo "$1: Program status in unknown."
	    ;;
    esac
}

do_status() {
    status_nsq nsqlookupd
    status_nsq nsqd
    status_nsq nsqadmin
}

do_start () {
    do_start_nsqlookupd
    do_start_nsqd
    do_start_nsqadmin

    sleep 1
    echo "--------------------"
    do_status
}

do_stop () {
    do_stop_nsqadmin
    do_stop_nsqd
    do_stop_nsqlookupd

    echo "--------------------"
    do_status
}


# Refers to https://github.com/coreos/etcd/blob/master/Documentation/op-guide/clustering.md
# All etcd related options are prefixed with "etcd.".
# Etcd stdout and stderr is redirected to <log-dir>/<etcd.name>-etcd.log.
do_start_nsqlookupd () {
    echo "staring nsqlookupd"
    start-stop-daemon --start --oknodo --background --no-close --make-pidfile --pidfile /tmp/nsqlookupd.pid --startas $BLDDIR/nsqlookupd -- -config=$MAINDIR/contrib/nsqlookupd.cfg \
       --etcd.name nsqlookupd0 \
       --etcd.data-dir /opt/nsqlooklookupd0 \
       --etcd.initial-advertise-peer-urls http://127.0.0.1:2380 \
       --etcd.listen-peer-urls http://127.0.0.1:2380 \
       --etcd.listen-client-urls http://127.0.0.1:2379 \
       --etcd.advertise-client-urls http://127.0.0.1:2379 \
       --etcd.initial-cluster-token etcd-cluster-1 \
       --etcd.initial-cluster nsqlookupd0=http://127.0.0.1:2380 \
       --etcd.initial-cluster-state new \
       --etcd.debug true \
       > /tmp/nsqlookupd0_stdout.log 2>&1
    sleep 1
    echo "started nsqlookupd"    
}

do_start_nsqd () {
    echo "staring nsqd"
    start-stop-daemon --start --oknodo --background --make-pidfile --pidfile /tmp/nsqd.pid --startas $BLDDIR/nsqd -- -config=$MAINDIR/contrib/nsqd.cfg
    sleep 1
    echo "started nsqd"    
}

do_start_nsqadmin () {
    echo "staring nsqadmin"
    start-stop-daemon --start --oknodo --background --make-pidfile --pidfile /tmp/nsqadmin.pid --startas $BLDDIR/nsqadmin -- -config=$MAINDIR/contrib/nsqadmin.cfg
    echo "started nsqadmin"    
}

stop_nsq() {
    start-stop-daemon --stop --oknodo --quiet --retry TERM/5/forever/KILL/1 --remove-pidfile --pidfile /tmp/$1.pid
}

do_stop_nsqlookupd () {
    stop_nsq nsqlookupd
}

do_stop_nsqd () {
    stop_nsq nsqd
}

do_stop_nsqadmin () {
    stop_nsq nsqadmin
}

do_clear () {
    echo "clearing date and log......"
    make clean
}

do_build () {
    make
}

case "$1" in
    start|stop|status|clear|start_nsqlookup|stop_nsqlookup|start_nsqd|stop_nsqd|start_nsqadmin|stop_nsqadmin|build)
        do_${1}
        ;;
    restart|reload|force-reload)
        do_stop && do_start
        ;;
    update)
        do_stop && do_clear && do_build && do_start
        ;;
    *)
        echo "Usage: cluster.sh {start|stop|status|clear|start_nsqlookup|stop_nsqlookup|start_nsqd|stop_nsqd|start_nsqadmin|stop_nsqadmin|build|update}"
        exit 1
        ;;
esac

exit 0
