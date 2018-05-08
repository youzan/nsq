#!/bin/bash
set -e

# a helper script to run tests

if ! which nsqd >/dev/null; then
    echo "missing nsqd binary" && exit 1
fi

if ! which nsqlookupd >/dev/null; then
    echo "missing nsqlookupd binary" && exit 1
fi

# run nsqlookupd
#LOOKUP_LOGFILE=$(mktemp -t nsqlookupd.XXXXXXX)
LOOKUP_LOGFILE="./lookup.log"
echo "starting nsqlookupd"
echo "  logging to $LOOKUP_LOGFILE"
nsqlookupd --alsologtostderr=true --log-level=3 >$LOOKUP_LOGFILE 2>&1 &
LOOKUPD_PID=$!
sleep 10

DATAPATH=$(mktemp -d -t nsqXXXXXX)
echo $DATAPATH

# run nsqd configured to use our lookupd above
rm -f *.dat
NSQD_LOGFILE="./nsqd.log"
echo "starting nsqd --data-path=$DATAPATH --lookupd-tcp-address=127.0.0.1:4160 --tls-cert=./test/server.pem --tls-key=./test/server.key --tls-root-ca-file=./test/ca.pem"
echo "  logging to $NSQD_LOGFILE"
nsqd --sync-timeout=100ms --broadcast-address=127.0.0.1 --alsologtostderr=true --log-level=3 --data-path=$DATAPATH --lookupd-tcp-address=127.0.0.1:4160 --tls-cert=./test/server.pem --tls-key=./test/server.key --tls-root-ca-file=./test/ca.pem >$NSQD_LOGFILE 2>&1 &
NSQD_PID=$!

sleep 10
tail -f $NSQD_LOGFILE &

cleanup() {
    echo "killing nsqd PID $NSQD_PID"
    kill -s TERM $NSQD_PID || cat $NSQD_LOGFILE
    echo "killing nsqlookupd PID $LOOKUPD_PID"
    kill -s TERM $LOOKUPD_PID || cat $LOOKUP_LOGFILE
}
trap cleanup INT TERM EXIT

go test  -timeout 250s
