#!/bin/bash
set -e
echo "" > coverage.txt

if [ "$TEST_RACE" = "false" ]; then
    GOMAXPROCS=1 go test -timeout 1900s `go list ./... | grep -v consistence | grep -v nsqadmin`
else
    for d in $(go list ./... | grep -v consistence | grep -v nsqadmin); do
        GOMAXPROCS=4 go test -timeout 1900s -race -coverprofile=profile.out -covermode=atomic $d
        if [ -f profile.out ]; then
            cat profile.out >> coverage.txt
            rm profile.out
        fi
    done
fi

# no tests, but a build is something
for dir in $(find apps bench -maxdepth 1 -type d) nsqadmin; do
    if grep -q '^package main$' $dir/*.go 2>/dev/null; then
        echo "building $dir"
        go build -o $dir/$(basename $dir) ./$dir
    else
        echo "(skipped $dir)"
    fi
done
