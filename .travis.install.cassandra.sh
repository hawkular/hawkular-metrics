#!/bin/bash

set -xe

wget http://downloads.datastax.com/community/dsc-cassandra-2.1.1-bin.tar.gz

tar -xzf dsc-cassandra-2.1.1-bin.tar.gz

mkdir dsc-cassandra-2.1.1/logs

cat /proc/sys/kernel/core_pattern

ulimit -c unlimited -S
ulimit -c

export HEAP_NEWSIZE="100M"
export MAX_HEAP_SIZE="1G"

nohup sh dsc-cassandra-2.1.1/bin/cassandra -f -p ${HOME}/cassandra.pid > dsc-cassandra-2.1.1/logs/stdout.log 2>&1 &

