#!/bin/bash

set -xe

sudo rm -rf /var/lib/cassandra/*

wget http://downloads.datastax.com/community/dsc-cassandra-2.1.1-bin.tar.gz

tar -xzf dsc-cassandra-2.1.1-bin.tar.gz

mkdir dsc-cassandra-2.1.1/logs

sudo ulimit -c

sudo bash -c 'ulimit -c unlimited; HEAP_NEWSIZE="100M";MAX_HEAP_SIZE="1G"; sh dsc-cassandra-2.1.1/bin/cassandra -f -p ${HOME}/cassandra.pid > dsc-cassandra-2.1.1/logs/stdout.log 2>&1'

