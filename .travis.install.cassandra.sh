#!/bin/bash

set -xe

sudo rm -rf /var/lib/cassandra/*

wget http://downloads.datastax.com/community/dsc-cassandra-2.1.1-bin.tar.gz

tar -xzf dsc-cassandra-2.1.1-bin.tar.gz

sudo sh dsc-cassandra-2.1.1/bin/cassandra -p ${HOME}/cassandra.pid

