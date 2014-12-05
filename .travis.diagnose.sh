#!/bin/bash

set -x

nc -z -w5 127.0.0.1 9042; echo $?

CASSANDRA_PID=`cat ${HOME}/cassandra.pid`

ps aux $CASSANDRA_PID

ps aux | grep java

cat `dsc-cassandra-2.1.1/bin/nodetool status`

cat dsc-cassandra-2.1.1/logs/*

