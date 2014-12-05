#!/bin/bash

set -x

nc -z -w5 127.0.0.1 9042; echo $?

CASSANDRA_PID=`cat ${HOME}/cassandra.pid`

ps -f -p $CASSANDRA_PID && cat `dsc-cassandra-2.1.1/bin/nodetool status`

ps aux | grep java

cat dsc-cassandra-2.1.1/logs/stderr.log

cat dsc-cassandra-2.1.1/logs/stdout.log

cat dsc-cassandra-2.1.1/logs/system.log

