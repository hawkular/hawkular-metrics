#!/bin/bash
#
# Copyright 2014-2015 Red Hat, Inc. and/or its affiliates
# and other contributors as indicated by the @author tags.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


set -x

nc -z -w5 127.0.0.1 9042; echo $?

CASSANDRA_PID=`cat ${HOME}/cassandra.pid`

ps -f -p $CASSANDRA_PID && cat `dsc-cassandra-2.1.1/bin/nodetool status`

ps aux | grep java

find -name *.hprof; echo $?

find -name 'core*'; echo $?

echo "##########################################################"
cat dsc-cassandra-2.1.1/logs/stdout.log
echo "##########################################################"

echo "##########################################################"
cat dsc-cassandra-2.1.1/logs/system.log
echo "##########################################################"

