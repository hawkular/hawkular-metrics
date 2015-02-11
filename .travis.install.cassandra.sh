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


set -xe
cVersion="2.1.1"

cd "${HOME}"

wget http://downloads.datastax.com/community/dsc-cassandra-${cVersion}-bin.tar.gz
tar -xzf dsc-cassandra-${cVersion}-bin.tar.gz
cHome="${HOME}/dsc-cassandra-${cVersion}"

mkdir "${cHome}/logs"

cat /proc/sys/kernel/core_pattern

ulimit -c unlimited -S
ulimit -c

export HEAP_NEWSIZE="100M"
export MAX_HEAP_SIZE="1G"

nohup sh ${cHome}/bin/cassandra -f -p ${HOME}/cassandra.pid > ${cHome}/logs/stdout.log 2>&1 &

