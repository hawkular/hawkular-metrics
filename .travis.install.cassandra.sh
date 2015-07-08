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

cd "${HOME}"

wget -O datastax-releases "http://downloads.datastax.com/community/?C=M;O=D"
LATEST_MICRO=`cat datastax-releases | grep -m 1 -o -E "dsc-cassandra-2\.1\.[0-9]+-bin\.tar.gz" | head -1 | cut -d '.' -f3 | cut -d '-' -f1`

CASSANDRA_VERSION="2.1.${LATEST_MICRO}"
CASSANDRA_BINARY="dsc-cassandra-${CASSANDRA_VERSION}-bin.tar.gz"

wget http://downloads.datastax.com/community/${CASSANDRA_BINARY}
tar -xzf ${CASSANDRA_BINARY}

CASSANDRA_HOME="${HOME}/dsc-cassandra-${CASSANDRA_VERSION}"

mkdir "${CASSANDRA_HOME}/logs"

export HEAP_NEWSIZE="100M"
export MAX_HEAP_SIZE="1G"

nohup sh ${CASSANDRA_HOME}/bin/cassandra -f -p ${HOME}/cassandra.pid > ${CASSANDRA_HOME}/logs/stdout.log 2>&1 &
