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

for args in "$@"
do
  case $args in
    --seeds=*)
      SEEDS="${args#*=}"
    ;;
    --cluster_name=*)
      CLUSTER_NAME="${args#*=}"
    ;;
    --data_volume=*)
      DATA_VOLUME="${args#*=}"
    ;;
    --seed_provider_classname=*)
      SEED_PROVIDER_CLASSNAME="${args#*=}"
    ;;
    --help)
      HELP=true
    ;;
  esac
done

if [ -n "$HELP" ]; then
  echo
  echo Starts up a Cassandra Docker image
  echo
  echo Usage: [OPTIONS]...
  echo
  echo Options:
  echo "  --seeds=SEEDS"
  echo "        comma separated list of hosts to use as a seed list"
  echo "        default: \$HOSTNAME"
  echo
  echo "  --cluster_name=NAME"
  echo "        the name to use for the cluster"
  echo "        default: test_cluster"
  echo
  echo "  --data_volume=VOLUME_PATH"
  echo "        the path to where the data volume should be located"
  echo "        default: \$CASSANDRA_HOME/data"
  echo
  echo "  --seed_provider_classname"
  echo "        the classname to use as the seed provider"
  echo "        default: org.apache.cassandra.locator.SimpleSeedProdiver"
  echo
  exit 0
fi


# set the hostname in the cassandra configuration file
sed -i 's/${HOSTNAME}/'$HOSTNAME'/g' /opt/apache-cassandra/conf/cassandra.yaml

# if the seed list is not set, set it to the hostname
if [ -n "$SEEDS" ]; then
  sed -i 's/${SEEDS}/'$SEEDS'/g' /opt/apache-cassandra/conf/cassandra.yaml
else
  sed -i 's/${SEEDS}/'$HOSTNAME'/g' /opt/apache-cassandra/conf/cassandra.yaml
fi

# set the cluster name if set, default to "test_cluster" if not set
if [ -n "$CLUSTER_NAME" ]; then
    sed -i 's/${CLUSTER_NAME}/'$CLUSTER_NAME'/g' /opt/apache-cassandra/conf/cassandra.yaml
else
    sed -i 's/${CLUSTER_NAME}/test_cluster/g' /opt/apache-cassandra/conf/cassandra.yaml
fi

# set the data volume if set, otherwise use the CASSANDRA_HOME location, otherwise default to '/cassandra_data'
if [ -n "$DATA_VOLUME" ]; then
    sed -i 's#${DATA_VOLUME}#'$DATA_VOLUME'#g' /opt/apache-cassandra/conf/cassandra.yaml
elif [ -n "$CASSANDRA_HOME" ]; then
    sed -i 's#${DATA_VOLUME}#'$CASSANDRA_HOME'/data#g' /opt/apache-cassandra/conf/cassandra.yaml
else
    sed -i 's#${DATA_VOLUME}#/cassandra_data#g' /opt/apache-cassandra/conf/cassandra.yaml
fi

# set the seed provider class name, otherwise default to the SimpleSeedProvider
if [ -n "$SEED_PROVIDER_CLASSNAME" ]; then
    sed -i 's#${SEED_PROVIDER_CLASSNAME}#'$SEED_PROVIDER_CLASSNAME'#g' /opt/apache-cassandra/conf/cassandra.yaml
else
    sed -i 's#${SEED_PROVIDER_CLASSNAME}#org.apache.cassandra.locator.SimpleSeedProvider#g' /opt/apache-cassandra/conf/cassandra.yaml
fi

if [ -n "$CASSANDRA_HOME" ]; then
  exec ${CASSANDRA_HOME}/bin/cassandra -f
else
  exec /opt/apache-cassandra/bin/cassandra -f
fi
