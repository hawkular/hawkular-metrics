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

#
# Send an availability data point to an Hawkukar Metrics server.
#
# See 'usage' function for basic understanding.
#
# Note that the value parameter expects integers. This makes it easy to use this script to process the exit code of
# a command or another script. For example:
# ===
# $> check_jvm_is_up
# $> CODE=$?
# $> send_availability "http://localhost:8080/hawkular/metrics" "my_tenant" "hostname.jvm" "$CODE"
# ===
#

#set -x

function usage {
  echo "Usage: send_availability url tenant metric <value>
  url: the Hawkular Metrics server URL, i.e. 'http://localhost:8080/hawkular/metrics'
  tenant: the id of the tenant in Hawkular Metrics
  metric: the id of the metric the data point belongs to
  value: if present, must be a number; 0 means 'up', any other value 'down'; absent means 'unknown'
"
}

function error {
  echo "ERROR: $1" 1>&2
}

MIN_ARGUMENTS=3
MAX_ARGUMENTS=$((MIN_ARGUMENTS + 1))

if [ $# -lt ${MIN_ARGUMENTS} -o $# -gt ${MAX_ARGUMENTS} ]; then
  error "Wrong number of arguments"
  usage
  exit 1
fi

if [ $# -eq ${MIN_ARGUMENTS} ]; then
  AVAIL="unknown"
else
  VALUE_ARG=$4
  if [ "$VALUE_ARG" -eq "$VALUE_ARG" ] 2>/dev/null; then
    if [ ${VALUE_ARG} -eq 0 ]; then
      AVAIL="up"
    else
      AVAIL="down"
    fi
  else
    error "Value argument must be an integer."
    usage
    exit 1
  fi
fi

HAWKULAR_METRICS_URL="$1/availability/data"
TENANT_ID=$2
METRIC_ID=$3

TIMESTAMP=`date +%s%3N`
read -r -d '' JSON <<- EOV
    [{"id":"${METRIC_ID}","data":[{"timestamp":${TIMESTAMP},"value":"${AVAIL}"}]}]
EOV

RESPONSE=$(curl -i --silent -H "Hawkular-Tenant: $TENANT_ID" -H "Content-Type: application/json" \
  ${HAWKULAR_METRICS_URL} --data ${JSON})
STATUS_LINE=$(echo ${RESPONSE} | head -n 1)
STATUS=$(echo ${STATUS_LINE} | awk '{print $2}')

if [ ${STATUS} -ne 200 ]; then
  error "Failed to persist availability data point: $RESPONSE"
  exit 1
fi

exit 0
