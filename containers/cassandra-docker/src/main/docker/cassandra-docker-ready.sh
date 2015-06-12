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

# Get the machines IP address from hosts
# TODO: find a better way to retrieve the IP address (without
# relying on the normal tools which are not installed in docker images by default)
HOSTIP=`cat /etc/hosts | grep $HOSTNAME | awk '{print $1}'`

while : ;do

  # Get the status of this machine from the Cassandra nodetool
  STATUS=`nodetool status | grep $HOSTIP | awk '{print $1}'`

  # Once the status is Up and Normal, then we are ready
  if [ $STATUS = "UN" ]; then
    exit 0
  fi

  sleep 1;

done
