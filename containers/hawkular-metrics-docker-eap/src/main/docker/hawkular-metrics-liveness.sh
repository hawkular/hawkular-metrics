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

STATUS_URL=http://localhost:$HAWKULAR_METRICS_ENDPOINT_PORT/hawkular/metrics/status
STATUS_CODE=`curl -L -s -o /dev/null -w "%{http_code}" $STATUS_URL`

if [ $STATUS_CODE -eq 200 ]; then
  STATUS_JSON=`curl -L -s -X GET $STATUS_URL`
  # if the status is STARTED, then its up and running
  if [ $STATUS_JSON = '{"MetricsService":"STARTED"}' ]; then
        exit 0
  fi
fi

exit 1 # conditions were not passed, exit with an error code
