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

# The status endpoint to test
STATUS_URL=http://localhost:$HAWKULAR_METRICS_ENDPOINT_PORT/hawkular/metrics/status

# The command to get the HTTP status code
STATUS_COMMAND='curl -L -s -o /dev/null -w "%{http_code}" $STATUS_URL'

STATUS_CODE=`eval $STATUS_COMMAND`
CURL_STATUS=$?
# Exit the loop when the status code is 200
while : ; do

  # if the page loaded, or its not yet available, or its temporarily unavailable then continue
  if [[ $CURL_STATUS -eq 7 || $STATUS_CODE -eq 200 || $STATUS_CODE -eq 404 || $STATUS_CODE -eq 503 ]]; then
    # if we receive a 200 status code, then read the state of the service
    if [ $STATUS_CODE -eq 200 ]; then
      STATUS_JSON=`curl -L -s -X GET $STATUS_URL`
      # if the status is STARTED, then its up and running
      if [ $STATUS_JSON = '{"MetricsService":"STARTED"}' ]; then
        exit 0
      # if the status is not STARTED or STARTING, then its either in error or stopped. Return error exit code
      elif [ $STATUS_JSON != '{"MetricsService":"STARTING"}' ]; then
        exit 1
      fi
    fi
    sleep 1;
    STATUS_CODE=`eval $STATUS_COMMAND`
    CURL_STATUS=$?
  else
    # we received some sort of error response, return error exit code
    exit 1;
  fi
done
