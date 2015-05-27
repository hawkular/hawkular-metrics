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

# The ping endpoint to test
PING_URL=http://$HOSTNAME:$HAWKULAR_METRICS_ENDPOINT_PORT/hawkular/metrics/ping

# The command to get the HTTP status code
STATUS_COMMAND='curl -H "Hawkular-Tenant: status" -L -s -o /dev/null -w "%{http_code}" $PING_URL'

STATUS_CODE=`eval $STATUS_COMMAND`

echo $STATUS_CODE

# Exit the loop when the status code is 200
until [ $STATUS_CODE -eq 200 ]; do
  # If the status code is a 404 or a 503 then try again
  if [[ $STATUS_CODE -eq 404 || $STATUS_CODE -eq 503 ]]; then
    sleep 1;
    STATUS_CODE=`eval $STATUS_COMMAND`
    echo $STATUS_CODE
  else
    exit 1;
  fi
done
