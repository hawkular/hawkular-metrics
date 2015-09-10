#!/bin/python
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

import os
import json
import urllib2

hawkularEndpointPort = os.environ.get("HAWKULAR_METRICS_ENDPOINT_PORT")
statusURL = "http://localhost:" + hawkularEndpointPort  + "/hawkular/metrics/status"
response = urllib2.urlopen(statusURL)

statusCode = response.getcode();

# if the status is 200, then continue
if (statusCode == 200):
  responseHTML = response.read() 
  jsonResponse = json.loads(responseHTML)
  # if the metrics service is started then we are good
  if (jsonResponse["MetricsService"] == "STARTED"):
    exit(0)

# conditions were not passed, exit with an error code
exit(1)
