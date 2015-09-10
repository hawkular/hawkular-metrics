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
import time

hawkularEndpointPort = os.environ.get("HAWKULAR_METRICS_ENDPOINT_PORT")

statusURL = "http://localhost:" + hawkularEndpointPort  + "/hawkular/metrics/status"

while True:
  try:
    response = urllib2.urlopen(statusURL)
    statusCode = response.getcode();
    if (statusCode == 200 or statusCode == 404 or statusCode == 503):
      if (statusCode == 200):
        jsonResponse = json.loads(response.read())
        if (jsonResponse["MetricsService"] == "STARTED"):
          exit(0)
        # If the status is not STARTED or STARTING then exit, something went wrong
        elif (jsonResponse["MetricsService"] != "STARTING"):
          exit(1)
      
    else:
      exit(1)

  except Exception:
    print "An Exception occured trying to connect to the endpoint."
 
  #sleep for 1 second and let the loop try over again  
  time.sleep(1)

