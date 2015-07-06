/*
 * Copyright 2014-2015 Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
var httpRequest;

if (window.XMLHttpRequest) {
  httpRequest = new XMLHttpRequest();
} else if (window.ActiveXObject) {
  try {
    httpRequest = new ActiveXObject("Msxml2.XMLHTTP");
  } catch (e) {
    httpRequest = new ActiveXObject("Microsoft.XMLHTTP");
  }
}

httpRequest.onreadystatechange = updateStatus;
httpRequest.open("GET", "/hawkular/metrics/status");
httpRequest.send();

function updateStatus() {
 if (httpRequest.readyState === 4) {
   if (httpRequest.status === 200) {
     statusJson = JSON.parse(httpRequest.responseText);
     document.getElementById("status").innerHTML = "Metrics Service :" + statusJson.MetricsService;
   } else if (httpRequest.status === 404 || httpRequest.status === 503) {
     document.getElementById("status").innerHTML = "The server is not available";
   } else {
     document.getElementById("status").innerHTML = "An error occured while accessing the server :" + httpRequest.responseText;
   }
 }
}
