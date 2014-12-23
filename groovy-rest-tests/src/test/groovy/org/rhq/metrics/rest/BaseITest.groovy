/*
 * Copyright 2014 Red Hat, Inc.
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

package org.rhq.metrics.rest

import groovy.json.JsonSlurper
import groovyx.net.http.HTTPBuilder
import org.junit.Test

import java.text.DateFormat
import java.text.SimpleDateFormat

import static org.joda.time.DateTime.now
import static org.joda.time.Seconds.seconds
import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertTrue

class BaseITest extends RESTTest {

  @Test
  void pingTest() {
    def response = rhqm.post(path: 'ping')
    assertEquals(200, response.status)

    DateFormat df = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);
    Date date = df.parse(response.data.value);
    Date now = new Date();

    long timeDifference = now.time - date.time;

    // This is a bit problematic and depends on the system where the test is executing
    assertTrue("Difference is " + timeDifference, timeDifference < seconds(60).toStandardDuration().millis);
  }

  @Test
  void pingTestWithJsonP() throws Exception {
    def client = new HTTPBuilder("http://$baseURI/")
    client.parser.'application/javascript' = client.parser.'text/plain'
    client.get(path: 'ping', query: [jsonp: 'jsonp'], {
      response, content ->
        assertEquals 200, response.status
        assertEquals "application/javascript;charset=utf-8", response.headers['Content-Type'].value

        String jsonpText = content.text
        assertTrue jsonpText.startsWith("jsonp(")
        assertTrue jsonpText.endsWith(");")

        def json = new JsonSlurper().parseText(jsonpText.substring(6, jsonpText.length() - 2))

        DateFormat df = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);
        Date date = df.parse(json.value);
        Date now = new Date();

        long timeDifference = now.time - date.time;

        // This is a bit problematic and depends on the system where the test is executing
        assertTrue "Difference is " + timeDifference, timeDifference < seconds(60).toStandardDuration().millis
    })
  }

  @Test
  void addAndGetValue() {
    def end = now().millis
    def start = end - 100
    def response = rhqm.post(path: "metrics/foo", body: [id: 'foo', timestamp: start + 10, value: 42])
    assertEquals(200, response.status)

    response = rhqm.get(path: 'metrics/foo', query: [start: start, end: end])
    assertEquals(200, response.status)
    assertEquals(start + 10, response.data[0].timestamp)
  }
}
