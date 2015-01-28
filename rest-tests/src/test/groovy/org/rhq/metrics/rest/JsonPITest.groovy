/**
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
package org.rhq.metrics.rest

import groovy.json.JsonSlurper
import groovy.util.slurpersupport.NodeChild
import groovyx.net.http.ContentType
import groovyx.net.http.HTTPBuilder
import org.junit.Test

import java.time.Instant

import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertNotNull
import static org.junit.Assert.assertTrue

/**
 * @author Thomas Segismont
 */
class JsonPITest {

  def baseURI = System.getProperty("rhq-metrics.base-uri") ?: "127.0.0.1:8080/rhq-metrics"
  def http = new HTTPBuilder("http://${baseURI}/")
  def callback = "marseille"

  @Test
  void jsonPShouldNotApplyToXml() {
    http.defaultRequestHeaders.Authorization = "Basic YWdlbnQ6NTczYjEwOTgtMTE1MC00OWYwLTliMTItMGZlMmFiNjYxZDEy"
    http.get(path: "ping", contentType: ContentType.XML, query: [jsonp: callback]) { resp, content ->
      assertEquals 200, resp.status
      assertEquals "application/xml", resp.contentType

      // Now check XML content
      NodeChild nodeChild = content;
      assertEquals "value", nodeChild.name()

      def value = nodeChild.attributes()."value"
      assertNotNull value

      checkPingDate value
    }
  }

  @Test
  void jsonPShouldNotApplyToJson() {
    http.defaultRequestHeaders.Authorization = "Basic YWdlbnQ6NTczYjEwOTgtMTE1MC00OWYwLTliMTItMGZlMmFiNjYxZDEy"
    http.get(path: "ping", contentType: ContentType.JSON, query: [jsonp: callback]) { resp, content ->
      assertEquals 200, resp.status
      assertEquals "application/json", resp.contentType

      // Now check JSON content
      def value = content."value"
      assertNotNull value

      checkPingDate value
    }
  }

  @Test
  void jsonPShouldApplyToJavascript() {
    http.defaultRequestHeaders.Authorization = "Basic YWdlbnQ6NTczYjEwOTgtMTE1MC00OWYwLTliMTItMGZlMmFiNjYxZDEy"
    // Do not try to parse Javascript as JSON
    http.parser."application/javascript" = http.parser."text/plain"

    http.get(path: "ping", contentType: "application/javascript", query: [jsonp: callback]) { resp, reader ->
      assertEquals 200, resp.status
      assertEquals "application/javascript", resp.contentType

      // Now check JSONP content
      def textValue = new StringWriter()
      textValue << reader
      textValue = textValue.toString()
      assertTrue textValue.startsWith("${callback}(")
      assertTrue textValue.endsWith(");")

      def jsonValue = textValue.substring "${callback}(".length(), textValue.length() - ");".length()
      def value = new JsonSlurper().parseText(jsonValue)."value";

      checkPingDate value
    }
  }

  static void checkPingDate(value) {
    Instant pingDate = Date.parseToStringDate(value).toInstant()
    Instant now = Instant.now();
    assertTrue now.isAfter(pingDate)
  }
}
