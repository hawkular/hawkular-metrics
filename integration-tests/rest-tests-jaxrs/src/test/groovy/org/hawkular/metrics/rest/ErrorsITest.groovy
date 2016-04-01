/*
 * Copyright 2014-2016 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.rest

import static org.junit.Assert.assertEquals
import static org.junit.Assert.fail

import static groovyx.net.http.ContentType.JSON
import static groovyx.net.http.ContentType.TEXT
import static groovyx.net.http.Method.GET
import static groovyx.net.http.Method.POST

import org.junit.Test

/**
 * @author Jeeva Kandasamy
 * @author Thomas Segismont
 */
class ErrorsITest extends RESTTest {
  def tenantId = nextTenantId()

  @Test
  void testNotAllowedException() {
    badPost(path: "gauges/test/tags", headers: [(tenantHeaderName): tenantId]) { exception ->
      assertEquals(405, exception.response.status)
    }
  }

  @Test
  void testNotFoundException() {
    badGet(path: "gaugesssss/test/raw", headers: [(tenantHeaderName): tenantId]) { exception ->
      assertEquals(404, exception.response.status)
    }
  }

  @Test
  void testNumberFormatException() {
    badGet(path: "gauges/test/stats", headers: [(tenantHeaderName): tenantId],
        query: [buckets: 999999999999999999999999]) { exception ->
      assertEquals(400, exception.response.status)
    }
  }

  @Test
  void testNotAcceptableException() {
    hawkularMetrics.request(GET) { request ->
      uri.path = "gauges/test/raw"
      headers = [(tenantHeaderName): tenantId, Accept: TEXT]

      response.success = { response ->
        fail("Expected error response, got ${response.statusLine}")
      }

      response.failure = { response ->
        assertEquals(406, response.status)
      }
    }
  }

  @Test
  void testNotSupportedException() {
    hawkularMetrics.request(POST) { request ->
      uri.path = "gauges/test/raw"
      body = ""
      requestContentType = TEXT
      headers = [(tenantHeaderName): tenantId, Accept: JSON]

      response.success = { response ->
        fail("Expected error response, got ${response.statusLine}")
      }

      response.failure = { response ->
        assertEquals(415, response.status)
      }
    }
  }
}