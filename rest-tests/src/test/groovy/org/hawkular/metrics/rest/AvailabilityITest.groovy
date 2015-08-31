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
package org.hawkular.metrics.rest

import static org.joda.time.DateTime.now
import static org.junit.Assert.assertEquals

import org.hawkular.metrics.core.api.AvailabilityType
import org.joda.time.DateTime
import org.junit.Test

/**
 * @author Thomas Segismont
 */
class AvailabilityITest extends RESTTest {

  @Test
  void shouldNotCreateMetricWithEmptyPayload() {
    def tenantId = nextTenantId()

    badPost(path: "availability", headers: [(tenantHeaderName): tenantId], body: "" /* Empty Body */) { exception ->
      assertEquals(400, exception.response.status)
    }
  }

  @Test
  void shouldNotAddAvailabilityForMetricWithEmptyPayload() {
    def tenantId = nextTenantId()

    badPost(path: "availability/pimpo/data", headers: [(tenantHeaderName): tenantId],
        body: "" /* Empty Body */) { exception ->
      assertEquals(400, exception.response.status)
    }

    badPost(path: "availability/pimpo/data", headers: [(tenantHeaderName): tenantId],
        body: [] /* Empty List */) { exception ->
      assertEquals(400, exception.response.status)
    }
  }

  @Test
  void shouldNotAddAvailabilityDataWithEmptyPayload() {
    def tenantId = nextTenantId()

    badPost(path: "availability/data", headers: [(tenantHeaderName): tenantId],
        body: "" /* Empty Body */) { exception ->
      assertEquals(400, exception.response.status)
    }

    badPost(path: "availability/data", headers: [(tenantHeaderName): tenantId],
        body: [] /* Empty List */) { exception ->
      assertEquals(400, exception.response.status)
    }
  }

  @Test
  void shouldNotTagAvailabilityDataWithEmptyPayload() {
    def tenantId = nextTenantId()

    badPost(path: "availability/pimpo/tag", headers: [(tenantHeaderName): tenantId],
        body: "" /* Empty Body */) { exception ->
      assertEquals(400, exception.response.status)
    }
  }


  @Test
  void shouldStoreLargePayloadSize() {
    def tenantId = nextTenantId()

    def points = []
    def availabilityTypes = AvailabilityType.values()
    for (i in 0..LARGE_PAYLOAD_SIZE + 10) {
      points.push(['timestamp': i, 'value': availabilityTypes[i % availabilityTypes.length]])
    }

    def response = hawkularMetrics.post(path: "availability/test/data", headers: [(tenantHeaderName): tenantId],
        body: points)
    assertEquals(200, response.status)
  }

  @Test
  void testAvailabilityGet() {
    DateTime start = now().minusMinutes(20)
    String tenantId = nextTenantId()
    String metric = 'A1'

    def response = hawkularMetrics.post(path: "availability/$metric/data", body: [
        [timestamp: start.millis, value: "up"]], headers: [(tenantHeaderName): tenantId])

    assertEquals(200, response.status)

    response = hawkularMetrics.get(path: "availability/$metric", headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertEquals(metric, response.data.id)
  }

  @Test
  void shouldNotCreateMetricWithEmptyTimestamp() {
    def tenantId = nextTenantId()

    badPost(path: "availability/data", headers: [(tenantHeaderName): tenantId], body: [
        [id: 'test1',
         data: [
            [timestamp: 123, value: "down"],
            [timestamp: 124, value: "down"],
            [timestamp: 125, value: "down"]
        ]],
        [id: 'test2',
         data: [
            [timestamp: 123, value: "down"],
            [timestamp: 124, value: "down"],
            [timestamp: 125, value: "down"],
            [value: "up"]
        ]]]) { exception ->
      assertEquals(400, exception.response.status)
    }

    badPost(path: "availability/test3/data", headers: [(tenantHeaderName): tenantId], body: [
            [timestamp: 123, value: "up"],
            [timestamp: 124, value: "up"],
            [timestamp: 125, value: "up"],
            [value: "up"]
        ]) { exception ->
      assertEquals(400, exception.response.status)
    }
  }
}
