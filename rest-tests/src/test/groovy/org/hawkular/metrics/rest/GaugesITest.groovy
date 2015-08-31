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

import static org.junit.Assert.assertEquals

import org.junit.Test

/**
 * @author Thomas Segismont
 */
class GaugesITest extends RESTTest {

  @Test
  void shouldNotCreateMetricWithEmptyPayload() {
    def tenantId = nextTenantId()

    badPost(path: "gauges", headers: [(tenantHeaderName): tenantId], body: "" /* Empty Body */) { exception ->
      assertEquals(400, exception.response.status)
    }
  }

  @Test
  void shouldNotAddDataForMetricWithEmptyPayload() {
    def tenantId = nextTenantId()

    badPost(path: "gauges/pimpo/data", headers: [(tenantHeaderName): tenantId],
        body: "" /* Empty Body */) { exception ->
      assertEquals(400, exception.response.status)
    }

    badPost(path: "gauges/pimpo/data", headers: [(tenantHeaderName): tenantId],
        body: [] /* Empty List */) { exception ->
      assertEquals(400, exception.response.status)
    }
  }

  @Test
  void shouldNotAddGaugeDataWithEmptyPayload() {
    def tenantId = nextTenantId()

    badPost(path: "gauges/data", headers: [(tenantHeaderName): tenantId],
        body: "" /* Empty Body */) { exception ->
      assertEquals(400, exception.response.status)
    }

    badPost(path: "gauges/data", headers: [(tenantHeaderName): tenantId],
        body: [] /* Empty List */) { exception ->
      assertEquals(400, exception.response.status)
    }
  }

  @Test
  void shouldNotTagGaugeDataWithEmptyPayload() {
    def tenantId = nextTenantId()

    badPost(path: "gauges/pimpo/tag", headers: [(tenantHeaderName): tenantId],
        body: "" /* Empty Body */) { exception ->
      assertEquals(400, exception.response.status)
    }
  }

  @Test
  void shouldStoreLargePayloadSize() {
    def tenantId = nextTenantId()

    def points = []
    for (i in 0..LARGE_PAYLOAD_SIZE + 10) {
      points.push(['timestamp': i, 'value': (double) i])
    }

    def response = hawkularMetrics.post(path: "gauges/test/data", headers: [(tenantHeaderName): tenantId], body: points)
    assertEquals(200, response.status)
  }

  @Test
  void shouldNotCreateMetricWithEmptyTimestamp() {
    def tenantId = nextTenantId()

    badPost(path: "gauges/data", headers: [(tenantHeaderName): tenantId], body: [
        [id: 'test1',
         data: [
            [timestamp: 123, value: 123],
            [timestamp: 124, value: 124],
            [timestamp: 125, value: 125]
        ]],
        [id: 'test2',
         data: [
            [timestamp: 123, value: 123],
            [timestamp: 124, value: 124],
            [timestamp: 125, value: 125],
            [value: 125]
        ]]]) { exception ->
      assertEquals(400, exception.response.status)
    }

    badPost(path: "gauges/test3/data", headers: [(tenantHeaderName): tenantId], body: [
            [timestamp: 123, value: 123],
            [timestamp: 124, value: 124],
            [timestamp: 125, value: 125],
            [value: 126]
        ]) { exception ->
      assertEquals(400, exception.response.status)
    }
  }
}
