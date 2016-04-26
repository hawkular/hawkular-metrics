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

import static org.joda.time.DateTime.now
import static org.junit.Assert.assertEquals

import org.joda.time.DateTime
import org.junit.Test

/**
 * @author Thomas Segismont
 */
class GaugesITest extends RESTTest {
  def tenantId = nextTenantId()

  @Test
  void shouldNotAcceptInvalidTimeRange() {
    badGet(path: "gauges/test/raw", headers: [(tenantHeaderName): tenantId],
        query: [start: 1000, end: 500]) { exception ->
      assertEquals(400, exception.response.status)
    }
  }

  @Test
  void shouldNotAcceptInvalidBucketConfig() {
    badGet(path: "gauges/test/stats", headers: [(tenantHeaderName): tenantId],
        query: [start: 500, end: 100, buckets: '10', bucketDuration: '10ms']) { exception ->
      assertEquals("Should fail when both bucket params are specified", 400, exception.response.status)
    }
  }

  @Test
  void shouldNotCreateMetricWithEmptyPayload() {
    badPost(path: "gauges", headers: [(tenantHeaderName): tenantId], body: "" /* Empty Body */) { exception ->
      assertEquals(400, exception.response.status)
    }
  }

  @Test
  void shouldNotAddDataForMetricWithEmptyPayload() {
    badPost(path: "gauges/pimpo/raw", headers: [(tenantHeaderName): tenantId],
        body: "" /* Empty Body */) { exception ->
      assertEquals(400, exception.response.status)
    }

    badPost(path: "gauges/pimpo/raw", headers: [(tenantHeaderName): tenantId],
        body: [] /* Empty List */) { exception ->
      assertEquals(400, exception.response.status)
    }
  }

  @Test
  void shouldNotAddGaugeDataWithEmptyPayload() {
    badPost(path: "gauges/raw", headers: [(tenantHeaderName): tenantId],
        body: "" /* Empty Body */) { exception ->
      assertEquals(400, exception.response.status)
    }

    badPost(path: "gauges/raw", headers: [(tenantHeaderName): tenantId],
        body: [] /* Empty List */) { exception ->
      assertEquals(400, exception.response.status)
    }
  }

  @Test
  void shouldStoreLargePayloadSize() {
    checkLargePayload("gauges", tenantId, { points, i -> points.push([timestamp: i, value: (double) i]) })
  }

  @Test
  void shouldNotAcceptDataWithEmptyTimestamp() {
    invalidPointCheck("gauges", tenantId, [[value: 5.5]])
  }

  @Test
  void shouldNotAcceptDataWithNullTimestamp() {
    invalidPointCheck("gauges", tenantId, [[timestamp: null, value: 5.5]])
  }

  @Test
  void shouldNotAcceptDataWithInvalidTimestamp() {
    invalidPointCheck("gauges", tenantId, [[timestamp: "aaa", value: 5.5]])
  }

  @Test
  void shouldNotAcceptDataWithEmptyValue() {
    invalidPointCheck("gauges", tenantId, [[timestamp: 13]])
  }

  @Test
  void shouldNotAcceptDataWithNullValue() {
    invalidPointCheck("gauges", tenantId, [[timestamp: 13, value: null]])
  }

  @Test
  void shouldNotAcceptDataWithInvalidValue() {
    invalidPointCheck("gauges", tenantId, [[timestamp: 13, value: ["dsqdqs"]]])
  }

  @Test
  void addDataForSingleGaugeAndFindWithLimitAndSort() {
    String gauge = "G1"
    DateTime start = now().minusHours(1)

    def response = hawkularMetrics.post(
        path: "gauges/$gauge/raw",
        headers: [(tenantHeaderName): tenantId],
        body: [
            [timestamp: start.millis, value: 100.1],
            [timestamp: start.plusMinutes(1).millis, value: 200.2],
            [timestamp: start.plusMinutes(2).millis, value: 300.3],
            [timestamp: start.plusMinutes(3).millis, value: 400.4],
            [timestamp: start.plusMinutes(4).millis, value: 500.5],
            [timestamp: start.plusMinutes(5).millis, value: 600.6],
            [timestamp: now().plusSeconds(30).millis, value: 750.7]
        ]
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.get(
        path: "gauges/$gauge/raw",
        headers: [(tenantHeaderName): tenantId],
        query: [limit: 2]
    )
    assertEquals(200, response.status)

    def expectedData = [
        [timestamp: start.plusMinutes(5).millis, value: 600.6],
        [timestamp: start.plusMinutes(4).millis, value: 500.5]
    ]
    assertEquals(expectedData, response.data)

    response = hawkularMetrics.get(
        path: "gauges/$gauge/raw",
        headers: [(tenantHeaderName): tenantId],
        query: [limit: 2, order: 'desc']
    )
    assertEquals(200, response.status)

    expectedData = [
        [timestamp: start.plusMinutes(5).millis, value: 600.6],
        [timestamp: start.plusMinutes(4).millis, value: 500.5]
    ]
    assertEquals(expectedData, response.data)

    response = hawkularMetrics.get(
        path: "gauges/$gauge/raw",
        headers: [(tenantHeaderName): tenantId],
        query: [limit: 3, order: 'asc']
    )
    assertEquals(200, response.status)

    expectedData = [
        [timestamp: start.millis, value: 100.1],
        [timestamp: start.plusMinutes(1).millis, value: 200.2],
        [timestamp: start.plusMinutes(2).millis, value: 300.3],
    ]
    assertEquals(expectedData, response.data)

    response = hawkularMetrics.get(
        path: "gauges/$gauge/raw",
        headers: [(tenantHeaderName): tenantId],
        query: [limit: 3, start: start.plusMinutes(1).millis]
    )
    assertEquals(200, response.status)

    expectedData = [
        [timestamp: start.plusMinutes(1).millis, value: 200.2],
        [timestamp: start.plusMinutes(2).millis, value: 300.3],
        [timestamp: start.plusMinutes(3).millis, value: 400.4]
    ]
    assertEquals(expectedData, response.data)

    response = hawkularMetrics.get(
        path: "gauges/$gauge/raw",
        headers: [(tenantHeaderName): tenantId],
        query: [limit: 3, end: (start.plusMinutes(5).millis + 1)]
    )
    assertEquals(200, response.status)

    expectedData = [
        [timestamp: start.plusMinutes(5).millis, value: 600.6],
        [timestamp: start.plusMinutes(4).millis, value: 500.5],
        [timestamp: start.plusMinutes(3).millis, value: 400.4]
    ]
    assertEquals(expectedData, response.data)

    response = hawkularMetrics.get(
        path: "gauges/$gauge/raw",
        headers: [(tenantHeaderName): tenantId],
        query: [limit: 3, start: (start.plusMinutes(1).millis - 1), order: 'desc']
    )
    assertEquals(200, response.status)

    expectedData = [
        [timestamp: start.plusMinutes(5).millis, value: 600.6],
        [timestamp: start.plusMinutes(4).millis, value: 500.5],
        [timestamp: start.plusMinutes(3).millis, value: 400.4]
    ]
    assertEquals(expectedData, response.data)

    response = hawkularMetrics.get(
        path: "gauges/$gauge/raw",
        headers: [(tenantHeaderName): tenantId],
        query: [limit: -1, order: 'desc']
    )
    assertEquals(200, response.status)

    expectedData = [
        [timestamp: start.plusMinutes(5).millis, value: 600.6],
        [timestamp: start.plusMinutes(4).millis, value: 500.5],
        [timestamp: start.plusMinutes(3).millis, value: 400.4],
        [timestamp: start.plusMinutes(2).millis, value: 300.3],
        [timestamp: start.plusMinutes(1).millis, value: 200.2],
        [timestamp: start.millis, value: 100.1],
    ]
    assertEquals(expectedData, response.data)

    response = hawkularMetrics.get(
        path: "gauges/$gauge/raw",
        headers: [(tenantHeaderName): tenantId],
        query: [limit: -100, order: 'asc']
    )
    assertEquals(200, response.status)

    expectedData = [
        [timestamp: start.millis, value: 100.1],
        [timestamp: start.plusMinutes(1).millis, value: 200.2],
        [timestamp: start.plusMinutes(2).millis, value: 300.3],
        [timestamp: start.plusMinutes(3).millis, value: 400.4],
        [timestamp: start.plusMinutes(4).millis, value: 500.5],
        [timestamp: start.plusMinutes(5).millis, value: 600.6],
    ]
    assertEquals(expectedData, response.data)
  }

  @Test
  void addTaggedDataPoints() {
    String tenantId = nextTenantId()
    DateTime start = now().minusMinutes(30)
    DateTime end = start.plusMinutes(10)
    String gauge = 'G1'

    def response = hawkularMetrics.post(
        path: "gauges/$gauge/raw",
        headers: [(tenantHeaderName): tenantId],
        body: [
            [
                timestamp: start.millis,
                value: 11.1,
                tags: [x: 1, y: 2]
            ],
            [
                timestamp: start.plusMinutes(1).millis,
                value: 22.2,
                tags: [y: 3, z: 5]
            ],
            [
                timestamp: start.plusMinutes(3).millis,
                value: 33.3,
                tags: [x: 4, z: 6]
            ]
        ]
    )
    assertEquals(200, response.status)
    response = hawkularMetrics.get(path: "gauges/$gauge/raw", headers: [(tenantHeaderName): tenantId])
    def expectedData = [
        [
            timestamp: start.plusMinutes(3).millis,
            value: 33.3,
            tags: [x: '4', z: '6']
        ],
        [
            timestamp: start.plusMinutes(1).millis,
            value: 22.2,
            tags: [y: '3', z: '5']
        ],
        [
            timestamp: start.millis,
            value: 11.1,
            tags: [x: '1', y: '2']
        ]
    ]
    assertEquals(expectedData, response.data)
  }
}
