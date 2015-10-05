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

import org.joda.time.DateTime
import org.junit.Test

/**
 * @author John Sanda
 */
class CountersITest extends RESTTest {
  def tenantId = nextTenantId()

  @Test
  void shouldNotAcceptInvalidTimeRange() {
    badGet(path: "counters/test/data", headers: [(tenantHeaderName): tenantId],
        query: [start: 1000, end: 500]) { exception ->
      assertEquals(400, exception.response.status)
    }
  }

  @Test
  void shouldNotAcceptInvalidBucketConfig() {
    badGet(path: "counters/test/data", headers: [(tenantHeaderName): tenantId],
        query: [start: 500, end: 100, buckets: '10', bucketDuration: '10ms']) { exception ->
      assertEquals("Should fail when both bucket params are specified", 400, exception.response.status)
    }
  }

  @Test
  void shouldNotCreateMetricWithEmptyPayload() {
    badPost(path: "counters", headers: [(tenantHeaderName): tenantId], body: "" /* Empty Body */) { exception ->
      assertEquals(400, exception.response.status)
    }
  }

  @Test
  void shouldNotAddDataForCounterWithEmptyPayload() {
    badPost(path: "counters/pimpo/data", headers: [(tenantHeaderName): tenantId],
        body: "" /* Empty Body */) { exception ->
      assertEquals(400, exception.response.status)
    }

    badPost(path: "counters/pimpo/data", headers: [(tenantHeaderName): tenantId],
        body: [] /* Empty List */) { exception ->
      assertEquals(400, exception.response.status)
    }
  }

  @Test
  void shouldNotAddDataWithEmptyPayload() {
    badPost(path: "counters/data", headers: [(tenantHeaderName): tenantId],
        body: "" /* Empty Body */) { exception ->
      assertEquals(400, exception.response.status)
    }

    badPost(path: "counters/data", headers: [(tenantHeaderName): tenantId],
        body: [] /* Empty List */) { exception ->
      assertEquals(400, exception.response.status)
    }
  }

  @Test
  void createSimpleCounter() {
    String id = "C1"

    def response = hawkularMetrics.post(
        path: "counters",
        headers: [(tenantHeaderName): tenantId],
        body: [id: id]
    )
    assertEquals(201, response.status)

    response = hawkularMetrics.get(path: "counters/$id", headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)

    def expectedData = [tenantId: tenantId, id: id, type: 'counter']
    assertEquals(expectedData, response.data)
  }

  @Test
  void shouldNotCreateDuplicateCounter() {
    String id = "C1"

    def response = hawkularMetrics.post(
        path: "counters",
        headers: [(tenantHeaderName): tenantId],
        body: [id: id]
    )
    assertEquals(201, response.status)

    badPost(path: 'counters', headers: [(tenantHeaderName): tenantId], body: [id: id]) { exception ->
      assertEquals(409, exception.response.status)
    }
  }

  @Test
  void createCounterWithTagsAndDataRetention() {
    String id = "C1"

    def response = hawkularMetrics.post(
        path: "counters",
        headers: [(tenantHeaderName): tenantId],
        body: [
            id: id,
            tags: [tag1: 'one', tag2: 'two'],
            dataRetention: 100
        ]
    )
    assertEquals(201, response.status)

    response = hawkularMetrics.get(path: "counters/$id", headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)

    def expectedData = [
        tenantId: tenantId,
        id: id,
        tags: [tag1: 'one', tag2: 'two'],
        dataRetention: 100,
        type: 'counter'
    ]
    assertEquals(expectedData, response.data)
  }

  @Test
  void createAndFindCounters() {
    String counter1 = "C1"
    String counter2 = "C2"

    def response = hawkularMetrics.post(
        path: "counters",
        headers: [(tenantHeaderName): tenantId],
        body: [id: counter1]
    )
    assertEquals(201, response.status)

    response = hawkularMetrics.post(
        path: "counters",
        headers: [(tenantHeaderName): tenantId],
        body: [
            id: counter2,
            tags: [tag1: 'one', tag2: 'two']
        ]
    )
    assertEquals(201, response.status)

    response = hawkularMetrics.get(
        path: "metrics",
        headers: [(tenantHeaderName): tenantId],
        query: [type: 'counter']
    )
    assertEquals(200, response.status)

    def expectedData = [
        [
            tenantId: tenantId,
            id: counter1,
            type: 'counter'
        ],
        [
            tenantId: tenantId,
            id: counter2,
            tags: [
                tag1: 'one',
                tag2: 'two'
            ],
            type: 'counter'
        ]
    ]
    assertEquals(expectedData, response.data)
  }

  @Test
  void addDataForMultipleCountersAndFindWithDateRange() {
    String counter1 = "C1"
    String counter2 = "C2"

    DateTime start = now().minusMinutes(5)

    def response = hawkularMetrics.post(
        path: "counters/data",
        headers: [(tenantHeaderName): tenantId],
        body: [
            [
                id: counter1,
                data: [
                    [timestamp: start.millis, value: 10],
                    [timestamp: start.plusMinutes(1).millis, value: 20]
                ]
            ],
            [
                id: counter2,
                data: [
                    [timestamp: start.millis, value: 150],
                    [timestamp: start.plusMinutes(1).millis, value: 225],
                    [timestamp: start.plusMinutes(2).millis, value: 300]
                ]
            ]
        ]
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.get(
        path: "counters/$counter1/data",
        headers: [(tenantHeaderName): tenantId],
        query: [start: start.millis, end: start.plusMinutes(1).millis]
    )
    assertEquals(200, response.status)

    def expectedData = [
        [timestamp: start.millis, value: 10]
    ]
    assertEquals(expectedData, response.data)

    response = hawkularMetrics.get(
        path: "counters/$counter2/data",
        headers: [(tenantHeaderName): tenantId],
        query: [start: start.millis, end: start.plusMinutes(2).millis]
    )
    assertEquals(200, response.status)

    expectedData = [
        [timestamp: start.millis, value: 150],
        [timestamp: start.plusMinutes(1).millis, value: 225]
    ]
    assertEquals(expectedData, response.data)
  }

  @Test
  void addDataForSingleCounterAndFindWithDefaultDateRange() {
    String counter = "C1"
    DateTime start = now().minusHours(8)

    def response = hawkularMetrics.post(
        path: "counters/$counter/data",
        headers: [(tenantHeaderName): tenantId],
        body: [
            [timestamp: start.millis, value: 100],
            [timestamp: start.plusHours(1).millis, value :200],
            [timestamp: start.plusHours(4).millis, value: 500],
            [timestamp: now().plusSeconds(30).millis, value: 750]
        ]
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.get(
        path: "counters/$counter/data",
        headers: [(tenantHeaderName): tenantId],
    )
    assertEquals(200, response.status)

    def expectedData = [
        [timestamp: start.plusHours(1).millis, value: 200],
        [timestamp: start.plusHours(4).millis, value: 500]
    ]
    assertEquals(expectedData, response.data)
  }

  @Test
  void findWhenThereIsNoData() {
    String counter1 = "C1"
    String counter2 = "C2"
    DateTime start = now().minusHours(3)

    def response = hawkularMetrics.post(
        path: "counters/data",
        headers: [(tenantHeaderName): tenantId],
        body: [
            [
                id: counter1,
                data: [
                    [timestamp: start.millis, value: 100],
                    [timestamp: start.plusHours(1).millis, value: 150]
                ]
            ]
        ]
    )
    assertEquals(200, response.status)

    // First query a counter that has data but outside of the date range for which data
    // points are available
    response = hawkularMetrics.get(
        path: "counters/$counter1/data",
        headers: [(tenantHeaderName): tenantId],
        query: [start: start.minusHours(5).millis, end: start.minusHours(4).millis]
    )
    assertEquals(204, response.status)

    // Now query a counter that has no dat at all
    response = hawkularMetrics.get(
        path: "counters/$counter2/data",
        headers: [(tenantHeaderName): tenantId]
    )
    assertEquals(204, response.status)
  }

  @Test
  void findCounterStats() {
    String counter = "C1"

    // Create the tenant
    def response = hawkularMetrics.post(
        path: "tenants",
        body: [id: tenantId]
    )
    assertEquals(201, response.status)

    response = hawkularMetrics.post(
        path: "counters/$counter/data",
        headers: [(tenantHeaderName): tenantId],
        body: [
            [timestamp: 60_000 * 1.0, value: 0],
            [timestamp: 60_000 * 1.5, value: 200],
            [timestamp: 60_000 * 3.5, value: 400],
            [timestamp: 60_000 * 5.0, value: 550],
            [timestamp: 60_000 * 7.0, value: 950],
            [timestamp: 60_000 * 7.5, value: 1000],
        ]
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.get(
        path: "counters/$counter/data",
        headers: [(tenantHeaderName): tenantId],
        query: [start: 60_000, end: 60_000 * 8, bucketDuration: '1mn']
    )
    assertEquals(200, response.status)

    def expectedData = []
    (1..7).each { i ->
      def bucketPoint = [start: 60_000 * i, end: 60_000 * (i + 1)]
      double val;
      switch (i) {
        case 1:
          bucketPoint.putAll([min: 0D, avg: 100D, median: 0D, max: 200D, percentile95th: 0D, empty: false] as Map)
          break;
        case 2:
        case 4:
        case 6:
          val = Double.NaN
          bucketPoint.putAll([min: val, avg: val, median: val, max: val, percentile95th: val, empty: true] as Map)
          break;
        case 3:
          bucketPoint.putAll([min: 400D, avg: 400D, median: 400D, max: 400D, percentile95th: 400D, empty: false] as Map)
          break;
        case 5:
          bucketPoint.putAll([min: 550D, avg: 550D, median: 550D, max: 550D, percentile95th: 550D, empty: false] as Map)
          break;
        case 7:
          bucketPoint.putAll([min: 950D, avg: 975D, median: 950D, max: 1000D, percentile95th: 950D, empty: false] as Map)
          break;
      }
      expectedData.push(bucketPoint);
    }

    assertNumericBucketsEquals(expectedData, response.data ?: [])
  }

  @Test
  void findRate() {
    String counter = "C1"

    // Create the tenant
    def response = hawkularMetrics.post(
        path: "tenants",
        body: [id: tenantId]
    )
    assertEquals(201, response.status)

    response = hawkularMetrics.post(
        path: "counters/$counter/data",
        headers: [(tenantHeaderName): tenantId],
        body: [
            [timestamp: 60_000 * 1.0, value: 0],
            [timestamp: 60_000 * 1.5, value: 200],
            [timestamp: 60_000 * 3.5, value: 400],
            [timestamp: 60_000 * 5.0, value: 550],
            [timestamp: 60_000 * 7.0, value: 950],
            [timestamp: 60_000 * 7.5, value: 1000],
        ]
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.get(
        path: "counters/$counter/rate",
        headers: [(tenantHeaderName): tenantId],
        query: [start: 0]
    )
    assertEquals(200, response.status)

    def expectedData = [
        [timestamp: (60_000 * 1.25).toLong(), value: 400],
        [timestamp: (60_000 * 2.5).toLong(), value: 100],
        [timestamp: (60_000 * 4.25).toLong(), value: 100],
        [timestamp: (60_000 * 6.0).toLong(), value: 200],
        [timestamp: (60_000 * 7.25).toLong(), value: 100],
    ]

    def actualData = response.data ?: []

    def msg = """
Expected: ${expectedData}
Actual:   ${response.data}
"""
    assertEquals(msg, expectedData.size(), actualData.size())
    for (i in 0..actualData.size() - 1) {
      assertRateEquals(msg, expectedData[i], actualData[i])
    }
  }

  @Test
  void findRateStats() {
    String counter = "C1"

    // Create the tenant
    def response = hawkularMetrics.post(
        path: "tenants",
        body: [id: tenantId]
    )
    assertEquals(201, response.status)

    response = hawkularMetrics.post(
        path: "counters/$counter/data",
        headers: [(tenantHeaderName): tenantId],
        body: [
            [timestamp: 60_000 * 1.0, value: 0],
            [timestamp: 60_000 * 1.5, value: 200],
            [timestamp: 60_000 * 3.5, value: 400],
            [timestamp: 60_000 * 5.0, value: 550],
            [timestamp: 60_000 * 7.0, value: 950],
            [timestamp: 60_000 * 7.5, value: 1000],
        ]
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.get(
        path: "counters/$counter/rate",
        headers: [(tenantHeaderName): tenantId],
        query: [start: 60_000, end: 60_000 * 8, bucketDuration: '1mn']
    )
    assertEquals(200, response.status)

    def expectedData = []
    (1..7).each { i ->
      def bucketPoint = [start: 60_000 * i, end: 60_000 * (i + 1)]
      double val;
      switch (i) {
        case 1:
          val = 400D;
          bucketPoint.putAll([min: val, avg: val, median: val, max: val, percentile95th: val, empty: false] as Map)
          break;
        case 3:
        case 5:
          val = Double.NaN
          bucketPoint.putAll([min: val, avg: val, median: val, max: val, percentile95th: val, empty: true] as Map)
          break;
        case 6:
          val = 200D;
          bucketPoint.putAll([min: val, avg: val, median: val, max: val, percentile95th: val, empty: false] as Map)
          break;
        default:
          val = 100D;
          bucketPoint.putAll([min: val, avg: val, median: val, max: val, percentile95th: val, empty: false] as Map)
          break;
      }
      expectedData.push(bucketPoint);
    }

    assertNumericBucketsEquals(expectedData, response.data ?: [])
  }

  static void assertRateEquals(String msg, def expected, def actual) {
    assertEquals(msg, expected.timestamp, actual.timestamp)
    assertDoubleEquals(msg, expected.value, actual.value)
  }

  @Test
  void shouldStoreLargePayload() {
    checkLargePayload("counters", tenantId, { points, i -> points.push([timestamp: i, value: i]) })
  }

  @Test
  void shouldNotAcceptDataWithEmptyTimestamp() {
    invalidPointCheck("counters", tenantId, [[value: "up"]])
  }

  @Test
  void shouldNotAcceptDataWithNullTimestamp() {
    invalidPointCheck("counters", tenantId, [[timestamp: null, value: 1]])
  }

  @Test
  void shouldNotAcceptDataWithInvalidTimestamp() {
    invalidPointCheck("counters", tenantId, [[timestamp: "aaa", value: 1]])
  }

  @Test
  void shouldNotAcceptDataWithEmptyValue() {
    invalidPointCheck("counters", tenantId, [[timestamp: 13]])
  }

  @Test
  void shouldNotAcceptDataWithNullValue() {
    invalidPointCheck("counters", tenantId, [[timestamp: 13, value: null]])
  }

  @Test
  void shouldNotAcceptDataWithInvalidValue() {
    invalidPointCheck("counters", tenantId, [[timestamp: 13, value: ["dsqdqs"]]])
  }
}
