/*
 * Copyright 2014-2017 Red Hat, Inc. and/or its affiliates
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

import org.hawkular.metrics.datetime.DateTimeService
import org.joda.time.DateTime
import org.junit.Test

import static java.lang.Double.NaN
import static org.joda.time.DateTime.now
import static org.junit.Assert.*

/**
 * @author John Sanda
 */
class CountersITest extends RESTTest {
  def tenantId = nextTenantId()

  @Test
  void shouldNotAcceptInvalidTimeRange() {
    badGet(path: "counters/test/raw", headers: [(tenantHeaderName): tenantId],
        query: [start: 1000, end: 500]) { exception ->
      assertEquals(400, exception.response.status)
    }
  }

  @Test
  void shouldNotAcceptInvalidBucketConfig() {
    badGet(path: "counters/test/raw", headers: [(tenantHeaderName): tenantId],
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
    badPost(path: "counters/pimpo/raw", headers: [(tenantHeaderName): tenantId],
        body: "" /* Empty Body */) { exception ->
      assertEquals(400, exception.response.status)
    }

    badPost(path: "counters/pimpo/raw", headers: [(tenantHeaderName): tenantId],
        body: [] /* Empty List */) { exception ->
      assertEquals(400, exception.response.status)
    }
  }

  @Test
  void shouldNotAddDataWithEmptyPayload() {
    badPost(path: "counters/raw", headers: [(tenantHeaderName): tenantId],
        body: "" /* Empty Body */) { exception ->
      assertEquals(400, exception.response.status)
    }

    badPost(path: "counters/raw", headers: [(tenantHeaderName): tenantId],
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
    assertContentEncoding(response)

    def expectedData = [tenantId: tenantId, id: id, type: 'counter', dataRetention: 7]
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
    assertContentEncoding(response)

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
    def tenantId = nextTenantId()

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
    assertContentEncoding(response)

    def expectedData = [
        [
            tenantId: tenantId,
            id: counter1,
            type: 'counter',
            dataRetention: 7
        ],
        [
            tenantId: tenantId,
            id: counter2,
            tags: [
                tag1: 'one',
                tag2: 'two'
            ],
            type: 'counter',
            dataRetention: 7
        ]
    ]

    assertEquals(2, response.data.size())
    assertTrue(response.data.contains([
      tenantId: tenantId,
        id: counter1,
        type: 'counter',
        dataRetention: 7
    ]))
    assertTrue(response.data.contains([
        tenantId: tenantId,
        id: counter2,
        tags: [
            tag1: 'one',
            tag2: 'two'
        ],
        type: 'counter',
        dataRetention: 7
    ]))
  }

  @Test
  void addDataForMultipleCountersAndFindWithDateRange() {
    String counter1 = "C1"
    String counter2 = "C2"

    DateTime start = now().minusMinutes(5)

    def response = hawkularMetrics.post(
        path: "counters/raw",
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
        path: "counters/$counter1/raw",
        headers: [(tenantHeaderName): tenantId],
        query: [start: start.millis, end: start.plusMinutes(1).millis]
    )
    assertEquals(200, response.status)
    assertContentEncoding(response)

    def expectedData = [
        [timestamp: start.millis, value: 10]
    ]
    assertEquals(expectedData, response.data)

    response = hawkularMetrics.get(
        path: "counters/$counter2/raw",
        headers: [(tenantHeaderName): tenantId],
        query: [start: start.millis, end: start.plusMinutes(2).millis]
    )
    assertEquals(200, response.status)
    assertContentEncoding(response)

    expectedData = [
        [timestamp: start.plusMinutes(1).millis, value: 225],
        [timestamp: start.millis, value: 150]
    ]
    assertEquals(expectedData, response.data)
  }

  @Test
  void addDataForSingleCounterAndFindWithDefaultDateRange() {
    String counter = "C1"
    DateTime start = now().minusHours(8)

    def response = hawkularMetrics.post(
        path: "counters/$counter/raw",
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
        path: "counters/$counter/raw",
        headers: [(tenantHeaderName): tenantId],
    )
    assertEquals(200, response.status)
    assertContentEncoding(response)

    def expectedData = [
        [timestamp: start.plusHours(4).millis, value: 500],
        [timestamp: start.plusHours(1).millis, value: 200]
    ]
    assertEquals(expectedData, response.data)
  }

  @Test
  void addDataForSingleCounterAndFindWithLimitAndSort() {
    String counter = "C1"
    DateTime start = now().minusHours(1)

    def response = hawkularMetrics.post(
        path: "counters/$counter/raw",
        headers: [(tenantHeaderName): tenantId],
        body: [
            [timestamp: start.millis, value: 100],
            [timestamp: start.plusMinutes(1).millis, value :200],
            [timestamp: start.plusMinutes(2).millis, value: 300],
            [timestamp: start.plusMinutes(3).millis, value: 400],
            [timestamp: start.plusMinutes(4).millis, value: 500],
            [timestamp: start.plusMinutes(5).millis, value: 600],
            [timestamp: now().plusSeconds(30).millis, value: 750]
        ]
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.get(
        path: "counters/$counter/raw",
        headers: [(tenantHeaderName): tenantId],
        query: [limit: 2]
    )
    assertEquals(200, response.status)
    assertContentEncoding(response)

    def expectedData = [
        [timestamp: start.plusMinutes(5).millis, value: 600],
        [timestamp: start.plusMinutes(4).millis, value: 500]
    ]
    assertEquals(expectedData, response.data)

    response = hawkularMetrics.get(
        path: "counters/$counter/raw",
        headers: [(tenantHeaderName): tenantId],
        query: [limit: 2, order: 'desc']
    )
    assertEquals(200, response.status)
    assertContentEncoding(response)

    expectedData = [
        [timestamp: start.plusMinutes(5).millis, value: 600],
        [timestamp: start.plusMinutes(4).millis, value: 500]
    ]
    assertEquals(expectedData, response.data)

    response = hawkularMetrics.get(
        path: "counters/$counter/raw",
        headers: [(tenantHeaderName): tenantId],
        query: [limit: 3, order: 'asc']
    )
    assertEquals(200, response.status)
    assertContentEncoding(response)

    expectedData = [
        [timestamp: start.millis, value: 100],
        [timestamp: start.plusMinutes(1).millis, value: 200],
        [timestamp: start.plusMinutes(2).millis, value: 300],
    ]
    assertEquals(expectedData, response.data)

    response = hawkularMetrics.get(
        path: "counters/$counter/raw",
        headers: [(tenantHeaderName): tenantId],
        query: [limit: 3, start: start.plusMinutes(1).millis]
    )
    assertEquals(200, response.status)
    assertContentEncoding(response)

    expectedData = [
        [timestamp: start.plusMinutes(1).millis, value: 200],
        [timestamp: start.plusMinutes(2).millis, value: 300],
        [timestamp: start.plusMinutes(3).millis, value: 400]
    ]
    assertEquals(expectedData, response.data)

    response = hawkularMetrics.get(
        path: "counters/$counter/raw",
        headers: [(tenantHeaderName): tenantId],
        query: [limit: 3, end: (start.plusMinutes(5).millis + 1)]
    )
    assertEquals(200, response.status)
    assertContentEncoding(response)

    expectedData = [
        [timestamp: start.plusMinutes(5).millis, value: 600],
        [timestamp: start.plusMinutes(4).millis, value: 500],
        [timestamp: start.plusMinutes(3).millis, value: 400]
    ]
    assertEquals(expectedData, response.data)

    response = hawkularMetrics.get(
        path: "counters/$counter/raw",
        headers: [(tenantHeaderName): tenantId],
        query: [limit: 3, start: (start.plusMinutes(1).millis - 1), order: 'desc']
    )
    assertEquals(200, response.status)

    expectedData = [
        [timestamp: start.plusMinutes(5).millis, value: 600],
        [timestamp: start.plusMinutes(4).millis, value: 500],
        [timestamp: start.plusMinutes(3).millis, value: 400]
    ]
    assertEquals(expectedData, response.data)

    response = hawkularMetrics.get(
        path: "counters/$counter/raw",
        headers: [(tenantHeaderName): tenantId],
        query: [limit: -1, order: 'desc']
    )
    assertEquals(200, response.status)
    assertContentEncoding(response)

    expectedData = [
        [timestamp: start.plusMinutes(5).millis, value: 600],
        [timestamp: start.plusMinutes(4).millis, value: 500],
        [timestamp: start.plusMinutes(3).millis, value: 400],
        [timestamp: start.plusMinutes(2).millis, value: 300],
        [timestamp: start.plusMinutes(1).millis, value: 200],
        [timestamp: start.millis, value: 100],
    ]
    assertEquals(expectedData, response.data)

    response = hawkularMetrics.get(
        path: "counters/$counter/raw",
        headers: [(tenantHeaderName): tenantId],
        query: [limit: -100, order: 'asc']
    )
    assertEquals(200, response.status)
    assertContentEncoding(response)

    expectedData = [
        [timestamp: start.millis, value: 100],
        [timestamp: start.plusMinutes(1).millis, value: 200],
        [timestamp: start.plusMinutes(2).millis, value: 300],
        [timestamp: start.plusMinutes(3).millis, value: 400],
        [timestamp: start.plusMinutes(4).millis, value: 500],
        [timestamp: start.plusMinutes(5).millis, value: 600],
    ]
    assertEquals(expectedData, response.data)
  }

  @Test
  void fromEarliestQueryGaugeData() {
    String tenantId = nextTenantId()
    String counter = "c1001"
    DateTime start = now().minusHours(10).plusMinutes(10)

    def response = hawkularMetrics.post(
        path: "counters/$counter/raw",
        headers: [(tenantHeaderName): tenantId],
        body: [
            [timestamp: start.millis, value: 100],
            [timestamp: start.plusHours(1).millis, value: 202],
            [timestamp: start.plusHours(2).millis, value: 303],
            [timestamp: start.plusHours(3).millis, value: 404],
            [timestamp: start.plusHours(4).millis, value: 505],
            [timestamp: start.plusHours(5).millis, value: 606],
            [timestamp: now().plusHours(6).millis, value: 757]
        ]
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.get(
        path: "counters/$counter/raw",
        headers: [(tenantHeaderName): tenantId],
        query: [order: 'asc']
    )
    assertEquals(200, response.status)
    assertContentEncoding(response)

    def expectedData = [
        [timestamp: start.plusHours(2).millis, value: 303],
        [timestamp: start.plusHours(3).millis, value: 404],
        [timestamp: start.plusHours(4).millis, value: 505],
        [timestamp: start.plusHours(5).millis, value: 606],
    ]

    assertEquals(expectedData, response.data)

    response = hawkularMetrics.get(
        path: "counters/$counter/raw",
        headers: [(tenantHeaderName): tenantId],
        query: [fromEarliest: true, order: 'asc']
    )
    assertEquals(200, response.status)
    assertContentEncoding(response)

    expectedData = [
        [timestamp: start.millis, value: 100],
        [timestamp: start.plusHours(1).millis, value: 202],
        [timestamp: start.plusHours(2).millis, value: 303],
        [timestamp: start.plusHours(3).millis, value: 404],
        [timestamp: start.plusHours(4).millis, value: 505],
        [timestamp: start.plusHours(5).millis, value: 606],
    ]

    assertEquals(expectedData, response.data)
  }

  @Test
  void findWhenThereIsNoData() {
    String counter1 = "C1"
    String counter2 = "C2"
    DateTime start = now().minusHours(3)

    def response = hawkularMetrics.post(
        path: "counters/raw",
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
        path: "counters/$counter1/raw",
        headers: [(tenantHeaderName): tenantId],
        query: [start: start.minusHours(5).millis, end: start.minusHours(4).millis]
    )
    assertEquals(204, response.status)

    // Now query a counter that has no dat at all
    response = hawkularMetrics.get(
        path: "counters/$counter2/raw",
        headers: [(tenantHeaderName): tenantId]
    )
    assertEquals(204, response.status)
  }

  @Test
  void findCounterStats() {
    String counter = "C1"

    def response = hawkularMetrics.post(
        path: "counters/$counter/raw",
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
        path: "counters/$counter/stats",
        headers: [(tenantHeaderName): tenantId],
        query: [start: 60_000, end: 60_000 * 8, bucketDuration: '1mn']
    )
    assertEquals(200, response.status)
    assertContentEncoding(response)

    def expectedData = []
    (1..7).each { i ->
      def bucketPoint = [start: 60_000 * i, end: 60_000 * (i + 1)]
      double val;
      switch (i) {
        case 1:
          bucketPoint.putAll([min: 0D, avg: 100D, median: 0D, max: 200D, sum: 200D, empty: false,
          samples: 2] as Map)
          break;
        case 2:
        case 4:
        case 6:
          val = NaN
          bucketPoint.putAll([empty: true] as Map)
          break;
        case 3:
          bucketPoint.putAll([min: 400D, avg: 400D, median: 400D, max: 400D, sum: 400D, empty: false,
          samples: 1] as Map)
          break;
        case 5:
          bucketPoint.putAll([min: 550D, avg: 550D, median: 550D, max: 550D, sum: 550D, empty: false,
          samples: 1] as Map)
          break;
        case 7:
          bucketPoint.putAll([min: 950D, avg: 975D, median: 950D, max: 1000D, sum: 1950D, empty: false,
          samples: 2] as Map)
          break;
      }
      expectedData.push(bucketPoint);
    }

    assertNumericBucketsEquals(expectedData, response.data ?: [])
  }

  @Test
  void findRate() {
    String counter = "C1"

    def response = hawkularMetrics.post(
        path: "counters/$counter/raw",
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
        query: [start: 0, order: 'asc']
    )
    assertEquals(200, response.status)
    assertContentEncoding(response)

    def expectedData = [
        [timestamp: (60_000 * 1.5).toLong(), value: 400],
        [timestamp: (60_000 * 3.5).toLong(), value: 100],
        [timestamp: (60_000 * 5.0).toLong(), value: 100],
        [timestamp: (60_000 * 7.0).toLong(), value: 200],
        [timestamp: (60_000 * 7.5).toLong(), value: 100],
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
  void findRateWhenThereAreResets() {
    String counter = 'C1'

    def response = hawkularMetrics.post(
        path: "counters/$counter/raw",
        headers: [(tenantHeaderName): tenantId],
        body: [
            [timestamp: 60_000 * 1.0, value: 1],
            [timestamp: 60_000 * 1.5, value: 2],
            [timestamp: 60_000 * 3.5, value: 3],
            [timestamp: 60_000 * 5.0, value: 1],
            [timestamp: 60_000 * 7.0, value: 2],
            [timestamp: 60_000 * 7.5, value: 3],
            [timestamp: 60_000 * 8.0, value: 1],
            [timestamp: 60_000 * 8.5, value: 2],
            [timestamp: 60_000 * 9.0, value: 3]
        ]
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.get(
        path: "counters/$counter/rate",
        headers: [(tenantHeaderName): tenantId],
        query: [start: 0, order: 'asc']
    )
    assertEquals(200, response.status)
    assertContentEncoding(response)

    def expectedData = [
        [timestamp: (60_000 * 1.5) as Long, value: 2],
        [timestamp: (60_000 * 3.5) as Long, value: 0.5],
        [timestamp: (60_000 * 7.0) as Long, value: 0.5],
        [timestamp: (60_000 * 7.5) as Long, value: 2],
        [timestamp: (60_000 * 8.5) as Long, value: 2],
        [timestamp: (60_000 * 9.0) as Long, value: 2]
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

    def response = hawkularMetrics.post(
        path: "counters/$counter/raw",
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
        path: "counters/$counter/rate/stats",
        headers: [(tenantHeaderName): tenantId],
        query: [start: 60_000, end: 60_000 * 8, bucketDuration: '1mn']
    )
    assertEquals(200, response.status)
    assertContentEncoding(response)

    def expectedData = []
    (1..7).each { i ->
      Map bucketPoint = [start: 60_000 * i, end: 60_000 * (i + 1)]
      double val
      switch (i) {
        case 1:
          val = 400D
          bucketPoint << [min: val, avg: val, median: val, max: val, sum: val, empty: false, samples: 1]
          break
        case 3:
          val = 100D
          bucketPoint << [min: val, avg: val, median: val, max: val, sum: val, empty: false, samples: 1]
          break
        case 5:
          val = 100D
          bucketPoint << [min: val, avg: val, median: val, max: val, sum: val, empty: false, samples: 1]
          break
        case 7:
          bucketPoint << [min: 100.0, max: 200.0, avg: 150.0, median: 100.0, sum: 300D, empty:false,
                          samples: 2]
          break
        default:
          bucketPoint << [empty: true]
          break
      }
      expectedData.push(bucketPoint);
    }

    assertNumericBucketsEquals(expectedData, response.data ?: [])
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

  @Test
  void percentileParameter() {
      String counter = "C1"

      def response = hawkularMetrics.post(
          path: "counters/$counter/raw",
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
          path: "counters/$counter/stats",
          headers: [(tenantHeaderName): tenantId],
          query: [start: 60_000, end: 60_000 * 8, buckets: '1', percentiles: '50.0,90.0,99.9']
      )
      assertEquals(200, response.status)
      assertContentEncoding(response)

      assertEquals(1, response.data.size)
      assertEquals(3, response.data[0].percentiles.size)

      assertEquals(50.0, response.data[0].percentiles[0].quantile)
      assertEquals(400, response.data[0].percentiles[0].value, 0.1)
  }

  @Test
  void findStackedStatsForMultipleCounters() {
    String tenantId = nextTenantId()
    DateTime start = now().minusMinutes(10)

    // Create some metrics
    def response = hawkularMetrics.post(path: 'counters', body: [
        id  : 'C1',
        tags: ['type': 'counter_cpu_usage', 'host': 'server1', 'env': 'stage']
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(201, response.status)

    response = hawkularMetrics.post(path: 'counters', body: [
        id  : 'C2',
        tags: ['type': 'counter_cpu_usage', 'host': 'server2', 'env': 'dev']
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(201, response.status)

    response = hawkularMetrics.post(path: 'counters', body: [
        id  : 'C3',
        tags: ['type': 'counter_cpu_usage', 'host': 'server3', 'env': 'stage']
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(201, response.status)

    Random rand = new Random()
    def randomList = []
    (1..10).each {
      randomList << rand.nextInt(100)
    }
    randomList.sort()

    def c1 = [
      [timestamp: start.millis, value: 510 + randomList[0]],
      [timestamp: start.plusMinutes(1).millis, value: 512 + randomList[1]],
      [timestamp: start.plusMinutes(2).millis, value: 514 + randomList[2]],
      [timestamp: start.plusMinutes(3).millis, value: 516 + randomList[3]],
      [timestamp: start.plusMinutes(4).millis, value: 518 + randomList[4]]
    ]
    def c2 = [
      [timestamp: start.millis, value: 378 + randomList[5]],
      [timestamp: start.plusMinutes(1).millis, value: 381 + randomList[6]],
      [timestamp: start.plusMinutes(2).millis, value: 384 + randomList[7]],
      [timestamp: start.plusMinutes(3).millis, value: 387 + randomList[8]],
      [timestamp: start.plusMinutes(4).millis, value: 390 + randomList[9]]
    ]

    // insert data points
    response = hawkularMetrics.post(path: "counters/raw", body: [
        [
            id: 'C1',
            data: c1
        ],
        [
            id: 'C2',
            data: c2
        ],
        [
            id: 'C3',
            data: [
                [timestamp: start.millis, value: 5712],
                [timestamp: start.plusMinutes(1).millis, value: 5773],
                [timestamp: start.plusMinutes(2).millis, value: 5949],
                [timestamp: start.plusMinutes(3).millis, value: 5979],
                [timestamp: start.plusMinutes(4).millis, value: 6548]
            ]
        ]
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)

    c1 = c1.take(c1.size() - 1)
    c2 = c2.take(c2.size() - 1)
    def combinedData = c1 + c2;

    //Get counter rates
    response = hawkularMetrics.get(
        path: 'counters/C1/rate/stats',
        query: [
            start: start.millis,
            end: start.plusMinutes(4).millis,
            buckets: 1,
        ],
        headers: [(tenantHeaderName): tenantId]
    )
    assertContentEncoding(response)
    def c1Rates = response.data[0];

    response = hawkularMetrics.get(
        path: 'counters/C2/rate/stats',
        query: [
            start: start.millis,
            end: start.plusMinutes(4).millis,
            buckets: 1,
        ],
        headers: [(tenantHeaderName): tenantId]
    )
    assertContentEncoding(response)
    def c2Rates = response.data[0];

    //Tests start here
    response = hawkularMetrics.get(
        path: 'counters/stats',
        query: [
            start: start.millis,
            end: start.plusMinutes(4).millis,
            buckets: 1,
            tags: 'type:counter_cpu_usage,host:server1|server2',
            stacked: true
        ],
        headers: [(tenantHeaderName): tenantId]
    )
    assertContentEncoding(response)
    assertEquals(200, response.status)

    assertEquals("Expected to get back one bucket", 1, response.data.size())

    def actualCounterBucketByTag = response.data[0]

    assertEquals("The start time is wrong", start.millis, actualCounterBucketByTag.start)
    assertEquals("The end time is wrong", start.plusMinutes(4).millis, actualCounterBucketByTag.end)
    assertDoubleEquals("The min is wrong", (c1.min {it.value}).value + (c2.min {it.value}).value,
        actualCounterBucketByTag.min)
    assertDoubleEquals("The max is wrong", (c1.max {it.value}).value + (c2.max {it.value}).value,
        actualCounterBucketByTag.max)
    assertDoubleEquals("The sum is wrong", (c1 + c2).sum() { it.value }, actualCounterBucketByTag.sum)
    assertDoubleEquals("The avg is wrong", avg(c1.collect {it.value}) + avg(c2.collect {it.value}),
        actualCounterBucketByTag.avg)
    assertEquals("The [empty] property is wrong", false, actualCounterBucketByTag.empty)
    assertTrue("Expected the [median] property to be set", actualCounterBucketByTag.median != null)

    response = hawkularMetrics.get(
        path: 'counters/stats',
        query: [
            start: start.millis,
            end: start.plusMinutes(4).millis,
            buckets: 1,
            metrics: ['C1', 'C2'],
            stacked: true
        ],
        headers: [(tenantHeaderName): tenantId]
    )
    assertContentEncoding(response)
    assertEquals(200, response.status)

    def actualCounterBucketById = response.data[0]

    assertEquals("Expected to get back one bucket", 1, response.data.size())
    assertEquals("Stacked stats when queried by tag are different than when queried by id", actualCounterBucketById, actualCounterBucketByTag)
  }

  @Test
  void findStackedStatsForMultipleCountersAsymmetricData() {
    String tenantId = nextTenantId()
    DateTime start = now().minusMinutes(10)

    // Create some metrics
    def response = hawkularMetrics.post(path: 'counters', body: [
        id  : 'C1',
        tags: ['type': 'counter_cpu_usage', 'host': 'server1', 'env': 'stage']
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(201, response.status)

    response = hawkularMetrics.post(path: 'counters', body: [
        id  : 'C2',
        tags: ['type': 'counter_cpu_usage', 'host': 'server2', 'env': 'dev']
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(201, response.status)

    response = hawkularMetrics.post(path: 'counters', body: [
        id  : 'C3',
        tags: ['type': 'counter_cpu_usage', 'host': 'server3', 'env': 'stage']
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(201, response.status)

    Random rand = new Random()
    def randomList = []
    (1..10).each {
      randomList << rand.nextInt(100)
    }
    randomList.sort()

    def c1 = [
      [timestamp: start.millis, value: 510 + randomList[0]],
      [timestamp: start.plusMinutes(1).millis, value: 512 + randomList[1]],
      [timestamp: start.plusMinutes(2).millis, value: 514 + randomList[2]],
      [timestamp: start.plusMinutes(3).millis, value: 516 + randomList[3]],
    ]
    def c2 = [
      [timestamp: start.plusMinutes(1).millis, value: 381 + randomList[5]],
      [timestamp: start.plusMinutes(3).millis, value: 387 + randomList[6]],
      [timestamp: start.plusMinutes(5).millis, value: 390 + randomList[7]]
    ]

    // insert data points
    response = hawkularMetrics.post(path: "counters/raw", body: [
        [
            id: 'C1',
            data: c1
        ],
        [
            id: 'C2',
            data: c2
        ],
        [
            id: 'C3',
            data: [
                [timestamp: start.millis, value: 5712],
                [timestamp: start.plusMinutes(1).millis, value: 5773],
                [timestamp: start.plusMinutes(2).millis, value: 5949],
                [timestamp: start.plusMinutes(3).millis, value: 5979],
                [timestamp: start.plusMinutes(4).millis, value: 6548]
            ]
        ]
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)

    //Tests start here
    response = hawkularMetrics.get(
        path: 'counters/stats',
        query: [
            start: start.millis,
            end: start.plusMinutes(5).millis,
            buckets: 5,
            tags: 'type:counter_cpu_usage,host:server1|server2',
            stacked: true
        ],
        headers: [(tenantHeaderName): tenantId]
    )
    assertContentEncoding(response)
    assertEquals(200, response.status)

    assertEquals("Expected to get back five buckets", 5, response.data.size())

    def actualCounterBucketsByTag = response.data

    assertStackedDataPointBucket(actualCounterBucketsByTag[0], start.millis, start.plusMinutes(1).millis, c1[0])
    assertStackedDataPointBucket(actualCounterBucketsByTag[1], start.plusMinutes(1).millis, start.plusMinutes(2).millis, c1[1], c2[0])
    assertStackedDataPointBucket(actualCounterBucketsByTag[2], start.plusMinutes(2).millis, start.plusMinutes(3).millis, c1[2])
    assertStackedDataPointBucket(actualCounterBucketsByTag[3], start.plusMinutes(3).millis, start.plusMinutes(4).millis, c1[3], c2[1])
    assertStackedDataPointBucket(actualCounterBucketsByTag[4], start.plusMinutes(4).millis, start.plusMinutes(5).millis)

    response = hawkularMetrics.get(
        path: 'counters/stats',
        query: [
            start: start.millis,
            end: start.plusMinutes(5).millis,
            buckets: 5,
            metrics: ['C1', 'C2'],
            stacked: true
        ],
        headers: [(tenantHeaderName): tenantId]
    )
    assertContentEncoding(response)
    assertEquals(200, response.status)

    def actualCounterBucketsById = response.data

    assertEquals("Expected to get back five buckets", 5, response.data.size())
    assertEquals("Stacked stats when queried by tag are different than when queried by id", actualCounterBucketsById, actualCounterBucketsByTag)
  }

  @Test
  void findSimpleStatsForMultipleCounters() {
    String tenantId = nextTenantId()
    DateTime start = now().minusMinutes(10)

    // Create some metrics
    def response = hawkularMetrics.post(path: 'counters', body: [
        id  : 'C1',
        tags: ['type': 'counter_cpu_usage', 'host': 'server1', 'env': 'stage']
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(201, response.status)

    response = hawkularMetrics.post(path: 'counters', body: [
        id  : 'C2',
        tags: ['type': 'counter_cpu_usage', 'host': 'server2', 'env': 'dev']
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(201, response.status)

    response = hawkularMetrics.post(path: 'counters', body: [
        id  : 'C3',
        tags: ['type': 'counter_cpu_usage', 'host': 'server3', 'env': 'stage']
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(201, response.status)

    Random rand = new Random()
    def randomList = []
    (1..10).each {
      randomList << rand.nextInt(100)
    }
    randomList.sort()

    def c1 = [
      [timestamp: start.millis, value: 510 + randomList[0]],
      [timestamp: start.plusMinutes(1).millis, value: 512 + randomList[1]],
      [timestamp: start.plusMinutes(2).millis, value: 514 + randomList[2]],
      [timestamp: start.plusMinutes(3).millis, value: 516 + randomList[3]],
      [timestamp: start.plusMinutes(4).millis, value: 518 + randomList[4]]
    ]
    def c2 = [
      [timestamp: start.millis, value: 378 + randomList[5]],
      [timestamp: start.plusMinutes(1).millis, value: 381 + randomList[6]],
      [timestamp: start.plusMinutes(2).millis, value: 384 + randomList[7]],
      [timestamp: start.plusMinutes(3).millis, value: 387 + randomList[8]],
      [timestamp: start.plusMinutes(4).millis, value: 390 + randomList[9]]
    ]

    // insert data points
    response = hawkularMetrics.post(path: "counters/raw", body: [
        [
            id: 'C1',
            data: c1
        ],
        [
            id: 'C2',
            data: c2
        ],
        [
            id: 'C3',
            data: [
                [timestamp: start.millis, value: 5712],
                [timestamp: start.plusMinutes(1).millis, value: 5773],
                [timestamp: start.plusMinutes(2).millis, value: 5949],
                [timestamp: start.plusMinutes(3).millis, value: 5979],
                [timestamp: start.plusMinutes(4).millis, value: 6548]
            ]
        ]
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)

    c1 = c1.take(c1.size() - 1)
    c2 = c2.take(c2.size() - 1)
    def combinedData = c1 + c2;

    //Get counter rates
    response = hawkularMetrics.get(
        path: 'counters/C1/rate/stats',
        query: [
            start: start.millis,
            end: start.plusMinutes(4).millis,
            buckets: 1,
        ],
        headers: [(tenantHeaderName): tenantId]
    )
    assertContentEncoding(response)
    def c1Rates = response.data[0];

    response = hawkularMetrics.get(
        path: 'counters/C2/rate/stats',
        query: [
            start: start.millis,
            end: start.plusMinutes(4).millis,
            buckets: 1,
        ],
        headers: [(tenantHeaderName): tenantId]
    )
    assertContentEncoding(response)
    def c2Rates = response.data[0];

    //Tests start here
    response = hawkularMetrics.get(
        path: 'counters/stats',
        query: [
            start: start.millis,
            end: start.plusMinutes(4).millis,
            buckets: 1,
            tags: 'type:counter_cpu_usage,host:server1|server2'
        ],
        headers: [(tenantHeaderName): tenantId]
    )
    assertEquals(200, response.status)
    assertContentEncoding(response)

    assertEquals("Expected to get back one bucket", 1, response.data.size())

    def expectedSimpleCounterBucketByTag = response.data[0]

    assertEquals("The start time is wrong", start.millis, expectedSimpleCounterBucketByTag.start)
    assertEquals("The end time is wrong", start.plusMinutes(4).millis, expectedSimpleCounterBucketByTag.end)
    assertDoubleEquals("The min is wrong", (combinedData.min {it.value}).value, expectedSimpleCounterBucketByTag.min)
    assertDoubleEquals("The max is wrong", (combinedData.max {it.value}).value, expectedSimpleCounterBucketByTag.max)
    assertDoubleEquals("The sum is wrong", combinedData.sum() { it.value }, expectedSimpleCounterBucketByTag.sum)
    assertDoubleEquals("The avg is wrong", avg(combinedData.collect {it.value}), expectedSimpleCounterBucketByTag.avg)
    assertEquals("The [empty] property is wrong", false, expectedSimpleCounterBucketByTag.empty)
    assertTrue("Expected the [median] property to be set", expectedSimpleCounterBucketByTag.median != null)

    response = hawkularMetrics.get(
        path: 'counters/stats',
        query: [
            start: start.millis,
            end: start.plusMinutes(4).millis,
            buckets: 1,
            metrics: ['C2', 'C1']
        ],
        headers: [(tenantHeaderName): tenantId]
    )
    assertEquals(200, response.status)
    assertContentEncoding(response)

    def actualSimpleCounterBucketById = response.data[0]

    assertEquals("Expected to get back one bucket", 1, response.data.size())
    assertEquals("The start time is wrong", start.millis, actualSimpleCounterBucketById.start)
    assertEquals("The end time is wrong", start.plusMinutes(4).millis, actualSimpleCounterBucketById.end)
    assertDoubleEquals("The min is wrong", (combinedData.min {it.value}).value, actualSimpleCounterBucketById.min)
    assertDoubleEquals("The max is wrong", (combinedData.max {it.value}).value, actualSimpleCounterBucketById.max)
    assertDoubleEquals("The sum is wrong", combinedData.sum() { it.value }, expectedSimpleCounterBucketByTag.sum)
    assertDoubleEquals("The avg is wrong", avg(combinedData.collect {it.value}), actualSimpleCounterBucketById.avg)
    assertEquals("The [empty] property is wrong", false, actualSimpleCounterBucketById.empty)
    assertTrue("Expected the [median] property to be set", actualSimpleCounterBucketById.median != null)
  }

  @Test
  void findStackedStatsForMultipleCounterRates() {
    String tenantId = nextTenantId()
    DateTime start = now().minusMinutes(10)

    // Create some metrics
    def response = hawkularMetrics.post(path: 'counters', body: [
        id  : 'C1',
        tags: ['type': 'counter_cpu_usage', 'host': 'server1', 'env': 'stage']
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(201, response.status)

    response = hawkularMetrics.post(path: 'counters', body: [
        id  : 'C2',
        tags: ['type': 'counter_cpu_usage', 'host': 'server2', 'env': 'dev']
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(201, response.status)

    response = hawkularMetrics.post(path: 'counters', body: [
        id  : 'C3',
        tags: ['type': 'counter_cpu_usage', 'host': 'server3', 'env': 'stage']
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(201, response.status)

    Random rand = new Random()
    def randomList = []
    (1..10).each {
      randomList << rand.nextInt(100)
    }
    randomList.sort()

    def c1 = [
      [timestamp: start.millis, value: 510 + randomList[0]],
      [timestamp: start.plusMinutes(1).millis, value: 512 + randomList[1]],
      [timestamp: start.plusMinutes(2).millis, value: 514 + randomList[2]],
      [timestamp: start.plusMinutes(3).millis, value: 516 + randomList[3]],
      [timestamp: start.plusMinutes(4).millis, value: 518 + randomList[4]]
    ]
    def c2 = [
      [timestamp: start.millis, value: 378 + randomList[5]],
      [timestamp: start.plusMinutes(1).millis, value: 381 + randomList[6]],
      [timestamp: start.plusMinutes(2).millis, value: 384 + randomList[7]],
      [timestamp: start.plusMinutes(3).millis, value: 387 + randomList[8]],
      [timestamp: start.plusMinutes(4).millis, value: 390 + randomList[9]]
    ]

    // insert data points
    response = hawkularMetrics.post(path: "counters/raw", body: [
        [
            id: 'C1',
            data: c1
        ],
        [
            id: 'C2',
            data: c2
        ],
        [
            id: 'C3',
            data: [
                [timestamp: start.millis, value: 5712],
                [timestamp: start.plusMinutes(1).millis, value: 5773],
                [timestamp: start.plusMinutes(2).millis, value: 5949],
                [timestamp: start.plusMinutes(3).millis, value: 5979],
                [timestamp: start.plusMinutes(4).millis, value: 6548]
            ]
        ]
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)

    c1 = c1.take(c1.size() - 1)
    c2 = c2.take(c2.size() - 1)
    def combinedData = c1 + c2;

    //Get counter rates
    response = hawkularMetrics.get(
        path: 'counters/C1/rate/stats',
        query: [
            start: start.millis,
            end: start.plusMinutes(4).millis,
            buckets: 1,
        ],
        headers: [(tenantHeaderName): tenantId]
    )
    assertContentEncoding(response)
    def c1Rates = response.data[0];

    response = hawkularMetrics.get(
        path: 'counters/C2/rate/stats',
        query: [
            start: start.millis,
            end: start.plusMinutes(4).millis,
            buckets: 1,
        ],
        headers: [(tenantHeaderName): tenantId]
    )
    assertContentEncoding(response)
    def c2Rates = response.data[0];

    //Tests start here
    response = hawkularMetrics.get(
        path: 'counters/rate/stats',
        query: [
            start: start.millis,
            end: start.plusMinutes(4).millis,
            buckets: 1,
            tags: 'type:counter_cpu_usage,host:server1|server2',
            stacked: true
        ],
        headers: [(tenantHeaderName): tenantId]
    )
    assertEquals(200, response.status)
    assertContentEncoding(response)

    assertEquals("Expected to get back one bucket", 1, response.data.size())

    def actualCounterRateBucketByTag = response.data[0]

    assertEquals("The start time is wrong", start.millis, actualCounterRateBucketByTag.start)
    assertEquals("The end time is wrong", start.plusMinutes(4).millis, actualCounterRateBucketByTag.end)
    assertDoubleEquals("The min is wrong", (c1Rates.min + c2Rates.min), actualCounterRateBucketByTag.min)
    assertDoubleEquals("The max is wrong", (c1Rates.max + c2Rates.max), actualCounterRateBucketByTag.max)
    assertDoubleEquals("The sum is wrong", c1Rates.sum + c2Rates.sum, actualCounterRateBucketByTag.sum)
    assertDoubleEquals("The avg is wrong", (c1Rates.avg + c2Rates.avg), actualCounterRateBucketByTag.avg)
    assertEquals("The [empty] property is wrong", false, actualCounterRateBucketByTag.empty)
    assertTrue("Expected the [median] property to be set", actualCounterRateBucketByTag.median != null)

    response = hawkularMetrics.get(
        path: 'counters/rate/stats',
        query: [
            start: start.millis,
            end: start.plusMinutes(4).millis,
            buckets: 1,
            metrics: ['C2', 'C1'],
            stacked: true
        ],
        headers: [(tenantHeaderName): tenantId]
    )
    assertEquals(200, response.status)
    assertContentEncoding(response)

    assertEquals("Expected to get back one bucket", 1, response.data.size())

    def actualCounterRateBucketById = response.data[0]

    assertEquals("Expected to get back one bucket", 1, response.data.size())
    assertEquals("The start time is wrong", start.millis, actualCounterRateBucketById.start)
    assertEquals("The end time is wrong", start.plusMinutes(4).millis, actualCounterRateBucketById.end)
    assertDoubleEquals("The min is wrong", (c1Rates.min + c2Rates.min), actualCounterRateBucketById.min)
    assertDoubleEquals("The max is wrong", (c1Rates.max + c2Rates.max), actualCounterRateBucketById.max)
    assertDoubleEquals("The sum is wrong", c1Rates.sum + c2Rates.sum, actualCounterRateBucketById.sum)
    assertDoubleEquals("The avg is wrong", (c1Rates.avg + c2Rates.avg), actualCounterRateBucketById.avg)
    assertEquals("The [empty] property is wrong", false, actualCounterRateBucketById.empty)
    assertTrue("Expected the [median] property to be set", actualCounterRateBucketById.median != null)
  }

  @Test
  void findSimpleStatsForMultipleCounterRates() {
    String tenantId = nextTenantId()
    DateTime start = now().minusMinutes(10)

    // Create some metrics
    def response = hawkularMetrics.post(path: 'counters', body: [
        id  : 'C1',
        tags: ['type': 'counter_cpu_usage', 'host': 'server1', 'env': 'stage']
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(201, response.status)

    response = hawkularMetrics.post(path: 'counters', body: [
        id  : 'C2',
        tags: ['type': 'counter_cpu_usage', 'host': 'server2', 'env': 'dev']
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(201, response.status)

    response = hawkularMetrics.post(path: 'counters', body: [
        id  : 'C3',
        tags: ['type': 'counter_cpu_usage', 'host': 'server3', 'env': 'stage']
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(201, response.status)

    Random rand = new Random()
    def randomList = []
    (1..10).each {
      randomList << rand.nextInt(100)
    }
    randomList.sort()

    def c1 = [
      [timestamp: start.millis, value: 510 + randomList[0]],
      [timestamp: start.plusMinutes(1).millis, value: 512 + randomList[1]],
      [timestamp: start.plusMinutes(2).millis, value: 514 + randomList[2]],
      [timestamp: start.plusMinutes(3).millis, value: 516 + randomList[3]],
      [timestamp: start.plusMinutes(4).millis, value: 518 + randomList[4]]
    ]
    def c2 = [
      [timestamp: start.millis, value: 378 + randomList[5]],
      [timestamp: start.plusMinutes(1).millis, value: 381 + randomList[6]],
      [timestamp: start.plusMinutes(2).millis, value: 384 + randomList[7]],
      [timestamp: start.plusMinutes(3).millis, value: 387 + randomList[8]],
      [timestamp: start.plusMinutes(4).millis, value: 390 + randomList[9]]
    ]

    // insert data points
    response = hawkularMetrics.post(path: "counters/raw", body: [
        [
            id: 'C1',
            data: c1
        ],
        [
            id: 'C2',
            data: c2
        ],
        [
            id: 'C3',
            data: [
                [timestamp: start.millis, value: 5712],
                [timestamp: start.plusMinutes(1).millis, value: 5773],
                [timestamp: start.plusMinutes(2).millis, value: 5949],
                [timestamp: start.plusMinutes(3).millis, value: 5979],
                [timestamp: start.plusMinutes(4).millis, value: 6548]
            ]
        ]
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)

    c1 = c1.take(c1.size() - 1)
    c2 = c2.take(c2.size() - 1)
    def combinedData = c1 + c2;

    //Get counter rates
    response = hawkularMetrics.get(
        path: 'counters/C1/rate/stats',
        query: [
            start: start.millis,
            end: start.plusMinutes(4).millis,
            buckets: 1,
        ],
        headers: [(tenantHeaderName): tenantId]
    )
    assertContentEncoding(response)
    def c1Rates = response.data[0];

    response = hawkularMetrics.get(
        path: 'counters/C2/rate/stats',
        query: [
            start: start.millis,
            end: start.plusMinutes(4).millis,
            buckets: 1,
        ],
        headers: [(tenantHeaderName): tenantId]
    )
    assertContentEncoding(response)
    def c2Rates = response.data[0];

    //Tests start here
    response = hawkularMetrics.get(
        path: 'counters/rate/stats',
        query: [
            start: start.millis,
            end: start.plusMinutes(4).millis,
            buckets: 1,
            tags: 'type:counter_cpu_usage,host:server1|server2',
            stacked: false
        ],
        headers: [(tenantHeaderName): tenantId]
    )
    assertEquals(200, response.status)
    assertContentEncoding(response)

    assertEquals("Expected to get back one bucket", 1, response.data.size())

    def actualCounterRateBucketByTag = response.data[0]

    assertEquals("The start time is wrong", start.millis, actualCounterRateBucketByTag.start)
    assertEquals("The end time is wrong", start.plusMinutes(4).millis, actualCounterRateBucketByTag.end)
    assertDoubleEquals("The min is wrong", Math.min(c1Rates.min, c2Rates.min), actualCounterRateBucketByTag.min)
    assertDoubleEquals("The max is wrong", Math.max(c1Rates.max, c2Rates.max), actualCounterRateBucketByTag.max)
    assertDoubleEquals("The sum is wrong", c1Rates.sum + c2Rates.sum, actualCounterRateBucketByTag.sum)
    assertDoubleEquals("The avg is wrong", (c1Rates.avg + c2Rates.avg)/2, actualCounterRateBucketByTag.avg)
    assertEquals("The [empty] property is wrong", false, actualCounterRateBucketByTag.empty)
    assertTrue("Expected the [median] property to be set", actualCounterRateBucketByTag.median != null)

    response = hawkularMetrics.get(
        path: 'counters/rate/stats',
        query: [
            start: start.millis,
            end: start.plusMinutes(4).millis,
            buckets: 1,
            metrics: ['C2', 'C1'],
            stacked: false
        ],
        headers: [(tenantHeaderName): tenantId]
    )
    assertEquals(200, response.status)
    assertContentEncoding(response)

    assertEquals("Expected to get back one bucket", 1, response.data.size())

    def actualCounterRateBucketById = response.data[0]

    assertEquals("Expected to get back one bucket", 1, response.data.size())
    assertEquals("The start time is wrong", start.millis, actualCounterRateBucketById.start)
    assertEquals("The end time is wrong", start.plusMinutes(4).millis, actualCounterRateBucketById.end)
    assertDoubleEquals("The min is wrong", Math.min(c1Rates.min, c2Rates.min), actualCounterRateBucketById.min)
    assertDoubleEquals("The max is wrong", Math.max(c1Rates.max, c2Rates.max), actualCounterRateBucketById.max)
    assertDoubleEquals("The avg is wrong", (c1Rates.avg + c2Rates.avg)/2, actualCounterRateBucketById.avg)
    assertEquals("The [empty] property is wrong", false, actualCounterRateBucketById.empty)
    assertTrue("Expected the [median] property to be set", actualCounterRateBucketById.median != null)
  }

  @Test
  void fromEarliestWithData() {
    String tenantId = nextTenantId()
    String metric = "testStats"

    def response = hawkularMetrics.post(path: 'counters', body: [
        id  : '$metric',
        tags: ['type': 'counter_cpu_usage', 'host': 'server1', 'env': 'stage']
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(201, response.status)

    response = hawkularMetrics.post(path: "counters/$metric/raw", body: [
        [timestamp: new DateTimeService().currentHour().minusHours(2).millis, value: 2]
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)

    response = hawkularMetrics.post(path: "counters/$metric/raw", body: [
        [timestamp: new DateTimeService().currentHour().minusHours(3).millis, value: 3]
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)

    response = hawkularMetrics.get(path: "counters/$metric/stats",
        query: [fromEarliest: "true", bucketDuration: "1h"], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertContentEncoding(response)
    assertEquals(4, response.data.size)

    def expectedArray = [3.0, 2.0, null, null].toArray()
    assertArrayEquals(expectedArray, response.data.min.toArray())
    assertArrayEquals(expectedArray, response.data.max.toArray())
    assertArrayEquals(expectedArray, response.data.avg.toArray())
  }

  @Test
  void fromEarliestWithoutDataAndBad() {
    String tenantId = nextTenantId()
    String metric = "testStats"

    def response = hawkularMetrics.post(path: 'counters', body: [
        id  : '$metric',
        tags: ['type': 'counter_cpu_usage', 'host': 'server1', 'env': 'stage']
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(201, response.status)

    response = hawkularMetrics.get(path: "counters/$metric/stats",
      query: [start: 1, end: now().millis, bucketDuration: "1000d"], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertContentEncoding(response)

    badGet(path: "counters/$metric/stats",
        query: [fromEarliest: "true", bucketDuration: "a"], headers: [(tenantHeaderName): tenantId]) {
        exception ->
          assertEquals(400, exception.response.status)
    }

    response = hawkularMetrics.get(path: "counters/$metric/stats",
        query: [fromEarliest: "true", bucketDuration: "1h"], headers: [(tenantHeaderName): tenantId])
    assertEquals(204, response.status)
    assertEquals(null, response.data)
  }

  @Test
  void addTaggedDataPoints() {
    String tenantId = nextTenantId()
    DateTime start = now().minusMinutes(30)
    String id = 'C1'

    def response = hawkularMetrics.post(
        path: "counters/$id/raw",
        headers: [(tenantHeaderName): tenantId],
        body: [
            [
                timestamp: start.millis,
                value: 11,
                tags: [x: 1, y: 2]
            ],
            [
                timestamp: start.plusMinutes(1).millis,
                value: 20,
                tags: [y: 3, z: 5]
            ],
            [
                timestamp: start.plusMinutes(3).millis,
                value: 33,
                tags: [x: 4, z: 6]
            ]
        ]
    )
    assertEquals(200, response.status)
    response = hawkularMetrics.get(path: "counters/$id/raw", headers: [(tenantHeaderName): tenantId])
    assertContentEncoding(response)
    def expectedData = [
        [
            timestamp: start.plusMinutes(3).millis,
            value: 33,
            tags: [x: '4', z: '6']
        ],
        [
            timestamp: start.plusMinutes(1).millis,
            value: 20,
            tags: [y: '3', z: '5']
        ],
        [
            timestamp: start.millis,
            value: 11,
            tags: [x: '1', y: '2']
        ]
    ]
    assertEquals(expectedData, response.data)
  }

  @Test
  void findTaggedDataPointsWithMultipleTagFilters() {
    String tenantId = nextTenantId()
    DateTime start = now().minusHours(2)
    String id = 'C1'

    def response = hawkularMetrics.post(
        path: "counters/$id/raw",
        headers: [(tenantHeaderName): tenantId],
        body: [
            [
                timestamp: start.millis,
                value: 11,
                tags: [x: '1', y: '1', z: '1']
            ],
            [
                timestamp: start.plusMinutes(2).millis,
                value: 13,
                tags: [x: '2', y: '2', z: '2']
            ],
            [
                timestamp: start.plusMinutes(4).millis,
                value: 14,
                tags: [x: '3', y: '2', z: '3']
            ],
            [
                timestamp: start.plusMinutes(6).millis,
                value: 15,
                tags: [x: '1', y: '3', z: '4']
            ]
        ]
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.get(
        path: "counters/$id/stats/tags/x:*,y:2,z:2|3", headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertContentEncoding(response)
    assertEquals(2, response.data.size())

    def expectedData = [
        'x:2,y:2,z:2': [
            tags: [x: '2', y: '2', z: '2'],
            max: 13,
            min: 13,
            sum: 13,
            avg: 13,
            median: 13,
            samples: 1
        ],
        'x:3,y:2,z:3': [
            tags: [x: '3', y: '2', z: '3'],
            max: 14,
            min: 14,
            sum: 14,
            avg: 14,
            median: 14,
            samples: 1
        ]
    ]
    assertTaggedBucketEquals(expectedData['x:2,y:2,z:2'], response.data['x:2,y:2,z:2'])
    assertTaggedBucketEquals(expectedData['x:3,y:2,z:3'], response.data['x:3,y:2,z:3'])
  }

  @Test
  void minMaxTimestamps() {
    def tenantId = nextTenantId()
    def metricId = 'minmaxtest'

    def response = hawkularMetrics.post(path: 'counters', headers: [(tenantHeaderName): tenantId], body: [
        id: metricId
    ])
    assertEquals(201, response.status)

    response = hawkularMetrics.get(path: "counters/${metricId}", query: [timestamps: "true"], headers: [
            (tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertFalse("Metric should not have the minTimestamp attribute: ${response.data}", response.data.containsKey('minTimestamp'))
    assertFalse("Metric should not have the maxTimestamp attribute: ${response.data}", response.data.containsKey('maxTimestamp'))

    response = hawkularMetrics.post(path: "counters/${metricId}/raw", headers: [(tenantHeaderName): tenantId], body: [
        [timestamp: 3, value: 4.2]
    ])
    assertEquals(200, response.status)

    response = hawkularMetrics.get(path: "counters/${metricId}", headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertContentEncoding(response)
    assertEquals(3, response.data.minTimestamp)
    assertEquals(3, response.data.maxTimestamp)

    response = hawkularMetrics.post(path: "counters/${metricId}/raw", headers: [(tenantHeaderName): tenantId], body: [
        [timestamp: 1, value: 2.2],
        [timestamp: 2, value: 1.2],
        [timestamp: 4, value: 7.2],
    ])
    assertEquals(200, response.status)

    response = hawkularMetrics.get(path: "counters/${metricId}", headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertContentEncoding(response)
    assertEquals(1, response.data.minTimestamp)
    assertEquals(4, response.data.maxTimestamp)

    response = hawkularMetrics.get(path: "counters", query: [timestamps: "true"], headers: [(tenantHeaderName):
                                                                                                   tenantId])
    assertEquals(200, response.status)
    assertContentEncoding(response)
    def metric = (response.data as List).find { it.id.equals(metricId) }
    assertEquals(1, metric.minTimestamp)
    assertEquals(4, metric.maxTimestamp)

    response = hawkularMetrics.get(path: "metrics", query: [timestamps: "true"], headers: [(tenantHeaderName):
                                                                                                   tenantId])
    assertEquals(200, response.status)
    assertContentEncoding(response)
    metric = (response.data as List).find { it.id.equals(metricId) }
    assertEquals(1, metric.minTimestamp)
    assertEquals(4, metric.maxTimestamp)

  }

  @Test
  void fetchRawDataFromMultipleCounters() {
    String tenantId = nextTenantId()
    DateTime start = DateTime.now().minusHours(2)

    def response = hawkularMetrics.post(
        path: "counters/raw",
        headers: [(tenantHeaderName): tenantId],
        body: [
            [
                id: 'C1',
                data: [
                    [timestamp: start.millis, value: 12],
                    [timestamp: start.plusMinutes(1).millis, value: 17]
                ]
            ],
            [
                id: 'C2',
                data: [
                    [timestamp: start.millis, value: 21],
                    [timestamp: start.plusMinutes(1).millis, value: 33]
                ]
            ],
            [
                id: 'C3',
                data: [
                    [timestamp: start.millis, value: 55],
                    [timestamp: start.plusMinutes(1).millis, value: 58]
                ]
            ]
        ]
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.post(
        path: "counters/raw/query",
        headers: [(tenantHeaderName): tenantId],
        body: [ids: ['C1', 'C2', 'C3']]
    )
    assertEquals(200, response.status)
    assertEquals(3, response.data.size)

    assertTrue(response.data.contains([
        id: 'C1',
        data: [
            [timestamp: start.plusMinutes(1).millis, value: 17],
            [timestamp: start.millis, value: 12]
        ]
    ]))

    assertTrue(response.data.contains([
        id: 'C2',
        data: [
            [timestamp: start.plusMinutes(1).millis, value: 33],
            [timestamp: start.millis, value: 21]
        ]
    ]))

    assertTrue(response.data.contains([
        id: 'C3',
        data: [
            [timestamp: start.plusMinutes(1).millis, value: 58],
            [timestamp: start.millis, value: 55]
        ]
    ]))
  }

  @Test
  void fetchMRawDataFromMultipleCountersWithQueryParams() {
    String tenantId = nextTenantId()
    DateTime start = DateTime.now().minusHours(4)

    def response = hawkularMetrics.post(
        path: "counters/raw",
        headers: [(tenantHeaderName): tenantId],
        body: [
            [
                id: 'C1',
                data: [
                    [timestamp: start.millis, value: 12],
                    [timestamp: start.plusHours(1).millis, value: 17],
                    [timestamp: start.plusHours(2).millis, value: 19],
                    [timestamp: start.plusHours(3).millis, value: 26],
                    [timestamp: start.plusHours(4).millis, value: 37]
                ]
            ],
            [
                id: 'C2',
                data: [
                    [timestamp: start.millis, value: 41],
                    [timestamp: start.plusHours(1).millis, value: 49],
                    [timestamp: start.plusHours(2).millis, value: 64],
                    [timestamp: start.plusHours(3).millis, value: 71],
                    [timestamp: start.plusHours(4).millis, value: 95]
                ]
            ],
            [
                id: 'C3',
                data: [
                    [timestamp: start.millis, value: 28],
                    [timestamp: start.plusHours(1).millis, value: 35],
                    [timestamp: start.plusHours(2).millis, value: 42],
                    [timestamp: start.plusHours(3).millis, value: 49],
                    [timestamp: start.plusHours(4).millis, value: 59]
                ]
            ]
        ]
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.post(
        path: "counters/raw/query",
        headers: [(tenantHeaderName): tenantId],
        body: [
            ids: ['C1', 'C2', 'C3'],
            start: start.plusHours(1).millis,
            end: start.plusHours(4).millis,
            limit: 2,
            order: 'desc'
        ]
    )

    assertEquals(200, response.status)

    assertEquals(3, response.data.size)

    assertTrue(response.data.contains([
        id: 'C1',
        data: [
            [timestamp: start.plusHours(3).millis, value: 26],
            [timestamp: start.plusHours(2).millis, value: 19]
        ]
    ]))

    assertTrue(response.data.contains([
        id: 'C2',
        data: [
            [timestamp: start.plusHours(3).millis, value: 71],
            [timestamp: start.plusHours(2).millis, value: 64]
        ]
    ]))

    assertTrue(response.data.contains([
        id: 'C3',
        data: [
            [timestamp: start.plusHours(3).millis, value: 49],
            [timestamp: start.plusHours(2).millis, value: 42]
        ]
    ]))

    // From Earliest
    response = hawkularMetrics.post(
        path: "counters/raw/query",
        headers: [(tenantHeaderName): tenantId],
        body: [
            ids: ['C1', 'C2', 'C3'],
            fromEarliest: true,
            order: 'asc'
        ]
    )

    assertEquals(200, response.status)
    assertEquals(3, response.data.size)
    assertTrue(response.data.contains([
        id: 'C1',
        data: [
            [timestamp: start.millis, value: 12],
            [timestamp: start.plusHours(1).millis, value: 17],
            [timestamp: start.plusHours(2).millis, value: 19],
            [timestamp: start.plusHours(3).millis, value: 26],
            [timestamp: start.plusHours(4).millis, value: 37]
        ]
    ]))

    assertTrue(response.data.contains([
        id: 'C2',
        data: [
            [timestamp: start.millis, value: 41],
            [timestamp: start.plusHours(1).millis, value: 49],
            [timestamp: start.plusHours(2).millis, value: 64],
            [timestamp: start.plusHours(3).millis, value: 71],
            [timestamp: start.plusHours(4).millis, value: 95]
        ]
    ]))

    assertTrue(response.data.contains([
        id: 'C3',
        data: [
            [timestamp: start.millis, value: 28],
            [timestamp: start.plusHours(1).millis, value: 35],
            [timestamp: start.plusHours(2).millis, value: 42],
            [timestamp: start.plusHours(3).millis, value: 49],
            [timestamp: start.plusHours(4).millis, value: 59]
        ]
    ]))
  }

  @Test
  void fetchRatesFromMultipleMetrics() {
    String tenantId = nextTenantId()
    def dataPoints = [
        [
            id: 'C1',
            data: [
                [timestamp: 60_000, value: 12],
                [timestamp: 60_000 * 1.5, value: 34],
                [timestamp: 60_000 * 2, value: 53],
                [timestamp: 60_000 * 2.5, value: 72],
                [timestamp: 60_000 * 3, value: 102]
            ]
        ],
        [
            id: 'C2',
            data: [
                [timestamp: 60_000, value: 14],
                [timestamp: 60_000 * 1.5, value: 26],
                [timestamp: 60_000 * 2, value: 51],
                [timestamp: 60_000 * 2.5, value: 88],
                [timestamp: 60_000 * 3, value: 109]
            ]
        ],
        [
            id: 'C3',
            data: [
                [timestamp: 60_000, value: 43],
                [timestamp: 60_000 * 1.5, value: 48],
                [timestamp: 60_000 * 2, value: 73],
                [timestamp: 60_000 * 2.5, value: 89],
                [timestamp: 60_000 * 3, value: 99]
            ]
        ]
    ]

    def response = hawkularMetrics.post(
        path: "counters/raw",
        headers: [(tenantHeaderName): tenantId],
        body: dataPoints
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.post(
        path: "counters/rate/query",
        headers: [(tenantHeaderName): tenantId],
        body: [
            ids: ['C1', 'C2', 'C3'],
            start: 60_000 * 1.5,
            end: 60_000 * 3,
            limit: 2,
            order: 'asc'
        ]
    )

    assertEquals(200, response.status)
    assertEquals(3, response.data.size())

    assertListOfGaugesContains(response.data, [
        id: 'C1',
        data: [
            [timestamp: 60_000 * 2, value: rate(dataPoints[0].data[2], dataPoints[0].data[1])],
            [timestamp: 60_000 * 2.5, value: rate(dataPoints[0].data[3], dataPoints[0].data[2])]
        ]
    ])

    assertListOfGaugesContains(response.data, [
        id: 'C2',
        data: [
            [timestamp: 60_000 * 2, value: rate(dataPoints[1].data[2], dataPoints[1].data[1])],
            [timestamp: 60_000 * 2.5, value: rate(dataPoints[1].data[3], dataPoints[1].data[2])]
        ]
    ])

    assertListOfGaugesContains(response.data, [
        id: 'C3',
        data: [
            [timestamp: 60_000 * 2, value: rate(dataPoints[2].data[2], dataPoints[2].data[1])],
            [timestamp: 60_000 * 2.5, value: rate(dataPoints[2].data[3], dataPoints[2].data[2])]
        ]
    ])
  }

  @Test
  void fetchRatesFromEarliest() {
    long start = DateTime.now().minusHours(4).getMillis()
    String tenantId = nextTenantId()
    def dataPoints = [
        [
            id: 'C1',
            data: [
                [timestamp: start + 60_000, value: 12],
                [timestamp: start + 60_000 * 1.5, value: 34],
                [timestamp: start + 60_000 * 2, value: 53]
            ]
        ],
        [
            id: 'C2',
            data: [
                [timestamp: start + 60_000, value: 14],
                [timestamp: start + 60_000 * 1.5, value: 26],
                [timestamp: start + 60_000 * 2, value: 51]
            ]
        ]
    ]

    def response = hawkularMetrics.post(
        path: "counters/raw",
        headers: [(tenantHeaderName): tenantId],
        body: dataPoints
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.post(
        path: "counters/rate/query",
        headers: [(tenantHeaderName): tenantId],
        body: [
            ids: ['C1', 'C2'],
            fromEarliest: true,
            order: 'desc'
        ]
    )

    assertEquals(200, response.status)
    assertEquals(2, response.data.size())

    def timestamp1 = start + 60_000 * 1.5
    def timestamp2 = start + 60_000 * 2
    def rate1_1 = rate(dataPoints[0].data[1], dataPoints[0].data[0])
    def rate1_2 = rate(dataPoints[0].data[2], dataPoints[0].data[1])
    def rate2_1 = rate(dataPoints[1].data[1], dataPoints[1].data[0])
    def rate2_2 = rate(dataPoints[1].data[2], dataPoints[1].data[1])
    assertListOfGaugesContains(response.data, [
        id: 'C1',
        data: [
            [timestamp: timestamp2, value: rate1_2],
            [timestamp: timestamp1, value: rate1_1]
        ]
    ])

    assertListOfGaugesContains(response.data, [
        id: 'C2',
        data: [
            [timestamp: timestamp2, value: rate2_2],
            [timestamp: timestamp1, value: rate2_1]
        ]
    ])
  }

  static void assertListOfGaugesContains(data, expected) {
    def actual = data.find { it.id == expected.id }
    printJson(actual)
    assertNotNull(actual)
    assertEquals(expected.id, actual.id)
    assertEquals(expected.data.size(), actual.data.size())
    expected.data.eachWithIndex { expectedDataPoint, index ->
      def actualDataPoint = actual.data[index]
      assertEquals(expectedDataPoint.timestamp as Long, actualDataPoint.timestamp as Long)
      assertDoubleEquals(expectedDataPoint.value, actualDataPoint.value)
    }
  }

  @Test
  void fetchMRawDataFromMultipleCountersByTag() {
    String tenantId = nextTenantId()
    DateTime start = now().minusHours(4)

    // Define tags
    def response = hawkularMetrics.post(path: 'counters', body: [id: 'A1', tags: [letter: 'A', number: '1']],
            headers: [(tenantHeaderName): tenantId])
    assertEquals(201, response.status)
    response = hawkularMetrics.post(path: 'counters', body: [id: 'A2', tags: [letter: 'A', number: '2']],
            headers: [(tenantHeaderName): tenantId])
    assertEquals(201, response.status)

    response = hawkularMetrics.post(
            path: "counters/raw",
            headers: [(tenantHeaderName): tenantId],
            body: [
                    [
                            id: 'A1',
                            data: [
                                    [timestamp: start.millis, value: 10],
                                    [timestamp: start.plusHours(1).millis, value: 20],
                                    [timestamp: start.plusHours(2).millis, value: 30],
                                    [timestamp: start.plusHours(3).millis, value: 20],
                                    [timestamp: start.plusHours(4).millis, value: 10]
                            ]
                    ],
                    [
                            id: 'A2',
                            data: [
                                    [timestamp: start.millis, value: 1],
                                    [timestamp: start.plusHours(1).millis, value: 0],
                                    [timestamp: start.plusHours(2).millis, value: 1],
                                    [timestamp: start.plusHours(3).millis, value: 0],
                                    [timestamp: start.plusHours(4).millis, value: 1]
                            ]
                    ]
            ]
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.post(
            path: "counters/raw/query",
            headers: [(tenantHeaderName): tenantId],
            body: [
                    tags: "letter:A",
                    start: start.plusHours(1).millis,
                    end: start.plusHours(4).millis,
                    limit: 2,
                    order: 'desc'
            ]
    )

    assertEquals(200, response.status)
    assertEquals(2, response.data.size)

    assertTrue(response.data.contains([
            id: 'A1',
            data: [
                    [timestamp: start.plusHours(3).millis, value: 20],
                    [timestamp: start.plusHours(2).millis, value: 30]
            ]
    ]))

    assertTrue(response.data.contains([
            id: 'A2',
            data: [
                    [timestamp: start.plusHours(3).millis, value: 0],
                    [timestamp: start.plusHours(2).millis, value: 1]
            ]
    ]))

    // Make sure the same request on GET endpoint gives the same result
    def responseGET = hawkularMetrics.get(
        path: "counters/tags/letter:A/raw",
        query: [
            start: start.plusHours(1).millis,
            end: start.plusHours(4).millis,
            limit: 2,
            order: 'desc'
        ],
        headers: [(tenantHeaderName): tenantId],
    )
    assertEquals(200, responseGET.status)
    assertEquals(response.data.sort(), responseGET.data.sort())

    response = hawkularMetrics.post(
            path: "counters/raw/query",
            headers: [(tenantHeaderName): tenantId],
            body: [
                    tags: "letter:A,number:1",
                    start: start.plusHours(1).millis,
                    end: start.plusHours(4).millis,
                    limit: 2,
                    order: 'desc'
            ]
    )

    assertEquals(200, response.status)
    assertEquals(1, response.data.size)

    assertTrue(response.data.contains([
            id: 'A1',
            data: [
                    [timestamp: start.plusHours(3).millis, value: 20],
                    [timestamp: start.plusHours(2).millis, value: 30]
            ]
    ]))

    // Make sure the same request on GET endpoint gives the same result
    responseGET = hawkularMetrics.get(
        path: "counters/tags/letter:A,number:1/raw",
        query: [
            start: start.plusHours(1).millis,
            end: start.plusHours(4).millis,
            limit: 2,
            order: 'desc'
        ],
        headers: [(tenantHeaderName): tenantId],
    )
    assertEquals(200, responseGET.status)
    assertEquals(response.data.sort(), responseGET.data.sort())
  }

}
