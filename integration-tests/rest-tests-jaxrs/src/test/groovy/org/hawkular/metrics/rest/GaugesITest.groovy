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

import org.joda.time.DateTime
import org.junit.Test

import static org.joda.time.DateTime.now
import static org.junit.Assert.*
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
  void fromEarliestQueryGaugeData() {
    String tenantId = nextTenantId()
    String gauge = "G1000"
    DateTime start = now().minusHours(10).plusMinutes(10)

    def response = hawkularMetrics.post(
        path: "gauges/$gauge/raw",
        headers: [(tenantHeaderName): tenantId],
        body: [
            [timestamp: start.millis, value: 100.1],
            [timestamp: start.plusHours(1).millis, value: 200.2],
            [timestamp: start.plusHours(2).millis, value: 300.3],
            [timestamp: start.plusHours(3).millis, value: 400.4],
            [timestamp: start.plusHours(4).millis, value: 500.5],
            [timestamp: start.plusHours(5).millis, value: 600.6],
            [timestamp: now().plusHours(6).millis, value: 750.7]
        ]
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.get(
        path: "gauges/$gauge/raw",
        headers: [(tenantHeaderName): tenantId],
        query: [order: 'asc']
    )
    assertEquals(200, response.status)
    assertContentEncoding(response)

    def expectedData = [
        [timestamp: start.plusHours(2).millis, value: 300.3],
        [timestamp: start.plusHours(3).millis, value: 400.4],
        [timestamp: start.plusHours(4).millis, value: 500.5],
        [timestamp: start.plusHours(5).millis, value: 600.6],
    ]

    assertEquals(expectedData, response.data)

    response = hawkularMetrics.get(
        path: "gauges/$gauge/raw",
        headers: [(tenantHeaderName): tenantId],
        query: [fromEarliest: true, order: 'asc']
    )
    assertEquals(200, response.status)
    assertContentEncoding(response)

    expectedData = [
        [timestamp: start.millis, value: 100.1],
        [timestamp: start.plusHours(1).millis, value: 200.2],
        [timestamp: start.plusHours(2).millis, value: 300.3],
        [timestamp: start.plusHours(3).millis, value: 400.4],
        [timestamp: start.plusHours(4).millis, value: 500.5],
        [timestamp: start.plusHours(5).millis, value: 600.6],
    ]

    assertEquals(expectedData, response.data)
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
    assertContentEncoding(response)

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
    assertContentEncoding(response)

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
    assertContentEncoding(response)

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
    assertContentEncoding(response)

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
    assertContentEncoding(response)

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
    assertContentEncoding(response)

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
    assertContentEncoding(response)

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
    assertContentEncoding(response)

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
  void minMaxTimestamps() {
    def tenantId = nextTenantId()
    def metricId = 'minmaxtest'

    def response = hawkularMetrics.post(path: 'gauges', headers: [(tenantHeaderName): tenantId], body: [
        id: metricId
    ])
    assertEquals(201, response.status)

    response = hawkularMetrics.get(path: "gauges/${metricId}", headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertContentEncoding(response)
    assertFalse("Metric should not have the minTimestamp attribute: ${response.data}", response.data.containsKey('minTimestamp'))
    assertFalse("Metric should not have the maxTimestamp attribute: ${response.data}", response.data.containsKey('maxTimestamp'))

    response = hawkularMetrics.post(path: "gauges/${metricId}/raw", headers: [(tenantHeaderName): tenantId], body: [
        [timestamp: 3, value: 4.2]
    ])
    assertEquals(200, response.status)

    response = hawkularMetrics.get(path: "gauges/${metricId}", headers: [(tenantHeaderName): tenantId])
    assertContentEncoding(response)
    assertEquals(200, response.status)
    assertEquals(3, response.data.minTimestamp)
    assertEquals(3, response.data.maxTimestamp)

    response = hawkularMetrics.post(path: "gauges/${metricId}/raw", headers: [(tenantHeaderName): tenantId], body: [
        [timestamp: 1, value: 2.2],
        [timestamp: 2, value: 1.2],
        [timestamp: 4, value: 7.2],
    ])
    assertEquals(200, response.status)

    response = hawkularMetrics.get(path: "gauges/${metricId}", headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertContentEncoding(response)
    assertEquals(1, response.data.minTimestamp)
    assertEquals(4, response.data.maxTimestamp)

    response = hawkularMetrics.get(path: "gauges", query: [timestamps: "true"], headers: [(tenantHeaderName): tenantId])
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
  void findRate() {
    String gauge = "G1"

    def response = hawkularMetrics.post(
        path: "gauges/$gauge/raw",
        headers: [(tenantHeaderName): tenantId],
        body: [
            [timestamp: 60_000 * 1.0, value: 321.8],
            [timestamp: 60_000 * 1.5, value: 475.3],
            [timestamp: 60_000 * 3.5, value: 125.1],
            [timestamp: 60_000 * 5.0, value: 123.6],
            [timestamp: 60_000 * 7.0, value: 468.8],
            [timestamp: 60_000 * 7.5, value: 568.1],
        ]
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.get(
        path: "gauges/$gauge/rate",
        headers: [(tenantHeaderName): tenantId],
        query: [start: 0, order: 'asc']
    )
    assertEquals(200, response.status)
    assertContentEncoding(response)

    def expectedData = [
        [timestamp: (60_000 * 1.5).toLong(), value: 307.0],
        [timestamp: (60_000 * 3.5).toLong(), value: -175.1],
        [timestamp: (60_000 * 5.0).toLong(), value: -1.0],
        [timestamp: (60_000 * 7.0).toLong(), value: 172.6],
        [timestamp: (60_000 * 7.5).toLong(), value: 198.6],
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
    String gauge = "G1"

    def response = hawkularMetrics.post(
        path: "gauges/$gauge/raw",
        headers: [(tenantHeaderName): tenantId],
        body: [
            [timestamp: 60_000 * 1.0, value: 321.8],
            [timestamp: 60_000 * 1.5, value: 475.3],
            [timestamp: 60_000 * 3.5, value: 125.1],
            [timestamp: 60_000 * 5.0, value: 123.6],
            [timestamp: 60_000 * 7.0, value: 468.8],
            [timestamp: 60_000 * 7.5, value: 568.1],
        ]
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.get(
        path: "gauges/$gauge/rate/stats",
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
          val = 307.0
          bucketPoint << [min: val, avg: val, median: val, max: val, sum: val, empty: false, samples: 1]
          break
        case 3:
          val = -175.1
          bucketPoint << [min: val, avg: val, median: val, max: val, sum: val, empty: false, samples: 1]
          break
        case 5:
          val = -1.0
          bucketPoint << [min: val, avg: val, median: val, max: val, sum: val, empty: false, samples: 1]
          break
        case 7:
          bucketPoint << [min: 172.6, max: 198.6, avg: 185.6, median: 172.6, sum: 371.2, empty: false, samples: 2]
          break
        default:
          bucketPoint << [empty: true]
          break
      }
      expectedData.push(bucketPoint)
    }

    assertNumericBucketsEquals(expectedData, response.data ?: [])
  }

  @Test
  void fetchStatsWithPercentiles() {
    String gauge = "G1"

    def response = hawkularMetrics.post(
        path: "gauges/$gauge/raw",
        headers: [(tenantHeaderName): tenantId],
        body: [
            [timestamp: 60_000 * 1.0, value: 321.8],
            [timestamp: 60_000 * 7.0, value: 468.0],
            [timestamp: 60_000 * 7.2, value: 472.0],
            [timestamp: 60_000 * 7.5, value: 968.0],
            [timestamp: 60_000 * 7.6, value: 474.0],
        ]
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.get(
        path: "gauges/$gauge/stats",
        headers: [(tenantHeaderName): tenantId],
        query: [start: 60_000, end: 60_000 * 8, buckets: 7, percentiles: '50,99']
    )
    assertEquals(200, response.status)
    assertContentEncoding(response)

    def expectedData = []
    (1..7).each { i ->
      Map bucketPoint = [start: 60_000 * i, end: 60_000 * (i + 1)]
      double val
      switch (i) {
        case 1:
          val = 321.8
          bucketPoint << [min: val, avg: val, median: val, max: val, sum: val,
                          percentiles: [
                              [value: val, originalQuantile: 50, quantile: 50.0],
                              [value: val, originalQuantile: 99, quantile: 99.0]
                          ],
                          empty: false, samples: 1]
          break
        case 7:
          bucketPoint << [min: 468.0, avg:595.5, median:472.0, max:968.0, sum:2382.0,
                          percentiles: [
                              [value: 472.0, originalQuantile: 50, quantile: 50.0],
                              [value: 474.0, originalQuantile: 99, quantile: 99.0]
                          ],
                          empty: false, samples: 4]
          break
        default:
          bucketPoint << [empty: true]
          break
      }
      expectedData.push(bucketPoint)
    }

    assertNumericBucketsEquals(expectedData, response.data ?: [])
  }

  @Test
  void fetchStackedStatsWithPercentiles() {
    String g1 = "G1"
    String g2 = "G2"

    def response = hawkularMetrics.post(
        path: "gauges/$g1/raw",
        headers: [(tenantHeaderName): tenantId],
        body: [
            [timestamp: 60_000 * 1.0, value: 321.8],
            [timestamp: 60_000 * 7.0, value: 468.0],
            [timestamp: 60_000 * 7.2, value: 472.0],
            [timestamp: 60_000 * 7.5, value: 968.0],
            [timestamp: 60_000 * 7.6, value: 474.0],
        ]
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.post(
        path: "gauges/$g2/raw",
        headers: [(tenantHeaderName): tenantId],
        body: [
            [timestamp: 60_000 * 1.0, value: 100.0],
            [timestamp: 60_000 * 7.0, value: 368.0],
            [timestamp: 60_000 * 7.2, value: 372.0],
            [timestamp: 60_000 * 7.5, value: 868.0],
            [timestamp: 60_000 * 7.6, value: 374.0],
        ]
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.post(
        path: "gauges/stats/query",
        headers: [(tenantHeaderName): tenantId, "Content-Type": "application/json"],
        body: [metrics: [g1, g2], start: 60_000, end: 60_000 * 8, buckets: 7, stacked: true, percentiles: '50,99']
    )
    assertEquals(200, response.status)
    assertContentEncoding(response)

    def expectedData = []
    (1..7).each { i ->
      Map bucketPoint = [start: 60_000 * i, end: 60_000 * (i + 1), min:0.0, avg:0.0, median:0.0, max:0.0, sum:0.0]
      double val
      switch (i) {
        case 1:
          val = 421.8
          bucketPoint << [min: val, avg: val, median: val, max: val, sum: val,
                          percentiles: [
                              [value: val, originalQuantile: 50, quantile: 50.0],
                              [value: val, originalQuantile: 99, quantile: 99.0]
                          ],
                          empty: false, samples: 2]
          break
        case 7:
          bucketPoint << [min: 836.0, avg:1091.0, median:844.0, max:1836.0, sum:4364.0,
                          percentiles: [
                              [value: 844.0, originalQuantile: 50, quantile: 50.0],
                              [value: 848.0, originalQuantile: 99, quantile: 99.0]
                          ],
                          empty: false, samples: 2] // should be 8??
          break
        default:
          bucketPoint << [empty: true]
          break
      }
      expectedData.push(bucketPoint)
    }

    assertNumericBucketsEquals(expectedData, response.data ?: [])
  }

  @Test
  void fetchRawDataFromMultipleGauges() {
    String tenantId = nextTenantId()
    DateTime start = DateTime.now().minusHours(2)

    def response = hawkularMetrics.post(
        path: "gauges/raw",
        headers: [(tenantHeaderName): tenantId],
        body: [
            [
                id: 'G1',
                data: [
                    [timestamp: start.millis, value: 1.23],
                    [timestamp: start.plusMinutes(1).millis, value: 3.45]
                ]
            ],
            [
                id: 'G2',
                data: [
                    [timestamp: start.millis, value: 1.45],
                    [timestamp: start.plusMinutes(1).millis, value: 2.36]
                ]
            ],
            [
                id: 'G3',
                data: [
                    [timestamp: start.millis, value: 4.45],
                    [timestamp: start.plusMinutes(1).millis, value: 5.55]
                ]
            ]
        ]
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.post(
        path: "gauges/raw/query",
        headers: [(tenantHeaderName): tenantId],
        body: [ids: ['G1', 'G2', 'G3']]
    )
    assertEquals(200, response.status)

    assertEquals(3, response.data.size)

    assertTrue(response.data.contains([
        id: 'G1',
        data: [
            [timestamp: start.plusMinutes(1).millis, value: 3.45],
            [timestamp: start.millis, value: 1.23]
        ]
    ]))

    assertTrue(response.data.contains([
        id: 'G2',
        data: [
            [timestamp: start.plusMinutes(1).millis, value: 2.36],
            [timestamp: start.millis, value: 1.45]
        ]
    ]))

    assertTrue(response.data.contains([
        id: 'G3',
        data: [
            [timestamp: start.plusMinutes(1).millis, value: 5.55],
            [timestamp: start.millis, value: 4.45]
        ]
    ]))
  }

  @Test
  void fetchMRawDataFromMultipleGaugesWithQueryParams() {
    String tenantId = nextTenantId()
    DateTime start = DateTime.now().minusHours(4)

    def response = hawkularMetrics.post(
        path: "gauges/raw",
        headers: [(tenantHeaderName): tenantId],
        body: [
            [
                id: 'G1',
                data: [
                    [timestamp: start.millis, value: 1.23],
                    [timestamp: start.plusHours(1).millis, value: 3.45],
                    [timestamp: start.plusHours(2).millis, value: 5.34],
                    [timestamp: start.plusHours(3).millis, value: 2.22],
                    [timestamp: start.plusHours(4).millis, value: 5.22]
                ]
            ],
            [
                id: 'G2',
                data: [
                    [timestamp: start.millis, value: 1.45],
                    [timestamp: start.plusHours(1).millis, value: 2.36],
                    [timestamp: start.plusHours(2).millis, value: 3.62],
                    [timestamp: start.plusHours(3).millis, value: 2.63],
                    [timestamp: start.plusHours(4).millis, value: 3.99]
                ]
            ],
            [
                id: 'G3',
                data: [
                    [timestamp: start.millis, value: 4.45],
                    [timestamp: start.plusHours(1).millis, value: 5.55],
                    [timestamp: start.plusHours(2).millis, value: 4.44],
                    [timestamp: start.plusHours(3).millis, value: 3.33],
                    [timestamp: start.plusHours(4).millis, value: 3.77]
                ]
            ]
        ]
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.post(
        path: "gauges/raw/query",
        headers: [(tenantHeaderName): tenantId],
        body: [
            ids: ['G1', 'G2', 'G3'],
            start: start.plusHours(1).millis,
            end: start.plusHours(4).millis,
            limit: 2,
            order: 'desc'
        ]
    )

    assertEquals(200, response.status)

    assertEquals(3, response.data.size)

    assertTrue(response.data.contains([
        id: 'G1',
        data: [
            [timestamp: start.plusHours(3).millis, value: 2.22],
            [timestamp: start.plusHours(2).millis, value: 5.34]
        ]
    ]))

    assertTrue(response.data.contains([
        id: 'G2',
        data: [
            [timestamp: start.plusHours(3).millis, value: 2.63],
            [timestamp: start.plusHours(2).millis, value: 3.62]
        ]
    ]))

    assertTrue(response.data.contains([
        id: 'G3',
        data: [
            [timestamp: start.plusHours(3).millis, value: 3.33],
            [timestamp: start.plusHours(2).millis, value: 4.44]
        ]
    ]))

    // From Earliest
    response = hawkularMetrics.post(
        path: "gauges/raw/query",
        headers: [(tenantHeaderName): tenantId],
        body: [
            ids: ['G1', 'G2', 'G3'],
            fromEarliest: true,
            order: 'desc'
        ]
    )

    assertEquals(200, response.status)
    assertEquals(3, response.data.size)
    assertTrue(response.data.contains([
        id: 'G1',
        data: [
            [timestamp: start.plusHours(4).millis, value: 5.22],
            [timestamp: start.plusHours(3).millis, value: 2.22],
            [timestamp: start.plusHours(2).millis, value: 5.34],
            [timestamp: start.plusHours(1).millis, value: 3.45],
            [timestamp: start.millis, value: 1.23]
        ]
    ]))

    assertTrue(response.data.contains([
        id: 'G2',
        data: [
            [timestamp: start.plusHours(4).millis, value: 3.99],
            [timestamp: start.plusHours(3).millis, value: 2.63],
            [timestamp: start.plusHours(2).millis, value: 3.62],
            [timestamp: start.plusHours(1).millis, value: 2.36],
            [timestamp: start.millis, value: 1.45]
        ]
    ]))

    assertTrue(response.data.contains([
        id: 'G3',
        data: [
            [timestamp: start.plusHours(4).millis, value: 3.77],
            [timestamp: start.plusHours(3).millis, value: 3.33],
            [timestamp: start.plusHours(2).millis, value: 4.44],
            [timestamp: start.plusHours(1).millis, value: 5.55],
            [timestamp: start.millis, value: 4.45]
        ]
    ]))
  }

  @Test
  void noResultForMultipleMetricsQuery() {
    def tenantId = nextTenantId()

    def response = hawkularMetrics.post(
        path: "gauges/rate/query",
        headers: [(tenantHeaderName): tenantId],
        body: [
            ids  : ['G1', 'G2', 'G3'],
            start: 60_000 * 1.5,
            end  : 60_000 * 3,
            limit: 2,
            order: 'asc'
        ]
    )

    assertEquals(204, response.status)
  }

  @Test
  void fetchRatesFromMultipleMetrics() {
    String tenantId = nextTenantId()
    def dataPoints = [
        [
            id: 'G1',
            data: [
                [timestamp: 60_000, value: 1.23],
                [timestamp: 60_000 * 1.5, value: 3.45],
                [timestamp: 60_000 * 2, value: 5.34],
                [timestamp: 60_000 * 2.5, value: 2.22],
                [timestamp: 60_000 * 3, value: 5.22]
            ]
        ],
        [
            id: 'G2',
            data: [
                [timestamp: 60_000, value: 1.45],
                [timestamp: 60_000 * 1.5, value: 2.36],
                [timestamp: 60_000 * 2, value: 3.62],
                [timestamp: 60_000 * 2.5, value: 2.63],
                [timestamp: 60_000 * 3, value: 3.99]
            ]
        ],
        [
            id: 'G3',
            data: [
                [timestamp: 60_000, value: 4.45],
                [timestamp: 60_000 * 1.5, value: 5.55],
                [timestamp: 60_000 * 2, value: 4.44],
                [timestamp: 60_000 * 2.5, value: 3.33],
                [timestamp: 60_000 * 3, value: 3.77]
            ]
        ]
    ]

    def response = hawkularMetrics.post(
        path: "gauges/raw",
        headers: [(tenantHeaderName): tenantId],
        body: dataPoints
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.post(
        path: "gauges/rate/query",
        headers: [(tenantHeaderName): tenantId],
        body: [
            ids: ['G1', 'G2', 'G3'],
            start: 60_000 * 1.5,
            end: 60_000 * 3,
            limit: 2,
            order: 'asc'
        ]
    )

    assertEquals(200, response.status)
    assertEquals(3, response.data.size())

    assertListOfGaugesContains(response.data, [
        id: 'G1',
        data: [
            [timestamp: 60_000 * 2, value: rate(dataPoints[0].data[2], dataPoints[0].data[1])],
            [timestamp: 60_000 * 2.5, value: rate(dataPoints[0].data[3], dataPoints[0].data[2])]
        ]
    ])

    assertListOfGaugesContains(response.data, [
        id: 'G2',
        data: [
            [timestamp: 60_000 * 2, value: rate(dataPoints[1].data[2], dataPoints[1].data[1])],
            [timestamp: 60_000 * 2.5, value: rate(dataPoints[1].data[3], dataPoints[1].data[2])]
        ]
    ])

    assertListOfGaugesContains(response.data, [
        id: 'G3',
        data: [
            [timestamp: 60_000 * 2, value: rate(dataPoints[2].data[2], dataPoints[2].data[1])],
            [timestamp: 60_000 * 2.5, value: rate(dataPoints[2].data[3], dataPoints[2].data[2])]
        ]
    ])
  }

  @Test
  void fetchRatesFromEarliest() {
    String tenantId = nextTenantId()
    long start = DateTime.now().minusHours(4).getMillis()
    def dataPoints = [
        [
            id: 'G1',
            data: [
                [timestamp: start + 60_000, value: 1.23],
                [timestamp: start + 60_000 * 1.5, value: 3.45],
                [timestamp: start + 60_000 * 2, value: 5.34]
            ]
        ],
        [
            id: 'G2',
            data: [
                [timestamp: start + 60_000, value: 1.45],
                [timestamp: start + 60_000 * 1.5, value: 2.36],
                [timestamp: start + 60_000 * 2, value: 3.62]
            ]
        ]
    ]

    def response = hawkularMetrics.post(
        path: "gauges/raw",
        headers: [(tenantHeaderName): tenantId],
        body: dataPoints
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.post(
        path: "gauges/rate/query",
        headers: [(tenantHeaderName): tenantId],
        body: [
            ids: ['G1', 'G2'],
            fromEarliest: true,
            order: 'asc'
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
        id: 'G1',
        data: [
            [timestamp: timestamp1, value: rate1_1],
            [timestamp: timestamp2, value: rate1_2]
        ]
    ])

    assertListOfGaugesContains(response.data, [
        id: 'G2',
        data: [
            [timestamp: timestamp1, value: rate2_1],
            [timestamp: timestamp2, value: rate2_2]
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
  void fetchMRawDataFromMultipleGaugesByTag() {
    String tenantId = nextTenantId()
    DateTime start = now().minusHours(4)

    // Define tags
    def response = hawkularMetrics.post(path: 'gauges', body: [id: 'A1', tags: [letter: 'A', number: '1']],
            headers: [(tenantHeaderName): tenantId])
    assertEquals(201, response.status)
    response = hawkularMetrics.post(path: 'gauges', body: [id: 'A2', tags: [letter: 'A', number: '2']],
            headers: [(tenantHeaderName): tenantId])
    assertEquals(201, response.status)

    response = hawkularMetrics.post(
            path: "gauges/raw",
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
            path: "gauges/raw/query",
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
                    [timestamp: start.plusHours(3).millis, value: 20.0],
                    [timestamp: start.plusHours(2).millis, value: 30.0]
            ]
    ]))

    assertTrue(response.data.contains([
            id: 'A2',
            data: [
                    [timestamp: start.plusHours(3).millis, value: 0.0],
                    [timestamp: start.plusHours(2).millis, value: 1.0]
            ]
    ]))

    // Make sure the same request on GET endpoint gives the same result
    def responseGET = hawkularMetrics.get(
        path: "gauges/tags/letter:A/raw",
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
            path: "gauges/raw/query",
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
                    [timestamp: start.plusHours(3).millis, value: 20.0],
                    [timestamp: start.plusHours(2).millis, value: 30.0]
            ]
    ]))

    // Make sure the same request on GET endpoint gives the same result
    responseGET = hawkularMetrics.get(
        path: "gauges/tags/letter:A,number:1/raw",
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

  @Test
  void fetchRawDataWithDatapointTags() {
    String tenantId = nextTenantId()
    DateTime start = DateTime.now().minusHours(2)

    def response = hawkularMetrics.post(
        path: "gauges/raw",
        headers: [(tenantHeaderName): tenantId],
        body: [
            [
                id: 'G1',
                data: [
                    [timestamp: start.millis, value: 1.23, tags: [someKey: "someValue"]],
                    [timestamp: start.plusMinutes(1).millis, value: 3.45]
                ]
            ]
        ]
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.post(
        path: "gauges/raw/query",
        headers: [(tenantHeaderName): tenantId],
        body: [ids: ['G1']]
    )
    assertEquals(200, response.status)

    assertEquals(1, response.data.size)

    assertTrue(response.data.contains([
        id: 'G1',
        data: [
            [timestamp: start.plusMinutes(1).millis, value: 3.45],
            [timestamp: start.millis, value: 1.23, tags: [someKey: "someValue"]]
        ]
    ]))
  }

}
