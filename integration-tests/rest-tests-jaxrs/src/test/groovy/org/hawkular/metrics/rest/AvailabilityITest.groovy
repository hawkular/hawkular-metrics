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

import org.hawkular.metrics.model.AvailabilityType
import org.joda.time.DateTime
import org.junit.Test

import static org.joda.time.DateTime.now
import static org.junit.Assert.*
/**
 * @author Thomas Segismont
 */
class AvailabilityITest extends RESTTest {
  def tenantId = nextTenantId()

  @Test
  void shouldNotAcceptInvalidTimeRange() {
    badGet(path: "availability/test/raw", headers: [(tenantHeaderName): tenantId],
        query: [start: 1000, end: 500]) { exception ->
      assertEquals(400, exception.response.status)
    }
  }

  @Test
  void shouldNotAcceptInvalidBucketConfig() {
    badGet(path: "availability/test/stats", headers: [(tenantHeaderName): tenantId],
        query: [start: 500, end: 100, buckets: '10', bucketDuration: '10ms']) { exception ->
      assertEquals("Should fail when both bucket params are specified", 400, exception.response.status)
    }
  }

  @Test
  void shouldNotCreateMetricWithEmptyPayload() {
    badPost(path: "availability", headers: [(tenantHeaderName): tenantId], body: "" /* Empty Body */) { exception ->
      assertEquals(400, exception.response.status)
    }
  }

  @Test
  void shouldNotAddAvailabilityForMetricWithEmptyPayload() {
    badPost(path: "availability/pimpo/raw", headers: [(tenantHeaderName): tenantId],
        body: "" /* Empty Body */) { exception ->
      assertEquals(400, exception.response.status)
    }

    badPost(path: "availability/pimpo/raw", headers: [(tenantHeaderName): tenantId],
        body: [] /* Empty List */) { exception ->
      assertEquals(400, exception.response.status)
    }
  }

  @Test
  void shouldNotAddAvailabilityDataWithEmptyPayload() {
    badPost(path: "availability/raw", headers: [(tenantHeaderName): tenantId],
        body: "" /* Empty Body */) { exception ->
      assertEquals(400, exception.response.status)
    }

    badPost(path: "availability/raw", headers: [(tenantHeaderName): tenantId],
        body: [] /* Empty List */) { exception ->
      assertEquals(400, exception.response.status)
    }
  }

  @Test
  void shouldStoreLargePayloadSize() {
    def availabilityTypes = AvailabilityType.values()
    checkLargePayload("availability", tenantId, { points, int i ->
      points.push([timestamp: i, value: availabilityTypes[i % availabilityTypes.length]])
    })
  }

  @Test
  void testAvailabilityGet() {
    DateTime start = now().minusMinutes(20)
    String metric = 'A1'

    def response = hawkularMetrics.post(path: "availability/$metric/raw", body: [
        [timestamp: start.millis, value: "up"]], headers: [(tenantHeaderName): tenantId])

    assertEquals(200, response.status)

    response = hawkularMetrics.get(path: "availability/$metric", headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertContentEncoding(response)
    assertEquals(metric, response.data.id)
  }

  @Test
  void shouldNotAcceptDataWithEmptyTimestamp() {
    invalidPointCheck("availability", tenantId, [[value: "up"]])
  }

  @Test
  void shouldNotAcceptDataWithNullTimestamp() {
    invalidPointCheck("availability", tenantId, [[timestamp: null, value: "up"]])
  }

  @Test
  void shouldNotAcceptDataWithInvalidTimestamp() {
    invalidPointCheck("availability", tenantId, [[timestamp: "aaa", value: "up"]])
  }

  @Test
  void shouldNotAcceptDataWithEmptyValue() {
    invalidPointCheck("availability", tenantId, [[timestamp: 13]])
  }

  @Test
  void shouldNotAcceptDataWithNullValue() {
    invalidPointCheck("availability", tenantId, [[timestamp: 13, value: null]])
  }

  @Test
  void shouldNotAcceptDataWithInvalidValue() {
    invalidPointCheck("availability", tenantId, [[timestamp: 13, value: ["dsqdqs"]]])
  }

  @Test
  void addTaggedDataPoints() {
    String tenantId = nextTenantId()
    DateTime start = now().minusMinutes(30)
    DateTime end = start.plusMinutes(10)
    String id = 'A1'

    def response = hawkularMetrics.post(
        path: "availability/$id/raw",
        headers: [(tenantHeaderName): tenantId],
        body: [
            [
                timestamp: start.millis,
                value: 'up',
                tags: [x: '1', y: '2']
            ],
            [
                timestamp: start.plusMinutes(1).millis,
                value: 'down',
                tags: [y: '3', z: '5']
            ],
            [
                timestamp: start.plusMinutes(3).millis,
                value: 'up',
                tags: [x: '4', z: '6']
            ]
        ]
    )
    assertEquals(200, response.status)
    response = hawkularMetrics.get(path: "availability/$id/raw", headers: [(tenantHeaderName): tenantId])
    assertContentEncoding(response)
    def expectedData = [
        [
            timestamp: start.plusMinutes(3).millis,
            value: 'up',
            tags: [x: '4', z: '6']
        ],
        [
            timestamp: start.plusMinutes(1).millis,
            value: 'down',
            tags: [y: '3', z: '5']
        ],
        [
            timestamp: start.millis,
            value: 'up',
            tags: [x: '1', y: '2']
        ]
    ]
    assertEquals(expectedData, response.data)
  }

  @Test
  void minMaxTimestamps() {
    def tenantId = nextTenantId()
    def metricId = 'minmaxtest'

    def response = hawkularMetrics.post(path: 'availability', headers: [(tenantHeaderName): tenantId], body: [
        id: metricId
    ])
    assertEquals(201, response.status)

    response = hawkularMetrics.get(path: "availability/${metricId}", headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertContentEncoding(response)
    assertFalse("Metric should not have the minTimestamp attribute: ${response.data}", response.data.containsKey('minTimestamp'))
    assertFalse("Metric should not have the maxTimestamp attribute: ${response.data}", response.data.containsKey('maxTimestamp'))

    response = hawkularMetrics.post(path: "availability/${metricId}/raw", headers: [(tenantHeaderName): tenantId], body: [
        [timestamp: 3, value: 'up']
    ])
    assertEquals(200, response.status)

    response = hawkularMetrics.get(path: "availability/${metricId}", headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertContentEncoding(response)
    assertEquals(3, response.data.minTimestamp)
    assertEquals(3, response.data.maxTimestamp)

    response = hawkularMetrics.post(path: "availability/${metricId}/raw", headers: [(tenantHeaderName): tenantId], body: [
        [timestamp: 1, value: 'down'],
        [timestamp: 2, value: 'up'],
        [timestamp: 4, value: 'down'],
    ])
    assertEquals(200, response.status)

    response = hawkularMetrics.get(path: "availability/${metricId}", headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertContentEncoding(response)
    assertEquals(1, response.data.minTimestamp)
    assertEquals(4, response.data.maxTimestamp)

    response = hawkularMetrics.get(path: "availability", query: [timestamps: 'true'], headers: [(tenantHeaderName):
                                                                                                   tenantId])
    assertEquals(200, response.status)
    assertContentEncoding(response)
    def metric = (response.data as List).find { it.id.equals(metricId) }
    assertEquals(1, metric.minTimestamp)
    assertEquals(4, metric.maxTimestamp)

    response = hawkularMetrics.get(path: "metrics", headers: [(tenantHeaderName): tenantId], query:
            ['timestamps': 'true'])
    assertEquals(200, response.status)
    assertContentEncoding(response)
    metric = (response.data as List).find { it.id.equals(metricId) }
    assertEquals(1, metric.minTimestamp)
    assertEquals(4, metric.maxTimestamp)
  }

  @Test
  void fetchRawDataFromMultipleAvailabilityMetrics() {
    String tenantId = nextTenantId()
    DateTime start = now().minusHours(2)

    def response = hawkularMetrics.post(
        path: "availability/raw",
        headers: [(tenantHeaderName): tenantId],
        body: [
            [
                id: 'A1',
                data: [
                    [timestamp: start.millis, value: 'up'],
                    [timestamp: start.plusMinutes(1).millis, value: 'down']
                ]
            ],
            [
                id: 'A2',
                data: [
                    [timestamp: start.millis, value: 'up'],
                    [timestamp: start.plusMinutes(1).millis, value: 'up']
                ]
            ],
            [
                id: 'A3',
                data: [
                    [timestamp: start.millis, value: 'down'],
                    [timestamp: start.plusMinutes(1).millis, value: 'down']
                ]
            ]
        ]
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.post(
        path: "availability/raw/query",
        headers: [(tenantHeaderName): tenantId],
        body: [ids: ['A1', 'A2', 'A3']]
    )
    assertEquals(200, response.status)

    assertEquals(3, response.data.size)

    assertTrue(response.data.contains([
        id: 'A1',
        data: [
            [timestamp: start.plusMinutes(1).millis, value: 'down'],
            [timestamp: start.millis, value: 'up']
        ]
    ]))

    assertTrue(response.data.contains([
        id: 'A2',
        data: [
            [timestamp: start.plusMinutes(1).millis, value: 'up'],
            [timestamp: start.millis, value: 'up']
        ]
    ]))

    assertTrue(response.data.contains([
        id: 'A3',
        data: [
            [timestamp: start.plusMinutes(1).millis, value: 'down'],
            [timestamp: start.millis, value: 'down']
        ]
    ]))
  }

  @Test
  void fetchMRawDataFromMultipleAvailabilityMetricsWithQueryParams() {
    String tenantId = nextTenantId()
    DateTime start = now().minusHours(4)

    def response = hawkularMetrics.post(
        path: "availability/raw",
        headers: [(tenantHeaderName): tenantId],
        body: [
            [
                id: 'A1',
                data: [
                    [timestamp: start.millis, value: 'up'],
                    [timestamp: start.plusHours(1).millis, value: 'up'],
                    [timestamp: start.plusHours(2).millis, value: 'down'],
                    [timestamp: start.plusHours(3).millis, value: 'down'],
                    [timestamp: start.plusHours(4).millis, value: 'up']
                ]
            ],
            [
                id: 'A2',
                data: [
                    [timestamp: start.millis, value: 'up'],
                    [timestamp: start.plusHours(1).millis, value: 'down'],
                    [timestamp: start.plusHours(2).millis, value: 'up'],
                    [timestamp: start.plusHours(3).millis, value: 'down'],
                    [timestamp: start.plusHours(4).millis, value: 'down']
                ]
            ],
            [
                id: 'A3',
                data: [
                    [timestamp: start.millis, value: 'down'],
                    [timestamp: start.plusHours(1).millis, value: 'up'],
                    [timestamp: start.plusHours(2).millis, value: 'up'],
                    [timestamp: start.plusHours(3).millis, value: 'up'],
                    [timestamp: start.plusHours(4).millis, value: 'down']
                ]
            ]
        ]
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.post(
        path: "availability/raw/query",
        headers: [(tenantHeaderName): tenantId],
        body: [
            ids: ['A1', 'A2', 'A3'],
            start: start.plusHours(1).millis,
            end: start.plusHours(4).millis,
            limit: 2,
            order: 'desc'
        ]
    )

    assertEquals(200, response.status)
    assertEquals(3, response.data.size)

    assertTrue(response.data.contains([
        id: 'A1',
        data: [
            [timestamp: start.plusHours(3).millis, value: 'down'],
            [timestamp: start.plusHours(2).millis, value: 'down']
        ]
    ]))

    assertTrue(response.data.contains([
        id: 'A2',
        data: [
            [timestamp: start.plusHours(3).millis, value: 'down'],
            [timestamp: start.plusHours(2).millis, value: 'up']
        ]
    ]))

    assertTrue(response.data.contains([
        id: 'A3',
        data: [
            [timestamp: start.plusHours(3).millis, value: 'up'],
            [timestamp: start.plusHours(2).millis, value: 'up']
        ]
    ]))

    // From Earliest
    response = hawkularMetrics.post(
        path: "availability/raw/query",
        headers: [(tenantHeaderName): tenantId],
        body: [
            ids: ['A1', 'A2', 'A3'],
            fromEarliest: true,
            order: 'desc'
        ]
    )

    assertEquals(200, response.status)
    assertEquals(3, response.data.size)
    assertTrue(response.data.contains([
        id: 'A1',
        data: [
            [timestamp: start.plusHours(4).millis, value: 'up'],
            [timestamp: start.plusHours(3).millis, value: 'down'],
            [timestamp: start.plusHours(2).millis, value: 'down'],
            [timestamp: start.plusHours(1).millis, value: 'up'],
            [timestamp: start.millis, value: 'up']
        ]
    ]))

    assertTrue(response.data.contains([
        id: 'A2',
        data: [
            [timestamp: start.plusHours(4).millis, value: 'down'],
            [timestamp: start.plusHours(3).millis, value: 'down'],
            [timestamp: start.plusHours(2).millis, value: 'up'],
            [timestamp: start.plusHours(1).millis, value: 'down'],
            [timestamp: start.millis, value: 'up']
        ]
    ]))

    assertTrue(response.data.contains([
        id: 'A3',
        data: [
            [timestamp: start.plusHours(4).millis, value: 'down'],
            [timestamp: start.plusHours(3).millis, value: 'up'],
            [timestamp: start.plusHours(2).millis, value: 'up'],
            [timestamp: start.plusHours(1).millis, value: 'up'],
            [timestamp: start.millis, value: 'down']
        ]
    ]))
  }

  @Test
  void fetchMRawDataFromMultipleAvailabilityMetricsByTag() {
    String tenantId = nextTenantId()
    DateTime start = now().minusHours(4)

    // Define tags
    def response = hawkularMetrics.post(path: 'availability', body: [id: 'A1', tags: [letter: 'A', number: '1']],
            headers: [(tenantHeaderName): tenantId])
    assertEquals(201, response.status)
    response = hawkularMetrics.post(path: 'availability', body: [id: 'A2', tags: [letter: 'A', number: '2']],
            headers: [(tenantHeaderName): tenantId])
    assertEquals(201, response.status)

    response = hawkularMetrics.post(
            path: "availability/raw",
            headers: [(tenantHeaderName): tenantId],
            body: [
                    [
                            id: 'A1',
                            data: [
                                    [timestamp: start.millis, value: 'up'],
                                    [timestamp: start.plusHours(1).millis, value: 'up'],
                                    [timestamp: start.plusHours(2).millis, value: 'down'],
                                    [timestamp: start.plusHours(3).millis, value: 'down'],
                                    [timestamp: start.plusHours(4).millis, value: 'up']
                            ]
                    ],
                    [
                            id: 'A2',
                            data: [
                                    [timestamp: start.millis, value: 'up'],
                                    [timestamp: start.plusHours(1).millis, value: 'down'],
                                    [timestamp: start.plusHours(2).millis, value: 'up'],
                                    [timestamp: start.plusHours(3).millis, value: 'down'],
                                    [timestamp: start.plusHours(4).millis, value: 'down']
                            ]
                    ]
            ]
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.post(
            path: "availability/raw/query",
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
                    [timestamp: start.plusHours(3).millis, value: 'down'],
                    [timestamp: start.plusHours(2).millis, value: 'down']
            ]
    ]))

    assertTrue(response.data.contains([
            id: 'A2',
            data: [
                    [timestamp: start.plusHours(3).millis, value: 'down'],
                    [timestamp: start.plusHours(2).millis, value: 'up']
            ]
    ]))

    // Make sure the same request on GET endpoint gives the same result
    def responseGET = hawkularMetrics.get(
        path: "availability/tags/letter:A/raw",
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
            path: "availability/raw/query",
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
                    [timestamp: start.plusHours(3).millis, value: 'down'],
                    [timestamp: start.plusHours(2).millis, value: 'down']
            ]
    ]))

    // Make sure the same request on GET endpoint gives the same result
    responseGET = hawkularMetrics.get(
        path: "availability/tags/letter:A,number:1/raw",
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
