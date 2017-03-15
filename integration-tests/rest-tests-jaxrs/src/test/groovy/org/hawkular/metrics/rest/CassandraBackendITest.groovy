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

import static org.joda.time.DateTime.now
import static org.joda.time.Seconds.seconds
import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertNotNull
import static org.junit.Assert.assertTrue

import org.hawkular.metrics.core.service.DateTimeService
import org.hawkular.metrics.model.MetricType
import org.hawkular.metrics.datetime.DateTimeService
import org.joda.time.DateTime
import org.junit.Test

class CassandraBackendITest extends RESTTest {

  def assertGaugeDataPointEquals = { expected, actual ->
    assertEquals("The timestamp does not match", expected.timestamp, actual.timestamp)
    assertTrue("Expected a value of $expected.value but found $actual.value",
        expected.value.compareTo(actual.value) == 0)
  }

  @Test
  void findMetricsWhenThereIsNoData() {
    def tenantId = nextTenantId()

    def response = hawkularMetrics.get(path: "metrics", query: [type: "gauge"], headers: [(tenantHeaderName): tenantId])
    assertEquals("Expected a 204 status code when no gauge metrics are found", 204, response.status)

    response = hawkularMetrics.get(path: "metrics", query: [type: "availability"], headers: [(tenantHeaderName): tenantId])
    assertEquals("Expected a 204 status code when no availability metrics are found", 204, response.status)
  }

  @Test
  void findGaugeDataForMetricWhenThereIsNoData() {
    def tenantId = nextTenantId()
    def metric = "N1"

    def response = hawkularMetrics.get(path: "gauges/missing/raw", headers: [(tenantHeaderName): tenantId])
    assertEquals("Expected a 204 response when the gauge metric does not exist", 204, response.status)

    response = hawkularMetrics.post(path: "gauges/$metric/raw", body: [
        [timestamp: now().minusHours(2).millis, value: 1.23],
        [timestamp: now().minusHours(1).millis, value: 3.21]
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)

    response = hawkularMetrics.get(path: "gauges/$metric/raw", query: [
        start: now().minusDays(3).millis, end: now().minusDays(2).millis], headers: [(tenantHeaderName): tenantId])
    assertEquals("Expected a 204 response when there is no data for the specified date range", 204, response.status)
  }

  @Test
  void findAvailabilityDataMetricWhenThereIsNoData() {
    def tenantId = nextTenantId()
    def metric = 'A1'

    def response = hawkularMetrics.get(path: "availability/missing/raw", headers: [(tenantHeaderName): tenantId])
    assertEquals("Expected a 204 response when the availability metric does not exist", 204, response.status)

    response = hawkularMetrics.post(path: "availability/$metric/raw", body: [
        [timestamp: now().minusHours(2).millis, value: 'up'],
        [timestamp: now().minusHours(1).millis, value: 'up']
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)

    response = hawkularMetrics.get(path: "availability/$metric/raw", query: [
        start: now().minusDays(3).millis, end: now().minusDays(2).millis], headers: [(tenantHeaderName): tenantId])
    assertEquals("Expected a 204 response when there is no data for the specified date range", 204, response.status)
  }

  @Test
  void simpleInsertAndQueryGaugeData() {
    DateTimeService dateTimeService = new DateTimeService()
    String tenantId = nextTenantId()
    String metric = 'n1'
    DateTime start = dateTimeService.currentHour().minusHours(1)
    DateTime end = start.plusHours(1)

    int numBuckets = 10
    long bucketSize = (end.millis - start.millis) / numBuckets
    def buckets = []
    numBuckets.times { buckets.add(start.millis + (it * bucketSize)) }

    def response = hawkularMetrics.post(path: "gauges/raw", body: [
        [id: 'test',
         data: [
            [timestamp: buckets[0], value: 12.22],
            [timestamp: buckets[0] + seconds(10).toStandardDuration().millis, value: 12.37],
            [timestamp: buckets[4], value: 25],
            [timestamp: buckets[4] + seconds(15).toStandardDuration().millis, value: 25],
            [timestamp: buckets[9], value: 18.367],
            [timestamp: buckets[9] + seconds(10).toStandardDuration().millis, value: 19.01]
        ]]], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)

    response = hawkularMetrics.get(path: "gauges/$metric/raw",
        query: [start: start.minusHours(12).millis, end: end.minusHours(11).millis], headers: [(tenantHeaderName): tenantId])
    assertEquals("Expected a 204 status code when there is no gauge data", 204, response.status)
  }

  @Test
  void getPeriods() {
    DateTime end = now()
    DateTime start = end.minusMinutes(30)
    String tenantId = nextTenantId()
    String metric = "n1"

    def response = hawkularMetrics.post(path: "gauges/$metric/raw", body: [
        [timestamp: start.millis, value: 22.3],
        [timestamp: start.plusMinutes(1).millis, value: 17.4],
        [timestamp: start.plusMinutes(2).millis, value: 16.6],
        [timestamp: start.plusMinutes(3).millis, value: 22.7],
        [timestamp: start.plusMinutes(4).millis, value: 23.3],
        [timestamp: start.plusMinutes(5).millis, value: 19.9],
        [timestamp: start.plusMinutes(6).millis, value: 21.2],
        [timestamp: start.plusMinutes(7).millis, value: 24.2],
        [timestamp: start.plusMinutes(8).millis, value: 26.6],
        [timestamp: start.plusMinutes(9).millis, value: 18.8],
        [timestamp: start.plusMinutes(10).millis, value: 20.0]
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)

    def getPeriods = { operation, threshold ->
      def periodsResponse = hawkularMetrics.get(path: "gauges/$metric/periods",
          query: [threshold: threshold, op: operation], headers: [(tenantHeaderName): tenantId])
      assertEquals(200, response.status)

      return periodsResponse
    }

    response = getPeriods("gt", 20)

    def expectedData = [
      [start.millis, start.millis],
      [start.plusMinutes(3).millis, start.plusMinutes(4).millis],
      [start.plusMinutes(6).millis, start.plusMinutes(8).millis]
    ]
    assertEquals(expectedData, response.data)

    response = getPeriods("lt", 20)

    expectedData = [
        [start.plusMinutes(1).millis, start.plusMinutes(2).millis],
        [start.plusMinutes(5).millis, start.plusMinutes(5).millis],
        [start.plusMinutes(9).millis, start.plusMinutes(9).millis]
    ]
    assertEquals(expectedData, response.data)

    response = getPeriods("gte", 20)

    expectedData = [
        [start.millis, start.millis],
        [start.plusMinutes(3).millis, start.plusMinutes(4).millis],
        [start.plusMinutes(6).millis, start.plusMinutes(8).millis],
        [start.plusMinutes(10).millis, start.plusMinutes(10).millis]
    ]
    assertEquals(expectedData, response.data)

    response = getPeriods("lte", 20)

    expectedData = [
        [start.plusMinutes(1).millis, start.plusMinutes(2).millis],
        [start.plusMinutes(5).millis, start.plusMinutes(5).millis],
        [start.plusMinutes(9).millis, start.plusMinutes(10).millis]
    ]
    assertEquals(expectedData, response.data)

    response = getPeriods("eq", 20)

    expectedData = [[start.plusMinutes(10).millis, start.plusMinutes(10).millis]]
    assertEquals(expectedData, response.data)

    response = getPeriods("neq", 20)

    expectedData = [[start.millis, start.plusMinutes(9).millis]]
    assertEquals(expectedData, response.data)

    badGet(path: "gauges/$metric/periods", query: [threshold: 20, op: "foo"], headers: [(tenantHeaderName): tenantId], { exception ->
      assertEquals(400, exception.response.status)
    })

    response = hawkularMetrics.get(path: "gauges/$metric/periods", query: [threshold: 20, op: "gt",
        start: start.minusMinutes(10).millis, end: start.minusMinutes(5).millis], headers: [(tenantHeaderName): tenantId])
    assertEquals(204, response.status)
  }

  @Test
  void insertGaugeDataForMultipleMetrics() {
    DateTime start = now().minusMinutes(10)
    def tenantId = nextTenantId()

    // Let's explicitly create one of the metrics with some tags and a data retention
    // so that we can verify we get back that info along with the data.
    def response = hawkularMetrics.post(path: "gauges", body: [
        id: 'm2',
        tags: [a: '1', b: '2'],
        dataRetention: 24
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(201, response.status)
    assertEquals("http://$baseURI/gauges/m2".toString(), response.getFirstHeader('location').value)

    response = hawkularMetrics.post(path: "gauges/raw", body: [
        [
            id: 'm1',
            data: [
                [timestamp: start.millis, value: 1.1],
                [timestamp: start.plusMinutes(1).millis, value: 1.2]
            ]
        ],
        [
            id: 'm2',
            data: [
                [timestamp: start.millis, value: 2.1],
                [timestamp: start.plusMinutes(1).millis, value: 2.2]
            ]
        ],
        [
            id: 'm3',
            data: [
                [timestamp: start.millis, value: 3.1],
                [timestamp: start.plusMinutes(1).millis, value: 3.2]
            ]
        ]
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)

    response = hawkularMetrics.get(path: "gauges/m2/raw", headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertEquals(
        [
            [timestamp: start.plusMinutes(1).millis, value: 2.2],
            [timestamp: start.millis, value: 2.1],
        ],
        response.data
    )
  }

  @Test
  void insertAvailabilityDataForMultipleMetrics() {
    DateTime start = now().minusMinutes(10)
    def tenantId = nextTenantId()

    // Let's explicitly create one of the metrics with some tags and a data retention
    // so that we can verify we get back that info along with the data.
    def response = hawkularMetrics.post(path: "availability", body: [
        id         : 'm2',
        tags         : [a: '1', b: '2'],
        dataRetention: 12
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(201, response.status)
    assertEquals(
        "http://$baseURI/availability/m2".toString(),
        response.getFirstHeader('location').value
    )

    response = hawkularMetrics.post(path: "availability/raw", body: [
        [
            id: 'm1',
            data: [
                [timestamp: start.millis, value: "down"],
                [timestamp: start.plusMinutes(1).millis, value: "up"]
            ]
        ],
        [
            id: 'm2',
            data: [
                [timestamp: start.millis, value: "up"],
                [timestamp: start.plusMinutes(1).millis, value: "up"]
            ]
        ],
        [
            id: 'm3',
            data: [
                [timestamp: start.millis, value: "down"],
                [timestamp: start.plusMinutes(1).millis, value: "down"]
            ]
        ]
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)

    response = hawkularMetrics.get(path: "availability/m2/raw", headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertEquals(
        [
            [timestamp: start.plusMinutes(1).millis, value: "up"],
            [timestamp: start.millis, value: "up"]
        ],
        response.data
    )
  }

  @Test
  void findDistinctAvailabilities() {
    DateTime start = now().minusMinutes(20)
    String tenantId = nextTenantId()
    String metric = 'A1'

    def response = hawkularMetrics.post(path: "availability/$metric/raw", body: [
        [timestamp: start.millis, value: "up"],
        [timestamp: start.plusMinutes(1).millis, value: "up"],
        [timestamp: start.plusMinutes(2).millis, value: "down"],
        [timestamp: start.plusMinutes(3).millis, value: "down"],
        [timestamp: start.plusMinutes(4).millis, value: "up"],
        [timestamp: start.plusMinutes(5).millis, value: "down"],
        [timestamp: start.plusMinutes(6).millis, value: "down"],
        [timestamp: start.plusMinutes(7).millis, value: "up"],
        [timestamp: start.plusMinutes(8).millis, value: "up"],
        [timestamp: start.plusMinutes(9).millis, value: "unknown"],
        [timestamp: start.plusMinutes(10).millis, value: "unknown"],
        [timestamp: start.plusMinutes(11).millis, value: "unknown"],
        [timestamp: start.plusMinutes(12).millis, value: "up"]
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)

    response = hawkularMetrics.get(path: "availability/$metric/raw", query: [distinct: "true"], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertEquals(
        [
            [timestamp: start.plusMinutes(12).millis, value: "up"],
            [timestamp: start.plusMinutes(11).millis, value: "unknown"],
            [timestamp: start.plusMinutes(8).millis, value: "up"],
            [timestamp: start.plusMinutes(6).millis, value: "down"],
            [timestamp: start.plusMinutes(4).millis, value: "up"],
            [timestamp: start.plusMinutes(3).millis, value: "down"],
            [timestamp: start.plusMinutes(1).millis, value: "up"]
        ],
        response.data
    )

    response = hawkularMetrics.get(path: "availability/$metric/raw", query: [distinct: "true", order: 'asc'], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertEquals(
        [
            [timestamp: start.millis, value: "up"],
            [timestamp: start.plusMinutes(2).millis, value: "down"],
            [timestamp: start.plusMinutes(4).millis, value: "up"],
            [timestamp: start.plusMinutes(5).millis, value: "down"],
            [timestamp: start.plusMinutes(7).millis, value: "up"],
            [timestamp: start.plusMinutes(9).millis, value: "unknown"],
            [timestamp: start.plusMinutes(12).millis, value: "up"]
       ],
        response.data
    )

    response = hawkularMetrics.get(path: "availability/$metric/raw", query: [distinct: "true", limit: 2], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertEquals(
        [
            [timestamp: start.plusMinutes(12).millis, value: "up"],
            [timestamp: start.plusMinutes(11).millis, value: "unknown"]
        ],
        response.data
    )

    response = hawkularMetrics.get(path: "availability/$metric/raw", query: [limit: 3], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertEquals(
        [
            [timestamp: start.plusMinutes(12).millis, value: "up"],
            [timestamp: start.plusMinutes(11).millis, value: "unknown"],
            [timestamp: start.plusMinutes(10).millis, value: "unknown"],
        ],
        response.data
    )

    response = hawkularMetrics.get(path: "availability/$metric/raw", query: [limit: 3, end: start.plusMinutes(14).millis], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertEquals(
        [
            [timestamp: start.plusMinutes(12).millis, value: "up"],
            [timestamp: start.plusMinutes(11).millis, value: "unknown"],
            [timestamp: start.plusMinutes(10).millis, value: "unknown"],
        ],
        response.data
    )

    response = hawkularMetrics.get(path: "availability/$metric/raw",
        query: [limit: 3, start: start.plusMinutes(4).millis, order: 'desc'],
        headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertEquals(
        [
            [timestamp: start.plusMinutes(12).millis, value: "up"],
            [timestamp: start.plusMinutes(11).millis, value: "unknown"],
            [timestamp: start.plusMinutes(10).millis, value: "unknown"],
        ],
        response.data
    )

    response = hawkularMetrics.get(path: "availability/$metric/raw", query: [limit: 4, order: 'asc'], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertEquals(
        [
            [timestamp: start.millis, value: "up"],
            [timestamp: start.plusMinutes(1).millis, value: "up"],
            [timestamp: start.plusMinutes(2).millis, value: "down"],
            [timestamp: start.plusMinutes(3).millis, value: "down"],
        ],
        response.data
    )

    response = hawkularMetrics.get(path: "availability/$metric/raw", query: [limit: 4, start: (start.millis - 1)], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertEquals(
        [
            [timestamp: start.millis, value: "up"],
            [timestamp: start.plusMinutes(1).millis, value: "up"],
            [timestamp: start.plusMinutes(2).millis, value: "down"],
            [timestamp: start.plusMinutes(3).millis, value: "down"],
        ],
        response.data
    )
  }

  @Test
  void findMetricsShouldFailProperlyWhenTypeIsMissingOrInvalid() {
    def tenantId = nextTenantId()

    def invalidType = MetricType.all().collect { it.text }.join("")
    badGet(path: "metrics", query: [type: invalidType],
        headers: [(tenantHeaderName): tenantId]) { exception ->
      // Invalid type
      assertEquals(400, exception.response.status)
      assertEquals(invalidType + " is not a recognized metric type", exception.response.data["errorMsg"])
    }

    badGet(path: "metrics", query: [type: MetricType.COUNTER_RATE.toString()],
        headers: [(tenantHeaderName): tenantId]) { exception ->
      // Not user definable type
      assertEquals(400, exception.response.status)
      assertEquals("Incorrect type param counter_rate", exception.response.data["errorMsg"])
    }

    def response = hawkularMetrics.get(path: "metrics", query: [type: MetricType.GAUGE.text],
        headers: [(tenantHeaderName): tenantId])
    // Works fine when type is correctly set (here 204 as not metric has been created)
    assertEquals(204, response.status)
  }

  @Test
  void findMetrics() {
    DateTime start = now().minusMinutes(20)
    def tenantId = nextTenantId()

    // First create a couple gauge metrics by only inserting data
    def response = hawkularMetrics.post(path: "gauges/raw", body: [
        [
            id: 'm11',
            data: [
                [timestamp: start.millis, value: 1.1],
                [timestamp: start.plusMinutes(1).millis, value: 1.2]
            ]
        ],
        [
            id: 'm12',
            data: [
                [timestamp: start.millis, value: 2.1],
                [timestamp: start.plusMinutes(1).millis, value: 2.2]
            ]
        ]
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)

    // Explicitly create a gauge metric
    response = hawkularMetrics.post(path: "gauges", body: [
        id: 'm13',
        tags: [a1: 'A', B1: 'B'],
        dataRetention: 32
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(201, response.status)

    // Now query for the gauge metrics
    response = hawkularMetrics.get(path: "metrics", query: [type: 'gauge', timestamps: 'true'], headers: [
            (tenantHeaderName): tenantId])

    assertEquals(200, response.status)
    assertEquals([
        [dataRetention: 7, tenantId: tenantId, id: 'm11', type: 'gauge', minTimestamp: start.millis, maxTimestamp: start.plusMinutes(1).millis],
        [dataRetention: 7, tenantId: tenantId, id: 'm12', type: 'gauge', minTimestamp: start.millis, maxTimestamp: start.plusMinutes(1).millis],
        [tenantId: tenantId, id: 'm13', tags: [a1: 'A', B1: 'B'], dataRetention: 32, type: 'gauge']
    ], (response.data as List).sort({ m1, m2 -> m1.id.compareTo(m2.id) }))

    // Create a couple availability metrics by only inserting data
    response = hawkularMetrics.post(path: "availability/raw", body: [
        [
            id: 'm14',
            data: [
                [timestamp: start.millis, value: 'up'],
                [timestamp: start.plusMinutes(1).millis, value: 'up']
            ]
        ],
        [
            id: 'm15',
            data: [
                [timestamp: start.millis, value: 'up'],
                [timestamp: start.plusMinutes(1).millis, value: 'down']
            ]
        ]
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)

    // Explicitly create an availability metric
    response = hawkularMetrics.post(path: "availability", body: [
        id: 'm16',
        tags: [a10: '10', a11: '11'],
        dataRetention: 7
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(201, response.status)

    // Query for the availability metrics
    response = hawkularMetrics.get(path: "metrics", query: [type: 'availability', timestamps: 'true'], headers: [
            (tenantHeaderName): tenantId])

    assertEquals(200, response.status)
    assertEquals([
        [dataRetention: 7, tenantId: tenantId, id: 'm14', type: 'availability', minTimestamp: start.millis, maxTimestamp: start.plusMinutes(1).millis],
        [dataRetention: 7, tenantId: tenantId, id: 'm15', type: 'availability', minTimestamp: start.millis, maxTimestamp: start.plusMinutes(1).millis],
        [tenantId: tenantId, id: 'm16', tags: [a10: '10', a11: '11'], dataRetention: 7, type: 'availability']
    ], (response.data as List).sort({ m1, m2 -> m1.id.compareTo(m2.id) }))

    // Explicitly create metrics with payload type
    response = hawkularMetrics.post(path: "metrics", body: [
        id: 'm17',
        tags: [a10: '10', a11: '11'],
        dataRetention: 7,
        type: 'availability'
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(201, response.status)

    // Fetch the metric
    response = hawkularMetrics.get(path: "availability/m17", headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertEquals([
        id: 'm17',
        tags: [a10: '10', a11: '11'],
        dataRetention: 7,
        type: 'availability',
        tenantId: tenantId
    ], response.data)
  }

  @Test
  void createEmptyMetric() {
    String tenantId = nextTenantId()

    // Create a gauge metric
    def response = hawkularMetrics.post(path: "gauges", body: [
            id: 'Empty1'
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(201, response.status)

    // Fetch the metric
    response = hawkularMetrics.get(path: "gauges/Empty1", headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertEquals([
            dataRetention: 7,
            tenantId: tenantId,
            id: 'Empty1',
            type: 'gauge'
    ], response.data)
  }

  @Test
  void testCreateTypeChecking() {
    String tenantId = nextTenantId()

    // Test gauges path
    badPost(path: "gauges", body: [id: 'N1', type: 'availability'], headers: [(tenantHeaderName): tenantId]) {
      exception ->
      assertEquals(400, exception.response.status)
      assertNotNull(exception.response.data['errorMsg'])
    }

    // Test availability path
    badPost(path: "availability", body: [id: 'N1', type: 'gauge'], headers: [(tenantHeaderName): tenantId]) {
      exception ->
        assertEquals(400, exception.response.status)
        assertNotNull(exception.response.data['errorMsg'])
    }

    // Test counter path
    badPost(path: "counters", body: [id: 'N1', type: 'availability'], headers: [(tenantHeaderName): tenantId]) {
      exception ->
        assertEquals(400, exception.response.status)
        assertNotNull(exception.response.data['errorMsg'])
    }

    // Test metrics path without given type
    badPost(path: "metrics", body: [id: 'N1'], headers: [(tenantHeaderName): tenantId]) {
      exception ->
        assertEquals(400, exception.response.status)
        assertNotNull(exception.response.data['errorMsg'])
    }
  }

  @Test
  void insertMetricsWithOverwriteViaTypeEndpoint() {
    def tenantId = nextTenantId()

    for (metricType in metricTypes) {
      //create metric
      def response = hawkularMetrics.post(path: metricType.path, body: [
          id: 'm2',
          tags: [a: '1', b: '2'],
          dataRetention: 24
      ], headers: [(tenantHeaderName): tenantId])
      assertEquals(201, response.status)
      assertEquals("http://$baseURI/${metricType.path}/m2".toString(), response.getFirstHeader('location').value)

      response = hawkularMetrics.get(path: "${metricType.path}/m2", headers: [(tenantHeaderName): tenantId])
      assertEquals(200, response.status)
      assertEquals([
            dataRetention: 24,
            id: 'm2',
            tags: [a: '1', b: '2'],
            tenantId: tenantId,
            type: metricType.type
      ], response.data)

      //try to create the metric again
      badPost(path: metricType.path, body: [
          id: 'm2',
          tags: [a: '1', b: '2'],
          dataRetention: 24
      ], headers: [(tenantHeaderName): tenantId]) { exception ->
        assertEquals(409, exception.response.status)
      }

      //try to create the metric again but with overwrite
      response = hawkularMetrics.post(path: metricType.path, query: [overwrite: true], body: [
          id: 'm2',
          tags: [c: '3', d: '4'],
          dataRetention: 55
      ], headers: [(tenantHeaderName): tenantId])
      assertEquals(201, response.status)
      assertEquals("http://$baseURI/${metricType.path}/m2".toString(), response.getFirstHeader('location').value)

      response = hawkularMetrics.get(path: "${metricType.path}/m2", headers: [(tenantHeaderName): tenantId])
      assertEquals(200, response.status)
      assertEquals([
            dataRetention: 55,
            id: 'm2',
            tags: [c: '3', d: '4'],
            tenantId: tenantId,
            type: metricType.type
      ], response.data)
    }
  }

  @Test
  void insertMetricsWithOverwriteViaMetricsEndpoint() {
    def tenantId = nextTenantId()

    for (metricType in metricTypes) {
      //create metric
      def response = hawkularMetrics.post(path: 'metrics', body: [
          id: 'm2',
          tags: [a: '1', b: '2'],
          dataRetention: 24,
          type: metricType.type
      ], headers: [(tenantHeaderName): tenantId])
      assertEquals(201, response.status)
      assertEquals("http://$baseURI/${metricType.path}/m2".toString(), response.getFirstHeader('location').value)

      response = hawkularMetrics.get(path: "${metricType.path}/m2", headers: [(tenantHeaderName): tenantId])
      assertEquals(200, response.status)
      assertEquals([
            dataRetention: 24,
            id: 'm2',
            tags: [a: '1', b: '2'],
            tenantId: tenantId,
            type: metricType.type
      ], response.data)

      //try to create the metric again
      badPost(path: 'metrics', body: [
          id: 'm2',
          tags: [a: '1', b: '2'],
          dataRetention: 24,
          type: metricType.type
      ], headers: [(tenantHeaderName): tenantId])  { exception ->
        assertEquals(409, exception.response.status)
      }

      //try to create the metric again but with overwrite
      response = hawkularMetrics.post(path: 'metrics', query: [overwrite: true], body: [
          id: 'm2',
          tags: [c: '3', d: '4'],
          dataRetention: 55,
          type: metricType.type
      ], headers: [(tenantHeaderName): tenantId])
      assertEquals(201, response.status)
      assertEquals("http://$baseURI/${metricType.path}/m2".toString(), response.getFirstHeader('location').value)

      response = hawkularMetrics.get(path: "${metricType.path}/m2", headers: [(tenantHeaderName): tenantId])
      assertEquals(200, response.status)
      assertEquals([
            dataRetention: 55,
            id: 'm2',
            tags: [c: '3', d: '4'],
            tenantId: tenantId,
            type: metricType.type
      ], response.data)
    }
  }
}
