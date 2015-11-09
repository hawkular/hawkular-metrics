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

import static groovyx.net.http.ContentType.TEXT
import static groovyx.net.http.Method.POST
import static org.joda.time.DateTime.now
import static org.junit.Assert.assertEquals
import static org.junit.Assert.fail

import org.apache.http.util.EntityUtils
import org.joda.time.DateTime
import org.junit.Test

/**
 * @author Thomas Segismont
 */
class InfluxITest extends RESTTest {
  def tenantId = nextTenantId()
  def gaugeName = '_gauge.test'
  def counterName = '_counter.test'

  @Test
  void testEmptyPayload() {
    hawkularMetrics.request(POST) { request ->
      uri.path = "db/${tenantId}/series"
      body = "" /* Empty body */
      requestContentType = 'application/json'
      headers = [Accept: 'application/json, text/plain']

      response.success = { response ->
        fail("Expected error response, got ${response.statusLine}")
      }

      response.failure = { response ->
        assertEquals(400, response.status)
        assertEquals(TEXT.toString(), response.contentType)
        assertEquals('Null objects', EntityUtils.toString(response.entity))
      }
    }
  }

  @Test
  void testListSeries() {
    postGaugeData('_gauge.serie1', now())
    postGaugeData('_counter.serie2', now())

    def response = hawkularMetrics.get(path: "db/${tenantId}/series", query: [q: 'list series'])
    assertEquals(200, response.status)

    def data = response.data ?: []
    def names = ['_counter.serie2', '_gauge.serie1']

    verifySeriesNames(names, data)
  }

  private static void verifySeriesNames(List<String> names, List data) {
    assertEquals(1, data.size())

    assertEquals('list_series_result', data[0].name)
    assertEquals(['time', 'name'], data[0].columns)

    def points = data[0].points ?: []
    assertEquals(names.size(), points.size())
    if (names.size() > 0) assertEquals(names as Set, points.collect { List point -> point[1] } as Set)
  }

  @Test
  void testListSeriesWithRegularExpression() {
    postGaugeData('_gauge.serie1', now())
    postGaugeData('_counter.serie2', now())

    def response = hawkularMetrics.get(path: "db/${tenantId}/series", query: [q: 'list series /rIe\\d/i'])
    assertEquals(200, response.status)

    def data = response.data ?: []
    def names = ['_counter.serie2', '_gauge.serie1']

    verifySeriesNames(names, data)

    response = hawkularMetrics.get(path: "db/${tenantId}/series", query: [q: 'list series /^rIe\\d/i'])
    assertEquals(200, response.status)

    data = response.data ?: []
    names = []

    verifySeriesNames(names, data)
  }

  @Test
  void testInfluxDataOrderedAsc() {
    def start = now().minus(4000)
    postGaugeData(gaugeName, start)

    def influxQuery = """select value from "${gaugeName}" order asc"""

    def response = hawkularMetrics.get(path: "db/${tenantId}/series", query: [q: influxQuery])
    assertEquals(200, response.status)

    assertEquals(
        [
            [
                columns: ['time', 'value'],
                name: gaugeName,
                points: [
                    [start.millis, 40.1],
                    [start.plus(1000).millis, 41.1],
                    [start.plus(2000).millis, 42.1],
                    [start.plus(3000).millis, 43.1],
                    [start.plus(4000).millis, 44.1]
                ]
            ]
        ],
        response.data
    )
  }

  @Test
  void testInfluxDataOrderedDescByDefault() {
    def start = now().minus(4000)
    postGaugeData(gaugeName, start)

    def influxQuery = """select value from "${gaugeName}" """

    def response = hawkularMetrics.get(path: "db/${tenantId}/series", query: [q: influxQuery])
    assertEquals(200, response.status)

    assertEquals(
        [
            [
                columns: ['time', 'value'],
                name: gaugeName,
                points: [
                    [start.plus(4000).millis, 44.1],
                    [start.plus(3000).millis, 43.1],
                    [start.plus(2000).millis, 42.1],
                    [start.plus(1000).millis, 41.1],
                    [start.millis, 40.1]
                ]
            ]
        ],
        response.data
    )
  }

  @Test
  void testInfluxAddGetOneGauge() {
    def start = now().minus(4000)
    postGaugeData(gaugeName, start)

    def influxQuery = """select mean(value) from "${gaugeName}" where time > now() - 30s group by time(30s)"""

    def response = hawkularMetrics.get(path: "db/${tenantId}/series", query: [q: influxQuery])
    assertEquals(200, response.status)

    assertEquals(
        [
            [
                columns: ['time', 'mean'],
                name: gaugeName,
                points: [
                    [start.plus(4000).millis, 42.1]
                ]
            ]
        ],
        response.data
    )
  }

  @Test
  void testInfluxAddGetOneCounter() {
    def start = now().minus(4000)
    postCounterData(counterName, start)

    def influxQuery = """select mean(value) from "${counterName}" where time > now() - 30s group by time(30s)"""

    def response = hawkularMetrics.get(path: "db/${tenantId}/series", query: [q: influxQuery])
    assertEquals(200, response.status)

    assertEquals(
        [
            [
                columns: ['time', 'mean'],
                name   : counterName,
                points : [
                    [start.plus(4000).millis, 42 as BigDecimal]
                ]
            ]
        ],
        response.data
    )
  }

  @Test
  void testInfluxLimitClause() {
    def start = now().minus(4000)
    postGaugeData(gaugeName, start)

    def influxQuery = """select value from "${gaugeName}" limit 2 order asc"""

    def response = hawkularMetrics.get(path: "db/${tenantId}/series", query: [q: influxQuery])
    assertEquals(200, response.status)

    assertEquals(
        [
            [
                columns: ['time', 'value'],
                name: gaugeName,
                points: [
                    [start.millis, 40.1],
                    [start.plus(1000).millis, 41.1]
                ]
            ]
        ],
        response.data
    )
  }

  @Test
  void testInfluxAddGetOneSillyMetric() {
    def start = now().minus(4000)
    postGaugeData(gaugeName, start)

    def influxQuery = """select mean(value) from "${gaugeName}" where time > '2013-08-12 23:32:01.232'
                         and time < '2013-08-13' group by time(30s)"""


    def response = hawkularMetrics.get(path: "db/${tenantId}/series", query: [q: influxQuery])
    assertEquals(200, response.status)

    assertEquals(
        [
            [
                columns: ['time', 'mean'],
                name: gaugeName
            ]
        ],
        response.data
    )
  }

  @Test
  void testWhereClauseWithoutTimeUnit() {
    def start = now().minus(4000)
    postGaugeData(gaugeName, start)

    def influxQuery = """select * from "${gaugeName}"
                         where time > ${start.plus(1000).millis * 1000}
                         and time < ${start.plus(3000).millis * 1000}"""

    def response = hawkularMetrics.get(path: "db/${tenantId}/series", query: [q: influxQuery])
    assertEquals(200, response.status)

    assertEquals(
        [
            [
                columns: ['time', 'value'],
                name: gaugeName,
                points: [
                    [start.plus(2000).millis, 42.1],
                    [start.plus(1000).millis, 41.1]
                ]
            ]
        ],
        response.data
    )
  }

  @Test
  void testGroupByWhenBucketSizeIsMetricResolution() {
    def response = hawkularMetrics.post(path: "db/${tenantId}/series", body: [
        [
            name: gaugeName,
            columns: ['time', 'value'],
            points : [
                [100, 40.1],
                [101, 41.1],
                [102, 42.1],
            ]
        ]
    ])
    assertEquals(200, response.status)

    def influxQuery = """select mean(value) from "${
      gaugeName
    }" where time > 100ms and time < 150ms group by time(1000u)"""

    response = hawkularMetrics.get(path: "db/${tenantId}/series", query: [q: influxQuery])
    assertEquals(200, response.status)

    assertEquals(
        [
            [
                columns: ['time', 'mean'],
                name: gaugeName,
                points : [
                    [100, 40.1],
                    [101, 41.1],
                    [102, 42.1],
                ]
            ]
        ],
        response.data
    )
  }

  @Test
  void testGroupByWithSubSecondBucketSize() {
    def response = hawkularMetrics.post(path: "db/${tenantId}/series", body: [
        [
            name: gaugeName,
            columns: ['time', 'value'],
            points : [
                [100, 40.1],
                [108, 41.1],
                [117, 42.1],
            ]
        ]
    ])
    assertEquals(200, response.status)

    def influxQuery = """select mean(value) from "${
      gaugeName
    }" where time > 100ms and time < 150ms group by time(10ms)"""

    response = hawkularMetrics.get(path: "db/${tenantId}/series", query: [q: influxQuery])
    assertEquals(200, response.status)

    assertEquals(
        [
            [
                columns: ['time', 'mean'],
                name: gaugeName,
                points : [
                    [108, 40.6],
                    [117, 42.1],
                ]
            ]
        ],
        response.data
    )
  }

  @Test
  void testInfluxTop() {
    def start = now().minus(4000)
    postGaugeData(gaugeName, start)

    def influxQuery = """select top(value, 3) from "${gaugeName}" where time > now() - 30s group by time(30s)"""

    def response = hawkularMetrics.get(path: "db/${tenantId}/series", query: [q: influxQuery])
    assertEquals(200, response.status)

    assertEquals(
        [
            [
                columns: ['time', 'top'],
                name: gaugeName,
                points: [
                    [start.plus(4000).millis, 44.1],
                    [start.plus(3000).millis, 43.1],
                    [start.plus(2000).millis, 42.1]
                ]
            ]
        ],
        response.data
    )
  }

  @Test
  void testInfluxBottom() {
    def start = now().minus(4000)
    postGaugeData(gaugeName, start)

    def influxQuery = """select bottom(value, 3) from "${gaugeName}" where time > now() - 30s
                         group by time(30s)"""

    def response = hawkularMetrics.get(path: "db/${tenantId}/series", query: [q: influxQuery])
    assertEquals(200, response.status)

    assertEquals(
        [
            [
                columns: ['time', 'bottom'],
                name: gaugeName,
                points: [
                    [start.millis, 40.1],
                    [start.plus(1000).millis, 41.1],
                    [start.plus(2000).millis, 42.1]
                ]
            ]
        ],
        response.data
    )
  }

  @Test
  void testInfluxStddev() {
    def start = now().minus(4000)
    postGaugeData(gaugeName, start)

    def influxQuery = """select stddev(value) from "${gaugeName}" where time > now() - 30s group by time(30s)"""

    def response = hawkularMetrics.get(path: "db/${tenantId}/series", query: [q: influxQuery])
    assertEquals(200, response.status)

    assertEquals(
        [
            [
                columns: ['time', 'stddev'],
                name: gaugeName,
                points: [
                    [start.plus(4000).millis, 1.5811388300841898]
                ]
            ]
        ],
        response.data
    )
  }

  @Test
  void testTimePrecisionQueryParameter() {
    def response = hawkularMetrics.post(path: "db/${tenantId}/series", query: [time_precision: 's'], body: [
        [
            name: gaugeName,
            columns: ['time', 'value'],
            points: [
                [1, 40.1],
                [2, 41.1],
                [3, 42.1],
            ]
        ]
    ])
    assertEquals(200, response.status)

    def influxQuery = """select * from "${gaugeName}" where time > 0"""

    response = hawkularMetrics.get(path: "db/${tenantId}/series", query: [q: influxQuery])
    assertEquals(200, response.status)
    assertEquals(
        [
            [
                columns: ['time', 'value'],
                name: gaugeName,
                points: [
                    [3_000, 42.1],
                    [2_000, 41.1],
                    [1_000, 40.1],
                ]
            ]
        ],
        response.data
    )

    response = hawkularMetrics.get(path: "db/${tenantId}/series", query: [q: influxQuery, time_precision: 'u'])
    assertEquals(200, response.status)
    assertEquals(
        [
            [
                columns: ['time', 'value'],
                name: gaugeName,
                points: [
                    [3_000_000, 42.1],
                    [2_000_000, 41.1],
                    [1_000_000, 40.1],
                ]
            ]
        ],
        response.data
    )
  }

  void postGaugeData(String timeseriesName, DateTime start) {
    postData(timeseriesName, start, 40.1)
  }

  void postCounterData(String timeseriesName, DateTime start) {
    postData(timeseriesName, start, 40)
  }

  void postData(String timeseriesName, DateTime start, Number dataStart) {
    def influxObject = [
        [
            name: timeseriesName,
            columns: ['time', 'value'],
            points: []
        ]
    ]
    5.times { i ->
      influxObject[0].points.push([start.millis + i * 1000, dataStart + i])
    }
    def response = hawkularMetrics.post(path: "db/${tenantId}/series", body: influxObject)
    assertEquals(200, response.status)
  }
}
