/**
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
 * @author Thomas Segismont
 */
class InfluxITest extends RESTTest {

  @Test
  void testListSeries() {
    def tenantId = nextTenantId()

    postData(tenantId, "serie1", now())
    postData(tenantId, "serie2", now())

    def response = hawkularMetrics.get(path: "db/${tenantId}/series", query: [q: "list series"])
    assertEquals(200, response.status)

    assertEquals(
        [
            [
                name   : "list_series_result",
                columns: ["time", "name"],
                points : [
                    [0, "serie1"],
                    [0, "serie2"],
                ]
            ]
        ],
        response.data
    )
  }

  @Test
  void testInfluxDataOrderedAsc() {
    def tenantId = nextTenantId()
    def timeseriesName = "influx.dataasc"
    def start = now().minus(4000)
    postData(tenantId, timeseriesName, start)

    def influxQuery = 'select value from "' + timeseriesName + '" order asc'

    def response = hawkularMetrics.get(path: "db/${tenantId}/series", query: [q: influxQuery])
    assertEquals(200, response.status)

    assertEquals(
        [
            [
                columns: ["time", "value"],
                name   : timeseriesName,
                points : [
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
    def tenantId = nextTenantId()
    def timeseriesName = "influx.datadesc"
    def start = now().minus(4000)
    postData(tenantId, timeseriesName, start)

    def influxQuery = 'select value from "' + timeseriesName + '"'

    def response = hawkularMetrics.get(path: "db/${tenantId}/series", query: [q: influxQuery])
    assertEquals(200, response.status)

    assertEquals(
        [
            [
                columns: ["time", "value"],
                name   : timeseriesName,
                points : [
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
  void testInfluxAddGetOneMetric() {
    def tenantId = nextTenantId()
    def timeseriesName = "influx.foo"
    def start = now().minus(4000)
    postData(tenantId, timeseriesName, start)

    def influxQuery = 'select mean(value) from "' + timeseriesName + '" where time > now() - 30s group by time(30s) '

    def response = hawkularMetrics.get(path: "db/${tenantId}/series", query: [q: influxQuery])
    assertEquals(200, response.status)

    assertEquals(
        [
            [
                columns: ["time", "mean"],
                name   : timeseriesName,
                points : [
                    [start.plus(4000).millis, 42.1]
                ]
            ]
        ],
        response.data
    )
  }

  @Test
  void testInfluxLimitClause() {
    def tenantId = nextTenantId()
    def timeseriesName = "influx.limitclause"
    def start = now().minus(4000)
    postData(tenantId, timeseriesName, start)

    def influxQuery = 'select value from "' + timeseriesName + '" limit 2 order asc '

    def response = hawkularMetrics.get(path: "db/${tenantId}/series", query: [q: influxQuery])
    assertEquals(200, response.status)

    assertEquals(
        [
            [
                columns: ["time", "value"],
                name   : timeseriesName,
                points : [
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
    def tenantId = nextTenantId()
    def timeseriesName = "influx.foo3"
    def start = now().minus(4000)
    postData(tenantId, timeseriesName, start)

    def influxQuery = 'select mean(value) from "' + timeseriesName + '''" where time > '2013-08-12 23:32:01.232'
                        and time < '2013-08-13' group by time(30s) '''


    def response = hawkularMetrics.get(path: "db/${tenantId}/series", query: [q: influxQuery])
    assertEquals(200, response.status)

    assertEquals(
        [
            [
                columns: ["time", "mean"],
                name  : timeseriesName,
                points: []
            ]
        ],
        response.data
    )
  }

  @Test
  void testInfluxTop() {
    def tenantId = nextTenantId()
    def timeseriesName = "influx.top"
    def start = now().minus(4000)
    postData(tenantId, timeseriesName, start)

    def influxQuery = 'select top(value, 3) from "' + timeseriesName + '" where time > now() - 30s group by time(30s)'

    def response = hawkularMetrics.get(path: "db/${tenantId}/series", query: [q: influxQuery])
    assertEquals(200, response.status)

    assertEquals(
        [
            [
                columns: ["time", "top"],
                name   : timeseriesName,
                points : [
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
   def tenantId = nextTenantId()
    def timeseriesName = "influx.bottom"
    def start = now().minus(4000)
   postData(tenantId, timeseriesName, start)

    def influxQuery = 'select bottom(value, 3) from "' + timeseriesName + '" where time > now() - 30s group by time(30s)'

    def response = hawkularMetrics.get(path: "db/${tenantId}/series", query: [q: influxQuery])
    assertEquals(200, response.status)

    assertEquals(
        [
            [
                columns: ["time", "bottom"],
                name   : timeseriesName,
                points : [
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
   def tenantId = nextTenantId()
    def timeseriesName = "influx.stddev"
    def start = now().minus(4000)
   postData(tenantId, timeseriesName, start)

    def influxQuery = 'select stddev(value) from "' + timeseriesName + '" where time > now() - 30s group by time(30s)'

    def response = hawkularMetrics.get(path: "db/${tenantId}/series", query: [q: influxQuery])
    assertEquals(200, response.status)

    assertEquals(
        [
            [
                columns: ["time", "stddev"],
                name   : timeseriesName,
                points : [
                    [start.plus(4000).millis, 1.5811388300841898]
                ]
            ]
        ],
        response.data
    )
  }

  private static void postData(String tenantId, String timeseriesName, DateTime start) {
    def response = hawkularMetrics.post(path: "db/${tenantId}/series", body: [
        [
            name: timeseriesName,
            columns: ["time", "value"],
            points: [
                [start.millis, 40.1],
                [start.plus(1000).millis, 41.1],
                [start.plus(2000).millis, 42.1],
                [start.plus(3000).millis, 43.1],
                [start.plus(4000).millis, 44.1]
            ]
        ]
    ])
    assertEquals(200, response.status)
  }
}