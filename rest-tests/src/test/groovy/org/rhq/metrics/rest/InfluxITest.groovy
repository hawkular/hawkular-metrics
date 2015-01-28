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
package org.rhq.metrics.rest

import org.joda.time.DateTime
import org.junit.Test

import static org.joda.time.DateTime.now
import static org.junit.Assert.assertEquals

/**
 * @author Thomas Segismont
 */
class InfluxITest extends RESTTest {

  static def tenantId = "influxtest"

  @Test
  void testInfluxDataOrderedAsc() {
    def timeseriesName = "influx.dataasc"
    def start = now().minus(2000)
    postData(timeseriesName, start)

    def influxQuery = 'select value from "' + timeseriesName + '" order asc'
    rhqm.defaultRequestHeaders.Authorization = "Basic YWdlbnQ6MjUyMzExMGYtZTk2ZS00NDg2LTk4NTAtOTRhODU3YzUzOTA5"

    def response = rhqm.get(path: "tenants/${tenantId}/influx/series", query: [q: influxQuery])
    assertEquals(200, response.status)

    assertEquals(
        [
            [
                columns: ["time", "value"],
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
  void testInfluxDataOrderedDescByDefault() {
    def timeseriesName = "influx.datadesc"
    def start = now().minus(2000)
    postData(timeseriesName, start)

    def influxQuery = 'select value from "' + timeseriesName + '"'
    rhqm.defaultRequestHeaders.Authorization = "Basic YWdlbnQ6MjUyMzExMGYtZTk2ZS00NDg2LTk4NTAtOTRhODU3YzUzOTA5"

    def response = rhqm.get(path: "tenants/${tenantId}/influx/series", query: [q: influxQuery])
    assertEquals(200, response.status)

    assertEquals(
        [
            [
                columns: ["time", "value"],
                name   : timeseriesName,
                points : [
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
    def timeseriesName = "influx.foo"
    def start = now().minus(2000)
    postData(timeseriesName, start)

    def influxQuery = 'select mean(value) from "' + timeseriesName + '" where time > now() - 30s group by time(30s) '
    rhqm.defaultRequestHeaders.Authorization = "Basic YWdlbnQ6MjUyMzExMGYtZTk2ZS00NDg2LTk4NTAtOTRhODU3YzUzOTA5"

    def response = rhqm.get(path: "tenants/${tenantId}/influx/series", query: [q: influxQuery])
    assertEquals(200, response.status)

    assertEquals(
        [
            [
                columns: ["time", "mean"],
                name   : timeseriesName,
                points : [
                    [start.plus(2000).millis, 41.1]
                ]
            ]
        ],
        response.data
    )
  }

  @Test
  void testInfluxLimitClause() {
    def timeseriesName = "influx.limitclause"
    def start = now().minus(2000)
    postData(timeseriesName, start)

    def influxQuery = 'select value from "' + timeseriesName + '" limit 2 order asc '
    rhqm.defaultRequestHeaders.Authorization = "Basic YWdlbnQ6MjUyMzExMGYtZTk2ZS00NDg2LTk4NTAtOTRhODU3YzUzOTA5"

    def response = rhqm.get(path: "tenants/${tenantId}/influx/series", query: [q: influxQuery])
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
    def timeseriesName = "influx.foo3"
    def start = now().minus(2000)
    postData(timeseriesName, start)

    def influxQuery = 'select mean(value) from "' + timeseriesName + '''" where time > '2013-08-12 23:32:01.232'
                        and time < '2013-08-13' group by time(30s) '''
    rhqm.defaultRequestHeaders.Authorization = "Basic YWdlbnQ6MjUyMzExMGYtZTk2ZS00NDg2LTk4NTAtOTRhODU3YzUzOTA5"


    def response = rhqm.get(path: "tenants/${tenantId}/influx/series", query: [q: influxQuery])
    assertEquals(200, response.status)

    assertEquals(
        [
            [
                columns: ["time", "mean"],
                name   : timeseriesName,
                points : []
            ]
        ],
        response.data
    )
  }

  private static void postData(String timeseriesName, DateTime start) {
    rhqm.defaultRequestHeaders.Authorization = "Basic YWdlbnQ6MjUyMzExMGYtZTk2ZS00NDg2LTk4NTAtOTRhODU3YzUzOTA5"
    def response = rhqm.post(path: "tenants/${tenantId}/influx/series", body: [
        [
            name: timeseriesName,
            columns: ["time", "value"],
            points: [
                [start.millis, 40.1],
                [start.plus(1000).millis, 41.1],
                [start.plus(2000).millis, 42.1]
            ]
        ]
    ])
    assertEquals(200, response.status)
  }
}
