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

import static java.util.concurrent.TimeUnit.MILLISECONDS

import static org.junit.Assert.assertArrayEquals
import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertNotNull

import org.hawkular.metrics.core.impl.DateTimeService
import org.influxdb.InfluxDB
import org.influxdb.InfluxDBFactory
import org.influxdb.dto.Serie
import org.influxdb.dto.Serie.Builder
import org.junit.Before
import org.junit.Test

/**
 * @author Jeeva Kandasamy
 * @author Thomas Segismont
 */
class InfluxDriverITest extends RESTTest {
  static InfluxDB influxDB = InfluxDBFactory.connect("http://" + baseURI + "/", "hawkular", "hawkular")

  def dbName = nextTenantId()
  def startTime = new DateTimeService().currentHour().minusDays(10) // some time in the past
  def testSerieName = "test"
  Serie testSerie

  @Before
  void getSerie() {
    testSerie = new Builder(testSerieName).columns("time", "value")
        .values(startTime.millis, 10.9)
        .values(startTime.plus(1000).millis, 11.9)
        .values(startTime.plus(2000).millis, 12.9)
        .values(startTime.plus(3000).millis, 13.9)
        .values(startTime.plus(4000).millis, 14.9)
        .values(startTime.plus(5000).millis, 15.9)
        .values(startTime.plus(6000).millis, 16.9)
        .values(startTime.plus(7000).millis, 17.9)
        .values(startTime.plus(8000).millis, 18.9)
        .build()
    influxDB.write(dbName, MILLISECONDS, testSerie)
  }

  @Test
  void testWrite() {
    def series = influxDB.query(dbName, "select * from " + testSerieName, MILLISECONDS)
    assertNotNull(series)
    assertEquals(1, series.size())
  }

  @Test
  void testQueryOrderedAsc() {
    def query = "select value from ${testSerieName} order asc"
    def series = influxDB.query(dbName, query, MILLISECONDS)
    assertNotNull(series)
    assertEquals(1, series.size())
    assertSeriesEquals(testSerie, series.get(0))
  }

  @Test
  void testQueryOrderedDesc() {
    def query = "select value from ${testSerieName} order desc"
    def series = influxDB.query(dbName, query, MILLISECONDS)
    assertNotNull(series)
    assertEquals(1, series.size())

    def expected = new Builder(testSerieName).columns(testSerie.columns)
    testSerie.rows.reverseEach { row -> expected.values(row.values() as Object[]) }
    assertSeriesEquals(expected.build(), series.get(0))
  }

  @Test
  void testQueryLimitClause() {
    def query = "select value from ${testSerieName} limit 3 order asc"
    def series = influxDB.query(dbName, query, MILLISECONDS)
    assertNotNull(series)
    assertEquals(1, series.size())

    def expected = new Builder(testSerieName).columns("time", "value")
        .values(startTime.millis, 10.9)
        .values(startTime.plus(1000).millis, 11.9)
        .values(startTime.plus(2000).millis, 12.9)
        .build()
    assertSeriesEquals(expected, series.get(0))
  }

  @Test
  void testQueryTop() {
    def query = "select top(value, 3) from ${testSerieName} where time > now() - 15d group by time(1w)"
    def series = influxDB.query(dbName, query, MILLISECONDS)
    assertNotNull(series)
    assertEquals(1, series.size())

    def expected = new Builder(testSerieName).columns("time", "top")
        .values(startTime.plus(8000).millis, 18.9)
        .values(startTime.plus(7000).millis, 17.9)
        .values(startTime.plus(6000).millis, 16.9)
        .build()
    assertSeriesEquals(expected, series.get(0))
  }

  @Test
  void testQueryBottom() {
    def query = "select bottom(value, 3) from ${testSerieName} where time > now() - 15d group by time(1w)"
    def series = influxDB.query(dbName, query, MILLISECONDS)
    assertNotNull(series)
    assertEquals(1, series.size())

    def expected = new Builder(testSerieName).columns("time", "bottom")
        .values(startTime.millis, 10.9)
        .values(startTime.plus(1000).millis, 11.9)
        .values(startTime.plus(2000).millis, 12.9)
        .build()
    assertSeriesEquals(expected, series.get(0))
  }

  @Test
  void testQueryStddev() {
    def query = "select stddev(value) from ${testSerieName} where time > now() - 15d group by time(1w)"
    def series = influxDB.query(dbName, query, MILLISECONDS)
    assertNotNull(series)
    assertEquals(1, series.size())

    def expected = new Builder(testSerieName).columns("time", "stddev")
        .values(startTime.plus(8000).millis, 2.7386127875258)
        .build()
    assertSeriesEquals(expected, series.get(0))
  }

  @Test
  void testQueryMean() {
    def query = "select mean(value) from ${testSerieName} where time > now() - 15d group by time(1w)"
    def series = influxDB.query(dbName, query, MILLISECONDS)
    assertNotNull(series)
    assertEquals(1, series.size())

    def expected = new Builder(testSerieName).columns("time", "mean")
        .values(startTime.plus(8000).millis, 14.9)
        .build()
    assertSeriesEquals(expected, series.get(0))
  }

  @Test
  void testQuerySum() {
    def query = "select sum(value) from ${testSerieName} where time > now() - 15d group by time(1w)"
    def series = influxDB.query(dbName, query, MILLISECONDS)
    assertNotNull(series)
    assertEquals(1, series.size())

    def expected = new Builder(testSerieName).columns("time", "sum")
        .values(startTime.plus(8000).millis, 134.1)
        .build()
    assertSeriesEquals(expected, series.get(0))
  }

  static void assertSeriesEquals(Serie expected, Serie actual) {
    def msg = """Expected: ${expected}
Actual: ${actual}
"""
    assertNotNull(msg, expected)
    assertNotNull(msg, actual)
    assertEquals(msg, expected.name, actual.name)
    assertEquals(msg, expected.rows.size(), actual.rows.size())
    assertArrayEquals(msg, expected.columns, actual.columns)
    for (int i in 0..expected.rows.size() - 1) {
      expected.columns.each { column ->
        assertDoubleEquals(msg, expected.rows.get(i).get(column), actual.rows.get(i).get(column))
      }
    }
  }
}
