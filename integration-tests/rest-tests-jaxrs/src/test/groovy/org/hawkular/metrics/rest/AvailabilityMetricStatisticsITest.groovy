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

import static java.lang.Double.NaN

import static org.joda.time.Minutes.minutes
import static org.joda.time.Seconds.seconds
import static org.junit.Assert.assertEquals

import org.hawkular.metrics.datetime.DateTimeService
import org.joda.time.DateTime
import org.junit.Test

import static org.joda.time.Minutes.minutes
import static org.joda.time.Seconds.seconds
import static org.junit.Assert.assertEquals
/**
 * @author Thomas Segismont
 */
class AvailabilityMetricStatisticsITest extends RESTTest {

  @Test
  void shouldNotAcceptInvalidParams() {
    String tenantId = nextTenantId()
    String metric = "test"

    def response = hawkularMetrics.post(path: "availability/$metric/raw", body: [
        [timestamp: new DateTimeService().currentHour().minusHours(1).millis, value: "up"]
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)

    badGet(path: "availability/$metric/stats", query: [buckets: 0], headers: [(tenantHeaderName): tenantId]) { exception ->
      // Bucket count = zero
      assertEquals(400, exception.response.status)
    }

    badGet(path: "availability/$metric/stats", query: [buckets: Integer.MAX_VALUE], headers: [(tenantHeaderName): tenantId]) { exception ->
      // Bucket size = zero
      assertEquals(400, exception.response.status)
    }

    badGet(path: "availability/$metric/stats", query: [bucketDuration: "1w"], headers: [(tenantHeaderName): tenantId]) { exception ->
      // Illegal duration
      assertEquals(400, exception.response.status)
    }

    badGet(path: "availability/$metric/stats",
        query: [start: 0, end: Long.MAX_VALUE, bucketDuration: "1ms"], headers: [(tenantHeaderName): tenantId]) {
      exception ->
        // Number of buckets is too large
        assertEquals(400, exception.response.status)
    }

    badGet(path: "availability/$metric/stats", query: [buckets: 1, bucketDuration: "1d"], headers: [(tenantHeaderName): tenantId]) { exception ->
      // Both buckets and bucketDuration parameters provided
      assertEquals(400, exception.response.status)
    }
  }

  @Test
  void emptyNotEmptyTest() {
    String tenantId = nextTenantId()
    String metric = "test"

    DateTimeService dateTimeService = new DateTimeService()
    DateTime start = dateTimeService.currentHour().minusHours(1)
    DateTime end = start.plusHours(1)

    int numBuckets = 3
    long bucketSize = (end.millis - start.millis) / numBuckets
    def buckets = []
    numBuckets.times { buckets.add(start.millis + (it * bucketSize)) }

    def response = hawkularMetrics.post(path: "availability/$metric/raw", body: [
        [timestamp: buckets[1] + seconds(60).toStandardDuration().millis, value: "up"],
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)

    response = hawkularMetrics.get(path: "availability/$metric/stats",
        query: [start: start.millis, end: end.millis, buckets: numBuckets], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)

    def expectedData = [
        [
            start           : buckets[0], end: buckets[0] + bucketSize, empty: true,
        ], [
            start           : buckets[1], end: buckets[1] + bucketSize, empty: false,
            downDuration: 0, lastNotUptime: 0, uptimeRatio: 1.0, notUpCount: 0,
        ], [
            start           : buckets[2], end: buckets[2] + bucketSize, empty: true
        ]
    ]

    assertEquals('The number of bucketed data points is wrong', expectedData.size(), response.data.size())
    expectedData.size().times { assertBucketEquals(expectedData[it], response.data[it]) }
  }

  @Test
  void bucketPointTest() {
    def tenantId = nextTenantId()
    def metric = "test"
    def start = new DateTimeService().currentHour().minusHours(24)
    def bucketsCount = 10

    for (step in 1..bucketsCount) {
      def hour = start.plusHours(step - 1)
      def nextTimestamp = { int index ->
        hour.plusMinutes(index).millis
      }

      def data = [];
      for (i in 1..60) {
        data.add([timestamp: nextTimestamp(i - 1), value: i % 4 == 0 ? "down" : "up"])
      }

      def response = hawkularMetrics.post(path: "availability/$metric/raw", body: data, headers: [(tenantHeaderName): tenantId])
      assertEquals(200, response.status)
    }

    def response = hawkularMetrics.get(path: "availability/$metric/stats",
        query: [start: start.millis, end: start.plusHours(bucketsCount).millis, bucketDuration: "1h"], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)

    def expectedData = []
    for (step in 1..bucketsCount) {
      expectedData.add([
          start           : start.plusHours(step - 1).millis,
          end             : start.plusHours(step).millis,
          downDuration: minutes(15).toStandardDuration().millis,
          lastNotUptime   : start.plusHours(step).millis,
          uptimeRatio     : 0.75,
          notUpCount      : 15,
          empty           : false
      ]
      )
    }

    assertEquals('The number of bucketed data points is wrong', expectedData.size(), response.data.size())
    expectedData.size().times { assertBucketEquals(expectedData[it], response.data[it]) }
  }

  private static void assertBucketEquals(def expected, def actual) {
    assertEquals(expected.start, actual.start)
    assertEquals(expected.end, actual.end)
    assertEquals(expected.empty, actual.empty)
    if (!expected.empty) {
      assertEquals(expected.downDuration, actual.downDuration)
      assertEquals(expected.lastNotUptime, actual.lastNotUptime)
      assertDoubleEquals(expected.uptimeRatio, actual.uptimeRatio)
      assertEquals(expected.notUpCount, actual.notUpCount)
    }
  }
}
