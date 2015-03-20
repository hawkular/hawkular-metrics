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

import static java.lang.Double.NaN
import static org.apache.commons.math3.stat.StatUtils.percentile
import static org.joda.time.Seconds.seconds
import static org.junit.Assert.assertEquals

import org.hawkular.metrics.core.impl.DateTimeService
import org.joda.time.DateTime
import org.junit.Test

/**
 * @author Thomas Segismont
 */
class NumericMetricStatisticsITest extends RESTTest {
  private static final int SAMPLE_SIZE = 200
  private static final int NUMBER_OF_BUCKETS = 10

  @Test
  void shouldNotAcceptInvalidParams() {
    String tenantId = nextTenantId()
    String metric = "test"

    def response = hawkularMetrics.post(path: "$tenantId/metrics/numeric/$metric/data", body: [
        [timestamp: new DateTimeService().currentHour().minusHours(1).millis, value: 1]
    ])
    assertEquals(200, response.status)

    badGet(path: "${tenantId}/metrics/numeric/$metric/data", query: [buckets: 0]) { exception ->
      // Bucket count = zero
      assertEquals(400, exception.response.status)
    }

    badGet(path: "${tenantId}/metrics/numeric/$metric/data", query: [buckets: Integer.MAX_VALUE]) { exception ->
      // Bucket size = zero
      assertEquals(400, exception.response.status)
    }

    badGet(path: "${tenantId}/metrics/numeric/$metric/data", query: [bucketDuration: "1w"]) { exception ->
      // Illegal duration
      assertEquals(400, exception.response.status)
    }

    badGet(path: "${tenantId}/metrics/numeric/$metric/data",
        query: [start: 0, end: Long.MAX_VALUE, bucketDuration: "1ms"]) {
      exception ->
        // Number of buckets is too large
        assertEquals(400, exception.response.status)
    }

    badGet(path: "${tenantId}/metrics/numeric/$metric/data", query: [buckets: 1, bucketDuration: "1d"]) { exception ->
      // Both buckets and bucketDuration parameters provided
      assertEquals(400, exception.response.status)
    }
  }

  @Test
  void emptyNotEmptyTest() {
    DateTimeService dateTimeService = new DateTimeService()
    String tenantId = nextTenantId()
    String metric = 'n1'
    DateTime start = dateTimeService.currentHour().minusHours(1)
    DateTime end = start.plusHours(1)

    int numBuckets = 10
    long bucketSize = (end.millis - start.millis) / numBuckets
    def buckets = []
    numBuckets.times { buckets.add(start.millis + (it * bucketSize)) }

    def response = hawkularMetrics.post(path: "$tenantId/metrics/numeric/$metric/data", body: [
        [timestamp: buckets[0], value: 12.22],
        [timestamp: buckets[0] + seconds(10).toStandardDuration().millis, value: 12.37],
        [timestamp: buckets[4], value: 25],
        [timestamp: buckets[4] + seconds(15).toStandardDuration().millis, value: 25],
        [timestamp: buckets[9], value: 18.367],
        [timestamp: buckets[9] + seconds(10).toStandardDuration().millis, value: 19.01]
    ])
    assertEquals(200, response.status)

    response = hawkularMetrics.get(path: "${tenantId}/metrics/numeric/$metric/data",
        query: [start: start.millis, end: end.millis, buckets: 10])
    assertEquals(200, response.status)

    def avg0 = (12.22 + 12.37) / 2
    def avg9 = (18.367 + 19.01) / 2

    def expectedData = [
        [
            start: buckets[0], end: buckets[0] + bucketSize, empty: false, min: 12.22,
            avg  : avg0, median: avg0, max: 12.37, percentile95th: 12.37
        ], [
            start: buckets[1], end: buckets[1] + bucketSize, empty: true, min: NaN,
            avg  : NaN, median: NaN, max: NaN, percentile95th: NaN
        ], [
            start: buckets[2], end: buckets[2] + bucketSize, empty: true, min: NaN,
            avg  : NaN, median: NaN, max: NaN, percentile95th: NaN
        ], [
            start: buckets[3], end: buckets[3] + bucketSize, empty: true, min: NaN,
            avg  : NaN, median: NaN, max: NaN, percentile95th: NaN
        ], [
            start: buckets[4], end: buckets[4] + bucketSize, empty: false, min: 25.0,
            avg  : 25.0, median: 25.0, max: 25.0, percentile95th: 25.0],
        [
            start: buckets[5], end: buckets[5] + bucketSize, empty: true, min: NaN,
            avg  : NaN, median: NaN, max: NaN, percentile95th: NaN
        ], [
            start: buckets[6], end: buckets[6] + bucketSize, empty: true, min: NaN,
            avg  : NaN, median: NaN, max: NaN, percentile95th: NaN
        ], [
            start: buckets[7], end: buckets[7] + bucketSize, empty: true, min: NaN,
            avg  : NaN, median: NaN, max: NaN, percentile95th: NaN
        ], [
            start: buckets[8], end: buckets[8] + bucketSize, empty: true, min: NaN,
            avg  : NaN, median: NaN, max: NaN, percentile95th: NaN
        ], [
            start: buckets[9], end: buckets[9] + bucketSize, empty: false, min: 18.367,
            avg  : avg9, median: avg9, max: 19.01, percentile95th: 19.01
        ]
    ]

    assertEquals('The number of bucketed data points is wrong', expectedData.size(), response.data.size())
    expectedData.size().times { assertBucketEquals(expectedData[it], response.data[it]) }
  }

  @Test
  void largeDataSetTest() {
    String tenantId = nextTenantId()
    String metric = "test"

    def start = new DateTimeService().currentHour().minusHours(24)

    for (step in 1..NUMBER_OF_BUCKETS) {
      def hour = start.plusHours(step - 1)

      def values = createSample(step)
      values = Arrays.asList(values)
      Collections.shuffle(values);

      def data = [];
      for (i in 1..values.size()) {
        data.add([timestamp: hour.plusMillis(i).millis, value: values[i - 1]])
      }

      def response = hawkularMetrics.post(path: "$tenantId/metrics/numeric/$metric/data", body: data)
      assertEquals(200, response.status)
    }

    def response = hawkularMetrics.get(path: "${tenantId}/metrics/numeric/$metric/data",
        query: [start: start.millis, end: start.plusHours(NUMBER_OF_BUCKETS).millis, bucketDuration: "1h"])
    assertEquals(200, response.status)

    def percentileBase = percentile(createSample(1), 95.0);
    def expectedData = []
    for (step in 1..NUMBER_OF_BUCKETS) {
      expectedData.add([
          start         : start.plusHours(step - 1).millis,
          end           : start.plusHours(step).millis,
          min           : step,
          avg           : step - 1 + (SAMPLE_SIZE + 1) / 2,
          median        : step - 1 + (SAMPLE_SIZE + 1) / 2,
          max           : step - 1 + SAMPLE_SIZE,
          percentile95th: step - 1 + percentileBase,
          empty         : false]
      )
    }

    assertEquals('The number of bucketed data points is wrong', expectedData.size(), response.data.size())
    expectedData.size().times { assertBucketEquals(expectedData[it], response.data[it]) }
  }

  private static double[] createSample(def step) {
    def values = new double[SAMPLE_SIZE];
    Arrays.setAll(values, { i -> (double) (i + step) })
    return values;
  }

  private static void assertBucketEquals(def expected, def actual) {
    assertEquals(expected.start, actual.start)
    assertEquals(expected.end, actual.end)
    assertEquals(expected.empty, actual.empty)
    assertDoubleEquals(expected.min, actual.min)
    assertDoubleEquals(expected.avg, actual.avg)
    assertDoubleEquals(expected.median, actual.median)
    assertDoubleEquals(expected.max, actual.max)
    assertDoubleEquals(expected.percentile95th, actual.percentile95th)
  }
}
