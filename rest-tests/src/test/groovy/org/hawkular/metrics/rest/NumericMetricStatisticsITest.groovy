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

import static org.apache.commons.math3.stat.StatUtils.percentile
import static org.junit.Assert.assertEquals

import org.hawkular.metrics.core.impl.DateTimeService
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
      assertEquals(SAMPLE_SIZE, response.status)
    }

    def response = hawkularMetrics.get(path: "${tenantId}/metrics/numeric/$metric/data",
        query: [start: start.millis, end: start.plusHours(NUMBER_OF_BUCKETS).millis, bucketDuration: "1h"])
    assertEquals(200, response.status)

    def percentileBase = percentile(createSample(1), 95.0);
    def expectedData = []
    for (step in 1..NUMBER_OF_BUCKETS) {
      expectedData.add([
          timestamp     : start.plusHours(step - 1).millis,
          min           : step,
          avg           : step - 1 + (SAMPLE_SIZE + 1) / 2,
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

}
