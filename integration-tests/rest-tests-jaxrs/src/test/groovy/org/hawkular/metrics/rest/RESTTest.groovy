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

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import groovy.json.JsonOutput
import groovyx.net.http.ContentEncoding
import groovyx.net.http.ContentType
import groovyx.net.http.HttpResponseDecorator
import groovyx.net.http.HttpResponseException
import groovyx.net.http.RESTClient
import org.apache.commons.math3.stat.descriptive.moment.Mean
import org.apache.commons.math3.stat.descriptive.rank.PSquarePercentile
import org.joda.time.DateTime
import org.junit.BeforeClass

import java.util.concurrent.atomic.AtomicInteger

import static junit.framework.Assert.assertNull
import static org.hawkular.metrics.core.service.transformers.BatchStatementTransformer.DEFAULT_BATCH_SIZE
import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertNotNull
import static org.junit.Assert.assertTrue

class RESTTest {

  static baseURI = System.getProperty('hawkular-metrics.base-uri') ?: '127.0.0.1:8080/hawkular/metrics'
  static final double DELTA = 0.001
  static final String TENANT_PREFIX = UUID.randomUUID().toString()
  static final AtomicInteger TENANT_ID_COUNTER = new AtomicInteger(0)
  static final int LARGE_PAYLOAD_SIZE = DEFAULT_BATCH_SIZE
  static ObjectMapper objectMapper = new ObjectMapper()
  static RESTClient hawkularMetrics

  static String tenantHeaderName = "Hawkular-Tenant"
  static String adminTokenHeaderName = "Hawkular-Admin-Token"

  static metricTypes =  [
    ["path": "gauges", "type": "gauge"],
    ["path": "availability", "type": "availability"],
    ["path": "counters", "type": "counter"],
    ["path": "strings", "type": "string"]
  ];

  @BeforeClass
  static void initClient() {
    objectMapper.configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true)
    hawkularMetrics = new RESTClient("http://$baseURI/", ContentType.JSON)
    hawkularMetrics.setContentEncoding(ContentEncoding.Type.GZIP)
    hawkularMetrics.parser['application/json'] = hawkularMetrics.parser['text/plain']
    hawkularMetrics.handler.success = { HttpResponseDecorator resp, Reader data ->
      if (data != null) {
        resp.setData(objectMapper.readValue(data, Object.class))
      }
      resp
    }
    hawkularMetrics.handler.failure = { HttpResponseDecorator resp, Reader data ->
      def text = null
      if (data != null) {
        text = data.text
        resp.setData(resp.contentType.equals('application/json') ? objectMapper.readValue(text, Object.class) : text)
      }
      def msg = "Got error response: ${resp.statusLine}"
      if (text != null) {
        msg = """${msg}
=== Response body
${text}
===
"""
      }
      System.err.println(msg)
      throw new HttpResponseException(resp)
    }
  }

  static String nextTenantId() {
    return "T${TENANT_PREFIX}${TENANT_ID_COUNTER.incrementAndGet()}"
  }

  static String getAdminToken() {
    return System.getProperty("hawkular.metrics.admin-token")
  }

  static void assertDoubleEquals(expected, actual) {
    assertDoubleEquals(null, expected, actual)
  }

  static void assertDoubleEquals(msg, expected, actual) {
    // If a bucket does not contain any data points, then the server returns
    // Double.NaN for max/min/avg. NaN is returned on the client and parsed as
    // a string. If the bucket contains data points, then the returned values
    // will be numeric.

    if (actual instanceof String) {
      assertEquals((String) msg, (Double) expected, Double.parseDouble(actual), DELTA)
    } else {
      assertEquals((String) msg, (Double) expected, (Double) actual, DELTA)
    }
  }

  static def badPost(args, errorHandler) {
    badRequest(hawkularMetrics.&post, args, errorHandler)
  }

  static def badPut(args, errorHandler) {
    badRequest(hawkularMetrics.&put, args, errorHandler, true)
  }

  static def badGet(args, errorHandler) {
    badRequest(hawkularMetrics.&get, args, errorHandler)
  }

  static def badDelete(args, errorHandler) {
    badRequest(hawkularMetrics.&delete, args, errorHandler)
  }

  static def badOptions(args, errorHandler) {
    badRequest(hawkularMetrics.&options, args, errorHandler)
  }

  static def badRequest(method, args, errorHandler, forceContentTypeOnEmptyBody = false) {
    if (forceContentTypeOnEmptyBody && args.body == null) {
      args.headers["Content-Type"] = "application/json"
    }
    try {
      method(args)
      throw new AssertionError("Expected exception to be thrown")
    } catch (HttpResponseException e) {
      errorHandler(e)
    }
  }

  static DateTime getTime() {
    def response = hawkularMetrics.get(path: "clock")
    return new DateTime((Long) response.data.now)
  }

  static void setTime(DateTime time) {
    def response = hawkularMetrics.put(path: "clock", body: [time: time.millis])
    assertEquals(200, response.status)
  }

  static void checkLargePayload(String pathBase, String tenantId, Closure pointGen) {
    def points = []
    for (i in 0..LARGE_PAYLOAD_SIZE + 10) {
      pointGen(points, i)
    }
    def response = hawkularMetrics.post(path: "${pathBase}/test/raw", headers: [(tenantHeaderName): tenantId],
        body: points)
    assertEquals(200, response.status)
  }

  static void invalidPointCheck(String pathBase, String tenantId, def data) {
    badPost(path: "${pathBase}/raw", headers: [(tenantHeaderName): tenantId], body: [
        [id: 'metric', data: data]
    ]) { exception ->
      assertEquals(400, exception.response.status)
    }
    badPost(path: "${pathBase}/metric/raw", headers: [(tenantHeaderName): tenantId], body: data) { exception ->
      assertEquals(400, exception.response.status)
    }
  }

  static void assertNumericBucketsEquals(List expected, List actual) {
    def msg = """
Expected:${expected}
Actual: ${actual}
"""
    assertEquals(msg, expected.size(), actual.size())
    expected.size().times { i ->
      assertNumericBucketEquals(msg, expected.get(i), actual.get(i))
    }
  }

  static void assertNumericBucketEquals(String msg, def expected, def actual) {
    assertEquals(msg, expected.start, actual.start)
    assertEquals(msg, expected.end, actual.end)
    assertEquals(msg, expected.empty, actual.empty)
    assertEquals(msg, expected.samples, actual.samples)
    if (!expected.empty) {
      assertDoubleEquals(msg, expected.min, actual.min)
      assertDoubleEquals(msg, expected.avg, actual.avg)
      assertDoubleEquals(msg, expected.median, actual.median)
      assertDoubleEquals(msg, expected.max, actual.max)
      assertDoubleEquals(msg, expected.sum, actual.sum)
      assertPercentilesEquals(expected.percentiles, actual.percentiles)
    }
  }

  static void assertPercentilesEquals(def expected, def actual) {
    if (expected == null) {
      assertNull(actual)
    } else {
      assertEquals(expected.size(), actual.size())
      expected.each { expectedP ->
        def actualP = actual.find { it.quantile == expectedP.quantile }
        assertNotNull("Expected to find $expectedP in $actual", actualP)
        assertDoubleEquals("The percentile value is wrong", expectedP.value, actualP.value)
      }
    }
  }

  static void assertAvailablityBucketEquals(String msg, def expected, def actual) {
    assertEquals(msg, expected.start, actual.start)
    assertEquals(msg, expected.end, actual.end)
    assertEquals(msg, expected.empty, actual.empty)
    if (!expected.empty) {
      assertEquals(msg, expected.lastNotUptime, actual.lastNotUptime)
      assertDoubleEquals(msg, expected.uptimeRatio, actual.uptimeRatio)
      assertEquals(msg, expected.notUpCount, actual.notUpCount)
      assertEquals(msg, expected.downDuration, actual.downDuration)
      assertEquals(msg, expected.lastNotUptime, actual.lastNotUptime)
      assertEquals(msg, expected.adminDuration, actual.adminDuration)
      assertEquals(msg, expected.unknownDuration, actual.unknownDuration)
      assertEquals(msg, expected.upDuration, actual.upDuration)
      assertEquals(msg, expected.notUpDuration, actual.notUpDuration)
      assertEquals(msg, expected.durationMap, actual.durationMap)
    }
  }

  static double avg(List values) {
    Mean mean = new Mean()
    values.each { mean.increment(it as double) }
    return mean.result
  }

  static double median(List values) {
    PSquarePercentile median = new PSquarePercentile(50.0)
    values.each { median.increment(it as double) }
    return median.result
  }

  static double percentile(double p, List values) {
    PSquarePercentile percentile = new PSquarePercentile(p)
    values.each { percentile.increment(it as double) }
    return percentile.result
  }

  static double rate(Map dataPointX, Map dataPointY) {
    double valueDiff = (dataPointY.value - dataPointX.value) as Double
    long timeDiff = (dataPointY.timestamp - dataPointX.timestamp) as Long
    return 60_000 * valueDiff / timeDiff
  }

  static void assertTaggedBucketEquals(def expected, def actual) {
    assertDoubleEquals(expected.max, actual.max)
    assertDoubleEquals(expected.min, actual.min)
    assertDoubleEquals(expected.sum, actual.sum)
    assertDoubleEquals(expected.avg, actual.avg)
    assertDoubleEquals(expected.median, actual.median)
    assertEquals(expected.samples, actual.samples)
  }

  static void assertRateEquals(String msg, def expected, def actual) {
    assertEquals(msg, expected.timestamp, actual.timestamp)
    assertDoubleEquals(msg, expected.value, actual.value)
  }

  static void assertStackedDataPointBucket(actualBucket, start, end, Object... dataPoints) {
    def min = 0
    def max = 0
    def avg = 0
    def sum = 0
    def empty = true
    def samples = null

    if (dataPoints.size() > 0) {
      min = dataPoints.sum {it.value}
      max = dataPoints.sum {it.value}
      avg = dataPoints.sum {it.value}
      sum = dataPoints.sum {it.value}
      empty = false
      samples = dataPoints.size()
    }

    assertEquals(start, actualBucket.start)
    assertEquals(end, actualBucket.end)

    assertDoubleEquals(min, actualBucket.min)
    assertDoubleEquals(max, actualBucket.max)
    assertDoubleEquals(avg, actualBucket.avg)
    assertDoubleEquals(sum, actualBucket.sum)
    assertEquals(empty, actualBucket.empty)
    assertEquals(samples, actualBucket.samples)
    assertTrue("Expected the [median] property to be set", actualBucket.median != null)
  }

  /**
   * Useful for debugging
   */
  static void printJson(data) {
    def json = JsonOutput.toJson(data)
    println "RESULTS:\n${JsonOutput.prettyPrint(json)}"

  }

  static void assertContentEncoding(response) {
    assertEquals('gzip', response.headers['Content-Encoding'].getValue())
  }
}
