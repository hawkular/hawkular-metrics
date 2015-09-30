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

import static org.hawkular.metrics.core.impl.transformers.BatchStatementStrategy.MAX_BATCH_SIZE
import static org.junit.Assert.assertEquals

import java.util.concurrent.atomic.AtomicInteger

import org.joda.time.DateTime
import org.junit.BeforeClass

import com.google.common.base.Charsets

import groovyx.net.http.ContentType
import groovyx.net.http.RESTClient

class RESTTest {

  static baseURI = System.getProperty('hawkular-metrics.base-uri') ?: '127.0.0.1:8080/hawkular/metrics'
  static final double DELTA = 0.001
  static final String TENANT_PREFIX = UUID.randomUUID().toString()
  static final AtomicInteger TENANT_ID_COUNTER = new AtomicInteger(0)
  static final int LARGE_PAYLOAD_SIZE = MAX_BATCH_SIZE
  static String tenantHeaderName = "Hawkular-Tenant";
  static RESTClient hawkularMetrics
  static defaultFailureHandler

  @BeforeClass
  static void initClient() {
    hawkularMetrics = new RESTClient("http://$baseURI/", ContentType.JSON)
    defaultFailureHandler = hawkularMetrics.handler.failure
    hawkularMetrics.handler.failure = { resp ->
      def msg = "Got error response: ${resp.statusLine}"
      if (resp.entity != null && resp.entity.contentLength != 0) {
        def baos = new ByteArrayOutputStream()
        resp.entity.writeTo(baos)
        def entity = new String(baos.toByteArray(), Charsets.UTF_8)
        msg = """${msg}
=== Response body
${entity}
===
"""
      }
      System.err.println(msg)
      return resp
    }
  }

  static String nextTenantId() {
    return "T${TENANT_PREFIX}${TENANT_ID_COUNTER.incrementAndGet()}"
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

  static def badRequest(method, args, errorHandler, forceContentTypeOnEmptyBody = false) {
    def originalFailureHandler = hawkularMetrics.handler.failure;

    if (forceContentTypeOnEmptyBody && args.body == null) {
      args.headers["Content-Type"] = "application/json"
    }

    hawkularMetrics.handler.failure = defaultFailureHandler;
    try {
      method(args)
      throw new AssertionError("Expected exception to be thrown")
    } catch (e) {
      errorHandler(e)
    } finally {
      hawkularMetrics.handler.failure = originalFailureHandler;
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
    def response = hawkularMetrics.post(path: "${pathBase}/test/data", headers: [(tenantHeaderName): tenantId],
        body: points)
    assertEquals(200, response.status)
  }

  static void invalidPointCheck(String pathBase, String tenantId, def data) {
    badPost(path: "${pathBase}/data", headers: [(tenantHeaderName): tenantId], body: [
        [id: 'metric', data: data]
    ]) { exception ->
      assertEquals(400, exception.response.status)
    }
    badPost(path: "${pathBase}/metric/data", headers: [(tenantHeaderName): tenantId], body: data) { exception ->
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
      assertDoubleEquals(msg, expected.percentile95th, actual.percentile95th)
    }
  }
}
