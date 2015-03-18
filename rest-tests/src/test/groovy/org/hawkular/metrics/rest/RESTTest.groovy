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

import static org.junit.Assert.assertEquals
import static org.junit.Assert.fail

import java.util.concurrent.atomic.AtomicInteger

import org.junit.BeforeClass

import groovyx.net.http.ContentType
import groovyx.net.http.RESTClient

class RESTTest {

  static baseURI = System.getProperty('hawkular-metrics.base-uri') ?: '127.0.0.1:8080/hawkular-metrics'
  static final double DELTA = 0.001
  static final String TENANT_PREFIX = UUID.randomUUID().toString()
  static final AtomicInteger TENANT_ID_COUNTER = new AtomicInteger(0)
  static RESTClient hawkularMetrics

  @BeforeClass
  static void initClient() {
    hawkularMetrics = new RESTClient("http://$baseURI/", ContentType.JSON)
  }

  static String nextTenantId() {
    return "T${TENANT_PREFIX}${TENANT_ID_COUNTER.incrementAndGet()}"
  }

  static void assertDoubleEquals(expected, actual) {
    // If a bucket does not contain any data points, then the server returns
    // Double.NaN for max/min/avg. NaN is returned on the client and parsed as
    // a string. If the bucket contains data points, then the returned values
    // will be numeric.

    if (actual instanceof String) {
      assertEquals((Double) expected, Double.parseDouble(actual), DELTA)
    } else {
      assertEquals((Double) expected, (Double) actual, DELTA)
    }
  }

  static def badPost(args, errorHandler) {
    try {
      def object = hawkularMetrics.post(args)
      fail("Expected exception to be thrown")
      return object
    } catch (e) {
      errorHandler(e)
    }
  }

  static def badGet(args, errorHandler) {
    try {
      def object = hawkularMetrics.get(args)
      fail("Expected exception to be thrown")
      return object
    } catch (e) {
      errorHandler(e)
    }
  }
}
