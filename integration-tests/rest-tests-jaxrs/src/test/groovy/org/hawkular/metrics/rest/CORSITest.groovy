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

import org.joda.time.DateTime
import org.junit.Before
import org.junit.Test

import static org.hawkular.jaxrs.filter.cors.Headers.*
import static org.joda.time.DateTime.now
import static org.junit.Assert.assertEquals
import static org.junit.Assume.assumeTrue

class CORSITest extends RESTTest {
  static final String testOrigin = System.getProperty("hawkular-metrics.test.origin", "http://test.hawkular.org")
  static final String testAccessControlAllowHeaders = DEFAULT_CORS_ACCESS_CONTROL_ALLOW_HEADERS + ',' +
    System.getProperty("hawkular-metrics.test.access-control-allow-headers")

  @Before
  void assumeIsMavenBuild() {
    def version = System.properties.getProperty('project.version')
    assumeTrue('This test only works in a Maven build', version != null)
  }

  @Test
  void testOptionsWithOrigin() {
    def response = hawkularMetrics.options(path: "ping",
        headers: [
            (ACCESS_CONTROL_REQUEST_METHOD): "POST",
            //this should be ignored by the container and reply with pre-configured headers
            (ACCESS_CONTROL_ALLOW_HEADERS): "test-header",
            (ORIGIN): testOrigin,
        ])

    def responseHeaders = "==== Response Headers = Start  ====\n"
    response.headers.each { responseHeaders += "${it.name} : ${it.value}\n" }
    responseHeaders += "==== Response Headers = End ====\n"

    //Expected a 200 because this is be a pre-flight call that should never reach the resource router
    assertEquals(200, response.status)
    assertEquals(null, response.getData())
    assertEquals(responseHeaders, DEFAULT_CORS_ACCESS_CONTROL_ALLOW_METHODS, response.headers[ACCESS_CONTROL_ALLOW_METHODS].value)
    assertEquals(responseHeaders, testAccessControlAllowHeaders, response.headers[ACCESS_CONTROL_ALLOW_HEADERS].value)
    assertEquals(responseHeaders, testOrigin, response.headers[ACCESS_CONTROL_ALLOW_ORIGIN].value)
    assertEquals(responseHeaders, "true", response.headers[ACCESS_CONTROL_ALLOW_CREDENTIALS].value)
    assertEquals(responseHeaders, (72 * 60 * 60) + "", response.headers[ACCESS_CONTROL_MAX_AGE].value)
  }

  @Test
  void testOptionsWithBadOrigin() {
    badOptions(path: "gauges/test/raw", headers: [
            (ACCESS_CONTROL_REQUEST_METHOD): "OPTIONS",
            (ORIGIN): "*"
    ]) { exception ->
      //Expected 400 because this pre-flight call that should never reach the resource router since
      //the request origin is bad
      assertEquals(400, exception.response.status)
      assertEquals(null, exception.response.getData())
    }

    def wrongSchemeOrigin = testOrigin.replaceAll("http://", "https://")
    badOptions(path: "gauges/test/raw", headers: [
            (ACCESS_CONTROL_REQUEST_METHOD): "GET",
            (ORIGIN): wrongSchemeOrigin
    ]) { exception ->
      //Expected 400 because this call should never reach the resource router since
      //the request origin is bad
      assertEquals(400, exception.response.status)
      assertEquals(null, exception.response.getData())
    }
  }

  @Test
  void testOptionsWithSubdomainOrigin() {
    //construct a subdomain with "tester." as prefix
    def subDomainOrigin = testOrigin.substring(0, testOrigin.indexOf("/") + 2) + "tester." + testOrigin.substring(testOrigin.indexOf("/") + 2)
    def response = hawkularMetrics.options(path: "gauges/test/raw",
        headers: [
            (ACCESS_CONTROL_REQUEST_METHOD): "GET",
            (ORIGIN): subDomainOrigin
        ])

    def responseHeaders = "==== Response Headers = Start  ====\n"
    response.headers.each { responseHeaders += "${it.name} : ${it.value}\n" }
    responseHeaders += "==== Response Headers = End ====\n"

    assertEquals(200, response.status)
    assertEquals(null, response.getData())
    assertEquals(responseHeaders, DEFAULT_CORS_ACCESS_CONTROL_ALLOW_METHODS, response.headers[ACCESS_CONTROL_ALLOW_METHODS].value)
    assertEquals(responseHeaders, testAccessControlAllowHeaders, response.headers[ACCESS_CONTROL_ALLOW_HEADERS].value)
    assertEquals(responseHeaders, subDomainOrigin, response.headers[ACCESS_CONTROL_ALLOW_ORIGIN].value)
    assertEquals(responseHeaders, "true", response.headers[ACCESS_CONTROL_ALLOW_CREDENTIALS].value)
    assertEquals(responseHeaders, (72 * 60 * 60) + "", response.headers[ACCESS_CONTROL_MAX_AGE].value)
  }

  @Test
  void testOptionsWithoutTenantIDAndData() {
    DateTime start = now().minusMinutes(20)
    def tenantId = nextTenantId()

    // First create a couple gauge metrics by only inserting data
    def response = hawkularMetrics.post(path: "gauges/raw", body: [
        [
            id: "m11",
            data: [
                [timestamp: start.millis, value: 1.1],
                [timestamp: start.plusMinutes(1).millis, value: 1.2]
            ]
        ],
        [
            id: "m12",
            data: [
                [timestamp: start.millis, value: 2.1],
                [timestamp: start.plusMinutes(1).millis, value: 2.2]
            ]
        ]
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)

    // Now query for the gauge metrics
    response = hawkularMetrics.get(path: "metrics", query: [type: "gauge", timestamps: "true"], headers: [
            (tenantHeaderName): tenantId])

    assertEquals(200, response.status)
    assertEquals([
        [dataRetention: 7, tenantId: tenantId, id: "m11", type: "gauge", minTimestamp: start.millis, maxTimestamp: start.plusMinutes(1).millis],
        [dataRetention: 7, tenantId: tenantId, id: "m12", type: "gauge", minTimestamp: start.millis, maxTimestamp: start.plusMinutes(1).millis],
    ], (response.data as List).sort({ m1, m2 -> m1.id.compareTo(m2.id) }))

    //Send a CORS pre-flight request and make sure it returns no data
    response = hawkularMetrics.options(path: "metrics",
        query: [type: "gauge"],
        headers: [
            (ACCESS_CONTROL_REQUEST_METHOD): "GET",
            (ORIGIN): testOrigin
        ])

    def responseHeaders = "==== Response Headers = Start  ====\n"
    response.headers.each { responseHeaders += "${it.name} : ${it.value}\n" }
    responseHeaders += "==== Response Headers = End ====\n"

    //Expected a 200 because this is be a pre-flight call that should never reach the resource router
    assertEquals(200, response.status)
    assertEquals(null, response.getData())
    assertEquals(responseHeaders, DEFAULT_CORS_ACCESS_CONTROL_ALLOW_METHODS, response.headers[ACCESS_CONTROL_ALLOW_METHODS].value)
    assertEquals(responseHeaders, testAccessControlAllowHeaders, response.headers[ACCESS_CONTROL_ALLOW_HEADERS].value)
    assertEquals(responseHeaders, testOrigin, response.headers[ACCESS_CONTROL_ALLOW_ORIGIN].value)
    assertEquals(responseHeaders, "true", response.headers[ACCESS_CONTROL_ALLOW_CREDENTIALS].value)
    assertEquals(responseHeaders, (72 * 60 * 60) + "", response.headers[ACCESS_CONTROL_MAX_AGE].value)

    //Requery "metrics" endpoint to make sure data gets returned and check headers
    response = hawkularMetrics.get(path: "metrics", query: [type: "gauge", timestamps: "true"],
        headers: [
            (tenantHeaderName): tenantId,
            (ORIGIN): testOrigin
        ])

    responseHeaders = "==== Response Headers = Start  ====\n"
    response.headers.each { responseHeaders += "${it.name} : ${it.value}\n" }
    responseHeaders += "==== Response Headers = End ====\n"

    assertEquals(200, response.status)
    assertEquals([
        [dataRetention: 7, tenantId: tenantId, id: "m11", type: "gauge", minTimestamp: start.millis, maxTimestamp: start.plusMinutes(1).millis],
        [dataRetention: 7, tenantId: tenantId, id: "m12", type: "gauge", minTimestamp: start.millis, maxTimestamp: start.plusMinutes(1).millis],
    ], (response.data as List).sort({ m1, m2 -> m1.id.compareTo(m2.id) }))
    assertEquals(responseHeaders, DEFAULT_CORS_ACCESS_CONTROL_ALLOW_METHODS, response.headers[ACCESS_CONTROL_ALLOW_METHODS].value)
    assertEquals(responseHeaders, testAccessControlAllowHeaders, response.headers[ACCESS_CONTROL_ALLOW_HEADERS].value)
    assertEquals(responseHeaders, testOrigin, response.headers[ACCESS_CONTROL_ALLOW_ORIGIN].value)
    assertEquals(responseHeaders, "true", response.headers[ACCESS_CONTROL_ALLOW_CREDENTIALS].value)
    assertEquals(responseHeaders, (72 * 60 * 60) + "", response.headers[ACCESS_CONTROL_MAX_AGE].value)
  }

  @Test
  void testBadOriginWithoutData() {
    DateTime start = now().minusMinutes(20)
    def tenantId = nextTenantId()

    // First create a couple gauge metrics by only inserting data
    def response = hawkularMetrics.post(path: "gauges/raw", body: [
        [
            id: "m11",
            data: [
                [timestamp: start.millis, value: 1.1],
                [timestamp: start.plusMinutes(1).millis, value: 1.2]
            ]
        ],
        [
            id: "m12",
            data: [
                [timestamp: start.millis, value: 2.1],
                [timestamp: start.plusMinutes(1).millis, value: 2.2]
            ]
        ]
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)

    // Now query for the gauge metrics
    response = hawkularMetrics.get(path: "metrics", query: [type: "gauge", timestamps: "true"], headers: [(tenantHeaderName): tenantId])

    assertEquals(200, response.status)
    assertEquals([
        [dataRetention: 7, tenantId: tenantId, id: "m11", type: "gauge", minTimestamp: start.millis, maxTimestamp: start.plusMinutes(1).millis],
        [dataRetention: 7, tenantId: tenantId, id: "m12", type: "gauge", minTimestamp: start.millis, maxTimestamp: start.plusMinutes(1).millis],
    ], (response.data as List).sort({ m1, m2 -> m1.id.compareTo(m2.id) }))

    //Send a CORS request and make sure it fails and returns no data
    def wrongSchemeOrigin = testOrigin.replaceAll("http://", "https://")
    badGet(path: "metrics", query: [type: "gauge"], headers: [
        (ACCESS_CONTROL_REQUEST_METHOD): "GET",
        (tenantHeaderName): tenantId,
        (ORIGIN): wrongSchemeOrigin
    ]) { exception ->
      //Expected 400 because this call should never reach the resource router since
      //the request origin is bad
      assertEquals(400, exception.response.status)
      assertEquals(null, exception.response.getData())

      def responseHeaders = "==== Response Headers = Start  ====\n"
      exception.response.headers.each { responseHeaders += "${it.name} : ${it.value}\n" }
      responseHeaders += "==== Response Headers = End ====\n"

      assertEquals(responseHeaders, DEFAULT_CORS_ACCESS_CONTROL_ALLOW_METHODS, exception.response.headers[ACCESS_CONTROL_ALLOW_METHODS].value)
      assertEquals(responseHeaders, testAccessControlAllowHeaders, exception.response.headers[ACCESS_CONTROL_ALLOW_HEADERS].value)
      assertEquals(responseHeaders, wrongSchemeOrigin, exception.response.headers[ACCESS_CONTROL_ALLOW_ORIGIN].value)
      assertEquals(responseHeaders, "true", exception.response.headers[ACCESS_CONTROL_ALLOW_CREDENTIALS].value)
      assertEquals(responseHeaders, (72 * 60 * 60) + "", exception.response.headers[ACCESS_CONTROL_MAX_AGE].value)
    }
  }

  @Test
  void testPostQueryReturnsHeaders() {
    DateTime start = now().minusMinutes(20)
    def tenantId = nextTenantId()

    // First create a couple gauge metrics by only inserting data
    def response = hawkularMetrics.post(path: "gauges/raw", body: [
        [
            id: "m12",
            data: [
                [timestamp: start.millis, value: 2.1],
                [timestamp: start.plusMinutes(1).millis, value: 2.2]
            ]
        ]
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)

    response = hawkularMetrics.post(
        path: "gauges/raw/query",
        headers: [(tenantHeaderName): tenantId, (ORIGIN): testOrigin],
        body: [
            ids: ['m12']
        ])

    assertEquals(200, response.status)
    assertEquals([[
        id: 'm12',
        data: [
            [timestamp: start.plusMinutes(1).millis, value: 2.2],
            [timestamp: start.millis, value: 2.1]
        ]
    ]], response.data)

    def responseHeaders = "==== Response Headers = Start  ====\n"
    response.headers.each { responseHeaders += "${it.name} : ${it.value}\n" }
    responseHeaders += "==== Response Headers = End ====\n"

    assertEquals(responseHeaders, DEFAULT_CORS_ACCESS_CONTROL_ALLOW_METHODS, response.headers[ACCESS_CONTROL_ALLOW_METHODS].value)
    assertEquals(responseHeaders, testAccessControlAllowHeaders, response.headers[ACCESS_CONTROL_ALLOW_HEADERS].value)
    assertEquals(responseHeaders, testOrigin, response.headers[ACCESS_CONTROL_ALLOW_ORIGIN].value)
    assertEquals(responseHeaders, "true", response.headers[ACCESS_CONTROL_ALLOW_CREDENTIALS].value)
    assertEquals(responseHeaders, (72 * 60 * 60) + "", response.headers[ACCESS_CONTROL_MAX_AGE].value)
  }
}
