/*
 * Copyright 2014-2016 Red Hat, Inc. and/or its affiliates
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

import static org.joda.time.Duration.standardMinutes
import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertNotNull
import static org.junit.Assert.assertTrue

import org.hawkular.metrics.core.service.DateTimeService
import org.joda.time.DateTime
import org.junit.Ignore
import org.junit.Test

/**
 * @author Thomas Segismont
 */
class TenantITest extends RESTTest {
  def tenantId = nextTenantId()

  @Test
  void createAndReadTest() {
    def secondTenantId = nextTenantId()

    def response = hawkularMetrics.post(path: "tenants", body: [
        id        : tenantId,
        retentions: [gauge: 45, availability: 30, counter: 13]
    ])
    assertEquals(201, response.status)
    assertEquals("http://$baseURI/tenants".toString(), response.getFirstHeader('location').value)

    response = hawkularMetrics.post(path: "tenants", body: [
        id        : secondTenantId,
        retentions: [gauge: 13, availability: 45, counter: 30]
    ])
    assertEquals(201, response.status)
    assertEquals("http://$baseURI/tenants".toString(), response.getFirstHeader('location').value)

    response = hawkularMetrics.get(path: "tenants")
    def expectedData = [
        [
            id        : tenantId,
            retentions: [gauge: 45, availability: 30, counter: 13]
        ],
        [
            id        : secondTenantId,
            retentions: [gauge: 13, availability: 45, counter: 30]
        ]
    ]

    assertTrue("${expectedData} not in ${response.data}", response.data.containsAll((expectedData)))
  }

  @Test
  void duplicateTenantTest() {
    def response = hawkularMetrics.post(path: 'tenants', body: [id: tenantId])
    assertEquals(201, response.status)
    assertEquals("http://$baseURI/tenants".toString(), response.getFirstHeader('location').value)

    badPost(path: 'tenants', body: [id: tenantId]) { exception ->
      assertEquals(409, exception.response.status)
    }

    response = hawkularMetrics.post(path: 'tenants', query: [overwrite: true], body: [
        id: tenantId,
        retentions: [gauge: 145, availability: 130, counter: 113]
    ])
    assertEquals(201, response.status)
    assertEquals("http://$baseURI/tenants".toString(), response.getFirstHeader('location').value)

    response = hawkularMetrics.get(path: "tenants")
    def expectedData = [
        [
            id        : tenantId,
            retentions: [gauge: 145, availability: 130, counter: 113]
        ]
    ]

    assertTrue("${expectedData} not in ${response.data}", response.data.containsAll((expectedData)))
  }

  @Test
  void invalidPayloadTest() {
    badPost(path: 'tenants', body: "" /* Empty body */) { exception ->
      assertEquals(400, exception.response.status)
      assertTrue(exception.response.data.containsKey("errorMsg"))
    }
  }

  @Test
  @Ignore
  void createImplicitTenantWhenInsertingGaugeDataPoints() {
    DateTimeService dateTimeService = new DateTimeService()
    DateTime start = dateTimeService.getTimeSlice(getTime(), standardMinutes(1))

    def response = hawkularMetrics.post(
        path: "gauges/G1/raw",
        headers: [(tenantHeaderName): tenantId],
        body: [
            [timestamp: start.minusMinutes(1).millis, value: 3.14]
        ]
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.get(path: "clock/wait", query: [duration: "31mn"])
    assertEquals("There was an error waiting: $response.data", 200, response.status)

    response = hawkularMetrics.get(path: "tenants")
    assertEquals(200, response.status)

    assertNotNull("tenantId = $tenantId, Response = $response.data", response.data.find { it.id == tenantId })
  }

  @Test
  @Ignore
  void createImplicitTenantWhenInsertingCounterDataPoints() {
    DateTimeService dateTimeService = new DateTimeService()
    DateTime start = dateTimeService.getTimeSlice(getTime(), standardMinutes(1))

    def response = hawkularMetrics.post(
        path: 'counters/C1/raw',
        headers: [(tenantHeaderName): tenantId],
        body: [
            [timestamp: start.minusMinutes(1).millis, value: 100]
        ]
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.get(path: 'clock/wait', query: [duration: '31mn'])
    assertEquals("There was an error waiting: $response.data", 200, response.status)

    response = hawkularMetrics.get(path: 'tenants')
    assertEquals(200, response.status)

    assertNotNull("tenantId = $tenantId, Response = $response.data", response.data.find { it.id == tenantId })
  }

  @Test
  @Ignore
  void createImplicitTenantWhenInsertingAvailabilityDataPoints() {
    DateTimeService dateTimeService = new DateTimeService()
    DateTime start = dateTimeService.getTimeSlice(getTime(), standardMinutes(1))

    def response = hawkularMetrics.post(
        path: 'availability/A1/raw',
        headers: [(tenantHeaderName): tenantId],
        body: [
            [timestamp: start.minusMinutes(1).millis, value: 'up']
        ]
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.get(path: 'clock/wait', query: [duration: '31mn'])
    assertEquals("There was an error waiting: $response.data", 200, response.status)

    response = hawkularMetrics.get(path: 'tenants')
    assertEquals(200, response.status)

    assertNotNull("tenantId = $tenantId, Response = $response.data", response.data.find { it.id == tenantId })
  }
}
