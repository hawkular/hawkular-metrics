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
import org.hawkular.metrics.core.impl.DateTimeService
import org.joda.time.DateTime
import org.junit.Test

import static org.joda.time.Duration.standardMinutes
import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertNotNull
import static org.junit.Assert.assertTrue
/**
 * @author Thomas Segismont
 */
class TenantITest extends RESTTest {

  @Test
  void createAndReadTest() {
    String firstTenantId = nextTenantId()
    String secondTenantId = nextTenantId()

    def response = hawkularMetrics.post(path: "tenants", body: [
        id: firstTenantId,
        //retentionSettings: [gauge: 45, availability: 30]
    ])
    assertEquals(201, response.status)
    assertEquals("http://$baseURI/tenants".toString(), response.getFirstHeader('location').value)

    response = hawkularMetrics.post(path: "tenants", body: [
        id: secondTenantId//,
        //retentionSettings: [gauge: 45, availability: 30]
    ])
    assertEquals(201, response.status)
    assertEquals("http://$baseURI/tenants".toString(), response.getFirstHeader('location').value)

    response = hawkularMetrics.get(path: "tenants")
    def expectedData = [
        [id: firstTenantId,
         //retentionSettings:[availability:30, gauge:45]
        ],
        [id: secondTenantId,
         //retentionSettings:[availability:30, gauge:45]
        ]]

    assertTrue("${expectedData} not in ${response.data}", response.data.containsAll((expectedData)))
  }

  @Test
  void duplicateTenantTest() {
    def tenantId = nextTenantId()

    def response = hawkularMetrics.post(path: 'tenants', body: [id: tenantId])
    assertEquals(201, response.status)
    assertEquals("http://$baseURI/tenants".toString(), response.getFirstHeader('location').value)

    badPost(path: 'tenants', body: [id: tenantId]) { exception ->
      assertEquals(409, exception.response.status)
    }
  }

  @Test
  void invalidPayloadTest() {
    badPost(path: 'tenants', body: "" /* Empty body */) { exception ->
      assertEquals(400, exception.response.status)
      assertTrue(exception.response.data.containsKey("errorMsg"))
    }
  }

//  @Test
  void createImplicitTenantWhenInsertingGaugeDataPoints() {
    String tenantId = nextTenantId()
    DateTimeService dateTimeService = new DateTimeService()
    DateTime start = dateTimeService.getTimeSlice(getTime(), standardMinutes(1))

    def response = hawkularMetrics.post(
        path: "gauges/G1/data",
        headers : [(tenantHeaderName): tenantId],
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

//  @Test
  void createImplicitTenantWhenInsertingCounterDataPoints() {
    String tenantId = nextTenantId()
    DateTimeService dateTimeService = new DateTimeService()
    DateTime start = dateTimeService.getTimeSlice(getTime(), standardMinutes(1))

    def response = hawkularMetrics.post(
        path: 'counters/C1/data',
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

//  @Test
  void createImplicitTenantWhenInsertingAvailabilityDataPoints() {
    String tenantId = nextTenantId()
    DateTimeService dateTimeService = new DateTimeService()
    DateTime start = dateTimeService.getTimeSlice(getTime(), standardMinutes(1))

    def response = hawkularMetrics.post(
        path: 'availability/A1/data',
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
