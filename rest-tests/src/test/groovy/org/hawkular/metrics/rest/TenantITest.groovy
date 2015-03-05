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
import static org.junit.Assert.assertTrue

import org.junit.Test

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
        //retentionSettings: [numeric: 45, availability: 30]
    ])
    assertEquals(201, response.status)
    assertEquals("http://$baseURI/tenants".toString(), response.getFirstHeader('location').value)

    response = hawkularMetrics.post(path: "tenants", body: [
        id: secondTenantId//,
        //retentionSettings: [numeric: 45, availability: 30]
    ])
    assertEquals(201, response.status)
    assertEquals("http://$baseURI/tenants".toString(), response.getFirstHeader('location').value)

    response = hawkularMetrics.get(path: "tenants")
    def expectedData = [
        [id: firstTenantId,
         //retentionSettings:[availability:30, numeric:45]
        ],
        [id: secondTenantId,
         //retentionSettings:[availability:30, numeric:45]
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
    badPost(path: 'tenants', body: [] /* Empty body */) { exception ->
      assertEquals(400, exception.response.status)
      assertTrue(exception.response.data.containsKey("errorMsg"))
    }
  }

}
