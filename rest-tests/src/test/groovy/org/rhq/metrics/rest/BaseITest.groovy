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
package org.rhq.metrics.rest

import org.junit.Test

import static org.joda.time.DateTime.now
import static org.junit.Assert.assertEquals

class BaseITest extends RESTTest {

  @Test
  void addAndGetValue() {
    rhqm.defaultRequestHeaders.Authorization = "Basic YWdlbnQ6ZWY2YzZkMDktNTczNC00MTAyLTg2ODMtZGYzZjMwZjA5YmEy"

    def end = now().millis
    def start = end - 100
    def tenantId = 'tenant-1'
    def metric = 'foo'
    def response = rhqm.post(path: "tenants/${tenantId}/metrics/numeric/${metric}/data", body: [[timestamp: start + 10, value: 42]])
    assertEquals(200, response.status)

    response = rhqm.get(path: "tenants/${tenantId}/metrics/numeric/${metric}/data", query: [start: start, end: end])
    assertEquals(200, response.status)
    assertEquals(start + 10, response.data.data[0].timestamp)
  }
}
