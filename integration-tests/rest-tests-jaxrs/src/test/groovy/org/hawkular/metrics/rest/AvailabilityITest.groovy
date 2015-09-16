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

import static org.joda.time.DateTime.now
import static org.junit.Assert.assertEquals

import org.hawkular.metrics.core.api.AvailabilityType
import org.joda.time.DateTime
import org.junit.Test

/**
 * @author Thomas Segismont
 */
class AvailabilityITest extends RESTTest {
  def tenantId = nextTenantId()

  @Test
  void shouldNotCreateMetricWithEmptyPayload() {
    badPost(path: "availability", headers: [(tenantHeaderName): tenantId], body: "" /* Empty Body */) { exception ->
      assertEquals(400, exception.response.status)
    }
  }

  @Test
  void shouldNotAddAvailabilityForMetricWithEmptyPayload() {
    badPost(path: "availability/pimpo/data", headers: [(tenantHeaderName): tenantId],
        body: "" /* Empty Body */) { exception ->
      assertEquals(400, exception.response.status)
    }

    badPost(path: "availability/pimpo/data", headers: [(tenantHeaderName): tenantId],
        body: [] /* Empty List */) { exception ->
      assertEquals(400, exception.response.status)
    }
  }

  @Test
  void shouldNotAddAvailabilityDataWithEmptyPayload() {
    badPost(path: "availability/data", headers: [(tenantHeaderName): tenantId],
        body: "" /* Empty Body */) { exception ->
      assertEquals(400, exception.response.status)
    }

    badPost(path: "availability/data", headers: [(tenantHeaderName): tenantId],
        body: [] /* Empty List */) { exception ->
      assertEquals(400, exception.response.status)
    }
  }

  @Test
  void shouldStoreLargePayloadSize() {
    def availabilityTypes = AvailabilityType.values()
    checkLargePayload("availability", tenantId, { points, int i ->
      points.push([timestamp: i, value: availabilityTypes[i % availabilityTypes.length]])
    })
  }

  @Test
  void testAvailabilityGet() {
    DateTime start = now().minusMinutes(20)
    String metric = 'A1'

    def response = hawkularMetrics.post(path: "availability/$metric/data", body: [
        [timestamp: start.millis, value: "up"]], headers: [(tenantHeaderName): tenantId])

    assertEquals(200, response.status)

    response = hawkularMetrics.get(path: "availability/$metric", headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertEquals(metric, response.data.id)
  }

  @Test
  void shouldNotAcceptDataWithEmptyTimestamp() {
    invalidPointCheck("availability", tenantId, [[value: "up"]])
  }

  @Test
  void shouldNotAcceptDataWithNullTimestamp() {
    invalidPointCheck("availability", tenantId, [[timestamp: null, value: "up"]])
  }

  @Test
  void shouldNotAcceptDataWithInvalidTimestamp() {
    invalidPointCheck("availability", tenantId, [[timestamp: "aaa", value: "up"]])
  }

  @Test
  void shouldNotAcceptDataWithEmptyValue() {
    invalidPointCheck("availability", tenantId, [[timestamp: 13]])
  }

  @Test
  void shouldNotAcceptDataWithNullValue() {
    invalidPointCheck("availability", tenantId, [[timestamp: 13, value: null]])
  }

  @Test
  void shouldNotAcceptDataWithInvalidValue() {
    invalidPointCheck("availability", tenantId, [[timestamp: 13, value: ["dsqdqs"]]])
  }

  @Test
  void verifyAvailabilityTagsQuery() {
    String tenantId = nextTenantId()

    def insertMetrics = [ [id: 'EmptyAvailability1', tags: [a: '1', c: '2']],
      [id: 'EmptyAvailability2', tags: [a: '1', c: '2']],
      [id: 'EmptyAvailability3', tags: [a: '1', c: '3']],
      [id: 'EmptyAvailability4', tags: [c:'4']]
    ]

    //insert metrics
    insertMetrics.each {
      def response = hawkularMetrics.post(path: "availability", body: it, headers: [(tenantHeaderName): tenantId])
      assertEquals(201, response.status)
    }

    //fetch all metrics
    insertMetrics.each {
      def response = hawkularMetrics.get(path: "availability/" + it.id, headers: [(tenantHeaderName): tenantId])
      assertEquals(200, response.status)
      assertEquals(it.id, response.data.id)
      assertEquals(it.tags, response.data.tags)
    }

    //various permutations for fetching by tags
    def response = hawkularMetrics.get(path: "availability/tags/c:2", headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertDefinitionArrayEquals([insertMetrics[0], insertMetrics[1]], response.data)

    response = hawkularMetrics.get(path: "availability", query: [tags: "c:2"], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertDefinitionArrayEquals([insertMetrics[0], insertMetrics[1]], response.data)

    response = hawkularMetrics.get(path: "availability/tags/a:1,c:2", headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertDefinitionArrayEquals([insertMetrics[0], insertMetrics[1], insertMetrics[2]], response.data)

    response = hawkularMetrics.get(path: "availability", query: [tags: "c:2,a:1"], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertDefinitionArrayEquals([insertMetrics[0], insertMetrics[1],insertMetrics[2]], response.data)

    response = hawkularMetrics.get(path: "availability/tags/a:1,c:2,c:4", headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertDefinitionArrayEquals(insertMetrics, response.data)

    response = hawkularMetrics.get(path: "availability", query: [tags: "c:2,a:1,c:4"], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertDefinitionArrayEquals(insertMetrics, response.data)
  }
}
