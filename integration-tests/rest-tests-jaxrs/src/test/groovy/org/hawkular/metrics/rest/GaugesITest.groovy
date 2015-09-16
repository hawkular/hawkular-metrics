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

import static org.junit.Assert.assertEquals

import org.junit.Test

/**
 * @author Thomas Segismont
 */
class GaugesITest extends RESTTest {
  def tenantId = nextTenantId()

  @Test
  void shouldNotCreateMetricWithEmptyPayload() {
    badPost(path: "gauges", headers: [(tenantHeaderName): tenantId], body: "" /* Empty Body */) { exception ->
      assertEquals(400, exception.response.status)
    }
  }

  @Test
  void shouldNotAddDataForMetricWithEmptyPayload() {
    badPost(path: "gauges/pimpo/data", headers: [(tenantHeaderName): tenantId],
        body: "" /* Empty Body */) { exception ->
      assertEquals(400, exception.response.status)
    }

    badPost(path: "gauges/pimpo/data", headers: [(tenantHeaderName): tenantId],
        body: [] /* Empty List */) { exception ->
      assertEquals(400, exception.response.status)
    }
  }

  @Test
  void shouldNotAddGaugeDataWithEmptyPayload() {
    badPost(path: "gauges/data", headers: [(tenantHeaderName): tenantId],
        body: "" /* Empty Body */) { exception ->
      assertEquals(400, exception.response.status)
    }

    badPost(path: "gauges/data", headers: [(tenantHeaderName): tenantId],
        body: [] /* Empty List */) { exception ->
      assertEquals(400, exception.response.status)
    }
  }

  @Test
  void shouldStoreLargePayloadSize() {
    checkLargePayload("gauges", tenantId, { points, i -> points.push([timestamp: i, value: (double) i]) })
  }

  @Test
  void shouldNotAcceptDataWithEmptyTimestamp() {
    invalidPointCheck("gauges", tenantId, [[value: 5.5]])
  }

  @Test
  void shouldNotAcceptDataWithNullTimestamp() {
    invalidPointCheck("gauges", tenantId, [[timestamp: null, value: 5.5]])
  }

  @Test
  void shouldNotAcceptDataWithInvalidTimestamp() {
    invalidPointCheck("gauges", tenantId, [[timestamp: "aaa", value: 5.5]])
  }

  @Test
  void shouldNotAcceptDataWithEmptyValue() {
    invalidPointCheck("gauges", tenantId, [[timestamp: 13]])
  }

  @Test
  void shouldNotAcceptDataWithNullValue() {
    invalidPointCheck("gauges", tenantId, [[timestamp: 13, value: null]])
  }

  @Test
  void shouldNotAcceptDataWithInvalidValue() {
    invalidPointCheck("gauges", tenantId, [[timestamp: 13, value: ["dsqdqs"]]])
  }

  @Test
  void verifyGaugeTagsQuery() {
    String tenantId = nextTenantId()

    def insertMetrics = [ [id: 'EmptyGauge1', tags: [a: '1', b: '2']],
      [id: 'EmptyGauge2', tags: [a: '1', b: '2']],
      [id: 'EmptyGauge3', tags: [a: '1', b: '3']],
      [id: 'EmptyGauge4', tags: [b:'4']]
    ]

    //insert metrics
    insertMetrics.each {
      def response = hawkularMetrics.post(path: "gauges", body: it, headers: [(tenantHeaderName): tenantId])
      assertEquals(201, response.status)
    }

    //fetch all metrics
    insertMetrics.each {
      def response = hawkularMetrics.get(path: "gauges/" + it.id, headers: [(tenantHeaderName): tenantId])
      assertEquals(200, response.status)
      assertEquals(it.id, response.data.id)
      assertEquals(it.tags, response.data.tags)
    }

    //various permutations for fetching by tags
    def response = hawkularMetrics.get(path: "gauges/tags/b:2", headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertDefinitionArrayEquals([insertMetrics[0], insertMetrics[1]], response.data)

    response = hawkularMetrics.get(path: "gauges", query: [tags: "b:2"], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertDefinitionArrayEquals([insertMetrics[0], insertMetrics[1]], response.data)

    response = hawkularMetrics.get(path: "gauges/tags/a:1,b:2", headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertDefinitionArrayEquals([insertMetrics[0], insertMetrics[1], insertMetrics[2]], response.data)

    response = hawkularMetrics.get(path: "gauges", query: [tags: "b:2,a:1"], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertDefinitionArrayEquals([insertMetrics[0], insertMetrics[1],insertMetrics[2]], response.data)

    response = hawkularMetrics.get(path: "gauges/tags/a:1,b:2,b:4", headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertDefinitionArrayEquals(insertMetrics, response.data)

    response = hawkularMetrics.get(path: "gauges", query: [tags: "b:2,a:1,b:4"], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertDefinitionArrayEquals(insertMetrics, response.data)
  }
}
