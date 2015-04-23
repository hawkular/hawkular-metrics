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
import static org.junit.Assert.assertNotNull

import org.junit.Test

/**
 * @author Thomas Segismont
 */
class TagsITest extends RESTTest {

  @Test
  void shouldNotAcceptMissingOrInvalidTags() {
    badDelete(path: "tenantId/gauges/id/tags/,") { exception ->
      assertEquals(400, exception.response.status)
    }
    badDelete(path: "tenantId/availability/id/tags/:5") { exception ->
      assertEquals(400, exception.response.status)
    }
    badGet(path: "tenantId/gauges") { exception ->
      // Missing query
      assertEquals(400, exception.response.status)
    }
    badGet(path: "tenantId/gauges", query: [tags: ",5:"]) { exception ->
      assertEquals(400, exception.response.status)
    }
    badGet(path: "tenantId/availability") { exception ->
      // Missing query
      assertEquals(400, exception.response.status)
    }
    badGet(path: "tenantId/availability", query: [tags: ",5:"]) { exception ->
      assertEquals(400, exception.response.status)
    }
    badGet(path: "tenantId/gauges/tags/:") { exception ->
      assertEquals(400, exception.response.status)
    }
    badGet(path: "tenantId/availability/tags/:") { exception ->
      assertEquals(400, exception.response.status)
    }
  }

  @Test
  void findMetricTagsWhenThereIsNoData() {
    def tenantId = nextTenantId()

    def response = hawkularMetrics.get(path: "$tenantId/gauges/missing/tags")
    assertEquals("Expected a 204 response when the gauge metric is not found", 204, response.status)

    response = hawkularMetrics.get(path: "$tenantId/availability/missing/tags")
    assertEquals("Expected a 204 response when the availability metric is not found", 204, response.status)
  }

  @Test
  void createMetricsAndUpdateTags() {
    String tenantId = nextTenantId()

    // Create a gauge metric
    def response = hawkularMetrics.post(path: "$tenantId/gauges", body: [
        id  : 'N1',
        tags: ['  a  1   ': '   A', 'bsq   d1': 'B   ']
    ])
    assertEquals(201, response.status)

    // Make sure we do not allow duplicates
    badPost(path: "$tenantId/gauges", body: [id: 'N1']) { exception ->
      assertEquals(409, exception.response.status)
      assertNotNull(exception.response.data['errorMsg'])
    }

    // Create a gauge metric that sets its data retention
    response = hawkularMetrics.post(path: "$tenantId/gauges", body: [
        id           : 'N2',
        tags         : [a2: '2', b2: 'B2'],
        dataRetention: 96
    ])
    assertEquals(201, response.status)

    // Create an availability metric
    response = hawkularMetrics.post(path: "$tenantId/availability", body: [
        id  : 'A1',
        tags: [a2: '2', b2: '2']
    ])
    assertEquals(201, response.status)

    // Create an availability metric that sets its data retention
    response = hawkularMetrics.post(path: "$tenantId/availability", body: [
        id           : 'A2',
        tags         : [a22: '22', b22: '22'],
        dataRetention: 48
    ])
    assertEquals(201, response.status)

    // Make sure we do not allow duplicates
    badPost(path: "$tenantId/availability", body: [id: 'A1']) { exception ->
      assertEquals(409, exception.response.status)
      assertNotNull(exception.response.data['errorMsg'])
    }

    // Fetch gauge tags
    response = hawkularMetrics.get(path: "$tenantId/gauges/N1/tags")
    assertEquals(200, response.status)
    assertEquals(
        [
            tenantId: tenantId,
            id      : 'N1',
            tags    : ['  a  1   ': '   A', 'bsq   d1': 'B   ']
        ],
        response.data
    )

    response = hawkularMetrics.get(path: "$tenantId/gauges/N2/tags")
    assertEquals(200, response.status)
    assertEquals(
        [
            tenantId     : tenantId,
            id           : 'N2',
            tags         : [a2: '2', b2: 'B2'],
            dataRetention: 96
        ],
        response.data
    )

    // Verify the response for a non-existent metric
    response = hawkularMetrics.get(path: "$tenantId/gauges/N-doesNotExist/tags")
    assertEquals(204, response.status)

    // Fetch availability metric tags
    response = hawkularMetrics.get(path: "$tenantId/availability/A1/tags")
    assertEquals(200, response.status)
    assertEquals(
        [
            tenantId: tenantId,
            id      : 'A1',
            tags    : [a2: '2', b2: '2']
        ],
        response.data
    )

    response = hawkularMetrics.get(path: "$tenantId/availability/A2/tags")
    assertEquals(200, response.status)
    assertEquals(
        [
            tenantId     : tenantId,
            id           : 'A2',
            tags         : [a22: '22', b22: '22'],
            dataRetention: 48
        ],
        response.data
    )

    // Verify the response for a non-existent metric
    response = hawkularMetrics.get(path: "$tenantId/gauges/A-doesNotExist/tags")
    assertEquals(204, response.status)

    // Update the gauge metric tags
    response = hawkularMetrics.put(path: "$tenantId/gauges/N1/tags", body: [a1: 'one', a2: '2', b1: 'B'])
    assertEquals(200, response.status)

    // Fetch the updated tags
    response = hawkularMetrics.get(path: "$tenantId/gauges/N1/tags")
    assertEquals(200, response.status)
    assertEquals(
        [
            tenantId: tenantId,
            id      : 'N1',
            tags    : ['  a  1   ': '   A', a1: 'one', a2: '2', b1: 'B', 'bsq   d1': 'B   ']
        ],
        response.data
    )

    // Delete a gauge metric tag
    response = hawkularMetrics.delete(path: "$tenantId/gauges/N1/tags/a2:2,b1:B")
    assertEquals(200, response.status)
    response = hawkularMetrics.get(path: "$tenantId/gauges/N1/tags")
    assertEquals(
        [
            tenantId: tenantId,
            id      : 'N1',
            tags    : ['  a  1   ': '   A', a1: 'one', 'bsq   d1': 'B   ']
        ],
        response.data
    )

    // Update the availability metric data
    response = hawkularMetrics.put(path: "$tenantId/availability/A1/tags", body: [a2: 'two', a3: 'THREE'])
    assertEquals(200, response.status)

    // Fetch the updated tags
    response = hawkularMetrics.get(path: "$tenantId/availability/A1/tags")
    assertEquals(200, response.status)
    assertEquals(
        [
            tenantId: tenantId,
            id      : 'A1',
            tags    : [a2: 'two', a3: 'THREE', b2: '2']
        ],
        response.data
    )

    // delete an availability metric tag
    response = hawkularMetrics.delete(path: "$tenantId/availability/A1/tags/a2:two,b2:2")
    assertEquals(200, response.status)

    response = hawkularMetrics.get(path: "$tenantId/availability/A1/tags")
    assertEquals(
        [
            tenantId: tenantId,
            id      : 'A1',
            tags    : [a3: 'THREE']
        ],
        response.data
    )
  }
}
