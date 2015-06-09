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
    String tenantId = nextTenantId()

    badDelete(path: "gauges/id/tags/,", headers: [(tenantHeaderName): tenantId]) { exception ->
      assertEquals(400, exception.response.status)
    }
    badDelete(path: "availability/id/tags/:5", headers: [(tenantHeaderName): tenantId]) { exception ->
      assertEquals(400, exception.response.status)
    }
    badGet(path: "gauges", headers: [(tenantHeaderName): tenantId]) { exception ->
      // Missing query
      assertEquals(400, exception.response.status)
    }
    badGet(path: "gauges", query: [tags: ",5:"], headers: [(tenantHeaderName): tenantId]) { exception ->
      assertEquals(400, exception.response.status)
    }
    badGet(path: "availability", headers: [(tenantHeaderName): tenantId]) { exception ->
      // Missing query
      assertEquals(400, exception.response.status)
    }
    badGet(path: "availability", query: [tags: ",5:"], headers: [(tenantHeaderName): tenantId]) { exception ->
      assertEquals(400, exception.response.status)
    }
    badGet(path: "gauges/tags/:", headers: [(tenantHeaderName): tenantId]) { exception ->
      assertEquals(400, exception.response.status)
    }
    badGet(path: "availability/tags/:", headers: [(tenantHeaderName): tenantId]) { exception ->
      assertEquals(400, exception.response.status)
    }
  }

  @Test
  void findMetricTagsWhenThereIsNoData() {
    def tenantId = nextTenantId()

    def response = hawkularMetrics.get(path: "gauges/missing/tags", headers: [(tenantHeaderName): tenantId])
    assertEquals("Expected a 204 response when the gauge metric is not found", 204, response.status)

    response = hawkularMetrics.get(path: "availability/missing/tags", headers: [(tenantHeaderName): tenantId])
    assertEquals("Expected a 204 response when the availability metric is not found", 204, response.status)

    response = hawkularMetrics.get(path: "gauges/missing", headers: [(tenantHeaderName): tenantId])
    assertEquals("Expected a 204 response when the gauge metric is not found", 204, response.status)
  }

  @Test
  void createMetricsAndUpdateTags() {
    String tenantId = nextTenantId()

    // Create a gauge metric
    def response = hawkularMetrics.post(path: "gauges", body: [
        id  : 'N1',
        tags: ['  a  1   ': '   A', 'bsq   d1': 'B   ']
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(201, response.status)

    // Fetch the metric
    response = hawkularMetrics.get(path: "gauges/N1", headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertEquals([
        tenantId: tenantId,
        id  : 'N1',
        tags: ['  a  1   ': '   A', 'bsq   d1': 'B   ']
    ], response.data)

    // Make sure we do not allow duplicates
    badPost(path: "gauges", body: [id: 'N1'], headers: [(tenantHeaderName): tenantId]) { exception ->
      assertEquals(409, exception.response.status)
      assertNotNull(exception.response.data['errorMsg'])
    }

    // Create a gauge metric that sets its data retention
    response = hawkularMetrics.post(path: "gauges", body: [
        id           : 'N2',
        tags         : [a2: '2', b2: 'B2'],
        dataRetention: 96
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(201, response.status)

    // Create an availability metric
    response = hawkularMetrics.post(path: "availability", body: [
        id  : 'A1',
        tags: [a2: '2', b2: '2']
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(201, response.status)

    // Create an availability metric that sets its data retention
    response = hawkularMetrics.post(path: "availability", body: [
        id           : 'A2',
        tags         : [a22: '22', b22: '22'],
        dataRetention: 48
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(201, response.status)

    // Make sure we do not allow duplicates
    badPost(path: "availability", body: [id: 'A1'], headers: [(tenantHeaderName): tenantId]) { exception ->
      assertEquals(409, exception.response.status)
      assertNotNull(exception.response.data['errorMsg'])
    }

    // Fetch gauge tags
    response = hawkularMetrics.get(path: "gauges/N1/tags", headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertEquals('  a  1   ': '   A', 'bsq   d1': 'B   ', response.data)

    response = hawkularMetrics.get(path: "gauges/N2/tags", headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertEquals([a2: '2', b2: 'B2'], response.data)

    // Verify the response for a non-existent metric
    response = hawkularMetrics.get(path: "gauges/N-doesNotExist/tags", headers: [(tenantHeaderName): tenantId])
    assertEquals(204, response.status)

    // Fetch availability metric tags
    response = hawkularMetrics.get(path: "availability/A1/tags", headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertEquals([a2: '2', b2: '2'], response.data)

    response = hawkularMetrics.get(path: "availability/A2/tags", headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertEquals([a22: '22', b22: '22'], response.data)

    // Verify the response for a non-existent metric
    response = hawkularMetrics.get(path: "gauges/A-doesNotExist/tags", headers: [(tenantHeaderName): tenantId])
    assertEquals(204, response.status)

    // Update the gauge metric tags
    response = hawkularMetrics.put(path: "gauges/N1/tags", body: [a1: 'one', a2: '2', b1: 'B'], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)

    // Fetch the updated tags
    response = hawkularMetrics.get(path: "gauges/N1/tags", headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertEquals(['  a  1   ': '   A', a1: 'one', a2: '2', b1: 'B', 'bsq   d1': 'B   '], response.data)

    // Delete a gauge metric tag
    response = hawkularMetrics.delete(path: "gauges/N1/tags/a2:2,b1:B", headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    response = hawkularMetrics.get(path: "gauges/N1/tags", headers: [(tenantHeaderName): tenantId])
    assertEquals(['  a  1   ': '   A', a1: 'one', 'bsq   d1': 'B   '], response.data)

    // Update the availability metric data
    response = hawkularMetrics.put(path: "availability/A1/tags", body: [a2: 'two', a3: 'THREE'], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)

    // Fetch the updated tags
    response = hawkularMetrics.get(path: "availability/A1/tags", headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertEquals([a2: 'two', a3: 'THREE', b2: '2'], response.data)

    // delete an availability metric tag
    response = hawkularMetrics.delete(path: "availability/A1/tags/a2:two,b2:2", headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)

    response = hawkularMetrics.get(path: "availability/A1/tags", headers: [(tenantHeaderName): tenantId])
    assertEquals([a3: 'THREE'], response.data)
  }
}
