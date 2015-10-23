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
import static org.junit.Assert.assertNotNull
import static org.junit.Assert.assertTrue

import org.junit.Test

/**
 * @author Thomas Segismont
 */
class TagsITest extends RESTTest {

  static metricTypes =  [
    ["path": "gauges", "type": "gauge"],
    ["path": "availability", "type": "availability"],
    ["path": "counters", "type": "counter"]
  ];

  @Test
  void shouldNotAcceptMissingOrInvalidTags() {
    metricTypes.each {
      def tenantId = nextTenantId()

      badDelete(path: it.path + "/id1/tags/,", headers: [(tenantHeaderName): tenantId]) { exception ->
        assertEquals(400, exception.response.status)
      }

      badDelete(path: it.path + "/id2/tags/:5", headers: [(tenantHeaderName): tenantId]) { exception ->
        assertEquals(400, exception.response.status)
      }

      badPut(path: it.path + "/id4/tags", headers: [(tenantHeaderName): tenantId]) { exception ->
          assertEquals(400, exception.response.status)
      }

      badPut(path: it.path + "/id5/tags", body:['': 'test'], headers: [(tenantHeaderName): tenantId]) { exception ->
          assertEquals(400, exception.response.status)
      }
    }
  }

  @Test
  void findMetricTagsWhenThereIsNoData() {
    metricTypes.each {
      def tenantId = nextTenantId()

      def response = hawkularMetrics.get(path: it.path + "/missing/tags", headers: [(tenantHeaderName): tenantId])
      assertEquals("Expected a 204 response when the " + it + " metric is not found", 204, response.status)

      response = hawkularMetrics.get(path: it.path + "/missing", headers: [(tenantHeaderName): tenantId])
      assertEquals("Expected a 204 response when the " + it + " metric is not found", 204, response.status)
    }
  }

  @Test
  void createMetricsAndUpdateTags() {
    metricTypes.each {
      String tenantId = nextTenantId()

      // Create a gauge metric
      def response = hawkularMetrics.post(path: it.path, body: [
          id  : 'N1',
          tags: ['  a  1   ': '   A', 'bsq   d1': 'B   ']
      ], headers: [(tenantHeaderName): tenantId])
      assertEquals(201, response.status)

      // Fetch the metric
      response = hawkularMetrics.get(path: it.path + "/N1", headers: [(tenantHeaderName): tenantId])
      assertEquals(200, response.status)
      assertEquals([
          tenantId: tenantId,
          id  : 'N1',
          tags: ['  a  1   ': '   A', 'bsq   d1': 'B   '],
          type: it.type
      ], response.data)

      // Make sure we do not allow duplicates
      badPost(path: it.path, body: [id: 'N1'], headers: [(tenantHeaderName): tenantId]) { exception ->
        assertEquals(409, exception.response.status)
        assertNotNull(exception.response.data['errorMsg'])
      }

      // Create another metric that sets its data retention
      response = hawkularMetrics.post(path: it.path, body: [
        id           : 'N2',
        tags         : [a2: '2', b2: 'B2'],
        dataRetention: 96
      ], headers: [(tenantHeaderName): tenantId])
      assertEquals(201, response.status)

      // Fetch tags
      response = hawkularMetrics.get(path: it.path + "/N1/tags", headers: [(tenantHeaderName): tenantId])
      assertEquals(200, response.status)
      assertEquals('  a  1   ': '   A', 'bsq   d1': 'B   ', response.data)

      response = hawkularMetrics.get(path: it.path + "/N2/tags", headers: [(tenantHeaderName): tenantId])
      assertEquals(200, response.status)
      assertEquals([a2: '2', b2: 'B2'], response.data)

      // Verify the response for a non-existent metric
      response = hawkularMetrics.get(path: it.path + "/N-doesNotExist/tags", headers: [(tenantHeaderName): tenantId])
      assertEquals(204, response.status)

      // Update the metric tags
      response = hawkularMetrics.put(path: it.path + "/N1/tags", body: [a1: 'one', a2: '2', b1: 'B'], headers: [(tenantHeaderName): tenantId])
      assertEquals(200, response.status)

      // Fetch the updated tags
      response = hawkularMetrics.get(path: it.path +  "/N1/tags", headers: [(tenantHeaderName): tenantId])
      assertEquals(200, response.status)
      assertEquals(['  a  1   ': '   A', a1: 'one', a2: '2', b1: 'B', 'bsq   d1': 'B   '], response.data)

      // Delete a gauge metric tag
      response = hawkularMetrics.delete(path: it.path + "/N1/tags/a2:2,b1:B", headers: [(tenantHeaderName): tenantId])
      assertEquals(200, response.status)
      response = hawkularMetrics.get(path: it.path + "/N1/tags", headers: [(tenantHeaderName): tenantId])
      assertEquals(['  a  1   ': '   A', a1: 'one', 'bsq   d1': 'B   '], response.data)
    }
  }

  @Test
  void findDefinitionsWithTags() {
    metricTypes.each {
      def tenantId = nextTenantId()

      // Create metric
      def response = hawkularMetrics.post(path: it.path, body: [
        id  : 'N1',
        tags: ['a1': 'A', 'd1': 'B']
      ], headers: [(tenantHeaderName): tenantId])
      assertEquals(201, response.status)

      response = hawkularMetrics.post(path: it.path, body: [
        id  : 'N2',
        tags: ['a1': 'A2']
      ], headers: [(tenantHeaderName): tenantId])
      assertEquals(201, response.status)

      // Fetch metrics using tags
      response = hawkularMetrics.get(path: "metrics",
        query: [tags: "a1:*"],
        headers: [(tenantHeaderName): tenantId])

      def alternateResponse = hawkularMetrics.get(path: it.path,
        query: [tags: "a1:*"],
        headers: [(tenantHeaderName): tenantId])

      [response, alternateResponse].each { lresponse ->
        assertEquals(200, lresponse.status)
        assertTrue(lresponse.data instanceof List)
        assertEquals(2, lresponse.data.size())
        assertTrue((lresponse.data ?: []).contains([
          tenantId: tenantId,
          id      : 'N1',
          tags    : ['a1': 'A', 'd1': 'B'],
          type: it.type
        ]))
        assertTrue((lresponse.data ?: []).contains([
          tenantId: tenantId,
          id      : 'N2',
          tags    : ['a1': 'A2'],
          type: it.type
        ]))
      }

      // Fetch with tags & type
      response = hawkularMetrics.get(path: "metrics",
        query: [tags: "a1:A,d1:B", type: it.type],
        headers: [(tenantHeaderName): tenantId])

      alternateResponse =   hawkularMetrics.get(path: it.path,
        query: [tags: "a1:A,d1:B"],
        headers: [(tenantHeaderName): tenantId])

      [response, alternateResponse].each {  lresponse ->
        assertEquals(200, lresponse.status)
        assertEquals([[
          tenantId: tenantId,
          id      : 'N1',
          tags    : ['a1': 'A', 'd1': 'B'],
          type: it.type
        ]], lresponse.data)
      }

      // Fetch with incorrect regexp
      badGet(path: "metrics", query: [tags: "a1:**", type: it.type],
        headers: [(tenantHeaderName): tenantId]) { exception ->
         // Missing query
        assertEquals(400, exception.response.status)
      }
    }
  }
}
