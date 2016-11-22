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

import static org.joda.time.DateTime.now
import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertNotNull
import static org.junit.Assert.assertTrue

import org.joda.time.DateTime
import org.junit.Test

/**
 * @author Thomas Segismont
 */
class TagsITest extends RESTTest {

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
          dataRetention: 7,
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

      // Delete a gauge metric tag (list may contain plain names or name:value pairs)
      response = hawkularMetrics.delete(path: it.path + "/N1/tags/a2,b1:B", headers: [(tenantHeaderName): tenantId])
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
        tags: ['a1': 'A', 'd1': 'B'],
        dataRetention: 7
      ], headers: [(tenantHeaderName): tenantId])
      assertEquals(201, response.status)

      response = hawkularMetrics.post(path: it.path, body: [
        id  : 'N2',
        tags: ['a1': 'A2'],
        dataRetention: 7
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
          dataRetention: 7,
          tenantId: tenantId,
          id      : 'N1',
          tags    : ['a1': 'A', 'd1': 'B'],
          type: it.type
        ]))
        assertTrue((lresponse.data ?: []).contains([
          dataRetention: 7,
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
          dataRetention: 7,
          type: it.type
        ]], lresponse.data)
      }

      // Fetch with incorrect regexp
      badGet(path: "metrics", query: [tags: "a1:**", type: it.type],
        headers: [(tenantHeaderName): tenantId]) { exception ->
         // Missing query
        assertEquals(400, exception.response.status)
      }

      // Fetch with tags & type & id filter
      response = hawkularMetrics.get(path: "metrics",
          query: [tags: "a1:*", type: it.type, id: ".2"],
          headers: [(tenantHeaderName): tenantId])
      assertEquals(200, response.status)
      assertEquals([[
                        tenantId: tenantId,
                        id      : 'N2',
                        tags    : ['a1': 'A2'],
                        dataRetention: 7,
                        type: it.type
                    ]], response.data)


      // Fetch empty result
      response = hawkularMetrics.get(path: "metrics",
          query: [tags: "notvalid:*", type: it.type],
          headers: [(tenantHeaderName): tenantId])
      assertEquals(204, response.status)
    }
  }

  @Test
  void findWithEncodedTagsValues() {
      metricTypes.each {
        def tenantId = nextTenantId()

        // Create metric
        def response = hawkularMetrics.post(path: it.path, body: [
            id  : 'N1',
            tags: ['a1': 'A/B', 'd1': 'B:A', 'c1': 'C,D']
        ], headers: [(tenantHeaderName): tenantId])
        assertEquals(201, response.status)

        // Fetch metrics using tags
        response = hawkularMetrics.get(path: "metrics",
            query: [tags: "d1:B%3AA"],
            headers: [(tenantHeaderName): tenantId])

        def alternateResponse = hawkularMetrics.get(path: it.path,
            query: [tags: "a1:A%2FB"],
            headers: [(tenantHeaderName): tenantId])

        def commaResponse = hawkularMetrics.get(path: it.path,
            query: [tags: "c1:C%2CD"],
            headers: [(tenantHeaderName): tenantId])

        [response, alternateResponse, commaResponse].each {  lresponse ->
          assertEquals(200, lresponse.status)
          assertEquals([[
                            tenantId: tenantId,
                            id      : 'N1',
                            tags    : ['a1': 'A/B', 'd1': 'B:A', 'c1': 'C,D'],
                            type: it.type,
                            dataRetention: 7
                        ]], lresponse.data)
        }
      }
  }

  @Test
  void findTagValues() {
    metricTypes.each {
      def tenantId = nextTenantId()

      // Create metric
      def response = hawkularMetrics.post(path: it.path, body: [
          id  : 'N1',
          tags: ['a1': 'A/B', 'd1': 'B:A']
      ], headers: [(tenantHeaderName): tenantId])
      assertEquals(201, response.status)

      response = hawkularMetrics.post(path: it.path, body: [
          id  : 'N2',
          tags: ['a1': 'a', 'd1': 'B:A']
      ], headers: [(tenantHeaderName): tenantId])
      assertEquals(201, response.status)

      // Fetch metrics using tags
      response = hawkularMetrics.get(path: it.path + "/tags/d1:B%3AA",
          headers: [(tenantHeaderName): tenantId])
      assertEquals(200, response.status)

      assertEquals([
                        d1: ['B:A']
                    ], response.data)

      response = hawkularMetrics.get(path: it.path + "/tags/a1:*,d1:B%3AA",
          headers: [(tenantHeaderName): tenantId])

      def genericResponse = hawkularMetrics.get(path: "metrics/tags/a1:*,d1:B%3AA",
          query: [type: it.type], headers: [(tenantHeaderName): tenantId])

      def untypedResponse = hawkularMetrics.get(path: "metrics/tags/a1:*,d1:B%3AA", headers: [(tenantHeaderName):
                                                                                                  tenantId])

      [response, genericResponse, untypedResponse].each { lresponse ->
        assertEquals(200, lresponse.status)
        assertEquals([
            a1: ['a', 'A/B'],
            d1: ['B:A']
        ], lresponse.data)
      }

      // Fetch empty
      response = hawkularMetrics.get(path: it.path + "/tags/g1:*",
          headers: [(tenantHeaderName): tenantId])
      assertEquals(204, response.status)

      // Find from generic endpoint
    }
  }

  @Test
  void findTagNames() {
    def tenantId = nextTenantId()

    metricTypes.each {

      // Create metric
      def response = hawkularMetrics.post(path: it.path, body: [
          id  : 'N1',
          tags: ['a1': 'A/B', 'd1': 'B:A']
      ], headers: [(tenantHeaderName): tenantId])
      assertEquals(201, response.status)

      response = hawkularMetrics.post(path: it.path, body: [
          id  : 'N2',
          tags: ['a1': 'a', 'd3': 'B:A']
      ], headers: [(tenantHeaderName): tenantId])
      assertEquals(201, response.status)
    }

    // Fetch available tag names
    def response = hawkularMetrics.get(path: "metrics/tags",
        headers: [(tenantHeaderName): tenantId]);
    assertEquals(200, response.status)
    assertEquals(3, response.data.size)

    assertTrue(response.data.contains('a1'))
    assertTrue(response.data.contains('d1'))
    assertTrue(response.data.contains('d3'))

    // Fetch with filter
    response = hawkularMetrics.get(path: "metrics/tags",
        query: [filter: 'd.*'],
        headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertEquals(2, response.data.size)
    assertTrue(response.data.contains('d1'))
    assertTrue(response.data.contains('d3'))

    // Fetch filter without match
    response = hawkularMetrics.get(path: "metrics/tags",
        query: [filter: 'e*'],
        headers: [(tenantHeaderName): tenantId])
    assertEquals(204, response.status)

    // Test type filter
    response = hawkularMetrics.delete(path: "gauges/N2/tags/d3", headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)

    response = hawkularMetrics.get(path: "metrics/tags",
        query: [filter: 'd.*', type: 'gauge'],
        headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertEquals(1, response.data.size)
    assertTrue(response.data.contains('d1'))
  }


  @Test
  void addTaggedDataPoints() {
    def dataValues = [:]
    dataValues['counter'] = [1, 99, 2]
    dataValues['gauge'] = [1.1, 2.2, 33.3]
    dataValues['availability'] = ["up", "down", "unknown"]
    dataValues['string'] = ["Test String 1", "String Test 2", "3 String Test"]

    metricTypes.each {
      String tenantId = nextTenantId()
      DateTime start = now().minusMinutes(30)
      DateTime end = start.plusMinutes(10)
      String metricId = 'G1'

      def response = hawkularMetrics.post(
          path: it.path + "/" + metricId + "/raw",
          headers: [(tenantHeaderName): tenantId],
          body: [
              [
                  timestamp: start.millis,
                  value: dataValues[it.type][0],
                  tags: [x: 1, y: 2]
              ],
              [
                  timestamp: start.plusMinutes(1).millis,
                  value: dataValues[it.type][1],
                  tags: [y: 3, z: 5]
              ],
              [
                  timestamp: start.plusMinutes(3).millis,
                  value: dataValues[it.type][2],
                  tags: [x: 4, z: 6]
              ]
          ]
      )
      assertEquals(200, response.status)
      response = hawkularMetrics.get(path: it.path + "/" + metricId + "/raw", headers: [(tenantHeaderName): tenantId])
      def expectedData = [
          [
              timestamp: start.plusMinutes(3).millis,
              value: dataValues[it.type][2],
              tags: [x: '4', z: '6']
          ],
          [
              timestamp: start.plusMinutes(1).millis,
              value: dataValues[it.type][1],
              tags: [y: '3', z: '5']
          ],
          [
              timestamp: start.millis,
              value: dataValues[it.type][0],
              tags: [x: '1', y: '2']
          ]
      ]
      assertEquals(expectedData, response.data)
    }
  }

  @Test
  void findDefinitionsWithIdFiltering() {
    metricTypes.each {
      def tenantId = nextTenantId()

      // Create metric
      def response = hawkularMetrics.post(path: it.path, body: [
          id  : 'N1',
          tags: ['a1': 'A', 'd1': 'B'],
          dataRetention: 7
      ], headers: [(tenantHeaderName): tenantId])
      assertEquals(201, response.status)

      response = hawkularMetrics.post(path: it.path, body: [
          id  : 'N2',
          tags: ['a1': 'A2'],
          dataRetention: 7
      ], headers: [(tenantHeaderName): tenantId])
      assertEquals(201, response.status)

      response = hawkularMetrics.post(path: it.path, body: [
          id  : '91c171ed-0294-44b3-bcdb-42253b58aa5a',
          tags: ['c1': 'C'],
          dataRetention: 7
      ], headers: [(tenantHeaderName): tenantId])
      assertEquals(201, response.status)

      response = hawkularMetrics.get(path: 'metrics',
          query: [id: 'N1|N2', type: it.type],
          headers: [(tenantHeaderName): tenantId])

      [response].each { lresponse ->
        assertEquals(200, lresponse.status)
        assertTrue(lresponse.data instanceof List)
        assertEquals(2, lresponse.data.size())
        assertTrue((lresponse.data ?: []).contains([
            dataRetention: 7,
            tenantId: tenantId,
            id      : 'N1',
            tags    : ['a1': 'A', 'd1': 'B'],
            type: it.type
        ]))
        assertTrue((lresponse.data ?: []).contains([
            dataRetention: 7,
            tenantId: tenantId,
            id      : 'N2',
            tags    : ['a1': 'A2'],
            type: it.type
        ]))
      }

      // Create metric with uuid, detect issue with regexp |
      response = hawkularMetrics.get(path: 'metrics',
          query: [id: '91c171ed-0294-44b3-bcdb-42253b58aa5a', type: it.type],
          headers: [(tenantHeaderName): tenantId])

        assertEquals(response.data, [[
            dataRetention: 7,
            tenantId: tenantId,
            id      : '91c171ed-0294-44b3-bcdb-42253b58aa5a',
            tags    : ['c1': 'C'],
            type: it.type
        ]])

        assertEquals(200, response.status)
        assertTrue(response.data instanceof List)
        assertEquals(1, response.data.size())
        assertTrue((response.data ?: []).contains([
            dataRetention: 7,
            tenantId: tenantId,
            id      : '91c171ed-0294-44b3-bcdb-42253b58aa5a',
            tags    : ['c1': 'C'],
            type: it.type
        ]))

      badGet(path: "metrics", query: [id: '91c171ed-0294-44b3-bcdb-42253b58aa5a'],
          headers: [(tenantHeaderName): tenantId]) { exception ->
        // Missing type
        assertEquals(400, exception.response.status)
      }
    }
  }
}
