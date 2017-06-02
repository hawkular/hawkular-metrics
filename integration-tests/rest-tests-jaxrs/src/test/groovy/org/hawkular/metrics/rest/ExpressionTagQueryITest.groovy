/*
 * Copyright 2014-2017 Red Hat, Inc. and/or its affiliates
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

import groovy.json.JsonOutput


/**
 * @author Stefan Negrea
 */
class ExpressionTagQueryITest extends RESTTest {

  static metrics =  [
    ['id': 'm1', 'tags': ['a1': 'd', 'b1': 'B', 'a2': 'a']],
    ['id': 'm2', 'tags': ['a1': 'xyz', 'b1': 'B', 'a2': 'b']],
    ['id': 'm3', 'tags': ['a1': 'abcd', 'b1': 'C']],
    ['id': 'm4', 'tags': ['a1': 'ab', 'b1': 'B']],
    ['id': 'm5', 'tags': ['a1': 'xyz', 'b1': 'C']],
    ['id': 'm6', 'tags': ['c1': 'C', 'a.b': 'c.d']]
  ];

  @Test
  void createMetricsAndUpdateTags() {
    metricTypes.each {
      String tenantId = nextTenantId()

      def metricType = it

      metrics.each {
        def response = hawkularMetrics.post(path: metricType.path, body: [
            id  : it.id,
            tags: it.tags
        ], headers: [(tenantHeaderName): tenantId])
        assertEquals(201, response.status)

        response = hawkularMetrics.get(path: metricType.path + '/' +it.id, headers: [(tenantHeaderName): tenantId])
        assertEquals(200, response.status)
        assertEquals([
            tenantId: tenantId,
            id  : it.id,
            tags: it.tags,
            dataRetention: 7,
            type: metricType.type
        ], response.data)
      }

      def response = hawkularMetrics.get(path: "metrics",
        query: [tags: "a1 ~ '*'"],
        headers: [(tenantHeaderName): tenantId])
      assertEquals(200, response.status)
      assertMetricListById(response.data, 'm1', 'm2', 'm3', 'm4', 'm5')

      response = hawkularMetrics.get(path: "metrics",
        query: [tags: "a1 ~ '*' AND b1 = 'B'"],
        headers: [(tenantHeaderName): tenantId])
      assertEquals(200, response.status)
      assertMetricListById(response.data, 'm1', 'm2', 'm4')

      response = hawkularMetrics.get(path: "metrics",
        query: [tags: "a1 ~ '*' AND b1 != 'B'"],
        headers: [(tenantHeaderName): tenantId])
      assertEquals(200, response.status)
      assertMetricListById(response.data, 'm3', 'm5')

      response = hawkularMetrics.get(path: "metrics",
        query: [tags: "a1 IN ['xyz','abcd'] AND b1 ~ '*'"],
        headers: [(tenantHeaderName): tenantId])
      assertEquals(200, response.status)
      assertMetricListById(response.data, 'm2', 'm3', 'm5')

      response = hawkularMetrics.get(path: "metrics",
        query: [tags: "c1 ~ '*' OR b1 != 'B'"],
        headers: [(tenantHeaderName): tenantId])
      assertEquals(200, response.status)
      assertMetricListById(response.data, 'm3', 'm5', 'm6')

      response = hawkularMetrics.get(path: "metrics",
        query: [tags: "c1 ~ '*' OR (b1 != 'B' AND a1 = 'abcd')"],
        headers: [(tenantHeaderName): tenantId])
      assertEquals(200, response.status)
      assertMetricListById(response.data, 'm3', 'm6')

      response = hawkularMetrics.get(path: "metrics",
        query: [tags: "a1 NOT IN ['xyz', 'abcd']" ],
        headers: [(tenantHeaderName): tenantId])
      assertEquals(200, response.status)
      assertMetricListById(response.data, 'm1', 'm4')

      response = hawkularMetrics.get(path: "metrics",
        query: [tags: "a1 NOT IN ['xyz','abcd'] OR b1 = 'B'" ],
        headers: [(tenantHeaderName): tenantId])
      assertEquals(200, response.status)
      assertMetricListById(response.data, 'm1', 'm2', 'm4')

      response = hawkularMetrics.get(path: "metrics",
        query: [tags: "a1 = 'd' OR ( a1 = 'ab' OR ( c1 ~ '*'))" ],
        headers: [(tenantHeaderName): tenantId])
      assertEquals(200, response.status)
      assertMetricListById(response.data, 'm1', 'm4', 'm6')

      response = hawkularMetrics.get(path: "metrics",
        query: [tags: "c1 = '100'"],
        headers: [(tenantHeaderName): tenantId])
      assertEquals(204, response.status)

      response = hawkularMetrics.get(path: "metrics",
        query: [tags: "c1 = 100"],
        headers: [(tenantHeaderName): tenantId])
      assertEquals(204, response.status)

      response = hawkularMetrics.get(path: "metrics",
        query: [tags: "a1 = d OR a1 = abcd"],
        headers: [(tenantHeaderName): tenantId])
      assertEquals(200, response.status)
      assertMetricListById(response.data, 'm1', 'm3')

      response = hawkularMetrics.get(path: "metrics",
        query: [tags: "c1 = '100' OR a1 = 'xyz' OR a1 IN ['abcd']" ],
        headers: [(tenantHeaderName): tenantId])
      assertEquals(200, response.status)
      assertMetricListById(response.data, 'm2', 'm3', 'm5')

      response = hawkularMetrics.get(path: "metrics",
        query: [tags: "c1 = '100' OR a1 = xyz OR a1 IN [abcd]" ],
        headers: [(tenantHeaderName): tenantId])
      assertEquals(200, response.status)
      assertMetricListById(response.data, 'm2', 'm3', 'm5')

      response = hawkularMetrics.get(path: "metrics",
        query: [tags: "A1 in ['xyz', 'abcd']"],
        headers: [(tenantHeaderName): tenantId])
      assertEquals(204, response.status)

      response = hawkularMetrics.get(path: "metrics",
        query: [tags: "a1 NOT IN ['xyz', 'abcd']"],
        headers: [(tenantHeaderName): tenantId])
      assertEquals(200, response.status)
      assertMetricListById(response.data, 'm1', 'm4')

      response = hawkularMetrics.get(path: "metrics",
        query: [tags: "a2"],
        headers: [(tenantHeaderName): tenantId])
      assertEquals(200, response.status)
      assertMetricListById(response.data, 'm1', 'm2')

      response = hawkularMetrics.get(path: "metrics",
        query: [tags: "not a2"],
        headers: [(tenantHeaderName): tenantId])
      assertEquals(200, response.status)
      assertMetricListById(response.data, 'm3', 'm4', 'm5', 'm6')

      response = hawkularMetrics.get(path: "metrics",
        query: [tags: "a.b"],
        headers: [(tenantHeaderName): tenantId])
      assertEquals(200, response.status)
      assertMetricListById(response.data, 'm6')

      response = hawkularMetrics.get(path: "metrics",
        query: [tags: "a.b = c.d"],
        headers: [(tenantHeaderName): tenantId])
      assertEquals(200, response.status)
      assertMetricListById(response.data, 'm6')

      response = hawkularMetrics.get(path: "metrics",
        query: [tags: "a.b = 'c.d'"],
        headers: [(tenantHeaderName): tenantId])
      assertEquals(200, response.status)
      assertMetricListById(response.data, 'm6')

      response = hawkularMetrics.get(path: "metrics",
        query: [tags: "a.b ~ 'c.*'"],
        headers: [(tenantHeaderName): tenantId])
      assertEquals(200, response.status)
      assertMetricListById(response.data, 'm6')

      response = hawkularMetrics.get(path: "metrics",
        query: [tags: "a1 !~ 'ab.*'"],
        headers: [(tenantHeaderName): tenantId])
      assertEquals(200, response.status)
      assertMetricListById(response.data, 'm1', 'm2', 'm5')

      response = hawkularMetrics.get(path: "metrics",
        query: [tags: "a1 !~ 'ab.+'"],
        headers: [(tenantHeaderName): tenantId])
      assertEquals(200, response.status)
      assertMetricListById(response.data, 'm1', 'm2', 'm4', 'm5')
    }
  }

  void assertMetricListById(actualMetrics, String... expectedMetricIds) {
    assertEquals(expectedMetricIds.length, actualMetrics.size)

    expectedMetricIds.each {
      def expectedMetricId = it
      def found = false

      actualMetrics.each {
        if (it['id'].equals(expectedMetricId)) {
          found = true
        }
      }

      if (!found) {
        System.out.println("Metric ${expectedMetricId} was not found in the list of retrieved metrics.");
      }

      assertTrue(found)
    }
  }
}