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

import groovy.json.JsonOutput


/**
 * @author Stefan Negrea
 */
class JsonTagsITest extends RESTTest {

  static metrics =  [
    ['id': 'm1', 'tags': ['a1': JsonOutput.toJson(['foo': 'd']), 'b1': 'B']],
    ['id': 'm2', 'tags': ['a1': JsonOutput.toJson(['foo':['bar': 3]]), 'b1': 'B']],
    ['id': 'm3', 'tags': ['a1': JsonOutput.toJson(['foo':['bar': 4]]), 'b1': 'C']],
    ['id': 'm4', 'tags': ['a1': JsonOutput.toJson(['bar': 'ab']), 'b1': 'B']],
    ['id': 'm5', 'tags': ['a1': JsonOutput.toJson(['fizz': ['foo': ['bar': 3]]]), 'b1': 'C']],
    ['id': 'm6', 'tags': ['c1': 'C']]
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

      //match 'a1' tags with original regex tag query
      def response = hawkularMetrics.get(path: "metrics",
        query: [tags: "a1:*"],
        headers: [(tenantHeaderName): tenantId])
      assertEquals(200, response.status)
      assertMetricListById(response.data, 'm1', 'm2', 'm3', 'm4', 'm5')

      //match 'a1' JSON tags that have a top property named "foo"
      response = hawkularMetrics.get(path: "metrics",
        query: [tags: 'a1:json:$.foo'],
        headers: [(tenantHeaderName): tenantId])
      assertEquals(200, response.status)
      assertMetricListById(response.data, 'm1', 'm2', 'm3')

      //match 'a1' JSON tags that have a property named 'bar' at any level of the JSON tree
      response = hawkularMetrics.get(path: "metrics",
        query: [tags: 'a1:json:$..bar'],
        headers: [(tenantHeaderName): tenantId])
      assertEquals(200, response.status)
      assertMetricListById(response.data, 'm2', 'm3', 'm4', 'm5')

      //match 'a1' JSON tags that have a property named 'foo'.'bar' at any level of the JSON tree
      response = hawkularMetrics.get(path: "metrics",
        query: [tags: 'a1:json:$..foo.bar'],
        headers: [(tenantHeaderName): tenantId])
      assertEquals(200, response.status)
      assertMetricListById(response.data, 'm2', 'm3', 'm5')

      //match 'a1' JSON tags that have a property 'foo'.'bar' != 3 at any level of the JSON tree
      response = hawkularMetrics.get(path: "metrics",
        query: [tags: 'a1:json:$..foo[?(@.bar != 3)]'],
        headers: [(tenantHeaderName): tenantId])
      assertEquals(200, response.status)
      assertMetricListById(response.data, 'm3')

      //match 'a1' JSON tags that have a property 'foo'.'bar' == 3 at any level of the JSON tree
      response = hawkularMetrics.get(path: "metrics",
        query: [tags: 'a1:json:$..foo[?(@.bar == 3)]'],
        headers: [(tenantHeaderName): tenantId])
      assertEquals(200, response.status)
      assertMetricListById(response.data, 'm2', 'm5')

      //match 'a1' JSON tags that have a property 'bar' == 'ab' or 'bar' == 3 at any level of the JSON tree
      response = hawkularMetrics.get(path: "metrics",
        query: [tags: 'a1:json:$..[?(@.bar == \'ab\' || @.bar == 3)]'],
        headers: [(tenantHeaderName): tenantId])
      assertEquals(200, response.status)
      assertMetricListById(response.data, 'm2', 'm4', 'm5')

      //match 'a1' JSON tags that have a property named 'bar' as a child of 'fizz', 'fizz' being a top level property
      response = hawkularMetrics.get(path: "metrics",
        query: [tags: 'a1:json:$.fizz..bar'],
        headers: [(tenantHeaderName): tenantId])
      assertEquals(200, response.status)
      assertMetricListById(response.data, 'm5')

      //match 'a1' JSON tags that have a property 'bar' == 3 as a child of 'fizz', 'fizz' being a top level property
      response = hawkularMetrics.get(path: "metrics",
        query: [tags: 'a1:json:$.fizz..[?(@.bar == 3)]'],
        headers: [(tenantHeaderName): tenantId])
      assertEquals(200, response.status)
      assertMetricListById(response.data, 'm5')

      //match 'a1' JSON tags that have a property 'bar' == 3 as a child of 'fizz', 'fizz' being a top level property
      // and also have a regex filter for c1 tag, no metrics satisfy both of these conditions
      response = hawkularMetrics.get(path: "metrics",
        query: [tags: 'a1:json:$.fizz..[?(@.bar == 3)],c1:*'],
        headers: [(tenantHeaderName): tenantId])
      assertEquals(204, response.status)

      //match 'a1' JSON tags that have a property 'foo' anywhere in the Json
      // and also have a regex filter for b1 == C
      response = hawkularMetrics.get(path: "metrics",
        query: [tags: 'a1:json:$..foo,b1:C'],
        headers: [(tenantHeaderName): tenantId])
      assertEquals(200, response.status)
      assertMetricListById(response.data, 'm3', 'm5')

      response = hawkularMetrics.get(path: "metrics",
        query: [tags: 'b1:C,a1:json:$..foo'],
        headers: [(tenantHeaderName): tenantId])
      assertEquals(200, response.status)
      assertMetricListById(response.data, 'm3', 'm5')

      //have two matchers for the same tag key
      response = hawkularMetrics.get(path: "metrics",
        query: [tags: 'a1:json:$..foo,a1:json:$..bar'],
        headers: [(tenantHeaderName): tenantId])
      assertEquals(200, response.status)
      assertMetricListById(response.data, 'm2', 'm3', 'm5')
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
