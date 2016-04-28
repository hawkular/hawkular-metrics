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

import org.joda.time.DateTime
import org.junit.Test

import static org.joda.time.DateTime.now
import static org.junit.Assert.assertEquals

/**
 * @author jsanda
 */
class StringITest extends RESTTest {

  def tenantId = nextTenantId()

  @Test
  void shouldNotAcceptInvalidTimeRange() {
    badGet(path: "strings/test/raw", headers: [(tenantHeaderName): tenantId],
        query: [start: 1000, end: 500]) { exception ->
      assertEquals(400, exception.response.status)
    }
  }

  @Test
  void shouldNotAddStringDataForMetricWithEmptyPayload() {
    badPost(path: "strings/MyString/raw", headers: [(tenantHeaderName): tenantId],
        body: "" /* Empty Body */) { exception ->
      assertEquals(400, exception.response.status)
    }

    badPost(path: "strings/MyString/raw", headers: [(tenantHeaderName): tenantId],
        body: [] /* Empty List */) { exception ->
      assertEquals(400, exception.response.status)
    }
  }

  @Test
  void shouldNotAddStringDataWithEmptyPayload() {
    badPost(path: "strings/raw", headers: [(tenantHeaderName): tenantId],
        body: "" /* Empty Body */) { exception ->
      assertEquals(400, exception.response.status)
    }

    badPost(path: "strings/raw", headers: [(tenantHeaderName): tenantId],
        body: [] /* Empty List */) { exception ->
      assertEquals(400, exception.response.status)
    }
  }

  @Test
  void addAndFetchDataForSingleMetric() {
    DateTime end = now()
    DateTime start = end.minusMinutes(20)
    String id = 'MyString'

    def response = hawkularMetrics.post(
        path: "strings/$id/raw",
        headers: [(tenantHeaderName): tenantId],
        body: [
            [
                timestamp: start.millis,
                value: 'stopped',
                tags: [x: '1', y: '2']
            ],
            [
                timestamp: start.plusMinutes(2).millis,
                value: 'starting',
                tags: [y: '3', z: '5']
            ],
            [
                timestamp: start.plusMinutes(4).millis,
                value: 'running',
                tags: [x: '4', z: '6']
            ]
        ]
    )
    assertEquals(200, response.status)
    response = hawkularMetrics.get(path: "strings/$id/raw", headers: [(tenantHeaderName): tenantId])
    def expectedData = [
        [
            timestamp: start.plusMinutes(4).millis,
            value: 'running',
            tags: [x: '4', z: '6']
        ],
        [
            timestamp: start.plusMinutes(2).millis,
            value: 'starting',
            tags: [y: '3', z: '5']
        ],
        [
            timestamp: start.millis,
            value: 'stopped',
            tags: [x: '1', y: '2']
        ]
    ]
    assertEquals(expectedData, response.data)
  }

  @Test
  void addDataForMultipleMetrics() {
    DateTime end = now()
    DateTime start = end.minusMinutes(20)

    def response = hawkularMetrics.post(
        path: 'strings/raw',
        headers: [(tenantHeaderName): tenantId],
        body: [
            [
                id: 'S1',
                data: [
                    [
                        timestamp: start.millis,
                        value: 'starting',
                        tags: [hostname: 'server1']
                    ],
                    [
                        timestamp: start.plusMinutes(5).millis,
                        value: 'running',
                        tags: [hostname: 'server1']
                    ]
                ]
            ],
            [
                id: 'S2',
                data: [
                    [
                        timestamp: start.plusMinutes(5).millis,
                        value: 'running',
                        tags: [hostname: 'server2']
                    ],
                    [
                        timestamp: start.plusMinutes(10).millis,
                        value: 'stopping',
                        tags: [hostname: 'server2']
                    ]
                ]
            ]
        ]
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.get(path: "strings/S1/raw", headers: [(tenantHeaderName): tenantId])
    def expectedData = [
        [
            timestamp: start.plusMinutes(5).millis,
            value: 'running',
            tags: [hostname: 'server1']
        ],
        [
            timestamp: start.millis,
            value: 'starting',
            tags: [hostname: 'server1']
        ],
    ]
    assertEquals(expectedData, response.data)

    response = hawkularMetrics.get(path: "strings/S2/raw", headers: [(tenantHeaderName): tenantId])
    expectedData = [
        [
            timestamp: start.plusMinutes(10).millis,
            value: 'stopping',
            tags: [hostname: 'server2']
        ],
        [
            timestamp: start.plusMinutes(5).millis,
            value: 'running',
            tags: [hostname: 'server2']
        ],
    ]
    assertEquals(expectedData, response.data)
  }

  @Test
  void findDistinctValuesInAscendingOrder() {
    DateTime end = now()
    DateTime start = end.minusMinutes(20)
    String id = 'MyString'

    def response = hawkularMetrics.post(
        path: "strings/$id/raw",
        headers: [(tenantHeaderName): tenantId],
        body: [
            [timestamp: start.millis, value: 'stopped'],
            [timestamp: start.plusMinutes(2).millis, value: 'stopped'],
            [timestamp: start.plusMinutes(4).millis, value: 'starting'],
            [timestamp: start.plusMinutes(6).millis, value: 'running'],
            [timestamp: start.plusMinutes(8).millis, value: 'running'],
            [timestamp: start.plusMinutes(10).millis, value: 'running'],
            [timestamp: start.plusMinutes(12).millis, value: 'stopping'],
            [timestamp: start.plusMinutes(14).millis, value: 'stopping'],
            [timestamp: start.plusMinutes(16).millis, value: 'stopped']
        ]
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.get(
        path: "strings/$id/raw",
        query: [
            start: start.plusMinutes(2).millis,
            end: start.plusMinutes(14).millis,
            order: 'asc',
            distinct: 'true'
        ],
        headers: [(tenantHeaderName): tenantId],
    )
    assertEquals(200, response.status)

    def expectedData = [
        [timestamp: start.plusMinutes(2).millis, value: 'stopped'],
        [timestamp: start.plusMinutes(4).millis, value: 'starting'],
        [timestamp: start.plusMinutes(6).millis, value: 'running'],
        [timestamp: start.plusMinutes(12).millis, value: 'stopping']
    ]
    assertEquals(expectedData, response.data)
  }
}
