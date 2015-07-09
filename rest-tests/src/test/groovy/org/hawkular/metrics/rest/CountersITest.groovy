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
import org.hawkular.metrics.core.impl.DateTimeService
import org.joda.time.DateTime
import org.joda.time.Duration
import org.junit.Test

import static org.joda.time.DateTime.now
import static org.junit.Assert.assertEquals

/**
 * @author John Sanda
 */
class CountersITest extends RESTTest {

  @Test
  void shouldNotCreateMetricWithEmptyPayload() {
    def tenantId = nextTenantId()

    badPost(path: "counters", headers: [(tenantHeaderName): tenantId], body: "" /* Empty Body */) { exception ->
      assertEquals(400, exception.response.status)
    }
  }

  @Test
  void shouldNotAddDataForCounterWithEmptyPayload() {
    def tenantId = nextTenantId()

    badPost(path: "counters/pimpo/data", headers: [(tenantHeaderName): tenantId],
        body: "" /* Empty Body */) { exception ->
      assertEquals(400, exception.response.status)
    }

    badPost(path: "counters/pimpo/data", headers: [(tenantHeaderName): tenantId],
        body: [] /* Empty List */) { exception ->
      assertEquals(400, exception.response.status)
    }
  }

  @Test
  void shouldNotAddDataWithEmptyPayload() {
    def tenantId = nextTenantId()

    badPost(path: "counters/data", headers: [(tenantHeaderName): tenantId],
        body: "" /* Empty Body */) { exception ->
      assertEquals(400, exception.response.status)
    }

    badPost(path: "counters/data", headers: [(tenantHeaderName): tenantId],
        body: [] /* Empty List */) { exception ->
      assertEquals(400, exception.response.status)
    }
  }

  @Test
  void createSimpleCounter() {
    String tenantId = nextTenantId()
    String id = "C1"

    def response = hawkularMetrics.post(
        path: "counters",
        headers: [(tenantHeaderName): tenantId],
        body: [id: id]
    )
    assertEquals(201, response.status)

    response = hawkularMetrics.get(path: "counters/$id", headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)

    def expectedData = [tenantId: tenantId, id: id]
    assertEquals(expectedData, response.data)
  }

  @Test
  void shouldNotCreateDuplicateCounter() {
    String tenantId = nextTenantId()
    String id = "C1"

    def response = hawkularMetrics.post(
        path: "counters",
        headers: [(tenantHeaderName): tenantId],
        body: [id: id]
    )
    assertEquals(201, response.status)

    badPost(path: 'counters', headers: [(tenantHeaderName): tenantId], body: [id: id]) { exception ->
      assertEquals(409, exception.response.status)
    }
  }

  @Test
  void createCounterWithTagsAndDataRetention() {
    String tenantId = nextTenantId()
    String id = "C1"

    def response = hawkularMetrics.post(
        path: "counters",
        headers: [(tenantHeaderName): tenantId],
        body: [
            id: id,
            tags: [tag1: 'one', tag2: 'two'],
            dataRetention: 100
        ]
    )
    assertEquals(201, response.status)

    response = hawkularMetrics.get(path: "counters/$id", headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)

    def expectedData = [
        tenantId: tenantId,
        id: id,
        tags: [tag1: 'one', tag2: 'two'],
        dataRetention: 100
    ]
    assertEquals(expectedData, response.data)
  }

  @Test
  void createAndFindCounters() {
    String tenantId = nextTenantId()
    String counter1 = "C1"
    String counter2 = "C2"

    def response = hawkularMetrics.post(
        path: "counters",
        headers: [(tenantHeaderName): tenantId],
        body: [id: counter1]
    )
    assertEquals(201, response.status)

    response = hawkularMetrics.post(
        path: "counters",
        headers: [(tenantHeaderName): tenantId],
        body: [
            id: counter2,
            tags: [tag1: 'one', tag2: 'two']
        ]
    )
    assertEquals(201, response.status)

    response = hawkularMetrics.get(
        path: "metrics",
        headers: [(tenantHeaderName): tenantId],
        query: [type: 'counter']
    )
    assertEquals(200, response.status)

    def expectedData = [
        [
            tenantId: tenantId,
            id: counter1
        ],
        [
            tenantId: tenantId,
            id: counter2,
            tags: [
                tag1: 'one',
                tag2: 'two'
            ]
        ]
    ]
    assertEquals(expectedData, response.data)
  }

  @Test
  void addDataForMultipleCountersAndFindhWithDateRange() {
    String tenantId = nextTenantId()
    String counter1 = "C1"
    String counter2 = "C2"

    DateTime start = now().minusMinutes(5)

    def response = hawkularMetrics.post(
        path: "counters/data",
        headers: [(tenantHeaderName): tenantId],
        body: [
            [
                id: counter1,
                data: [
                    [timestamp: start.millis, value: 10],
                    [timestamp: start.plusMinutes(1).millis, value: 20]
                ]
            ],
            [
                id: counter2,
                data: [
                    [timestamp: start.millis, value: 150],
                    [timestamp: start.plusMinutes(1).millis, value: 225],
                    [timestamp: start.plusMinutes(2).millis, value: 300]
                ]
            ]
        ]
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.get(
        path: "counters/$counter1/data",
        headers: [(tenantHeaderName): tenantId],
        query: [start: start.millis, end: start.plusMinutes(1).millis]
    )
    assertEquals(200, response.status)

    def expectedData = [
        [timestamp: start.millis, value: 10]
    ]
    assertEquals(expectedData, response.data)

    response = hawkularMetrics.get(
        path: "counters/$counter2/data",
        headers: [(tenantHeaderName): tenantId],
        query: [start: start.millis, end: start.plusMinutes(2).millis]
    )
    assertEquals(200, response.status)

    expectedData = [
        [timestamp: start.plusMinutes(1).millis, value: 225],
        [timestamp: start.millis, value: 150]
    ]
    assertEquals(expectedData, response.data)
  }

  @Test
  void addDataForSingleCounterAndFindWithDefaultDateRange() {
    String tenantId = nextTenantId()
    String counter = "C1"
    DateTime start = now().minusHours(8)

    def response = hawkularMetrics.post(
        path: "counters/$counter/data",
        headers: [(tenantHeaderName): tenantId],
        body: [
            [timestamp: start.millis, value: 100],
            [timestamp: start.plusHours(1).millis, value :200],
            [timestamp: start.plusHours(4).millis, value: 500],
            [timestamp: now().plusSeconds(30).millis, value: 750]
        ]
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.get(
        path: "counters/$counter/data",
        headers: [(tenantHeaderName): tenantId],
    )
    assertEquals(200, response.status)

    def expectedData = [
        [timestamp: start.plusHours(4).millis, value: 500],
        [timestamp: start.plusHours(1).millis, value: 200]
    ]
    assertEquals(expectedData, response.data)
  }

  @Test
  void findWhenThereIsNoData() {
    String tenantId = nextTenantId()
    String counter1 = "C1"
    String counter2 = "C2"
    DateTime start = now().minusHours(3)

    def response = hawkularMetrics.post(
        path: "counters/data",
        headers: [(tenantHeaderName): tenantId],
        body: [
            [
                id: counter1,
                data: [
                    [timestamp: start.millis, value: 100],
                    [timestamp: start.plusHours(1).millis, value: 150]
                ]
            ]
        ]
    )
    assertEquals(200, response.status)

    // First query a counter that has data but outside of the date range for which data
    // points are available
    response = hawkularMetrics.get(
        path: "counters/$counter1/data",
        headers: [(tenantHeaderName): tenantId],
        query: [start: start.minusHours(5).millis, end: start.minusHours(4).millis]
    )
    assertEquals(204, response.status)

    // Now query a counter that has no dat at all
    response = hawkularMetrics.get(
        path: "counters/$counter2/data",
        headers: [(tenantHeaderName): tenantId]
    )
    assertEquals(204, response.status)
  }

  @Test
  void findRate() {
    String tenantId = nextTenantId()
    String counter = "C1"
    DateTimeService dateTimeService = new DateTimeService()
    DateTime start = dateTimeService.getTimeSlice(now(), Duration.standardSeconds(5)).plusSeconds(5)

    def response = hawkularMetrics.post(
        path: "counters",
        headers: [(tenantHeaderName): tenantId],
        body: [id: counter]
    )
    assertEquals(201, response.status)

    response = hawkularMetrics.post(
        path: "counters/$counter/data",
        headers: [(tenantHeaderName): tenantId],
        body: [
            [timestamp: start.millis, value: 100],
            [timestamp: start.plusSeconds(1).millis, value : 200],
            [timestamp: start.plusSeconds(3).millis, value : 345],
            [timestamp: start.plusSeconds(5).millis, value : 515],
            [timestamp: start.plusSeconds(7).millis, value : 595],
            [timestamp: start.plusSeconds(9).millis, value : 747]
        ]
    )
    assertEquals(200, response.status)

    while (now().isBefore(start.plusSeconds(20))) {
      Thread.sleep(100)
    }

    response = hawkularMetrics.get(
        path: "counters/$counter/rate",
        headers: [(tenantHeaderName): tenantId],
        query: [start: start.millis, end: start.plusSeconds(10).millis]
    )
    assertEquals(200, response.status)

    def expectedData = [
        [
            timestamp: start.plusSeconds(5).millis,
            value: calculateRate(747, start.plusSeconds(5), start.plusSeconds(10))
        ],
        [
            timestamp: start.millis,
            value: calculateRate(345, start, start.plusSeconds(5))
        ],
    ]

    assertEquals("Expected to get back two data points", 2, response.data.size())
    assertRateEquals(expectedData[0], response.data[0])
    assertRateEquals(expectedData[1], response.data[1])
  }

  static double calculateRate(double value, DateTime start, DateTime end) {
    return (value / (end.millis - start.millis)) * 1000.0
  }

  static void assertRateEquals(def expected, def actual) {
    assertEquals("The timestamp does not match the expected value", expected.timestamp, actual.timestamp)
    assertDoubleEquals(expected.value, actual.value)
  }

}
