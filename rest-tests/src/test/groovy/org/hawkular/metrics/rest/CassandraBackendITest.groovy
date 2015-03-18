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
import org.junit.Test

import static org.joda.time.DateTime.now
import static org.joda.time.Seconds.seconds
import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertNotNull
import static org.junit.Assert.assertTrue

class CassandraBackendITest extends RESTTest {

  def assertNumericDataPointEquals = { expected, actual ->
    assertEquals("The timestamp does not match", expected.timestamp, actual.timestamp)
    assertTrue("Expected a value of $expected.value but found $actual.value",
        expected.value.compareTo(actual.value) == 0)
  }

  @Test
  void simpleInsertAndQueryNumericData() {
    DateTimeService dateTimeService = new DateTimeService()
    String tenantId = nextTenantId()
    String metric = 'n1'
    DateTime start = dateTimeService.currentHour().minusHours(1)
    DateTime end = start.plusHours(1)

    int numBuckets = 10
    long bucketSize = (end.millis - start.millis) / numBuckets
    def buckets = []
    numBuckets.times { buckets.add(start.millis + (it * bucketSize)) }

    def response = hawkularMetrics.post(path: "$tenantId/metrics/numeric/data", body: [
        [id: 'test',
         data: [
            [timestamp: buckets[0], value: 12.22],
            [timestamp: buckets[0] + seconds(10).toStandardDuration().millis, value: 12.37],
            [timestamp: buckets[4], value: 25],
            [timestamp: buckets[4] + seconds(15).toStandardDuration().millis, value: 25],
            [timestamp: buckets[9], value: 18.367],
            [timestamp: buckets[9] + seconds(10).toStandardDuration().millis, value: 19.01]
        ]]])
    assertEquals(200, response.status)
  }

  @Test
  void getPeriods() {
    DateTime end = now()
    DateTime start = end.minusMinutes(30)
    String tenantId = nextTenantId()
    String metric = "n1"

    def response = hawkularMetrics.post(path: "$tenantId/metrics/numeric/$metric/data", body: [
        [timestamp: start.millis, value: 22.3],
        [timestamp: start.plusMinutes(1).millis, value: 17.4],
        [timestamp: start.plusMinutes(2).millis, value: 16.6],
        [timestamp: start.plusMinutes(3).millis, value: 22.7],
        [timestamp: start.plusMinutes(4).millis, value: 23.3],
        [timestamp: start.plusMinutes(5).millis, value: 19.9],
        [timestamp: start.plusMinutes(6).millis, value: 21.2],
        [timestamp: start.plusMinutes(7).millis, value: 24.2],
        [timestamp: start.plusMinutes(8).millis, value: 26.6],
        [timestamp: start.plusMinutes(9).millis, value: 18.8],
        [timestamp: start.plusMinutes(10).millis, value: 20.0]
    ])
    assertEquals(200, response.status)

    def getPeriods = { operation, threshold ->
      def periodsResponse = hawkularMetrics.get(path: "$tenantId/metrics/numeric/$metric/periods",
          query: [threshold: threshold, op: operation])
      assertEquals(200, response.status)

      return periodsResponse
    }

    response = getPeriods("gt", 20)

    def expectedData = [
      [start.millis, start.millis],
      [start.plusMinutes(3).millis, start.plusMinutes(4).millis],
      [start.plusMinutes(6).millis, start.plusMinutes(8).millis]
    ]
    assertEquals(expectedData, response.data)

    response = getPeriods("lt", 20)

    expectedData = [
        [start.plusMinutes(1).millis, start.plusMinutes(2).millis],
        [start.plusMinutes(5).millis, start.plusMinutes(5).millis],
        [start.plusMinutes(9).millis, start.plusMinutes(9).millis]
    ]
    assertEquals(expectedData, response.data)

    response = getPeriods("gte", 20)

    expectedData = [
        [start.millis, start.millis],
        [start.plusMinutes(3).millis, start.plusMinutes(4).millis],
        [start.plusMinutes(6).millis, start.plusMinutes(8).millis],
        [start.plusMinutes(10).millis, start.plusMinutes(10).millis]
    ]
    assertEquals(expectedData, response.data)

    response = getPeriods("lte", 20)

    expectedData = [
        [start.plusMinutes(1).millis, start.plusMinutes(2).millis],
        [start.plusMinutes(5).millis, start.plusMinutes(5).millis],
        [start.plusMinutes(9).millis, start.plusMinutes(10).millis]
    ]
    assertEquals(expectedData, response.data)

    response = getPeriods("eq", 20)

    expectedData = [[start.plusMinutes(10).millis, start.plusMinutes(10).millis]]
    assertEquals(expectedData, response.data)

    response = getPeriods("neq", 20)

    expectedData = [[start.millis, start.plusMinutes(9).millis]]
    assertEquals(expectedData, response.data)

    badGet(path: "$tenantId/metrics/numeric/$metric/periods", query: [threshold: 20, op: "foo"], { exception ->
      assertEquals(400, exception.response.status)
    })

    response = hawkularMetrics.get(path: "$tenantId/metrics/numeric/$metric/periods", query: [threshold: 20, op: "gt",
        start: start.minusMinutes(10).millis, end: start.minusMinutes(5).millis])
    assertEquals(204, response.status)
  }

  @Test
  void insertNumericDataForMultipleMetrics() {
    DateTime start = now().minusMinutes(10)
    def tenantId = nextTenantId()

    def response = hawkularMetrics.post(path: 'tenants', body: [id: tenantId])
    assertEquals(201, response.status)
    assertEquals("http://$baseURI/tenants".toString(), response.getFirstHeader('location').value)

    // Let's explicitly create one of the metrics with some tags and a data retention
    // so that we can verify we get back that info along with the data.
    response = hawkularMetrics.post(path: "$tenantId/metrics/numeric", body: [
        id: 'm2',
        tags: [a: '1', b: '2'],
        dataRetention: 24
    ])
    assertEquals(201, response.status)
    assertEquals("http://$baseURI/$tenantId/metrics/numeric/m2".toString(), response.getFirstHeader('location').value)

    response = hawkularMetrics.post(path: "$tenantId/metrics/numeric/data", body: [
        [
            id: 'm1',
            data: [
                [timestamp: start.millis, value: 1.1],
                [timestamp: start.plusMinutes(1).millis, value: 1.2]
            ]
        ],
        [
            id: 'm2',
            data: [
                [timestamp: start.millis, value: 2.1],
                [timestamp: start.plusMinutes(1).millis, value: 2.2]
            ]
        ],
        [
            id: 'm3',
            data: [
                [timestamp: start.millis, value: 3.1],
                [timestamp: start.plusMinutes(1).millis, value: 3.2]
            ]
        ]
    ])
    assertEquals(200, response.status)

    response = hawkularMetrics.get(path: "${tenantId}/metrics/numeric/m2/data")
    assertEquals(200, response.status)
    assertEquals(
        [
            [timestamp: start.plusMinutes(1).millis, value: 2.2],
            [timestamp: start.millis, value: 2.1],
        ],
        response.data
    )
  }

  @Test
  void insertAvailabilityDataForMultipleMetrics() {
    DateTime start = now().minusMinutes(10)
    def tenantId = nextTenantId()

    def response = hawkularMetrics.post(path: 'tenants', body: [id: tenantId])
    assertEquals(201, response.status)
    assertEquals("http://$baseURI/tenants".toString(), response.getFirstHeader('location').value)

    // Let's explicitly create one of the metrics with some tags and a data retention
    // so that we can verify we get back that info along with the data.
    response = hawkularMetrics.post(path: "$tenantId/metrics/availability", body: [
        id         : 'm2',
        tags         : [a: '1', b: '2'],
        dataRetention: 12
    ])
    assertEquals(201, response.status)
    assertEquals(
        "http://$baseURI/$tenantId/metrics/availability/m2".toString(),
        response.getFirstHeader('location').value
    )

    response = hawkularMetrics.post(path: "${tenantId}/metrics/availability/data", body: [
        [
            id: 'm1',
            data: [
                [timestamp: start.millis, value: "down"],
                [timestamp: start.plusMinutes(1).millis, value: "up"]
            ]
        ],
        [
            id: 'm2',
            data: [
                [timestamp: start.millis, value: "up"],
                [timestamp: start.plusMinutes(1).millis, value: "up"]
            ]
        ],
        [
            id: 'm3',
            data: [
                [timestamp: start.millis, value: "down"],
                [timestamp: start.plusMinutes(1).millis, value: "down"]
            ]
        ]
    ])
    assertEquals(200, response.status)

    response = hawkularMetrics.get(path: "${tenantId}/metrics/availability/m2/data")
    assertEquals(200, response.status)
    assertEquals(
        [
         [timestamp: start.plusMinutes(1).millis, value: "up"],
         [timestamp: start.millis, value: "up"]
        ],
        response.data
    )
  }

  @Test
  void createMetricsAndUpdateTags() {
    String tenantId = nextTenantId()

    // Create a numeric metric
    def response = hawkularMetrics.post(path: "$tenantId/metrics/numeric", body: [
        id: 'N1',
        tags: [a1: 'A', b1: 'B']
    ])
    assertEquals(201, response.status)

    // Make sure we do not allow duplicates
    badPost(path: "$tenantId/metrics/numeric", body: [id: 'N1']) { exception ->
      assertEquals(409, exception.response.status)
      assertNotNull(exception.response.data['errorMsg'])
    }

    // Create a numeric metric that sets its data retention
    response = hawkularMetrics.post(path: "$tenantId/metrics/numeric", body: [
        id: 'N2',
        tags: [a2: '2', b2: 'B2'],
        dataRetention: 96
    ])
    assertEquals(201, response.status)

    // Create an availability metric
    response = hawkularMetrics.post(path: "$tenantId/metrics/availability", body: [
        id: 'A1',
        tags: [a2: '2', b2: '2']
    ])
    assertEquals(201, response.status)

    // Create an availability metric that sets its data retention
    response = hawkularMetrics.post(path: "$tenantId/metrics/availability", body: [
        id: 'A2',
        tags: [a22: '22', b22: '22'],
        dataRetention: 48
    ])
    assertEquals(201, response.status)

    // Make sure we do not allow duplicates
    badPost(path: "$tenantId/metrics/availability", body: [id: 'A1']) { exception ->
      assertEquals(409, exception.response.status)
      assertNotNull(exception.response.data['errorMsg'])
    }

    // Fetch numeric tags
    response = hawkularMetrics.get(path: "$tenantId/metrics/numeric/N1/tags")
    assertEquals(200, response.status)
    assertEquals(
        [
            tenantId: tenantId,
            id: 'N1',
            tags: [a1: 'A', b1: 'B']
        ],
        response.data
    )

    response = hawkularMetrics.get(path: "$tenantId/metrics/numeric/N2/tags")
    assertEquals(200, response.status)
    assertEquals(
        [
            tenantId: tenantId,
            id: 'N2',
            tags: [a2: '2', b2: 'B2'],
            dataRetention: 96
        ],
        response.data
    )

    // Verify the response for a non-existent metric
    response = hawkularMetrics.get(path: "$tenantId/metrics/numeric/N-doesNotExist/tags")
    assertEquals(204, response.status)

    // Fetch availability metric tags
    response = hawkularMetrics.get(path: "$tenantId/metrics/availability/A1/tags")
    assertEquals(200, response.status)
    assertEquals(
        [
            tenantId: tenantId,
            id: 'A1',
            tags: [a2: '2', b2: '2']
        ],
        response.data
    )

    response = hawkularMetrics.get(path: "$tenantId/metrics/availability/A2/tags")
    assertEquals(200, response.status)
    assertEquals(
        [
            tenantId: tenantId,
            id: 'A2',
            tags: [a22: '22', b22: '22'],
            dataRetention: 48
        ],
        response.data
    )

    // Verify the response for a non-existent metric
    response = hawkularMetrics.get(path: "$tenantId/metrics/numeric/A-doesNotExist/tags")
    assertEquals(204, response.status)

    // Update the numeric metric tags
    response = hawkularMetrics.put(path: "$tenantId/metrics/numeric/N1/tags", body: [a1: 'one', a2: '2', b1: 'B'])
    assertEquals(200, response.status)

    // Fetch the updated tags
    response = hawkularMetrics.get(path: "$tenantId/metrics/numeric/N1/tags")
    assertEquals(200, response.status)
    assertEquals(
        [
            tenantId: tenantId,
            id: 'N1',
            tags: [a1: 'one', a2: '2', b1: 'B']
        ],
        response.data
    )

    // Delete a numeric metric tag
    response = hawkularMetrics.delete(path: "$tenantId/metrics/numeric/N1/tags/a2:2,b1:B")
    assertEquals(200, response.status)
    response = hawkularMetrics.get(path: "$tenantId/metrics/numeric/N1/tags")
    assertEquals(
        [
            tenantId: tenantId,
            id: 'N1',
            tags: [a1: 'one']
        ],
        response.data
    )

    // Update the availability metric data
    response = hawkularMetrics.put(path: "$tenantId/metrics/availability/A1/tags", body: [a2: 'two', a3: 'THREE'])
    assertEquals(200, response.status)

    // Fetch the updated tags
    response = hawkularMetrics.get(path: "$tenantId/metrics/availability/A1/tags")
    assertEquals(200, response.status)
    assertEquals(
        [
            tenantId: tenantId,
            id: 'A1',
            tags: [a2: 'two', a3: 'THREE', b2: '2']
        ],
        response.data
    )

    // delete an availability metric tag
    response = hawkularMetrics.delete(path: "$tenantId/metrics/availability/A1/tags/a2:two,b2:2")
    assertEquals(200, response.status)

    response = hawkularMetrics.get(path: "$tenantId/metrics/availability/A1/tags")
    assertEquals(
        [
            tenantId: tenantId,
            id    : 'A1',
            tags    : [a3: 'THREE']
        ],
        response.data
    )
  }

  @Test
  void findMetrics() {
    DateTime start = now().minusMinutes(20)
    def tenantId = nextTenantId()

    // First create a couple numeric metrics by only inserting data
    def response = hawkularMetrics.post(path: "$tenantId/metrics/numeric/data", body: [
        [
            id: 'm11',
            data: [
                [timestamp: start.millis, value: 1.1],
                [timestamp: start.plusMinutes(1).millis, value: 1.2]
            ]
        ],
        [
            id: 'm12',
            data: [
                [timestamp: start.millis, value: 2.1],
                [timestamp: start.plusMinutes(1).millis, value: 2.2]
            ]
        ]
    ])
    assertEquals(200, response.status)

    // Explicitly create a numeric metric
    response = hawkularMetrics.post(path: "$tenantId/metrics/numeric", body: [
        id: 'm13',
        tags: [a1: 'A', B1: 'B'],
        dataRetention: 32
    ])
    assertEquals(201, response.status)

    // Now query for the numeric metrics
    response = hawkularMetrics.get(path: "$tenantId/metrics", query: [type: 'num'])
    assertEquals(200, response.status)
    assertEquals(
        [
            [tenantId: tenantId, id: 'm11'],
            [tenantId: tenantId, id: 'm12'],
            [tenantId: tenantId, id: 'm13', tags: [a1: 'A', B1: 'B'], dataRetention: 32]
        ],
        response.data
    )

    // Create a couple availability metrics by only inserting data
    response = hawkularMetrics.post(path: "$tenantId/metrics/availability/data", body: [
        [
            id: 'm14',
            data: [
                [timestamp: start.millis, value: 'up'],
                [timestamp: start.plusMinutes(1).millis, value: 'up']
            ]
        ],
        [
            id: 'm15',
            data: [
                [timestamp: start.millis, value: 'up'],
                [timestamp: start.plusMinutes(1).millis, value: 'down']
            ]
        ]
    ])
    assertEquals(200, response.status)

    // Explicitly create an availability metric
    response = hawkularMetrics.post(path: "$tenantId/metrics/availability", body: [
        id: 'm16',
        tags: [a10: '10', a11: '11'],
        dataRetention: 7
    ])
    assertEquals(201, response.status)

    // Query for the availability metrics
    response = hawkularMetrics.get(path: "$tenantId/metrics", query: [type: 'avail'])
    assertEquals(200, response.status)
    assertEquals(
        [
            [tenantId: tenantId, id: 'm14'],
            [tenantId: tenantId, id: 'm15'],
            [tenantId: tenantId, id: 'm16', tags: [a10: '10', a11: '11'], dataRetention: 7]
        ],
        response.data
    )
  }

  @Test
  void findNumericDataByTags() {
    DateTime start = now().minusMinutes(30)
    def tenantId = nextTenantId()

    def response = hawkularMetrics.post(path: "$tenantId/metrics/numeric/data", body: [
        [
            id: 'n1',
            data: [
                [timestamp: start.millis, value: 34.21],
                [timestamp: start.plusMinutes(5).millis, value: 35.19],
                [timestamp: start.plusMinutes(10).millis, value: 34.67],
                [timestamp: start.plusMinutes(15).millis, value: 32.89]
            ]
        ],
        [
            id: 'n2',
            data: [
                [timestamp: start.millis, value: 10.0],
                [timestamp: start.plusMinutes(2).millis, value: 24.0],
                [timestamp: start.plusMinutes(4).millis, value: 27.0],
                [timestamp: start.plusMinutes(6).millis, value: 39.0],
                [timestamp: start.plusMinutes(8).millis, value: 56.0],
                [timestamp: start.plusMinutes(10).millis, value: 62.0],
                [timestamp: start.plusMinutes(12).millis, value: 82.0],
            ]
        ],
        [
            id: 'n3',
            data: [
                [timestamp: start.millis, value: 259.11],
                [timestamp: start.plusMinutes(4).millis, value: 272.54],
                [timestamp: start.plusMinutes(8).millis, value: 266.08],
                [timestamp: start.plusMinutes(12).millis, value: 269.43]
            ]
        ],
        [
            id: 'n4',
            data: [
                [timestamp: start.plusMinutes(8).millis, value: 174],
                [timestamp: start.plusMinutes(9).millis, value: 181],
                [timestamp: start.plusMinutes(10).millis, value: 188],
                [timestamp: start.plusMinutes(11).millis, value: 203],
                [timestamp: start.plusMinutes(12).millis, value: 229],
            ]
        ]
    ])
    assertEquals(200, response.status)

    def tagData = { metric, tags ->
      response = hawkularMetrics.post(path: "$tenantId/metrics/numeric/$metric/tag", body: [
          start : start.plusMinutes(6).millis,
          end   : start.plusMinutes(10).millis,
          tags  : tags
      ])
      assertEquals(200, response.status)
    }

    tagData('n1', [t1: "1", t2: "2", t3: "3"])
    tagData('n2', [t2: "2", t3: "three", t4: "4"])
    tagData('n3', [t3: "3", t4: "4", t5: "five"])
    tagData('n4', [t3: "3", t4: "4"])

    response = hawkularMetrics.get(path: "$tenantId/tags/numeric/t3:3,t4:4")
    assertEquals(200, response.status)

    def expected = [
        n3: [
            [timestamp: start.plusMinutes(8).millis, value: 266.08]
        ],
        n4: [
            [timestamp: start.plusMinutes(8).millis, value: 174],
            [timestamp: start.plusMinutes(9).millis, value: 181]
        ]
    ]

    expected.n3.eachWithIndex { expectedDataPoint, i ->
      assertNumericDataPointEquals(expectedDataPoint, response.data.n3[i])
    }
    expected.n4.eachWithIndex { expectedDataPoint, i ->
      assertNumericDataPointEquals(expectedDataPoint, response.data.n4[i])
    }
  }

    void assertMetricsEquals(Map expected, Map actual) {
      expected.each { k, v ->
        assertNotNull("The metrics do not contain an entry for $k", actual[k])
      assertMetricEquals(v, actual[k])
    }
  }

  void assertMetricEquals(Map expected, Map actual) {
    assertEquals("The tenantId does not match", expected.tenantId, actual.tenantId)
    assertEquals("The metric name does not match", expected.id, actual.id)
    assertEquals("The number of data points does not match", expected.data.size, actual.data.size)
    expected.data.eachWithIndex { expectedDataPoint, i ->
      assertNumericDataPointEquals(expectedDataPoint, actual.data[i]) }
  }
}
