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
package org.rhq.metrics.rest

import org.joda.time.DateTime
import org.joda.time.Seconds
import org.junit.Test
import org.rhq.metrics.impl.DateTimeService

import static org.joda.time.DateTime.now
import static org.joda.time.Seconds.seconds
import static org.junit.Assert.assertEquals
import static org.junit.Assert.fail

class CassandraBackendITest extends RESTTest {

  static final double DELTA = 0.001

  @Test
  void queryForBucketedNumericData() {
    DateTimeService dateTimeService = new DateTimeService()
    String tenantId = 'tenant-0'
    String metric = 'n1'
    DateTime start = dateTimeService.currentHour().minusHours(1)
    DateTime end = start.plusHours(1)

    int numBuckets = 10
    long bucketSize = (end.millis - start.millis) / numBuckets
    def buckets = []
    numBuckets.times { buckets.add(start.millis + (it * bucketSize))}

    def response = rhqm.post(path: "$tenantId/metrics/numeric/$metric/data", body: [
        [timestamp: buckets[0], value: 12.22],
        [timestamp: buckets[0] + seconds(10).toStandardDuration().millis, value: 12.37],
        [timestamp: buckets[4], value: 25],
        [timestamp: buckets[4] + seconds(15).toStandardDuration().millis, value: 25],
        [timestamp: buckets[9], value: 18.367],
        [timestamp: buckets[9] + seconds(10).toStandardDuration().millis, value: 19.01]
    ])
    assertEquals(200, response.status)

    response = rhqm.get(path: "${tenantId}/metrics/numeric/$metric/data", query: [start: start.millis, end: end.millis,
        buckets: 10, bucketWidthSeconds: 0])
    assertEquals(200, response.status)

    def expectedData = [
      tenantId: tenantId,
      name: metric,
      data: [
        [timestamp: buckets[0], empty: false, max: 12.37, min: 12.22, avg: (12.22 + 12.37) / 2, value: 0, id: metric],
        [timestamp: buckets[1], empty: true, max: Double.NaN, min: Double.NaN, avg: Double.NaN, value: 0, id: metric],
        [timestamp: buckets[2], empty: true, max: Double.NaN, min: Double.NaN, avg: Double.NaN, value: 0, id: metric],
        [timestamp: buckets[3], empty: true, max: Double.NaN, min: Double.NaN, avg: Double.NaN, value: 0, id: metric],
        [timestamp: buckets[4], empty: false, max: 25.0, min: 25.0, avg: 25.0, value: 0, id: metric],
        [timestamp: buckets[5], empty: true, max: Double.NaN, min: Double.NaN, avg: Double.NaN, value: 0, id: metric],
        [timestamp: buckets[6], empty: true, max: Double.NaN, min: Double.NaN, avg: Double.NaN, value: 0, id: metric],
        [timestamp: buckets[7], empty: true, max: Double.NaN, min: Double.NaN, avg: Double.NaN, value: 0, id: metric],
        [timestamp: buckets[8], empty: true, max: Double.NaN, min: Double.NaN, avg: Double.NaN, value: 0, id: metric],
        [timestamp: buckets[9], empty: false, max: 19.01, min: 18.367, avg: (18.367 + 19.01) / 2, value: 0, id: metric],
      ]
    ]

    def assertBucketEquals = { expected, actual ->
      assertEquals(expected.timestamp, actual.timestamp)
      assertEquals(expected.empty, actual.empty)
      assertEquals(expected.id, actual.id)
      assertDoubleEquals(expected.max, actual.max)
      assertDoubleEquals(expected.min, actual.min)
      assertDoubleEquals(expected.avg, actual.avg)
    }

    assertBucketedDataEquals(expectedData, response.data, assertBucketEquals)

    response = rhqm.get(path: "$tenantId/metrics/numeric/$metric/data", query: [start: start.millis, end: end.millis,
        buckets: 10, bucketWidthSeconds: 0, skipEmpty: true])
    assertEquals(200, response.status)

    expectedData = [
      tenantId: tenantId,
      name: metric,
      data: [
        [timestamp: buckets[0], empty: false, max: 12.37, min: 12.22, avg: (12.22 + 12.37) / 2, value: 0, id: metric],
        [timestamp: buckets[4], empty: false, max: 25.0, min: 25.0, avg: 25.0, value: 0, id: metric],
        [timestamp: buckets[9], empty: false, max: 19.01, min: 18.367, avg: (18.367 + 19.01) / 2, value: 0, id: metric],
      ]
    ]

    assertBucketedDataEquals(expectedData, response.data, assertBucketEquals)
  }

  void assertBucketedDataEquals(def expected, def actual, Closure verifyBucket) {
    // When I first wrote this method, I was thinking that different verifications on the
    // buckets might be performed based on the query parameters used, hence the
    // verifyBucket argument. If different verifications are not needed, then we should
    // just put the additional verification code directly in this method.

    assertEquals(expected.tenantId, actual.tenantId)
    assertEquals(expected.name, actual.name)
    assertEquals('The number of bucketed data points is wrong', expected.size(), actual.size())
    expected.size().times { verifyBucket(expected.data[it], actual.data[it]) }
  }

  void assertDoubleEquals(expected, actual) {
    // If a bucket does not contain any data points, then the server returns
    // Double.NaN for max/min/avg. NaN is returned on the client and parsed as
    // a string. If the bucket contains data points, then the returned values
    // will be numeric.

    if (actual instanceof String) {
      assertEquals((Double) expected, Double.parseDouble(actual), DELTA)
    } else {
      assertEquals((Double) expected, (Double) actual, DELTA)
    }
  }

  @Test
  void insertNumericDataForMultipleMetrics() {
    DateTime start = now().minusMinutes(10)
    def tenantId = 'tenant-1'

    def response = rhqm.post(path: 'tenants', body: [id: tenantId])
    assertEquals(200, response.status)

    // Let's explicitly create one of the metrics with some meta data and a data retention
    // so that we can verify we get back that info along with the data.
    response = rhqm.post(path: "$tenantId/metrics/numeric", body: [
        name: 'm2',
        metadata: [a: '1', b: '2'],
        dataRetention: 24
    ])
    assertEquals(200, response.status)

    response = rhqm.post(path: "${tenantId}/metrics/numeric/data", body: [
        [
          name: 'm1',
          data: [
            [timestamp: start.millis, value: 1.1],
            [timestamp: start.plusMinutes(1).millis, value: 1.2]
          ]
        ],
        [
          name: 'm2',
          data: [
            [timestamp: start.millis, value: 2.1],
            [timestamp: start.plusMinutes(1).millis, value: 2.2]
          ]
        ],
        [
          name: 'm3',
          data: [
            [timestamp: start.millis, value: 3.1],
            [timestamp: start.plusMinutes(1).millis, value: 3.2]
          ]
        ]
    ])
    assertEquals(200, response.status)

    response = rhqm.get(path: "${tenantId}/metrics/numeric/m2/data")
    assertEquals(200, response.status)
    assertEquals(
        [
          tenantId: 'tenant-1',
          name: 'm2',
          metadata: [a: '1', b: '2'],
          dataRetention: 24,
          data: [
            [timestamp: start.plusMinutes(1).millis, value: 2.2],
            [timestamp: start.millis, value: 2.1],
          ]
        ],
        response.data
    )
  }

  @Test
  void insertAvailabilityDataForMultipleMetrics() {
    DateTime start = now().minusMinutes(10)
    def tenantId = 'tenant-2'

    def response = rhqm.post(path: 'tenants', body: [id: tenantId])
    assertEquals(200, response.status)

    // Let's explicitly create one of the metrics with some meta data and a data retention
    // so that we can verify we get back that info along with the data.
    response = rhqm.post(path: "$tenantId/metrics/availability", body: [
            name: 'm2',
            metadata: [a: '1', b: '2'],
            dataRetention: 12
    ])
    assertEquals(200, response.status)

    response = rhqm.post(path: "${tenantId}/metrics/availability/data", body: [
        [
          name: 'm1',
          data: [
            [timestamp: start.millis, value: "down"],
            [timestamp: start.plusMinutes(1).millis, value: "up"]
          ]
        ],
        [
          name: 'm2',
          data: [
            [timestamp: start.millis, value: "up"],
            [timestamp: start.plusMinutes(1).millis, value: "up"]
          ]
        ],
        [
          name: 'm3',
          data: [
            [timestamp: start.millis, value: "down"],
            [timestamp: start.plusMinutes(1).millis, value: "down"]
          ]
        ]
    ])
    assertEquals(200, response.status)

    response = rhqm.get(path: "${tenantId}/metrics/availability/m2/data")
    assertEquals(200, response.status)
    assertEquals(
        [
          tenantId: 'tenant-2',
          name: 'm2',
          metadata: [a: '1', b: '2'],
          dataRetention: 12,
          data: [
            [timestamp: start.plusMinutes(1).millis, value: "up"],
            [timestamp: start.millis, value: "up"]
          ]
        ],
        response.data
    )
  }

  @Test
  void createMetricsAndUpdateMetadata() {
    // Create a numeric metric
    def response = rhqm.post(path: "tenant-3/metrics/numeric", body: [
        name: 'N1',
        metadata: [a1: 'A', b1: 'B']
    ])
    assertEquals(200, response.status)

    // Make sure we do not allow duplicates
    badPost(path: "tenant-3/metrics/numeric", body: [name: 'N1']) { exception ->
      assertEquals(400, exception.response.status)
    }

    // Create a numeric metric that sets its data retention
    response = rhqm.post(path: 'tenant-3/metrics/numeric', body: [
        name: 'N2',
        metadata: [a2: '2', b2: 'B2'],
        dataRetention: 96
    ])
    assertEquals(200, response.status)

    // Create an availability metric
    response = rhqm.post(path: "tenant-3/metrics/availability", body: [
        name: 'A1',
        metadata: [a2: '2', b2: '2']
    ])
    assertEquals(200, response.status)

    // Create an availability metric that sets its data retention
    response = rhqm.post(path: "tenant-3/metrics/availability", body: [
        name: 'A2',
        metadata: [a22: '22', b22: '22'],
        dataRetention: 48
    ])
    assertEquals(200, response.status)

    // Make sure we do not allow duplicates
    badPost(path: "tenant-3/metrics/availability", body: [name: 'A1']) { exception ->
      assertEquals(400, exception.response.status)
    }

    // Fetch numeric meta data
    response = rhqm.get(path: "tenant-3/metrics/numeric/N1/meta")
    assertEquals(200, response.status)
    assertEquals(
        [
          tenantId: 'tenant-3',
          name: 'N1',
          metadata: [a1: 'A', b1: 'B']
        ],
        response.data
    )

    response = rhqm.get(path: 'tenant-3/metrics/numeric/N2/meta')
    assertEquals(200, response.status)
    assertEquals(
        [
          tenantId: 'tenant-3',
          name: 'N2',
          metadata: [a2: '2', b2: 'B2'],
          dataRetention: 96
        ],
        response.data
    )

    // Verify the response for a non-existent metric
    response = rhqm.get(path: "tenant-3/metrics/numeric/N-doesNotExist/meta")
    assertEquals(204, response.status)

    // Fetch availability metric meta data
    response = rhqm.get(path: "tenant-3/metrics/availability/A1/meta")
    assertEquals(200, response.status)
    assertEquals(
        [
          tenantId: 'tenant-3',
          name: 'A1',
          metadata: [a2: '2', b2: '2']
        ],
        response.data
    )

    response = rhqm.get(path: "tenant-3/metrics/availability/A2/meta")
    assertEquals(200, response.status)
    assertEquals(
        [
          tenantId: 'tenant-3',
          name: 'A2',
          metadata: [a22: '22', b22: '22'],
          dataRetention: 48
        ],
        response.data
    )

    // Verify the response for a non-existent metric
    response = rhqm.get(path: "tenant-3/metrics/numeric/A-doesNotExist/meta")
    assertEquals(204, response.status)

    // Update the numeric metric meta data
    response = rhqm.put(path: "tenant-3/metrics/numeric/N1/meta", body: [a1: 'one', a2: '2', '[delete]': ['b1']])
    assertEquals(200, response.status)

    // Fetch the updated meta data
    response = rhqm.get(path: "tenant-3/metrics/numeric/N1/meta")
    assertEquals(200, response.status)
    assertEquals(
        [
          tenantId: 'tenant-3',
          name: 'N1',
          metadata: [a1: 'one', a2: '2']
        ],
        response.data
    )

    // Update the availability metric data
    response = rhqm.put(path: "tenant-3/metrics/availability/A1/meta", body: [a2: 'two', a3: 'THREE', '[delete]': ['b2']])
    assertEquals(200, response.status)

    // Fetch the updated meta data
    response = rhqm.get(path: "tenant-3/metrics/availability/A1/meta")
    assertEquals(200, response.status)
    assertEquals(
        [
          tenantId: 'tenant-3',
          name: 'A1',
          metadata: [a2: 'two', a3: 'THREE']
        ],
        response.data
    )
  }

  @Test
  void findMetrics() {
    DateTime start = now().minusMinutes(20)
    def tenantId = 'tenant-4'

    // First create a couple numeric metrics by only inserting data
    def response = rhqm.post(path: "$tenantId/metrics/numeric/data", body: [
        [
          name: 'm11',
          data: [
            [timestamp: start.millis, value: 1.1],
            [timestamp: start.plusMinutes(1).millis, value: 1.2]
          ]
        ],
        [
          name: 'm12',
          data: [
            [timestamp: start.millis, value: 2.1],
            [timestamp: start.plusMinutes(1).millis, value: 2.2]
          ]
        ]
    ])
    assertEquals(200, response.status)

    // Explicitly create a numeric metric
    response = rhqm.post(path: "$tenantId/metrics/numeric", body: [
        name: 'm13',
        metadata: [a1: 'A', B1: 'B'],
        dataRetention: 32
    ])
    assertEquals(200, response.status)

    // Now query for the numeric metrics
    response = rhqm.get(path: "$tenantId/metrics", query: [type: 'num'])
    assertEquals(200, response.status)
    assertEquals(
        [
          [tenantId: 'tenant-4', name: 'm11'],
          [tenantId: 'tenant-4', name: 'm12'],
          [tenantId: 'tenant-4', name: 'm13', metadata: [a1: 'A', B1: 'B'], dataRetention: 32]
        ],
        response.data
    )

    // Create a couple availability metrics by only inserting data
    response = rhqm.post(path: "$tenantId/metrics/availability/data", body: [
        [
          name: 'm14',
          data: [
            [timestamp: start.millis, value: 'up'],
            [timestamp: start.plusMinutes(1).millis, value: 'up']
                    ]
        ],
        [
          name: 'm15',
          data: [
            [timestamp: start.millis, value: 'up'],
            [timestamp: start.plusMinutes(1).millis, value: 'down']
          ]
        ]
    ])
    assertEquals(200, response.status)

    // Explicitly create an availability metric
    response = rhqm.post(path: "$tenantId/metrics/availability", body: [
        name: 'm16',
        metadata: [a10: '10', a11: '11'],
        dataRetention: 7
    ])
    assertEquals(200, response.status)

    // Query for the availability metrics
    response = rhqm.get(path: "$tenantId/metrics", query: [type: 'avail'])
    assertEquals(200, response.status)
    assertEquals(
        [
          [tenantId: 'tenant-4', name: 'm14'],
          [tenantId: 'tenant-4', name: 'm15'],
          [tenantId: 'tenant-4', name: 'm16', metadata: [a10: '10', a11: '11'], dataRetention: 7]
        ],
        response.data
    )
  }

  static def badPost(args, errorHandler) {
    try {
      def object = rhqm.post(args)
      fail("Expected exception to be thrown")
      return object
    } catch (e) {
      errorHandler(e)
    }
  }

}
