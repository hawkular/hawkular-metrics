package org.rhq.metrics.rest

import org.joda.time.DateTime
import org.junit.Test

import static org.joda.time.DateTime.now
import static org.junit.Assert.assertEquals
import static org.junit.Assert.fail

class CassandraBackendITest extends RESTTest {

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
