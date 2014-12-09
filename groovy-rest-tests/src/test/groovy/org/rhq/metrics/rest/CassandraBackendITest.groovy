package org.rhq.metrics.rest

import org.joda.time.DateTime
import org.junit.Test

import static junit.framework.Assert.fail
import static junit.framework.TestCase.assertEquals
import static org.joda.time.DateTime.now

class CassandraBackendITest extends RESTTest {

//  Session session;
//  MetricsServiceCassandra metricsServer;
//  String keyspace = System.getProperty("keyspace") ?: "rhq_metrics_rest_tests"

  @Test
  void insertNumericDataForMultipleMetrics() {
    DateTime start = now().minusMinutes(10)
    def tenantId = 'tenant-1'

    def response = rhqm.post(path: 'tenants', body: [id: tenantId])
    assertEquals(response.status, 200)

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
    assertEquals(response.status, 200)

    response = rhqm.get(path: "${tenantId}/metrics/numeric/m2/data")
    assertEquals(response.status, 200)
    assertEquals(response.data,
        [
          tenantId: 'tenant-1',
          name: 'm2',
          data: [
            [timestamp: start.plusMinutes(1).millis, value: 2.2],
            [timestamp: start.millis, value: 2.1],
          ]
        ]
    )
  }

  @Test
  void insertAvailabilityDataForMultipleMetrics() {
    DateTime start = now().minusMinutes(10)
    def tenantId = 'tenant-2'

    def response = rhqm.post(path: 'tenants', body: [id: tenantId])
    assertEquals(response.status, 200)

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
    assertEquals(response.status, 200)

    response = rhqm.get(path: "${tenantId}/metrics/availability/m2/data")
    assertEquals(response.status, 200)
    assertEquals(response.data,
        [
          tenantId: 'tenant-2',
          name: 'm2',
          data: [
            [timestamp: start.plusMinutes(1).millis, value: "up"],
            [timestamp: start.millis, value: "up"]
          ]
        ]
    )
  }

  @Test
  void createMetricsAndUpdateMetadata() {
    // Create a numeric metric
    def response = rhqm.post(path: "tenant-3/metrics/numeric", body: [
        name: 'N1',
        metadata: [a1: 'A', b1: 'B']
    ])
    assertEquals(response.status, 200)

    // Make sure we do not allow duplicates
    badPost(path: "tenant-3/metrics/numeric", body: [name: 'N1']) { exception ->
      assertEquals(exception.response.status, 400)
    }

    // Create an availability metric
    response = rhqm.post(path: "tenant-3/metrics/availability", body: [
        name: 'A1',
        metadata: [a2: '2', b2: '2']
    ])
    assertEquals(response.status, 200)

    // Make sure we do not allow duplicates
    badPost(path: "tenant-3/metrics/availability", body: [name: 'A1']) { exception ->
      assertEquals(exception.response.status, 400)
    }

    // Fetch numeric meta data
    response = rhqm.get(path: "tenant-3/metrics/numeric/N1/meta")
    assertEquals(response.status, 200)
    assertEquals(response.data,
        [
          tenantId: 'tenant-3',
          name: 'N1',
          metadata: [a1: 'A', b1: 'B']
        ]
    )

    // Verify the response for a non-existent metric
    response = rhqm.get(path: "tenant-3/metrics/numeric/N2/meta")
    assertEquals(response.status, 204)

    // Fetch availability metric meta data
    response = rhqm.get(path: "tenant-3/metrics/availability/A1/meta")
    assertEquals(response.status, 200)
    assertEquals(response.data,
        [
          tenantId: 'tenant-3',
          name: 'A1',
          metadata: [a2: '2', b2: '2']
        ]
    )

    // Verify the response for a non-existent metric
    response = rhqm.get(path: "tenant-3/metrics/numeric/A2/meta")
    assertEquals(response.status, 204)

    String delete = '[delete]'

    // Update the numeric metric meta data
    response = rhqm.put(path: "tenant-3/metrics/numeric/N1/meta", body: [a1: 'one', a2: '2', '[delete]': ['b1']])
    assertEquals(response.status, 200)

    // Fetch the updated meta data
    response = rhqm.get(path: "tenant-3/metrics/numeric/N1/meta")
    assertEquals(response.status, 200)
    assertEquals(response.data, [
        tenantId: 'tenant-3',
        name: 'N1',
        metadata: [a1: 'one', a2: '2']
    ])

    // Update the availability metric data
    response = rhqm.put(path: "tenant-3/metrics/availability/A1/meta", body: [a2: 'two', a3: 'THREE', '[delete]': ['b2']])
    assertEquals(response.status, 200)

    // Fetch the updated meta data
    response = rhqm.get(path: "tenant-3/metrics/availability/A1/meta")
    assertEquals(response.status, 200)
    assertEquals(response.data, [
        tenantId: 'tenant-3',
        name: 'A1',
        metadata: [a2: 'two', a3: 'THREE']
    ])
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
    assertEquals(response.status, 200)

    // Explicitly create a numeric metric
    response = rhqm.post(path: "$tenantId/metrics/numeric", body: [
        name: 'm13',
        metadata: [a1: 'A', B1: 'B']
    ])
    assertEquals(response.status, 200)

    // Now query for the numeric metrics
    response = rhqm.get(path: "$tenantId/metrics", query: [type: 'num'])
    assertEquals(response.status, 200)
    assertEquals(response.data, [
        [tenantId: 'tenant-4', name: 'm11'],
        [tenantId: 'tenant-4', name: 'm12'],
        [tenantId: 'tenant-4', name: 'm13', metadata: [a1: 'A', B1: 'B']]
    ])

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
    assertEquals(response.status, 200)

    // Explicitly create an availability metric
    response = rhqm.post(path: "$tenantId/metrics/availability", body: [
        name: 'm16',
        metadata: [a10: '10', a11: '11']
    ])
    assertEquals(response.status, 200)

    // Query for the availability metrics
    response = rhqm.get(path: "$tenantId/metrics", query: [type: 'avail'])
    assertEquals(response.status, 200)
    assertEquals(response.data, [
        [tenantId: 'tenant-4', name: 'm14'],
        [tenantId: 'tenant-4', name: 'm15'],
        [tenantId: 'tenant-4', name: 'm16', metadata: [a10: '10', a11: '11']]
    ])
  }

  def badPost(args, errorHandler) {
    try {
      return rhqm.post(args)
      fail("Expected exception to be thrown")
    } catch (e) {
      errorHandler(e)
    }
  }

}
