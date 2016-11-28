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

import static junit.framework.Assert.assertNull
import static org.joda.time.Duration.standardMinutes
import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertNotNull
import static org.junit.Assert.assertTrue

import org.hawkular.metrics.datetime.DateTimeService
import org.joda.time.DateTime
import org.junit.Ignore
import org.junit.Test

/**
 * @author Thomas Segismont
 */
class TenantITest extends RESTTest {
  def tenantId = nextTenantId()

  @Test
  void createAndReadTest() {
    def secondTenantId = nextTenantId()

    def response = hawkularMetrics.post(path: "tenants",
      headers: [
        (tenantHeaderName): tenantId,
        (adminTokenHeaderName): adminToken
      ],
      body: [
        id        : tenantId,
        retentions: [gauge: 45, availability: 30, counter: 13]
    ])
    assertEquals(201, response.status)
    assertEquals("http://$baseURI/tenants".toString(), response.getFirstHeader('location').value)

    response = hawkularMetrics.post(path: "tenants",
      headers: [
        (tenantHeaderName): tenantId,
        (adminTokenHeaderName): adminToken
      ],
      body: [
        id        : secondTenantId,
        retentions: [gauge: 13, availability: 45, counter: 30]
    ])
    assertEquals(201, response.status)
    assertEquals("http://$baseURI/tenants".toString(), response.getFirstHeader('location').value)

    response = hawkularMetrics.get(path: "tenants",
      headers: [
        (tenantHeaderName): tenantId,
        (adminTokenHeaderName): adminToken
      ]
    )
    def expectedData = [
        [
            id        : tenantId,
            retentions: [gauge: 45, availability: 30, counter: 13]
        ],
        [
            id        : secondTenantId,
            retentions: [gauge: 13, availability: 45, counter: 30]
        ]
    ]

    assertTrue("${expectedData} not in ${response.data}", response.data.containsAll((expectedData)))
  }

  @Test
  void duplicateTenantTest() {
    def response = hawkularMetrics.post(path: 'tenants',
      headers: [
        (tenantHeaderName): tenantId,
        (adminTokenHeaderName): adminToken
      ],
      body: [id: tenantId])
    assertEquals(201, response.status)
    assertEquals("http://$baseURI/tenants".toString(), response.getFirstHeader('location').value)

    badPost(path: 'tenants',
      headers: [
        (tenantHeaderName): tenantId,
        (adminTokenHeaderName): adminToken
      ],
      body: [id: tenantId]) { exception ->
      assertEquals(409, exception.response.status)
    }

    response = hawkularMetrics.post(path: 'tenants',
      headers: [
        (tenantHeaderName): tenantId,
        (adminTokenHeaderName): adminToken
      ],
      query: [overwrite: true],
      body: [
        id: tenantId,
        retentions: [gauge: 145, availability: 130, counter: 113]
    ])
    assertEquals(201, response.status)
    assertEquals("http://$baseURI/tenants".toString(), response.getFirstHeader('location').value)

    response = hawkularMetrics.get(path: "tenants",
      headers: [
        (tenantHeaderName): tenantId,
        (adminTokenHeaderName): adminToken
      ])
    def expectedData = [
        [
            id        : tenantId,
            retentions: [gauge: 145, availability: 130, counter: 113]
        ]
    ]

    assertTrue("${expectedData} not in ${response.data}", response.data.containsAll((expectedData)))
  }

  @Test
  void invalidPayloadTest() {
    badPost(path: 'tenants',
      headers: [
        (tenantHeaderName): tenantId,
        (adminTokenHeaderName): adminToken
      ],
      body: "" /* Empty body */) { exception ->
      assertEquals(400, exception.response.status)
      assertTrue(exception.response.data.containsKey("errorMsg"))
    }

    // Missing admin header
    badPost(path: 'tenants',
        headers: [
            (tenantHeaderName): tenantId,
        ],
        body: "" /* Empty body */) { exception ->
      assertEquals(400, exception.response.status)
      assertTrue(exception.response.data.containsKey("errorMsg"))
    }

    // Incorrect admin header
    badPost(path: 'tenants',
        headers: [
            (tenantHeaderName): tenantId,
            (adminTokenHeaderName): "mustard"
        ],
        body: "" /* Empty body */) { exception ->
      assertEquals(403, exception.response.status)
      assertTrue(exception.response.data.containsKey("errorMsg"))
    }
  }

  @Test
  @Ignore
  void createImplicitTenantWhenInsertingGaugeDataPoints() {
    DateTimeService dateTimeService = new DateTimeService()
    DateTime start = dateTimeService.getTimeSlice(getTime(), standardMinutes(1))

    def response = hawkularMetrics.post(
        path: "gauges/G1/raw",
        headers: [
          (tenantHeaderName): tenantId,
          (adminTokenHeaderName): adminToken
        ],
        body: [
            [timestamp: start.minusMinutes(1).millis, value: 3.14]
        ]
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.get(path: "clock/wait", query: [duration: "31mn"])
    assertEquals("There was an error waiting: $response.data", 200, response.status)

    response = hawkularMetrics.get(path: "tenants", headers: [(adminTokenHeaderName): adminToken])
    assertEquals(200, response.status)

    assertNotNull("tenantId = $tenantId, Response = $response.data", response.data.find { it.id == tenantId })
  }

  @Test
  @Ignore
  void createImplicitTenantWhenInsertingCounterDataPoints() {
    DateTimeService dateTimeService = new DateTimeService()
    DateTime start = dateTimeService.getTimeSlice(getTime(), standardMinutes(1))

    def response = hawkularMetrics.post(
        path: 'counters/C1/raw',
        headers: [
          (tenantHeaderName): tenantId,
          (adminTokenHeaderName): adminToken
        ],
        body: [
            [timestamp: start.minusMinutes(1).millis, value: 100]
        ]
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.get(path: 'clock/wait', query: [duration: '31mn'])
    assertEquals("There was an error waiting: $response.data", 200, response.status)

    response = hawkularMetrics.get(path: 'tenants',
      headers: [
        (tenantHeaderName): tenantId,
        (adminTokenHeaderName): adminToken
      ])

    assertEquals(200, response.status)

    assertNotNull("tenantId = $tenantId, Response = $response.data", response.data.find { it.id == tenantId })
  }

  @Test
  @Ignore
  void createImplicitTenantWhenInsertingAvailabilityDataPoints() {
    DateTimeService dateTimeService = new DateTimeService()
    DateTime start = dateTimeService.getTimeSlice(getTime(), standardMinutes(1))

    def response = hawkularMetrics.post(
        path: 'availability/A1/raw',
        headers: [
          (tenantHeaderName): tenantId,
          (adminTokenHeaderName): adminToken
        ],
        body: [
            [timestamp: start.minusMinutes(1).millis, value: 'up']
        ]
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.get(path: 'clock/wait', query: [duration: '31mn'])
    assertEquals("There was an error waiting: $response.data", 200, response.status)

    response = hawkularMetrics.get(path: 'tenants',
      headers: [
        (tenantHeaderName): tenantId,
        (adminTokenHeaderName): adminToken
      ])
    assertEquals(200, response.status)

    assertNotNull("tenantId = $tenantId, Response = $response.data", response.data.find { it.id == tenantId })
  }

  @Test
  void deleteTenantHavingNoMetrics() {
    def response = hawkularMetrics.post(path: "tenants",
      headers: [
        (tenantHeaderName): tenantId,
        (adminTokenHeaderName): adminToken
      ],
      body: [
        id: tenantId,
        retentions: [gauge: 45, availability: 30, counter: 13]
      ])
    assertEquals(201, response.status)

    response = hawkularMetrics.get(path:'scheduler/clock', headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)

    DateTime now = new DateTime(response.data.time as Long)

    response = hawkularMetrics.delete(path: "tenants/$tenantId",
      headers: [(tenantHeaderName): tenantId, (adminTokenHeaderName): adminToken])
    assertEquals(200, response.status)

    response = hawkularMetrics.put(
        path: 'scheduler/clock',
        headers: [(tenantHeaderName): tenantId],
        body: [time: now.plusMinutes(1).millis]
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.get(path: 'tenants',
      headers: [
        (tenantHeaderName): tenantId,
        (adminTokenHeaderName): adminToken
      ])
    assertEquals(200, response.status)
    assertNull(response.data.find { it.id == tenantId })
  }

  @Test
  void deleteTenantHavingMetrics() {
    def response = hawkularMetrics.get(path:'scheduler/clock', headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)

    DateTime start = new DateTime(response.data.time as Long)

    response = hawkularMetrics.post(
        path: "metrics/raw",
        headers: [(tenantHeaderName): tenantId],
        body: [
            gauges: [
                [
                    id: 'G1',
                    data:  [
                        [timestamp: start.millis, value: 10.032],
                        [timestamp: start.plusMinutes(1).millis, value: 9.589]
                    ],
                ],
                [
                    id: 'G2',
                    data: [
                        [timestamp: start.millis, value: 33.51],
                        [timestamp: start.plusMinutes(1).millis, value: 57.327]
                    ]
                ]
            ],
            counters: [
                [
                    id: 'C1',
                    data: [
                        [timestamp: start.millis, value: 10],
                        [timestamp: start.plusMinutes(1).millis, value: 20]
                    ]
                ],
                [
                    id: 'C2',
                    data: [
                        [timestamp: start.millis, value: 150],
                        [timestamp: start.plusMinutes(1).millis, value: 225],
                        [timestamp: start.plusMinutes(2).millis, value: 300]
                    ]
                ]
            ],
            availabilities: [
                [
                    id: 'A1',
                    data: [
                        [timestamp: start.millis, value: "down"],
                        [timestamp: start.plusMinutes(1).millis, value: "up"]
                    ]
                ],
                [
                    id: 'A2',
                    data: [
                        [timestamp: start.millis, value: "up"],
                        [timestamp: start.plusMinutes(1).millis, value: "up"]
                    ]
                ]
            ],
            strings: [
                [
                    id: 'S1',
                    data: [
                        [timestamp: start.millis, value: 'server accepting writes'],
                        [timestamp: start.plusMinutes(1).millis, value: 'server accepting reads']
                    ]
                ],
                [
                    id: 'S2',
                    data: [
                        [timestamp: start.millis, value: 'entering maintenance mode'],
                        [timestamp: start.plusMinutes(1).millis, value: 'rebuilding index']
                    ]
                ]
            ]
        ]
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.delete(path: "tenants/$tenantId",
      headers: [(tenantHeaderName): tenantId, (adminTokenHeaderName): adminToken])
    assertEquals(200, response.status)

    response = hawkularMetrics.put(
        path: 'scheduler/clock',
        headers: [(tenantHeaderName): tenantId],
        body: [time: start.plusMinutes(1).millis]
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.post(
        path: "gauges/raw/query",
        headers: [(tenantHeaderName): tenantId],
        body: [ids: ['G1', 'G2'], start: start.millis, end: start.plusMinutes(2).millis]
    )
    assertEquals(204, response.status)

    response = hawkularMetrics.post(
        path: "counters/raw/query",
        headers: [(tenantHeaderName): tenantId],
        body: [ids: ['C1', 'C2'], start: start.millis, end: start.plusMinutes(2).millis]
    )
    assertEquals(204, response.status)

    response = hawkularMetrics.post(
        path: "availability/raw/query",
        headers: [(tenantHeaderName): tenantId],
        body: [ids: ['A1', 'A2'], start: start.millis, end: start.plusMinutes(2).millis]
    )
    assertEquals(204, response.status)

    response = hawkularMetrics.post(
        path: "strings/raw/query",
        headers: [(tenantHeaderName): tenantId],
        body: [ids: ['S1', 'S2'], start: start.millis, end: start.plusMinutes(2).millis]
    )
    assertEquals(204, response.status)

    ['gauge', 'counter', 'availability', 'string'].each { type ->
      response = hawkularMetrics.get(path: "metrics", query: [type: type],
        headers: [(tenantHeaderName): tenantId, (adminTokenHeaderName): adminToken])
      assertEquals(204, response.status)
    }

    response = hawkularMetrics.get(path: 'tenants',
      headers: [(tenantHeaderName): tenantId, (adminTokenHeaderName): adminToken])
    assertEquals(200, response.status)
    assertNull(response.data.find { it.id == tenantId })
  }

  @Test
  void deleteNonexistentTenant() {
    def response = hawkularMetrics.get(path:'scheduler/clock', headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)

    DateTime start = new DateTime(response.data.time as Long)

    response = hawkularMetrics.delete(path: "tenants/$tenantId",
      headers: [(tenantHeaderName): tenantId, (adminTokenHeaderName): adminToken])
    assertEquals(200, response.status)

    response = hawkularMetrics.put(
        path: 'scheduler/clock',
        headers: [(tenantHeaderName): tenantId],
        body: [time: start.plusMinutes(1).millis]
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.get(path: 'tenants',
      headers: [(tenantHeaderName): tenantId, (adminTokenHeaderName): adminToken])
    assertEquals(200, response.status)
    assertNull(response.data.find { it.id == tenantId })
  }

  @Test
  void deleteTenantTwiceConcurrently() {
    def response = hawkularMetrics.get(path:'scheduler/clock', headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)

    DateTime start = new DateTime(response.data.time as Long)

    response = hawkularMetrics.post(
        path: "metrics/raw",
        headers: [(tenantHeaderName): tenantId],
        body: [
            gauges: [
                [
                    id: 'G1',
                    data:  [
                        [timestamp: start.millis, value: 10.032],
                        [timestamp: start.plusMinutes(1).millis, value: 9.589]
                    ],
                ],
                [
                    id: 'G2',
                    data: [
                        [timestamp: start.millis, value: 33.51],
                        [timestamp: start.plusMinutes(1).millis, value: 57.327]
                    ]
                ]
            ],
            counters: [
                [
                    id: 'C1',
                    data: [
                        [timestamp: start.millis, value: 10],
                        [timestamp: start.plusMinutes(1).millis, value: 20]
                    ]
                ],
                [
                    id: 'C2',
                    data: [
                        [timestamp: start.millis, value: 150],
                        [timestamp: start.plusMinutes(1).millis, value: 225],
                        [timestamp: start.plusMinutes(2).millis, value: 300]
                    ]
                ]
            ],
            availabilities: [
                [
                    id: 'A1',
                    data: [
                        [timestamp: start.millis, value: "down"],
                        [timestamp: start.plusMinutes(1).millis, value: "up"]
                    ]
                ],
                [
                    id: 'A2',
                    data: [
                        [timestamp: start.millis, value: "up"],
                        [timestamp: start.plusMinutes(1).millis, value: "up"]
                    ]
                ]
            ],
            strings: [
                [
                    id: 'S1',
                    data: [
                        [timestamp: start.millis, value: 'server accepting writes'],
                        [timestamp: start.plusMinutes(1).millis, value: 'server accepting reads']
                    ]
                ],
                [
                    id: 'S2',
                    data: [
                        [timestamp: start.millis, value: 'entering maintenance mode'],
                        [timestamp: start.plusMinutes(1).millis, value: 'rebuilding index']
                    ]
                ]
            ]
        ]
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.delete(path: "tenants/$tenantId",
      headers: [(tenantHeaderName): tenantId, (adminTokenHeaderName): adminToken])
    assertEquals(200, response.status)

    response = hawkularMetrics.delete(path: "tenants/$tenantId",
      headers: [(tenantHeaderName): tenantId, (adminTokenHeaderName): adminToken])
    assertEquals(200, response.status)

    response = hawkularMetrics.put(
        path: 'scheduler/clock',
        headers: [(tenantHeaderName): tenantId],
        body: [time: start.plusMinutes(1).millis]
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.post(
        path: "gauges/raw/query",
        headers: [(tenantHeaderName): tenantId],
        body: [ids: ['G1', 'G2'], start: start.millis, end: start.plusMinutes(2).millis]
    )
    assertEquals(204, response.status)

    response = hawkularMetrics.post(
        path: "counters/raw/query",
        headers: [(tenantHeaderName): tenantId],
        body: [ids: ['C1', 'C2'], start: start.millis, end: start.plusMinutes(2).millis]
    )
    assertEquals(204, response.status)

    response = hawkularMetrics.post(
        path: "availability/raw/query",
        headers: [(tenantHeaderName): tenantId],
        body: [ids: ['A1', 'A2'], start: start.millis, end: start.plusMinutes(2).millis]
    )
    assertEquals(204, response.status)

    response = hawkularMetrics.post(
        path: "strings/raw/query",
        headers: [(tenantHeaderName): tenantId],
        body: [ids: ['S1', 'S2'], start: start.millis, end: start.plusMinutes(2).millis]
    )
    assertEquals(204, response.status)

    ['gauge', 'counter', 'availability', 'string'].each { type ->
      response = hawkularMetrics.get(path: "metrics", query: [type: type], headers: [(tenantHeaderName): tenantId])
      assertEquals(204, response.status)
    }

    response = hawkularMetrics.get(path: 'tenants',
      headers: [(tenantHeaderName): tenantId, (adminTokenHeaderName): adminToken])
    assertEquals(200, response.status)
    assertNull(response.data.find { it.id == tenantId })
  }
}
