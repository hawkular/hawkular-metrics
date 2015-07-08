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

import org.joda.time.DateTime
import org.junit.Test

import static org.junit.Assert.assertEquals

/**
 * @author jsanda
 */
class MetricsITest extends RESTTest {

  @Test
  void addMixedData() {
    String tenantId = nextTenantId()
    DateTime start = DateTime.now().minusMinutes(10)

    def response = hawkularMetrics.post(
        path: "metrics/data",
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
            ]
        ]
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.get(path: 'gauges/G1/data', headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertEquals(
        [
            [timestamp: start.plusMinutes(1).millis, value: 9.589],
            [timestamp: start.millis, value: 10.032]
        ],
        response.data
    )

    response = hawkularMetrics.get(path: 'gauges/G2/data', headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertEquals(
        [
            [timestamp: start.plusMinutes(1).millis, value: 57.327],
            [timestamp: start.millis, value: 33.51],
        ],
        response.data
    )

    response = hawkularMetrics.get(path: 'counters/C1/data', headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertEquals(
        [
            [timestamp: start.plusMinutes(1).millis, value: 20],
            [timestamp: start.millis, value: 10]
        ],
        response.data
    )

    response = hawkularMetrics.get(path: 'counters/C2/data', headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertEquals(
        [
            [timestamp: start.plusMinutes(2).millis, value: 300],
            [timestamp: start.plusMinutes(1).millis, value: 225],
            [timestamp: start.millis, value: 150]
        ],
        response.data
    )

    response = hawkularMetrics.get(path: 'availability/A1/data', headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertEquals(
        [
            [timestamp: start.millis, value: "down"],
            [timestamp: start.plusMinutes(1).millis, value: "up"]
        ],
        response.data
    )

    response = hawkularMetrics.get(path: 'availability/A2/data', headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertEquals(
        [
            [timestamp: start.millis, value: "up"],
            [timestamp: start.plusMinutes(1).millis, value: "up"]
        ],
        response.data
    )
  }

  @Test
  void addMixedDataMissingGauges() {
    String tenantId = nextTenantId()
    DateTime start = DateTime.now().minusMinutes(10)

    def response = hawkularMetrics.post(
        path: "metrics/data",
        headers: [(tenantHeaderName): tenantId],
        body: [
            counters: [
                [
                    id: 'GC1',
                    data: [
                        [timestamp: start.millis, value: 10],
                        [timestamp: start.plusMinutes(1).millis, value: 20]
                    ]
                ]
            ],
            availabilities: [
                [
                    id: 'GA1',
                    data: [
                        [timestamp: start.millis, value: "down"],
                        [timestamp: start.plusMinutes(1).millis, value: "up"]
                    ]
                ]
            ]
        ]
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.get(path: 'counters/GC1/data', headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertEquals(
        [
            [timestamp: start.plusMinutes(1).millis, value: 20],
            [timestamp: start.millis, value: 10]
        ],
        response.data
    )

    response = hawkularMetrics.get(path: 'availability/GA1/data', headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertEquals(
        [
            [timestamp: start.millis, value: "down"],
            [timestamp: start.plusMinutes(1).millis, value: "up"]
        ],
        response.data
    )
  }

  @Test
  void addMixedDataMissingCounters() {
    String tenantId = nextTenantId()
    DateTime start = DateTime.now().minusMinutes(10)

    def response = hawkularMetrics.post(
        path: "metrics/data",
        headers: [(tenantHeaderName): tenantId],
        body: [
            gauges: [
                [
                    id: 'CG1',
                    data:  [
                        [timestamp: start.millis, value: 10.032],
                        [timestamp: start.plusMinutes(1).millis, value: 9.589]
                    ],
                ]
            ],
            availabilities: [
                [
                    id: 'CA1',
                    data: [
                        [timestamp: start.millis, value: "down"],
                        [timestamp: start.plusMinutes(1).millis, value: "up"]
                    ]
                ]
            ]
        ]
    )
    assertEquals(200, response.status)


    response = hawkularMetrics.get(path: 'gauges/CG1/data', headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertEquals(
        [
            [timestamp: start.plusMinutes(1).millis, value: 9.589],
            [timestamp: start.millis, value: 10.032]
        ],
        response.data
    )

    response = hawkularMetrics.get(path: 'availability/CA1/data', headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertEquals(
        [
            [timestamp: start.millis, value: "down"],
            [timestamp: start.plusMinutes(1).millis, value: "up"]
        ],
        response.data
    )
  }

  @Test
  void addMixedDataMissingAvailabilities() {
    String tenantId = nextTenantId()
    DateTime start = DateTime.now().minusMinutes(10)

    def response = hawkularMetrics.post(
        path: "metrics/data",
        headers: [(tenantHeaderName): tenantId],
        body: [
            gauges: [
                [
                    id: 'AG1',
                    data:  [
                        [timestamp: start.millis, value: 10.032],
                        [timestamp: start.plusMinutes(1).millis, value: 9.589]
                    ],
                ]
            ],
            counters: [
                [
                    id: 'AC1',
                    data: [
                        [timestamp: start.millis, value: 10],
                        [timestamp: start.plusMinutes(1).millis, value: 20]
                    ]
                ]
            ]
        ]
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.get(path: 'gauges/AG1/data', headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertEquals(
        [
            [timestamp: start.plusMinutes(1).millis, value: 9.589],
            [timestamp: start.millis, value: 10.032]
        ],
        response.data
    )

    response = hawkularMetrics.get(path: 'counters/AC1/data', headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertEquals(
        [
            [timestamp: start.plusMinutes(1).millis, value: 20],
            [timestamp: start.millis, value: 10]
        ],
        response.data
    )
  }

  @Test
  void addMixedDataInvalidRequestPayload() {
    String tenantId = nextTenantId()

    badPost( path: "metrics/data",
        body: [],
        headers: [(tenantHeaderName): tenantId]) { exception ->
      // Missing type
      assertEquals(400, exception.response.status)
    }
  }

  @Test
  void addMixedDataMissingRequestPayload() {
    String tenantId = nextTenantId()

    badPost( path: "metrics/data",
        body: "",
        headers: [(tenantHeaderName): tenantId]) { exception ->
      // Missing type
      assertEquals(400, exception.response.status)
    }
  }

  @Test
  void addMixedDataEmptyRequestPayload() {
    String tenantId = nextTenantId()

    badPost( path: "metrics/data",
        body: new HashMap(),
        headers: [(tenantHeaderName): tenantId]) { exception ->
      // Missing type
      assertEquals(400, exception.response.status)
    }
  }

  @Test
  void addMixedDataMissingData() {
    String tenantId = nextTenantId()

    badPost( path: "metrics/data",
        body: [
         gauges: [
         ],
         counters: [
         ]
        ],
        headers: [(tenantHeaderName): tenantId]) { exception ->
      // Missing type
      assertEquals(400, exception.response.status)
    }
  }
}