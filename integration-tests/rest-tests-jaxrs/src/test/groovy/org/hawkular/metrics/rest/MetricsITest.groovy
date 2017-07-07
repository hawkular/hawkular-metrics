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

import org.joda.time.DateTime
import org.junit.Test

import static org.junit.Assert.assertEquals
/**
 * @author jsanda
 */
class MetricsITest extends RESTTest {

 @Test
  void dualPathTest() {
    String tenantId = nextTenantId()
    DateTime start = DateTime.now().minusMinutes(10)

    def response = hawkularMetrics.post(
        path: "metrics/raw",
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

    response = hawkularMetrics.get(path: 'metrics', headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertEquals(2, response.data.size)

    response = hawkularMetrics.get(path: 'm', headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertEquals(2, response.data.size)
  }

  @Test
  void addMixedData() {
    String tenantId = nextTenantId()
    DateTime start = DateTime.now().minusMinutes(10)

    def response = hawkularMetrics.post(
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

    response = hawkularMetrics.get(path: 'gauges/G1/raw', headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertEquals(
        [
            [timestamp: start.plusMinutes(1).millis, value: 9.589],
            [timestamp: start.millis, value: 10.032]
        ],
        response.data
    )

    response = hawkularMetrics.get(path: 'gauges/G2/raw', headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertEquals(
        [
            [timestamp: start.plusMinutes(1).millis, value: 57.327],
            [timestamp: start.millis, value: 33.51],
        ],
        response.data
    )

    response = hawkularMetrics.get(path: 'counters/C1/raw', headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertEquals(
        [
            [timestamp: start.plusMinutes(1).millis, value: 20],
            [timestamp: start.millis, value: 10]
        ],
        response.data
    )

    response = hawkularMetrics.get(path: 'counters/C2/raw', headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertEquals(
        [
            [timestamp: start.plusMinutes(2).millis, value: 300],
            [timestamp: start.plusMinutes(1).millis, value: 225],
            [timestamp: start.millis, value: 150]
        ],
        response.data
    )

    response = hawkularMetrics.get(path: 'availability/A1/raw', headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertEquals(
        [
            [timestamp: start.plusMinutes(1).millis, value: "up"],
            [timestamp: start.millis, value: "down"]
        ],
        response.data
    )

    response = hawkularMetrics.get(path: 'availability/A2/raw', headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertEquals(
        [
            [timestamp: start.plusMinutes(1).millis, value: "up"],
            [timestamp: start.millis, value: "up"]
        ],
        response.data
    )

    response = hawkularMetrics.get(path: 'strings/S1/raw', headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertEquals(
        [
            [timestamp: start.plusMinutes(1).millis, value: "server accepting reads"],
            [timestamp: start.millis, value: "server accepting writes"]
        ],
        response.data
    )

    response = hawkularMetrics.get(path: 'strings/S2/raw', headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertEquals(
        [
            [timestamp: start.plusMinutes(1).millis, value: "rebuilding index"],
            [timestamp: start.millis, value: "entering maintenance mode"]
        ],
        response.data
    )
  }

  @Test
  void addMixedDataMissingGauges() {
    String tenantId = nextTenantId()
    DateTime start = DateTime.now().minusMinutes(10)

    def response = hawkularMetrics.post(
        path: "metrics/raw",
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

    response = hawkularMetrics.get(path: 'counters/GC1/raw', headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertEquals(
        [
            [timestamp: start.plusMinutes(1).millis, value: 20],
            [timestamp: start.millis, value: 10]
        ],
        response.data
    )

    response = hawkularMetrics.get(path: 'availability/GA1/raw', headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertEquals(
        [
            [timestamp: start.plusMinutes(1).millis, value: "up"],
            [timestamp: start.millis, value: "down"]
        ],
        response.data
    )
  }

  @Test
  void addMixedDataMissingCounters() {
    String tenantId = nextTenantId()
    DateTime start = DateTime.now().minusMinutes(10)

    def response = hawkularMetrics.post(
        path: "metrics/raw",
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


    response = hawkularMetrics.get(path: 'gauges/CG1/raw', headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertEquals(
        [
            [timestamp: start.plusMinutes(1).millis, value: 9.589],
            [timestamp: start.millis, value: 10.032]
        ],
        response.data
    )

    response = hawkularMetrics.get(path: 'availability/CA1/raw', headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertEquals(
        [
            [timestamp: start.plusMinutes(1).millis, value: "up"],
            [timestamp: start.millis, value: "down"]
        ],
        response.data
    )
  }

  @Test
  void addMixedDataMissingAvailabilities() {
    String tenantId = nextTenantId()
    DateTime start = DateTime.now().minusMinutes(10)

    def response = hawkularMetrics.post(
        path: "metrics/raw",
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

    response = hawkularMetrics.get(path: 'gauges/AG1/raw', headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)
    assertEquals(
        [
            [timestamp: start.plusMinutes(1).millis, value: 9.589],
            [timestamp: start.millis, value: 10.032]
        ],
        response.data
    )

    response = hawkularMetrics.get(path: 'counters/AC1/raw', headers: [(tenantHeaderName): tenantId])
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

    badPost( path: "metrics/raw",
        body: [],
        headers: [(tenantHeaderName): tenantId]) { exception ->
      // Missing type
      assertEquals(400, exception.response.status)
    }
  }

  @Test
  void addMixedDataMissingRequestPayload() {
    String tenantId = nextTenantId()

    badPost( path: "metrics/raw",
        body: "",
        headers: [(tenantHeaderName): tenantId]) { exception ->
      // Missing type
      assertEquals(400, exception.response.status)
    }
  }

  @Test
  void addMixedDataEmptyRequestPayload() {
    String tenantId = nextTenantId()

    badPost( path: "metrics/raw",
        body: new HashMap(),
        headers: [(tenantHeaderName): tenantId]) { exception ->
      // Missing type
      assertEquals(400, exception.response.status)
    }
  }

  @Test
  void addMixedDataMissingData() {
    String tenantId = nextTenantId()

    badPost( path: "metrics/raw",
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

  static def createGauges(String tenantId, List gauges) {
    gauges.each { gauge ->
      def response  = hawkularMetrics.post(
          path: 'gauges',
          body: [id: gauge.id, tags: gauge.tags],
          headers: [(tenantHeaderName): tenantId]
      )
      assertEquals(201, response.status)
    }
  }

  static def insertGaugeDataPoints(String tenantId, List gauges) {
    def response = hawkularMetrics.post(
        path: 'gauges/raw',
        body: gauges,
        headers: [(tenantHeaderName): tenantId]
    )
    assertEquals(200, response.status)
  }

  static def createCounters(String tenantId, List counters) {
    counters.each { counter ->
      def response  = hawkularMetrics.post(
          path: 'counters',
          body: [id: counter.id, tags: counter.tags],
          headers: [(tenantHeaderName): tenantId]
      )
      assertEquals(201, response.status)
    }
  }

  static def insertCounterDataPoints(String tenantId, List counters) {
    def response = hawkularMetrics.post(
        path: 'counters/raw',
        body: counters,
        headers: [(tenantHeaderName): tenantId]
    )
    assertEquals(200, response.status)
  }

  static def withDataPoints(String tenantId, Closure callback) {
    def createGauge = { id, tags ->
      def response = hawkularMetrics.post(
          path: 'gauges',
          body: [id: id, tags: tags],
          headers: [(tenantHeaderName): tenantId]
      )
      assertEquals(201, response.status)
    }
    def createCounter = { id, tags ->
      def response = hawkularMetrics.post(
          path: 'counters',
          body: [id: id, tags: tags],
          headers: [(tenantHeaderName): tenantId]
      )
      assertEquals(201, response.status)
    }
    def createAvailability = { id, tags ->
      def response = hawkularMetrics.post(
          path: 'availability',
          body: [id: id, tags: tags],
          headers: [(tenantHeaderName): tenantId]
      )
      assertEquals(201, response.status)
    }

    createGauge('G1', [x: 1, y: 1, z: 1])
    createGauge('G2', [x: 1, y: 2, z: 2])
    createGauge('G3', [x: 2, y: 3, z: 1])

    createCounter('C1', [x: 1, y: 1, z: 3])
    createCounter('C2', [x: 1, y: 2, z: 1])
    createCounter('C3', [x: 2, y: 3, z: 1])

    createAvailability('A1', [x: 1, y: 1, z: 3])
    createAvailability('A2', [x: 1, y: 2, z: 1])
    createAvailability('A3', [x: 2, y: 3, z: 1])

    def response = hawkularMetrics.post(
        path: "gauges/raw",
        headers: [(tenantHeaderName): tenantId],
        body: [
            [
                id: 'G1',
                data: [
                    [timestamp: 100, value: 1.23],
                    [timestamp: 200, value: 3.45],
                    [timestamp: 300, value: 5.34],
                    [timestamp: 400, value: 2.22],
                    [timestamp: 500, value: 5.22]
                ]
            ],
            [
                id: 'G2',
                data: [
                    [timestamp: 100, value: 1.45],
                    [timestamp: 200, value: 2.36],
                    [timestamp: 300, value: 3.62],
                    [timestamp: 400, value: 2.63],
                    [timestamp: 500, value: 3.99]
                ]
            ],
            [
                id: 'G3',
                data: [
                    [timestamp: 100, value: 4.45],
                    [timestamp: 200, value: 5.55],
                    [timestamp: 300, value: 4.44],
                    [timestamp: 400, value: 3.33],
                    [timestamp: 500, value: 3.77]
                ]
            ]
        ]
    )
    assertEquals(200, response.status)
    response = hawkularMetrics.post(
        path: "counters/raw",
        headers: [(tenantHeaderName): tenantId],
        body: [
            [
                id: 'C1',
                data: [
                    [timestamp: 100, value: 12],
                    [timestamp: 200, value: 17],
                    [timestamp: 300, value: 19],
                    [timestamp: 400, value: 26],
                    [timestamp: 500, value: 37]
                ]
            ],
            [
                id: 'C2',
                data: [
                    [timestamp: 100, value: 41],
                    [timestamp: 200, value: 49],
                    [timestamp: 300, value: 64],
                    [timestamp: 400, value: 71],
                    [timestamp: 500, value: 95]
                ]
            ],
            [
                id: 'C3',
                data: [
                    [timestamp: 100, value: 28],
                    [timestamp: 200, value: 35],
                    [timestamp: 300, value: 42],
                    [timestamp: 400, value: 49],
                    [timestamp: 500, value: 59]
                ]
            ]
        ]
    )
    assertEquals(200, response.status)
    response = hawkularMetrics.post(
        path: 'availability/raw',
        headers: [(tenantHeaderName): tenantId],
        body: [
            [
                id: 'A1',
                data: [
                    [timestamp: 100, value: 'up'],
                    [timestamp: 200, value: 'up'],
                    [timestamp: 300, value: 'down'],
                    [timestamp: 400, value: 'down'],
                    [timestamp: 500, value: 'up']
                ]
            ],
            [
                id: 'A2',
                data: [
                    [timestamp: 100, value: 'down'],
                    [timestamp: 200, value: 'up'],
                    [timestamp: 300, value: 'down'],
                    [timestamp: 400, value: 'up'],
                    [timestamp: 500, value: 'up']
                ]
            ],
            [
                id: 'A3',
                data: [
                    [timestamp: 100, value: 'up'],
                    [timestamp: 200, value: 'down'],
                    [timestamp: 300, value: 'down'],
                    [timestamp: 400, value: 'up'],
                    [timestamp: 500, value: 'up']
                ]
            ]
        ]
    )
    assertEquals(200, response.status)
    callback()
  }

  @Test
  void fetchStats() {
    String tenantId = nextTenantId()
    withDataPoints(tenantId, {
      def response = hawkularMetrics.post(
          path: 'metrics/stats/query',
          headers: [(tenantHeaderName): tenantId],
          body: [
              metrics: [
                  gauge: ['G1', 'G3'],
                  counter: ['C2', 'C3'],
                  availability: ['A2', 'A3']
              ],
              buckets: 2,
              start: 200,
              end: 500
          ]
      )
      assertEquals(200, response.status)

      def expected = [
          gauge: [
              G1: [
                  [
                      start: 200,
                      end: 350,
                      samples: 2,
                      max: 5.34,
                      min: 3.45,
                      avg: avg([3.45, 5.34]),
                      sum: 3.45 + 5.34,
                      median: median([3.45, 5.34]),
                      empty: false
                  ],
                  [
                      start: 350,
                      end: 500,
                      max: 2.22,
                      min: 2.22,
                      avg: 2.22,
                      median: 2.22,
                      sum: 2.22,
                      samples: 1,
                      empty: false
                  ]
              ],
              G3: [
                  [
                      start: 200,
                      end: 350,
                      max: 5.55,
                      min: 4.44,
                      avg: avg([5.55, 4.44]),
                      median: median([5.55, 4.44]),
                      sum: 5.55 + 4.44,
                      samples: 2,
                      empty: false
                  ],
                  [
                      start: 350,
                      end: 500,
                      max: 3.33,
                      min: 3.33,
                      avg: 3.33,
                      median: 3.33,
                      sum: 3.33,
                      samples: 1,
                      empty: false
                  ]
              ]
          ],
          counter: [
              C2: [
                  [
                      start: 200,
                      end: 350,
                      max: 64,
                      min: 49,
                      avg: avg([49, 64]),
                      median: median([49, 64]),
                      sum: 49 + 64,
                      samples: 2,
                      empty: false
                  ],
                  [
                      start: 350,
                      end: 500,
                      max: 71,
                      min: 71,
                      avg: 71,
                      median: 71,
                      sum: 71,
                      samples: 1,
                      empty: false
                  ]
              ],
              C3: [
                  [
                      start: 200,
                      end: 350,
                      max: 42,
                      min: 35,
                      avg: avg([35, 42]),
                      median: median([35, 42]),
                      sum: 35 + 42,
                      samples: 2,
                      empty: false
                  ],
                  [
                      start: 350,
                      end: 500,
                      max: 49,
                      min: 49,
                      avg: 49,
                      median: 49,
                      sum: 49,
                      samples: 1,
                      empty: false
                  ]
              ]
          ],
          availability: [
              A2: [
                  [
                      start: 200,
                      end: 350,
                      durationMap: [
                          "down": 50,
                          "up": 100
                      ],
                      lastNotUptime: 350,
                      uptimeRatio: 100 / 150,
                      notUpCount: 1,
                      downDuration: 50,
                      lastNotUptime: 350,
                      empty: false,
                      adminDuration: 0,
                      unknownDuration: 0,
                      upDuration: 100,
                      notUpDuration: 50
                  ],
                  [
                      start: 350,
                      end: 500,
                      durationMap: [
                          "up": 150
                      ],
                      lastNotUptime: 0,
                      uptimeRatio: 1.0,
                      notUpCount: 0,
                      downDuration: 0,
                      lastNotUptime: 0,
                      empty: false,
                      adminDuration: 0,
                      unknownDuration: 0,
                      upDuration: 150,
                      notUpDuration: 0
                  ]
              ],
              A3: [
                  [
                      start: 200,
                      end: 350,
                      durationMap: [
                          "down": 150
                      ],
                      lastNotUptime: 350,
                      uptimeRatio: 0.0,
                      notUpCount: 1,
                      downDuration: 150,
                      lastNotUptime: 350,
                      empty: false,
                      adminDuration: 0,
                      unknownDuration: 0,
                      upDuration: 0,
                      notUpDuration: 150
                  ],
                  [
                      start: 350,
                      end: 500,
                      durationMap: [
                          "up": 150
                      ],
                      lastNotUptime: 0,
                      uptimeRatio: 1.0,
                      notUpCount: 0,
                      downDuration: 0,
                      lastNotUptime: 0,
                      empty: false,
                      adminDuration: 0,
                      unknownDuration: 0,
                      upDuration: 150,
                      notUpDuration: 0
                  ]
              ]
          ]
      ]

      def verifyResponse = {
        assertEquals(expected.size(), response.data.size())
        assertEquals(expected.gauge.size(), response.data.gauge.size())
        assertEquals(expected.gauge.G1.size(), response.data.gauge.G1.size())
        assertNumericBucketEquals('The data for G1[0] does not match', expected.gauge.G1[0], response.data.gauge.G1[0])
        assertNumericBucketEquals('The data for G1[1] does not match', expected.gauge.G1[1], response.data.gauge.G1[1])
        assertEquals(expected.gauge.G3.size(), response.data.gauge.G3.size())
        assertNumericBucketEquals('The data for G3[0] does not match', expected.gauge.G3[0], response.data.gauge.G3[0])
        assertNumericBucketEquals('The data for G3[1] does not match', expected.gauge.G3[1], response.data.gauge.G3[1])


        assertEquals(expected.counter.size(), response.data.counter.size())
        assertEquals(expected.counter.C2.size(), response.data.counter.C2.size())
        assertNumericBucketEquals('The data for C2[0] does not match', expected.counter.C2[0],
            response.data.counter.C2[0])
        assertNumericBucketEquals('The data for C2[1] does not match', expected.counter.C2[1],
            response.data.counter.C2[1])
        assertEquals(expected.counter.C3.size(), response.data.counter.C3.size())
        assertNumericBucketEquals('The data for C3[0] does not match', expected.counter.C3[0],
            response.data.counter.C3[0])
        assertNumericBucketEquals('The data for C3[1] does not match', expected.counter.C3[1],
            response.data.counter.C3[1])

        assertEquals(expected.availability.size(), response.data.availability.size())
        assertEquals(expected.availability.A2.size(), response.data.availability.A2.size())
        assertAvailablityBucketEquals("The data for A2[0] does not match", expected.availability.A2[0],
            response.data.availability.A2[0])
        assertAvailablityBucketEquals("The data for A2[1] does not match", expected.availability.A2[1],
            response.data.availability.A2[1])
        assertEquals(expected.availability.A3.size(), response.data.availability.A3.size())
        assertAvailablityBucketEquals('The data for A3[0] does not match', expected.availability.A3[0],
            response.data.availability.A3[0])
        assertAvailablityBucketEquals('The data for A3[1] does not match', expected.availability.A3[1],
            response.data.availability.A3[1])
      }

      verifyResponse()

      // now query using bucketDuration instead of buckets
      response = hawkularMetrics.post(
          path: 'metrics/stats/query',
          headers: [(tenantHeaderName): tenantId],
          body: [
              metrics: [
                  gauge: ['G1', 'G3'],
                  counter: ['C2', 'C3'],
                  availability: ['A2', 'A3']
              ],
              bucketDuration: '150ms',
              start: 200,
              end: 500
          ]
      )
      assertEquals(200, response.status)

      verifyResponse()

      // now query by tags instead of ids
      response = hawkularMetrics.post(
          path: 'metrics/stats/query',
          headers: [(tenantHeaderName): tenantId],
          body: [
              buckets: 2,
              start: 200,
              end: 500,
              tags: 'z:1'
          ]
      )

      verifyResponse()
    })
  }

  @Test
  void fetchGaugeStats() {
    String tenantId = nextTenantId()
    withDataPoints(tenantId, {
      def response = hawkularMetrics.post(
          path: 'metrics/stats/query',
          headers: [(tenantHeaderName): tenantId],
          body: [
              metrics: [
                  gauge: ['G1', 'G3'],
              ],
              buckets: 2,
              start: 200,
              end: 500
          ]
      )
      assertEquals(200, response.status)

      def expected = [
          gauge: [
              G1: [
                  [
                      start: 200,
                      end: 350,
                      samples: 2,
                      max: 5.34,
                      min: 3.45,
                      avg: avg([3.45, 5.34]),
                      sum: 3.45 + 5.34,
                      median: median([3.45, 5.34]),
                      empty: false
                  ],
                  [
                      start: 350,
                      end: 500,
                      max: 2.22,
                      min: 2.22,
                      avg: 2.22,
                      median: 2.22,
                      sum: 2.22,
                      samples: 1,
                      empty: false
                  ]
              ],
              G3: [
                  [
                      start: 200,
                      end: 350,
                      max: 5.55,
                      min: 4.44,
                      avg: avg([5.55, 4.44]),
                      median: median([5.55, 4.44]),
                      sum: 5.55 + 4.44,
                      samples: 2,
                      empty: false
                  ],
                  [
                      start: 350,
                      end: 500,
                      max: 3.33,
                      min: 3.33,
                      avg: 3.33,
                      median: 3.33,
                      sum: 3.33,
                      samples: 1,
                      empty: false
                  ]
              ]
          ]
      ]
      assertEquals(expected.size(), response.data.size())
      assertEquals(expected.gauge.G1.size(), response.data.gauge.G1.size())
      assertNumericBucketEquals('The data for G1[0] does not match', expected.gauge.G1[0], response.data.gauge.G1[0])
      assertNumericBucketEquals('The data for G1[1] does not match', expected.gauge.G1[1], response.data.gauge.G1[1])
      assertEquals(expected.gauge.G3.size(), response.data.gauge.G3.size())
      assertNumericBucketEquals('The data for G3[0] does not match', expected.gauge.G3[0], response.data.gauge.G3[0])
      assertNumericBucketEquals('The data for G3[1] does not match', expected.gauge.G3[1], response.data.gauge.G3[1])
    })
  }

  @Test
  void fetchGaugeStatsWithRates() {
    String tenantId = nextTenantId()
    withDataPoints(tenantId, {
      def response = hawkularMetrics.post(
          path: 'metrics/stats/query',
          headers: [(tenantHeaderName): tenantId],
          body: [
              metrics: [
                  gauge: ['G1', 'G3'],
              ],
              types: ['gauge', 'gauge_rate'],
              buckets: 2,
              start: 200,
              end: 500
          ]
      )
      assertEquals(200, response.status)

      def expected = [
          gauge: [
              G1: [
                  [
                      start: 200,
                      end: 350,
                      samples: 2,
                      max: 5.34,
                      min: 3.45,
                      avg: avg([3.45, 5.34]),
                      sum: 3.45 + 5.34,
                      median: median([3.45, 5.34]),
                      empty: false
                  ],
                  [
                      start: 350,
                      end: 500,
                      max: 2.22,
                      min: 2.22,
                      avg: 2.22,
                      median: 2.22,
                      sum: 2.22,
                      samples: 1,
                      empty: false
                  ]
              ],
              G3: [
                  [
                      start: 200,
                      end: 350,
                      max: 5.55,
                      min: 4.44,
                      avg: avg([5.55, 4.44]),
                      median: median([5.55, 4.44]),
                      sum: 5.55 + 4.44,
                      samples: 2,
                      empty: false
                  ],
                  [
                      start: 350,
                      end: 500,
                      max: 3.33,
                      min: 3.33,
                      avg: 3.33,
                      median: 3.33,
                      sum: 3.33,
                      samples: 1,
                      empty: false
                  ]
              ]
          ],
          gauge_rate: [
              G1: [
                  [
                      start: 200,
                      end: 350,
                      max: 1134,
                      min: 1134,
                      avg: 1134,
                      median: 1134,
                      sum: 1134,
                      samples: 1,
                      empty: false
                  ],
                  [
                      start: 350,
                      end: 500,
                      max: -1872,
                      min: -1872,
                      avg: -1872,
                      median: -1872,
                      sum: -1872,
                      samples: 1,
                      empty: false
                  ]
              ],
              G3: [
                  [
                      start: 200,
                      end: 350,
                      max: -666,
                      min: -666,
                      avg: -666,
                      median: -666,
                      sum: -666,
                      samples: 1,
                      empty: false
                  ],
                  [
                      start: 350,
                      end: 500,
                      max: -666,
                      min: -666,
                      avg: -666,
                      median: -666,
                      sum: -666,
                      samples: 1,
                      empty: false
                  ]
              ]
          ]
      ]
      assertEquals(expected.size(), response.data.size())
      assertEquals(expected.gauge.size(), response.data.gauge.size())
      assertEquals(expected.gauge.G1.size(), response.data.gauge.G1.size())
      assertNumericBucketEquals('The data for G1[0] does not match', expected.gauge.G1[0], response.data.gauge.G1[0])
      assertNumericBucketEquals('The data for G1[1] does not match', expected.gauge.G1[1], response.data.gauge.G1[1])
      assertEquals(expected.gauge.G3.size(), response.data.gauge.G3.size())
      assertNumericBucketEquals('The data for G3[0] does not match', expected.gauge.G3[0], response.data.gauge.G3[0])
      assertNumericBucketEquals('The data for G3[1] does not match', expected.gauge.G3[1], response.data.gauge.G3[1])

      assertEquals(expected.gauge_rate.size(), response.data.gauge_rate.size())
      assertEquals(expected.gauge_rate.G1.size(), response.data.gauge_rate.G1.size())
      assertNumericBucketEquals('The rate data for G1[0] does not match', expected.gauge_rate.G1[0],
          response.data.gauge_rate.G1[0])
      assertNumericBucketEquals('The rate data for G1[1] does not match', expected.gauge_rate.G1[1],
          response.data.gauge_rate.G1[1])
    })
  }

  @Test
  void fetchGaugeStatsWithPercentiles() {
    String tenantId = nextTenantId()
    withDataPoints(tenantId, {
      def response = hawkularMetrics.post(
          path: 'metrics/stats/query',
          headers: [(tenantHeaderName): tenantId],
          body: [
              metrics: [
                  gauge: ['G1', 'G3'],
              ],
              buckets: 2,
              percentiles: '95,99',
              start: 200,
              end: 500
          ]
      )
      assertEquals(200, response.status)

      def expected = [
          gauge: [
              G1: [
                  [
                      start: 200,
                      end: 350,
                      samples: 2,
                      max: 5.34,
                      min: 3.45,
                      avg: avg([3.45, 5.34]),
                      sum: 3.45 + 5.34,
                      median: median([3.45, 5.34]),
                      percentiles: [
                          [quantile: 95, value: percentile(95, [3.45, 5.34])],
                          [quantile: 99, value: percentile(99, [3.45, 5.34])]
                      ],
                      empty: false
                  ],
                  [
                      start: 350,
                      end: 500,
                      max: 2.22,
                      min: 2.22,
                      avg: 2.22,
                      median: 2.22,
                      sum: 2.22,
                      percentiles: [
                          [quantile: 95, value: percentile(95, [2.22])],
                          [quantile: 99, value: percentile(99, [2.22])]
                      ],
                      samples: 1,
                      empty: false
                  ]
              ],
              G3: [
                  [
                      start: 200,
                      end: 350,
                      max: 5.55,
                      min: 4.44,
                      avg: avg([5.55, 4.44]),
                      median: median([5.55, 4.44]),
                      sum: 5.55 + 4.44,
                      percentiles: [
                          [quantile: 95, value: percentile(95, [4.44, 5.55])],
                          [quantile: 99, value: percentile(99, [4.44, 5.55])]
                      ],
                      samples: 2,
                      empty: false
                  ],
                  [
                      start: 350,
                      end: 500,
                      max: 3.33,
                      min: 3.33,
                      avg: 3.33,
                      median: 3.33,
                      sum: 3.33,
                      percentiles: [
                          [quantile: 95, value: percentile(95, [3.33])],
                          [quantile: 99, value: percentile(99, [3.33])]
                      ],
                      samples: 1,
                      empty: false
                  ]
              ]
          ]
      ]

      assertEquals(expected.size(), response.data.size())
      assertEquals(expected.gauge.G1.size(), response.data.gauge.G1.size())
      assertNumericBucketEquals('The data for G1[0] does not match', expected.gauge.G1[0], response.data.gauge.G1[0])
      assertNumericBucketEquals('The data for G1[1] does not match', expected.gauge.G1[1], response.data.gauge.G1[1])
      assertEquals(expected.gauge.G3.size(), response.data.gauge.G3.size())
      assertNumericBucketEquals('The data for G3[0] does not match', expected.gauge.G3[0], response.data.gauge.G3[0])
      assertNumericBucketEquals('The data for G3[1] does not match', expected.gauge.G3[1], response.data.gauge.G3[1])
    })
  }

  @Test
  void fetchCounterStats() {
    String tenantId = nextTenantId()
    withDataPoints(tenantId, {
      def response = hawkularMetrics.post(
          path: 'metrics/stats/query',
          headers: [(tenantHeaderName): tenantId],
          body: [
              metrics: [
                  counter: ['C2', 'C3'],
              ],
              buckets: 2,
              start: 200,
              end: 500
          ]
      )
      assertEquals(200, response.status)

      def expected = [
          counter: [
              C2: [
                  [
                      start: 200,
                      end: 350,
                      max: 64,
                      min: 49,
                      avg: avg([49, 64]),
                      median: median([49, 64]),
                      sum: 49 + 64,
                      samples: 2,
                      empty: false
                  ],
                  [
                      start: 350,
                      end: 500,
                      max: 71,
                      min: 71,
                      avg: 71,
                      median: 71,
                      sum: 71,
                      samples: 1,
                      empty: false
                  ]
              ],
              C3: [
                  [
                      start: 200,
                      end: 350,
                      max: 42,
                      min: 35,
                      avg: avg([35, 42]),
                      median: median([35, 42]),
                      sum: 35 + 42,
                      samples: 2,
                      empty: false
                  ],
                  [
                      start: 350,
                      end: 500,
                      max: 49,
                      min: 49,
                      avg: 49,
                      median: 49,
                      sum: 49,
                      samples: 1,
                      empty: false
                  ]
              ]
          ]
      ]
      assertEquals(expected.size(), response.data.size())

      assertEquals(expected.counter.size(), response.data.counter.size())
      assertEquals(expected.counter.C2.size(), response.data.counter.C2.size())
      assertNumericBucketEquals('The data for C2[0] does not match', expected.counter.C2[0],
          response.data.counter.C2[0])
      assertNumericBucketEquals('The data for C2[1] does not match', expected.counter.C2[1],
          response.data.counter.C2[1])
      assertEquals(expected.counter.C3.size(), response.data.counter.C3.size())
      assertNumericBucketEquals('The data for C3[0] does not match', expected.counter.C3[0],
          response.data.counter.C3[0])
      assertNumericBucketEquals('The data for C3[1] does not match', expected.counter.C3[1],
          response.data.counter.C3[1])
    })
  }

  @Test
  void fetchCounterStatsWithRates() {
    String tenantId = nextTenantId()
    withDataPoints(tenantId, {
      def response = hawkularMetrics.post(
          path: 'metrics/stats/query',
          headers: [(tenantHeaderName): tenantId],
          body: [
              metrics: [
                  counter: ['C2', 'C3'],
              ],
              types: ['counter', 'counter_rate'],
              buckets: 2,
              start: 200,
              end: 500
          ]
      )
      assertEquals(200, response.status)

      def expected = [
          counter: [
              C2: [
                  [
                      start: 200,
                      end: 350,
                      max: 64,
                      min: 49,
                      avg: avg([49, 64]),
                      median: median([49, 64]),
                      sum: 49 + 64,
                      samples: 2,
                      empty: false
                  ],
                  [
                      start: 350,
                      end: 500,
                      max: 71,
                      min: 71,
                      avg: 71,
                      median: 71,
                      sum: 71,
                      samples: 1,
                      empty: false
                  ]
              ],
              C3: [
                  [
                      start: 200,
                      end: 350,
                      max: 42,
                      min: 35,
                      avg: avg([35, 42]),
                      median: median([35, 42]),
                      sum: 35 + 42,
                      samples: 2,
                      empty: false
                  ],
                  [
                      start: 350,
                      end: 500,
                      max: 49,
                      min: 49,
                      avg: 49,
                      median: 49,
                      sum: 49,
                      samples: 1,
                      empty: false
                  ]
              ]
          ],
          counter_rate: [
              C2: [
                  [
                      start: 200,
                      end: 350,
                      max: 9000,
                      avg: 9000,
                      min: 9000,
                      sum: 9000,
                      median: 9000,
                      samples: 1,
                      empty: false
                  ],
                  [
                      start: 350,
                      end: 500,
                      max: 4200,
                      min: 4200,
                      avg: 4200,
                      sum: 4200,
                      median: 4200,
                      samples: 1,
                      empty: false
                  ]
              ],
              C3: [
                  [
                      start: 200,
                      end: 350,
                      max: 4200,
                      min: 4200,
                      avg: 4200,
                      sum: 4200,
                      median: 4200,
                      samples: 1,
                      empty: false
                  ],
                  [
                      start: 350,
                      end: 500,
                      max: 4200,
                      min: 4200,
                      avg: 4200,
                      sum: 4200,
                      median: 4200,
                      samples: 1,
                      empty: false
                  ]
              ]
          ]
      ]
      assertEquals(expected.size(), response.data.size())

      assertEquals(expected.counter.size(), response.data.counter.size())
      assertEquals(expected.counter.C2.size(), response.data.counter.C2.size())
      assertNumericBucketEquals('The data for C2[0] does not match', expected.counter.C2[0],
          response.data.counter.C2[0])
      assertNumericBucketEquals('The data for C2[1] does not match', expected.counter.C2[1],
          response.data.counter.C2[1])
      assertEquals(expected.counter.C3.size(), response.data.counter.C3.size())
      assertNumericBucketEquals('The data for C3[0] does not match', expected.counter.C3[0],
          response.data.counter.C3[0])
      assertNumericBucketEquals('The data for C3[1] does not match', expected.counter.C3[1],
          response.data.counter.C3[1])

      assertEquals(expected.counter_rate.size(), response.data.counter_rate.size())
      assertEquals(expected.counter_rate.C2.size(), response.data.counter_rate.C2.size())
      assertNumericBucketEquals('The rate data for C2[0] does not match', expected.counter_rate.C2[0],
          response.data.counter_rate.C2[0])
      assertNumericBucketEquals('The rate data for C2[1] does not match', expected.counter_rate.C2[1],
          response.data.counter_rate.C2[1])
      assertEquals(expected.counter_rate.C3.size(), response.data.counter_rate.C3.size())
      assertNumericBucketEquals('The rate data for C3[0] does not match', expected.counter_rate.C3[0],
          response.data.counter_rate.C3[0])
      assertNumericBucketEquals('The rate data for C3[1] does not match', expected.counter_rate.C3[1],
          response.data.counter_rate.C3[1])
    })
  }

  @Test
  void fetchGaugeAndCounterRateStats() {
    String tenantId = nextTenantId()
    withDataPoints(tenantId, {
      def response = hawkularMetrics.post(
          path: 'metrics/stats/query',
          headers: [(tenantHeaderName): tenantId],
          body: [
              metrics    : [
                  gauge: ['G1', 'G3'],
                  counter: ['C2', 'C3']
              ],
              types: ['gauge', 'counter_rate'],
              buckets    : 2,
              start      : 200,
              end        : 500
          ]
      )
      assertEquals(200, response.status)
      def expected = [
          gauge: [
              G1: [
                  [
                      start: 200,
                      end: 350,
                      samples: 2,
                      max: 5.34,
                      min: 3.45,
                      avg: avg([3.45, 5.34]),
                      sum: 3.45 + 5.34,
                      median: median([3.45, 5.34]),
                      empty: false
                  ],
                  [
                      start: 350,
                      end: 500,
                      max: 2.22,
                      min: 2.22,
                      avg: 2.22,
                      median: 2.22,
                      sum: 2.22,
                      samples: 1,
                      empty: false
                  ]
              ],
              G3: [
                  [
                      start: 200,
                      end: 350,
                      max: 5.55,
                      min: 4.44,
                      avg: avg([5.55, 4.44]),
                      median: median([5.55, 4.44]),
                      sum: 5.55 + 4.44,
                      samples: 2,
                      empty: false
                  ],
                  [
                      start: 350,
                      end: 500,
                      max: 3.33,
                      min: 3.33,
                      avg: 3.33,
                      median: 3.33,
                      sum: 3.33,
                      samples: 1,
                      empty: false
                  ]
              ]
          ],
          counter_rate: [
              C2: [
                  [
                      start: 200,
                      end: 350,
                      max: 9000,
                      avg: 9000,
                      min: 9000,
                      sum: 9000,
                      median: 9000,
                      samples: 1,
                      empty: false
                  ],
                  [
                      start: 350,
                      end: 500,
                      max: 4200,
                      min: 4200,
                      avg: 4200,
                      sum: 4200,
                      median: 4200,
                      samples: 1,
                      empty: false
                  ]
              ],
              C3: [
                  [
                      start: 200,
                      end: 350,
                      max: 4200,
                      min: 4200,
                      avg: 4200,
                      sum: 4200,
                      median: 4200,
                      samples: 1,
                      empty: false
                  ],
                  [
                      start: 350,
                      end: 500,
                      max: 4200,
                      min: 4200,
                      avg: 4200,
                      sum: 4200,
                      median: 4200,
                      samples: 1,
                      empty: false
                  ]
              ]
          ]
      ]

      assertEquals(expected.size(), response.data.size())
      assertEquals(expected.gauge.size(), response.data.gauge.size())
      assertEquals(expected.gauge.G1.size(), response.data.gauge.G1.size())
      assertNumericBucketEquals('The data for G1[0] does not match', expected.gauge.G1[0], response.data.gauge.G1[0])
      assertNumericBucketEquals('The data for G1[1] does not match', expected.gauge.G1[1], response.data.gauge.G1[1])
      assertEquals(expected.gauge.G3.size(), response.data.gauge.G3.size())
      assertNumericBucketEquals('The data for G3[0] does not match', expected.gauge.G3[0], response.data.gauge.G3[0])

      assertEquals(expected.counter_rate.size(), response.data.counter_rate.size())
      assertEquals(expected.counter_rate.C2.size(), response.data.counter_rate.C2.size())
      assertNumericBucketEquals('The rate data for C2[0] does not match', expected.counter_rate.C2[0],
          response.data.counter_rate.C2[0])
      assertNumericBucketEquals('The rate data for C2[1] does not match', expected.counter_rate.C2[1],
          response.data.counter_rate.C2[1])
      assertEquals(expected.counter_rate.C3.size(), response.data.counter_rate.C3.size())
      assertNumericBucketEquals('The rate data for C3[0] does not match', expected.counter_rate.C3[0],
          response.data.counter_rate.C3[0])
      assertNumericBucketEquals('The rate data for C3[1] does not match', expected.counter_rate.C3[1],
          response.data.counter_rate.C3[1])
    })
  }

  @Test
  void fetchRateStats() {
    String tenantId = nextTenantId()
    withDataPoints(tenantId, {
      def response = hawkularMetrics.post(
          path: 'metrics/stats/query',
          headers: [(tenantHeaderName): tenantId],
          body: [
              metrics: [
                  gauge: ['G1', 'G3'],
                  counter: ['C2', 'C3']
              ],
              types: ['gauge_rate', 'counter_rate'],
              buckets: 2,
              start: 200,
              end: 500
          ]
      )
      assertEquals(200, response.status)

      def expected = [
          gauge_rate: [
              G1: [
                  [
                      start: 200,
                      end: 350,
                      max: 1134,
                      min: 1134,
                      avg: 1134,
                      median: 1134,
                      sum: 1134,
                      samples: 1,
                      empty: false
                  ],
                  [
                      start: 350,
                      end: 500,
                      max: -1872,
                      min: -1872,
                      avg: -1872,
                      median: -1872,
                      sum: -1872,
                      samples: 1,
                      empty: false
                  ]
              ],
              G3: [
                  [
                      start: 200,
                      end: 350,
                      max: -666,
                      min: -666,
                      avg: -666,
                      median: -666,
                      sum: -666,
                      samples: 1,
                      empty: false
                  ],
                  [
                      start: 350,
                      end: 500,
                      max: -666,
                      min: -666,
                      avg: -666,
                      median: -666,
                      sum: -666,
                      samples: 1,
                      empty: false
                  ]
              ]
          ],
          counter_rate: [
              C2: [
                  [
                      start: 200,
                      end: 350,
                      max: 9000,
                      avg: 9000,
                      min: 9000,
                      sum: 9000,
                      median: 9000,
                      samples: 1,
                      empty: false
                  ],
                  [
                      start: 350,
                      end: 500,
                      max: 4200,
                      min: 4200,
                      avg: 4200,
                      sum: 4200,
                      median: 4200,
                      samples: 1,
                      empty: false
                  ]
              ],
              C3: [
                  [
                      start: 200,
                      end: 350,
                      max: 4200,
                      min: 4200,
                      avg: 4200,
                      sum: 4200,
                      median: 4200,
                      samples: 1,
                      empty: false
                  ],
                  [
                      start: 350,
                      end: 500,
                      max: 4200,
                      min: 4200,
                      avg: 4200,
                      sum: 4200,
                      median: 4200,
                      samples: 1,
                      empty: false
                  ]
              ]
          ]
      ]

      assertEquals(expected.size(), response.data.size())
      assertEquals(expected.gauge_rate.size(), response.data.gauge_rate.size())
      assertEquals(expected.gauge_rate.G1.size(), response.data.gauge_rate.G1.size())
      assertNumericBucketEquals('The rate data for G1[0] does not match', expected.gauge_rate.G1[0],
          response.data.gauge_rate.G1[0])
      assertNumericBucketEquals('The rate data for G1[1] does not match', expected.gauge_rate.G1[1],
          response.data.gauge_rate.G1[1])

      assertEquals(expected.counter_rate.size(), response.data.counter_rate.size())
      assertEquals(expected.counter_rate.C2.size(), response.data.counter_rate.C2.size())
      assertNumericBucketEquals('The rate data for C2[0] does not match', expected.counter_rate.C2[0],
          response.data.counter_rate.C2[0])
      assertNumericBucketEquals('The rate data for C2[1] does not match', expected.counter_rate.C2[1],
          response.data.counter_rate.C2[1])
      assertEquals(expected.counter_rate.C3.size(), response.data.counter_rate.C3.size())
      assertNumericBucketEquals('The rate data for C3[0] does not match', expected.counter_rate.C3[0],
          response.data.counter_rate.C3[0])
      assertNumericBucketEquals('The rate data for C3[1] does not match', expected.counter_rate.C3[1],
          response.data.counter_rate.C3[1])
    })
  }

  @Test
  void fetchAvailabilityStats() {
    String tenantId = nextTenantId()
    withDataPoints(tenantId, {
      def response = hawkularMetrics.post(
          path: 'metrics/stats/query',
          headers: [(tenantHeaderName): tenantId],
          body: [
              metrics: [
                  availability: ['A2', 'A3'],
              ],
              buckets: 2,
              start: 200,
              end: 500
          ]
      )
      assertEquals(200, response.status)

      def expected = [
          availability: [
              A2: [
                  [
                      start: 200,
                      end: 350,
                      durationMap: [
                          "down": 50,
                          "up": 100
                      ],
                      lastNotUptime: 350,
                      uptimeRatio: 100 / 150,
                      notUpCount: 1,
                      downDuration: 50,
                      lastNotUptime: 350,
                      empty: false,
                      adminDuration: 0,
                      unknownDuration: 0,
                      upDuration: 100,
                      notUpDuration: 50
                  ],
                  [
                      start: 350,
                      end: 500,
                      durationMap: [
                          "up": 150
                      ],
                      lastNotUptime: 0,
                      uptimeRatio: 1.0,
                      notUpCount: 0,
                      downDuration: 0,
                      lastNotUptime: 0,
                      empty: false,
                      adminDuration: 0,
                      unknownDuration: 0,
                      upDuration: 150,
                      notUpDuration: 0
                  ]
              ],
              A3: [
                  [
                      start: 200,
                      end: 350,
                      durationMap: [
                          "down": 150
                      ],
                      lastNotUptime: 350,
                      uptimeRatio: 0.0,
                      notUpCount: 1,
                      downDuration: 150,
                      lastNotUptime: 350,
                      empty: false,
                      adminDuration: 0,
                      unknownDuration: 0,
                      upDuration: 0,
                      notUpDuration: 150
                  ],
                  [
                      start: 350,
                      end: 500,
                      durationMap: [
                          "up": 150
                      ],
                      lastNotUptime: 0,
                      uptimeRatio: 1.0,
                      notUpCount: 0,
                      downDuration: 0,
                      lastNotUptime: 0,
                      empty: false,
                      adminDuration: 0,
                      unknownDuration: 0,
                      upDuration: 150,
                      notUpDuration: 0
                  ]
              ]
          ]
      ]

      assertEquals(expected.size(), response.data.size())

      assertEquals(expected.availability.size(), response.data.availability.size())
      assertEquals(expected.availability.A2.size(), response.data.availability.A2.size())
      assertAvailablityBucketEquals("The data for A2[0] does not match", expected.availability.A2[0],
          response.data.availability.A2[0])
      assertAvailablityBucketEquals("The data for A2[1] does not match", expected.availability.A2[1],
          response.data.availability.A2[1])
      assertEquals(expected.availability.A3.size(), response.data.availability.A3.size())
      assertAvailablityBucketEquals('The data for A3[0] does not match', expected.availability.A3[0],
          response.data.availability.A3[0])
      assertAvailablityBucketEquals('The data for A3[1] does not match', expected.availability.A3[1],
          response.data.availability.A3[1])
    })
  }

  @Test
  void fetchGaugeAndCounterRateStatsByTags() {
    String tenantId = nextTenantId()
    withDataPoints(tenantId, {
      // now query by tags instead of ids
      def response = hawkularMetrics.post(
          path: 'metrics/stats/query',
          headers: [(tenantHeaderName): tenantId],
          body: [
              buckets: 2,
              start: 200,
              end: 500,
              tags: 'z:1',
              types: ['gauge', 'counter_rate']
          ]
      )
      assertEquals(200, response.status)

      def expected = [
          gauge: [
              G1: [
                  [
                      start: 200,
                      end: 350,
                      samples: 2,
                      max: 5.34,
                      min: 3.45,
                      avg: avg([3.45, 5.34]),
                      sum: 3.45 + 5.34,
                      median: median([3.45, 5.34]),
                      empty: false
                  ],
                  [
                      start: 350,
                      end: 500,
                      max: 2.22,
                      min: 2.22,
                      avg: 2.22,
                      median: 2.22,
                      sum: 2.22,
                      samples: 1,
                      empty: false
                  ]
              ],
              G3: [
                  [
                      start: 200,
                      end: 350,
                      max: 5.55,
                      min: 4.44,
                      avg: avg([5.55, 4.44]),
                      median: median([5.55, 4.44]),
                      sum: 5.55 + 4.44,
                      samples: 2,
                      empty: false
                  ],
                  [
                      start: 350,
                      end: 500,
                      max: 3.33,
                      min: 3.33,
                      avg: 3.33,
                      median: 3.33,
                      sum: 3.33,
                      samples: 1,
                      empty: false
                  ]
              ]
          ],
          counter_rate: [
              C2: [
                  [
                      start: 200,
                      end: 350,
                      max: 9000,
                      avg: 9000,
                      min: 9000,
                      sum: 9000,
                      median: 9000,
                      samples: 1,
                      empty: false
                  ],
                  [
                      start: 350,
                      end: 500,
                      max: 4200,
                      min: 4200,
                      avg: 4200,
                      sum: 4200,
                      median: 4200,
                      samples: 1,
                      empty: false
                  ]
              ],
              C3: [
                  [
                      start: 200,
                      end: 350,
                      max: 4200,
                      min: 4200,
                      avg: 4200,
                      sum: 4200,
                      median: 4200,
                      samples: 1,
                      empty: false
                  ],
                  [
                      start: 350,
                      end: 500,
                      max: 4200,
                      min: 4200,
                      avg: 4200,
                      sum: 4200,
                      median: 4200,
                      samples: 1,
                      empty: false
                  ]
              ]
          ]
      ]

      assertEquals(expected.size(), response.data.size())
      assertEquals(expected.gauge.size(), response.data.gauge.size())
      assertEquals(expected.gauge.G1.size(), response.data.gauge.G1.size())
      assertNumericBucketEquals('The data for G1[0] does not match', expected.gauge.G1[0], response.data.gauge.G1[0])
      assertNumericBucketEquals('The data for G1[1] does not match', expected.gauge.G1[1], response.data.gauge.G1[1])
      assertEquals(expected.gauge.G3.size(), response.data.gauge.G3.size())
      assertNumericBucketEquals('The data for G3[0] does not match', expected.gauge.G3[0], response.data.gauge.G3[0])

      assertEquals(expected.counter_rate.size(), response.data.counter_rate.size())
      assertEquals(expected.counter_rate.C2.size(), response.data.counter_rate.C2.size())
      assertNumericBucketEquals('The rate data for C2[0] does not match', expected.counter_rate.C2[0],
          response.data.counter_rate.C2[0])
      assertNumericBucketEquals('The rate data for C2[1] does not match', expected.counter_rate.C2[1],
          response.data.counter_rate.C2[1])
      assertEquals(expected.counter_rate.C3.size(), response.data.counter_rate.C3.size())
      assertNumericBucketEquals('The rate data for C3[0] does not match', expected.counter_rate.C3[0],
          response.data.counter_rate.C3[0])
      assertNumericBucketEquals('The rate data for C3[1] does not match', expected.counter_rate.C3[1],
          response.data.counter_rate.C3[1])
    })
  }

  @Test
  void fetchGaugeRateAndCounterStatsByTags() {
    String tenantId = nextTenantId()
    withDataPoints(tenantId, {
      // now query by tags instead of ids
      def response = hawkularMetrics.post(
          path: 'metrics/stats/query',
          headers: [(tenantHeaderName): tenantId],
          body: [
              buckets: 2,
              start: 200,
              end: 500,
              tags: 'z:1',
              types: ['gauge_rate', 'counter']
          ]
      )
      assertEquals(200, response.status)

      def expected = [
          gauge_rate: [
              G1: [
                  [
                      start: 200,
                      end: 350,
                      max: 1134,
                      min: 1134,
                      avg: 1134,
                      median: 1134,
                      sum: 1134,
                      samples: 1,
                      empty: false
                  ],
                  [
                      start: 350,
                      end: 500,
                      max: -1872,
                      min: -1872,
                      avg: -1872,
                      median: -1872,
                      sum: -1872,
                      samples: 1,
                      empty: false
                  ]
              ],
              G3: [
                  [
                      start: 200,
                      end: 350,
                      max: -666,
                      min: -666,
                      avg: -666,
                      median: -666,
                      sum: -666,
                      samples: 1,
                      empty: false
                  ],
                  [
                      start: 350,
                      end: 500,
                      max: -666,
                      min: -666,
                      avg: -666,
                      median: -666,
                      sum: -666,
                      samples: 1,
                      empty: false
                  ]
              ]
          ],
          counter: [
              C2: [
                  [
                      start: 200,
                      end: 350,
                      max: 64,
                      min: 49,
                      avg: avg([49, 64]),
                      median: median([49, 64]),
                      sum: 49 + 64,
                      samples: 2,
                      empty: false
                  ],
                  [
                      start: 350,
                      end: 500,
                      max: 71,
                      min: 71,
                      avg: 71,
                      median: 71,
                      sum: 71,
                      samples: 1,
                      empty: false
                  ]
              ],
              C3: [
                  [
                      start: 200,
                      end: 350,
                      max: 42,
                      min: 35,
                      avg: avg([35, 42]),
                      median: median([35, 42]),
                      sum: 35 + 42,
                      samples: 2,
                      empty: false
                  ],
                  [
                      start: 350,
                      end: 500,
                      max: 49,
                      min: 49,
                      avg: 49,
                      median: 49,
                      sum: 49,
                      samples: 1,
                      empty: false
                  ]
              ]
          ]
      ]

      assertEquals(expected.size(), response.data.size())
      assertEquals(expected.counter.size(), response.data.counter.size())
      assertEquals(expected.counter.C2.size(), response.data.counter.C2.size())
      assertNumericBucketEquals('The data for C2[0] does not match', expected.counter.C2[0],
          response.data.counter.C2[0])
      assertNumericBucketEquals('The data for C2[1] does not match', expected.counter.C2[1],
          response.data.counter.C2[1])
      assertEquals(expected.counter.C3.size(), response.data.counter.C3.size())
      assertNumericBucketEquals('The data for C3[0] does not match', expected.counter.C3[0],
          response.data.counter.C3[0])
      assertNumericBucketEquals('The data for C3[1] does not match', expected.counter.C3[1],
          response.data.counter.C3[1])

      assertEquals(expected.gauge_rate.size(), response.data.gauge_rate.size())
      assertEquals(expected.gauge_rate.G1.size(), response.data.gauge_rate.G1.size())
      assertNumericBucketEquals('The rate data for G1[0] does not match', expected.gauge_rate.G1[0],
          response.data.gauge_rate.G1[0])
      assertNumericBucketEquals('The rate data for G1[1] does not match', expected.gauge_rate.G1[1],
          response.data.gauge_rate.G1[1])
    })
  }

  @Test
  void fetchGaugeAndCounterStatsWithRatesByTags() {
    String tenantId = nextTenantId()
    withDataPoints(tenantId, {
      // now query by tags instead of ids
      def response = hawkularMetrics.post(
          path: 'metrics/stats/query',
          headers: [(tenantHeaderName): tenantId],
          body: [
              buckets: 2,
              start: 200,
              end: 500,
              tags: 'z:1',
              types: ['gauge', 'gauge_rate', 'counter', 'counter_rate']
          ]
      )
      assertEquals(200, response.status)

      def expected = [
          gauge: [
              G1: [
                  [
                      start: 200,
                      end: 350,
                      samples: 2,
                      max: 5.34,
                      min: 3.45,
                      avg: avg([3.45, 5.34]),
                      sum: 3.45 + 5.34,
                      median: median([3.45, 5.34]),
                      empty: false
                  ],
                  [
                      start: 350,
                      end: 500,
                      max: 2.22,
                      min: 2.22,
                      avg: 2.22,
                      median: 2.22,
                      sum: 2.22,
                      samples: 1,
                      empty: false
                  ]
              ],
              G3: [
                  [
                      start: 200,
                      end: 350,
                      max: 5.55,
                      min: 4.44,
                      avg: avg([5.55, 4.44]),
                      median: median([5.55, 4.44]),
                      sum: 5.55 + 4.44,
                      samples: 2,
                      empty: false
                  ],
                  [
                      start: 350,
                      end: 500,
                      max: 3.33,
                      min: 3.33,
                      avg: 3.33,
                      median: 3.33,
                      sum: 3.33,
                      samples: 1,
                      empty: false
                  ]
              ]
          ],
          gauge_rate: [
              G1: [
                  [
                      start: 200,
                      end: 350,
                      max: 1134,
                      min: 1134,
                      avg: 1134,
                      median: 1134,
                      sum: 1134,
                      samples: 1,
                      empty: false
                  ],
                  [
                      start: 350,
                      end: 500,
                      max: -1872,
                      min: -1872,
                      avg: -1872,
                      median: -1872,
                      sum: -1872,
                      samples: 1,
                      empty: false
                  ]
              ],
              G3: [
                  [
                      start: 200,
                      end: 350,
                      max: -666,
                      min: -666,
                      avg: -666,
                      median: -666,
                      sum: -666,
                      samples: 1,
                      empty: false
                  ],
                  [
                      start: 350,
                      end: 500,
                      max: -666,
                      min: -666,
                      avg: -666,
                      median: -666,
                      sum: -666,
                      samples: 1,
                      empty: false
                  ]
              ]
          ],
          counter: [
              C2: [
                  [
                      start: 200,
                      end: 350,
                      max: 64,
                      min: 49,
                      avg: avg([49, 64]),
                      median: median([49, 64]),
                      sum: 49 + 64,
                      samples: 2,
                      empty: false
                  ],
                  [
                      start: 350,
                      end: 500,
                      max: 71,
                      min: 71,
                      avg: 71,
                      median: 71,
                      sum: 71,
                      samples: 1,
                      empty: false
                  ]
              ],
              C3: [
                  [
                      start: 200,
                      end: 350,
                      max: 42,
                      min: 35,
                      avg: avg([35, 42]),
                      median: median([35, 42]),
                      sum: 35 + 42,
                      samples: 2,
                      empty: false
                  ],
                  [
                      start: 350,
                      end: 500,
                      max: 49,
                      min: 49,
                      avg: 49,
                      median: 49,
                      sum: 49,
                      samples: 1,
                      empty: false
                  ]
              ]
          ],
          counter_rate: [
              C2: [
                  [
                      start: 200,
                      end: 350,
                      max: 9000,
                      avg: 9000,
                      min: 9000,
                      sum: 9000,
                      median: 9000,
                      samples: 1,
                      empty: false
                  ],
                  [
                      start: 350,
                      end: 500,
                      max: 4200,
                      min: 4200,
                      avg: 4200,
                      sum: 4200,
                      median: 4200,
                      samples: 1,
                      empty: false
                  ]
              ],
              C3: [
                  [
                      start: 200,
                      end: 350,
                      max: 4200,
                      min: 4200,
                      avg: 4200,
                      sum: 4200,
                      median: 4200,
                      samples: 1,
                      empty: false
                  ],
                  [
                      start: 350,
                      end: 500,
                      max: 4200,
                      min: 4200,
                      avg: 4200,
                      sum: 4200,
                      median: 4200,
                      samples: 1,
                      empty: false
                  ]
              ]
          ]
      ]

      assertEquals(expected.size(), response.data.size())
      assertEquals(expected.gauge.size(), response.data.gauge.size())
      assertEquals(expected.gauge.G1.size(), response.data.gauge.G1.size())
      assertNumericBucketEquals('The data for G1[0] does not match', expected.gauge.G1[0], response.data.gauge.G1[0])
      assertNumericBucketEquals('The data for G1[1] does not match', expected.gauge.G1[1], response.data.gauge.G1[1])
      assertEquals(expected.gauge.G3.size(), response.data.gauge.G3.size())
      assertNumericBucketEquals('The data for G3[0] does not match', expected.gauge.G3[0], response.data.gauge.G3[0])
      assertNumericBucketEquals('The data for G3[1] does not match', expected.gauge.G3[1], response.data.gauge.G3[1])

      assertEquals(expected.counter.size(), response.data.counter.size())
      assertEquals(expected.counter.C2.size(), response.data.counter.C2.size())
      assertNumericBucketEquals('The data for C2[0] does not match', expected.counter.C2[0],
          response.data.counter.C2[0])
      assertNumericBucketEquals('The data for C2[1] does not match', expected.counter.C2[1],
          response.data.counter.C2[1])
      assertEquals(expected.counter.C3.size(), response.data.counter.C3.size())
      assertNumericBucketEquals('The data for C3[0] does not match', expected.counter.C3[0],
          response.data.counter.C3[0])
      assertNumericBucketEquals('The data for C3[1] does not match', expected.counter.C3[1],
          response.data.counter.C3[1])

      assertEquals(expected.gauge_rate.size(), response.data.gauge_rate.size())
      assertEquals(expected.gauge_rate.G1.size(), response.data.gauge_rate.G1.size())
      assertNumericBucketEquals('The rate data for G1[0] does not match', expected.gauge_rate.G1[0],
          response.data.gauge_rate.G1[0])
      assertNumericBucketEquals('The rate data for G1[1] does not match', expected.gauge_rate.G1[1],
          response.data.gauge_rate.G1[1])

      assertEquals(expected.counter_rate.size(), response.data.counter_rate.size())
      assertEquals(expected.counter_rate.C2.size(), response.data.counter_rate.C2.size())
      assertNumericBucketEquals('The rate data for C2[0] does not match', expected.counter_rate.C2[0],
          response.data.counter_rate.C2[0])
      assertNumericBucketEquals('The rate data for C2[1] does not match', expected.counter_rate.C2[1],
          response.data.counter_rate.C2[1])
      assertEquals(expected.counter_rate.C3.size(), response.data.counter_rate.C3.size())
      assertNumericBucketEquals('The rate data for C3[0] does not match', expected.counter_rate.C3[0],
          response.data.counter_rate.C3[0])
      assertNumericBucketEquals('The rate data for C3[1] does not match', expected.counter_rate.C3[1],
          response.data.counter_rate.C3[1])
    })
  }

  @Test
  void shouldNotFetchStatsWithoutBucketParam() {
    String tenantId = nextTenantId()

    badPost( path: "metrics/stats/query",
        body: [
            metrics: [
                counter: ['C2', 'C3'],
            ],
            start: 200,
            end: 500
        ],
        headers: [(tenantHeaderName): tenantId]) { exception ->
      assertEquals(400, exception.response.status)
    }
  }

  @Test
  void batchQueriesForGaugesAndCounterRates() {
    String tenantId = nextTenantId()

    createGauges(tenantId, [
        [id: 'G1', tags: [x: 1, y: 1, z: 1]],
        [id: 'G2', tags: [x: 1, y: 2, z: 2]],
        [id: 'G3', tags: [x: 2, y: 3, z: 1]]
    ])
    createCounters(tenantId, [
        [id: 'C1', tags: [x: 1, y: 1, z: 3]],
        [id: 'C2', tags: [x: 1, y: 2, z: 1]],
        [id: 'C3', tags: [x: 2, y: 3, z: 1]]
    ])
    insertGaugeDataPoints(tenantId, [
        [
            id: 'G1',
            data: [
                [timestamp: 100, value: 1.23],
                [timestamp: 200, value: 3.45],
                [timestamp: 300, value: 5.34],
                [timestamp: 400, value: 2.22],
                [timestamp: 500, value: 5.22]
            ]
        ],
        [
            id: 'G2',
            data: [
                [timestamp: 100, value: 1.45],
                [timestamp: 200, value: 2.36],
                [timestamp: 300, value: 3.62],
                [timestamp: 400, value: 2.63],
                [timestamp: 500, value: 3.99]
            ]
        ],
        [
            id: 'G3',
            data: [
                [timestamp: 100, value: 4.45],
                [timestamp: 200, value: 5.55],
                [timestamp: 300, value: 4.44],
                [timestamp: 400, value: 3.33],
                [timestamp: 500, value: 3.77]
            ]
        ]
    ])
    insertCounterDataPoints(tenantId, [
        [
            id: 'C1',
            data: [
                [timestamp: 100, value: 12],
                [timestamp: 200, value: 17],
                [timestamp: 300, value: 19],
                [timestamp: 400, value: 26],
                [timestamp: 500, value: 37]
            ]
        ],
        [
            id: 'C2',
            data: [
                [timestamp: 100, value: 41],
                [timestamp: 200, value: 49],
                [timestamp: 300, value: 64],
                [timestamp: 400, value: 71],
                [timestamp: 500, value: 95]
            ]
        ],
        [
            id: 'C3',
            data: [
                [timestamp: 100, value: 28],
                [timestamp: 200, value: 35],
                [timestamp: 300, value: 42],
                [timestamp: 400, value: 49],
                [timestamp: 500, value: 59]
            ]
        ]
    ])

    def response = hawkularMetrics.post(
        path: 'metrics/stats/batch/query',
        headers: [(tenantHeaderName): tenantId],
        body: [
            q1: [
                metrics: [
                    gauge: ['G1', 'G3'],
                ],
                buckets: 2,
                start: 200,
                end: 500
            ],
            q2: [
                tags: 'z = 1',
                types: ['counter_rate'],
                buckets: 2,
                start: 200,
                end: 500
            ]
        ]
    )
    assertEquals(200, response.status)

    def expected = [
        q1: [
            gauge: [
                G1: [
                    [
                        start: 200,
                        end: 350,
                        samples: 2,
                        max: 5.34,
                        min: 3.45,
                        avg: avg([3.45, 5.34]),
                        sum: 3.45 + 5.34,
                        median: median([3.45, 5.34]),
                        empty: false
                    ],
                    [
                        start: 350,
                        end: 500,
                        max: 2.22,
                        min: 2.22,
                        avg: 2.22,
                        median: 2.22,
                        sum: 2.22,
                        samples: 1,
                        empty: false
                    ]
                ],
                G3: [
                    [
                        start: 200,
                        end: 350,
                        max: 5.55,
                        min: 4.44,
                        avg: avg([5.55, 4.44]),
                        median: median([5.55, 4.44]),
                        sum: 5.55 + 4.44,
                        samples: 2,
                        empty: false
                    ],
                    [
                        start: 350,
                        end: 500,
                        max: 3.33,
                        min: 3.33,
                        avg: 3.33,
                        median: 3.33,
                        sum: 3.33,
                        samples: 1,
                        empty: false
                    ]
                ]
            ]
        ],
        q2: [
            counter_rate: [
                C2: [
                    [
                        start: 200,
                        end: 350,
                        max: 9000,
                        avg: 9000,
                        min: 9000,
                        sum: 9000,
                        median: 9000,
                        samples: 1,
                        empty: false
                    ],
                    [
                        start: 350,
                        end: 500,
                        max: 4200,
                        min: 4200,
                        avg: 4200,
                        sum: 4200,
                        median: 4200,
                        samples: 1,
                        empty: false
                    ]
                ],
                C3: [
                    [
                        start: 200,
                        end: 350,
                        max: 4200,
                        min: 4200,
                        avg: 4200,
                        sum: 4200,
                        median: 4200,
                        samples: 1,
                        empty: false
                    ],
                    [
                        start: 350,
                        end: 500,
                        max: 4200,
                        min: 4200,
                        avg: 4200,
                        sum: 4200,
                        median: 4200,
                        samples: 1,
                        empty: false
                    ]
                ]
            ]
        ]
    ]
    assertEquals(expected.size(), response.data.size())
    assertEquals(expected.q1.size(), response.data.q1.size())
    assertEquals(expected.q1.gauge.G1.size(), response.data.q1.gauge.G1.size())
    assertNumericBucketEquals('The data for G1[0] does not match', expected.q1.gauge.G1[0],
        response.data.q1.gauge.G1[0])
    assertNumericBucketEquals('The data for G1[1] does not match', expected.q1.gauge.G1[1],
        response.data.q1.gauge.G1[1])
    assertEquals(expected.q1.gauge.G3.size(), response.data.q1.gauge.G3.size())
    assertNumericBucketEquals('The data for G3[0] does not match', expected.q1.gauge.G3[0],
        response.data.q1.gauge.G3[0])
    assertNumericBucketEquals('The data for G3[1] does not match', expected.q1.gauge.G3[1],
        response.data.q1.gauge.G3[1])

    printJson("Q2: " + response.data.q2)

    assertEquals(expected.q2.counter_rate.size(), response.data.q2.counter_rate.size())
    assertEquals(expected.q2.counter_rate.C2.size(), response.data.q2.counter_rate.C2.size())
    assertNumericBucketEquals('The rate data for C2[0] does not match', expected.q2.counter_rate.C2[0],
        response.data.q2.counter_rate.C2[0])
    assertNumericBucketEquals('The rate data for C2[1] does not match', expected.q2.counter_rate.C2[1],
        response.data.q2.counter_rate.C2[1])
    assertEquals(expected.q2.counter_rate.C3.size(), response.data.q2.counter_rate.C3.size())
    assertNumericBucketEquals('The rate data for C3[0] does not match', expected.q2.counter_rate.C3[0],
        response.data.q2.counter_rate.C3[0])
    assertNumericBucketEquals('The rate data for C3[1] does not match', expected.q2.counter_rate.C3[1],
        response.data.q2.counter_rate.C3[1])
  }

    @Test
    void relativeTimeStamps() {
        String tenantId = nextTenantId()
        DateTime start = DateTime.now().minusMinutes(10)

        def createResponse = hawkularMetrics.post(
                path: "metrics/raw",
                headers: [(tenantHeaderName): tenantId],
                body: [
                        counters: [
                                [
                                        id: 'RC',
                                        data: [
                                                [timestamp: start.millis, value: 10],
                                                [timestamp: start.plusMinutes(5).millis, value: 20]
                                        ]
                                ]
                        ],
                        gauges: [
                                [
                                        id: 'RG',
                                        data: [
                                                [timestamp: start.millis, value: 25.4],
                                                [timestamp: start.plusMinutes(5).millis, value: 15.8]
                                        ]
                                ]
                        ]
                ]
        )
        assertEquals(200, createResponse.status)

        // now query by tags instead of ids
        def response = hawkularMetrics.post(
                path: 'metrics/stats/query',
                headers: [(tenantHeaderName): tenantId],
                body: [
                        buckets: 1,
                        start: "-11mn",
                        end: "-9mn",
                        metrics: [
                                gauge: ['RG'],
                                counter: ['RC']
                        ]
                ]
        )

        def expected = [
                gauge: [
                        RG: [
                                [
                                        start: start.minusMinutes(1).millis,
                                        end: start.plusMinutes(1).millis,
                                        max: 25.4,
                                        min: 25.4,
                                        avg: 25.4,
                                        median: 25.4,
                                        sum: 25.4,
                                        samples: 1,
                                        empty: false
                                ]
                        ]
                ],
                counter: [
                        RC: [
                                [
                                        start: start.minusMinutes(1).millis,
                                        end: start.plusMinutes(1).millis,
                                        max: 10,
                                        min: 10,
                                        avg: 10,
                                        median: 10,
                                        sum: 10,
                                        samples: 1,
                                        empty: false
                                ]
                        ]
                ]
        ]

        assertEquals(expected.gauge.size(), response.data.gauge.size())
        assertEquals(expected.gauge.RG.size(), response.data.gauge.RG.size())
        assertEquals(expected.gauge.RG[0].avg, response.data.gauge.RG[0].avg)
        assertEquals(1, response.data.gauge.RG[0].samples)
        assertEquals(expected.counter.size(), response.data.counter.size())
        assertEquals(expected.counter.RC.size(), response.data.counter.RC.size())
        assertEquals(expected.counter.RC[0].avg, response.data.counter.RC[0].avg, 0)
        assertEquals(1, response.data.counter.RC[0].samples)

        // now query by tags instead of ids
        response = hawkularMetrics.post(
                path: 'metrics/stats/query',
                headers: [(tenantHeaderName): tenantId],
                body: [
                        buckets: 1,
                        start: "-6mn",
                        end: "-4mn",
                        metrics: [
                                gauge: ['RG'],
                                counter: ['RC']
                        ]
                ]
        )

        expected = [
                gauge: [
                        RG: [
                                [
                                        start: start.minusMinutes(1).millis,
                                        end: start.plusMinutes(1).millis,
                                        max: 15.8,
                                        min: 15.8,
                                        avg: 15.8,
                                        median: 15.8,
                                        sum: 15.8,
                                        samples: 1,
                                        empty: false
                                ]
                        ]
                ],
                counter: [
                        RC: [
                                [
                                        start: start.minusMinutes(1).millis,
                                        end: start.plusMinutes(1).millis,
                                        max: 20,
                                        min: 20,
                                        avg: 20,
                                        median: 20,
                                        sum: 20,
                                        samples: 1,
                                        empty: false
                                ]
                        ]
                ]
        ]

        assertEquals(expected.gauge.size(), response.data.gauge.size())
        assertEquals(expected.gauge.RG.size(), response.data.gauge.RG.size())
        assertEquals(expected.gauge.RG[0].avg, response.data.gauge.RG[0].avg)
        assertEquals(1, response.data.gauge.RG[0].samples)
        assertEquals(expected.counter.size(), response.data.counter.size())
        assertEquals(expected.counter.RC.size(), response.data.counter.RC.size())
        assertEquals(expected.counter.RC[0].avg, response.data.counter.RC[0].avg, 0)
        assertEquals(1, response.data.counter.RC[0].samples)
    }

  @Test
  void createAndDeleteMetrics() {
    def dataPointsMap = [:]

    dataPointsMap['gauge'] = [1.2D, 2.3D, 3.4D, 4.5D]
    dataPointsMap['counter'] = [12L, 23L, 34L, 45L]
    dataPointsMap['availability'] = ['down', 'up', 'up', 'down']
    dataPointsMap['string'] = ['1.2d', '2.3d', '3.4d', '4.5d' ]

    metricTypes.each {
      String tenantId = nextTenantId()

      def metricType = it
      final int iterations = 4

      def mList = [];
      for (int i = 0; i < iterations; i++) {
          def id = metricType.type + '-test-' + i

          def tags = [:];
          for (int j = 0; j < iterations; j++) {
              tags.put('test' + j, 'test' +  metricType.type + j);
          }

          def dataPoints = [];
          for (int j = 0; j < dataPointsMap[metricType.type].size; j++) {
              def timestamp = j + 1
              dataPoints.add([timestamp: timestamp, value: dataPointsMap[metricType.type][j]])
          }

          def response = hawkularMetrics.post(
            path: metricType.path, body: [
            id: id,
            tags: tags
          ], headers: [(tenantHeaderName): tenantId])
          assertEquals(201, response.status)

          response = hawkularMetrics.post(
            path: "${metricType.path}/${id}/raw",
            headers: [(tenantHeaderName): tenantId],
            body: dataPoints
          )
          assertEquals(200, response.status)

          mList.add([id: id, tags: tags, dataPoints: dataPoints]);
      }

      def deletedMetrics = [];
      mList.each {
        def metric= it;

        def response = hawkularMetrics.get(path: "${metricType.path}/${metric.id}", headers: [(tenantHeaderName): tenantId])
        assertEquals(200, response.status)
        assertContentEncoding(response)
        assertEquals(metric.tags, response.data.tags)

        response = hawkularMetrics.get(
          path: "${metricType.path}/${metric.id}/raw",
          headers: [(tenantHeaderName): tenantId],
          query: [order: 'asc', start: 0, end: 100]
        )
        assertEquals(200, response.status)
        assertContentEncoding(response)
        assertEquals(metric.dataPoints.toString(), response.data.toString())

        response = hawkularMetrics.delete(path: "${metricType.path}/${metric.id}", headers: [(tenantHeaderName): tenantId])
        assertEquals(200, response.status)
        deletedMetrics.add(metric.id);

        mList.each {
          def checkMetric= it;

          response = hawkularMetrics.get(path: "${metricType.path}/${checkMetric.id}", headers: [(tenantHeaderName): tenantId])
          if (deletedMetrics.contains(checkMetric.id)) {
              assertEquals(204, response.status)
          } else {
            assertEquals(200, response.status)
            assertContentEncoding(response)
            assertEquals(checkMetric.tags, response.data.tags)
          }

          response = hawkularMetrics.get(
            path: "${metricType.path}/${checkMetric.id}/raw",
            headers: [(tenantHeaderName): tenantId],
            query: [order: 'asc', start: 0, end: 100]
          )
          if (deletedMetrics.contains(checkMetric.id)) {
            assertEquals(204, response.status)
          } else {
            assertEquals(200, response.status)
            assertContentEncoding(response)
            assertEquals(checkMetric.dataPoints.toString(), response.data.toString())
          }
        }
      }
    }
  }
}