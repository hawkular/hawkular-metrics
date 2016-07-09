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

import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertTrue

/**
 * @author jsanda
 */
class MetricsITest extends RESTTest {

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

  def withGaugeAndCounterData(String tenantId, Closure callback) {
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
    callback()
  }

  @Test
  void fetchStatsFromGaugesAndCounters() {
    String tenantId = nextTenantId()

    withGaugeAndCounterData(tenantId, {
      def response = hawkularMetrics.post(
          path: 'metrics/stats/query',
          headers: [(tenantHeaderName): tenantId],
          body: [
              metrics: [
                  gauge: ['G1', 'G3'],
                  counter: ['C2', 'C3']
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
    })
  }

  @Test
  void fetchStatsFromGauges() {
    String tenantId = nextTenantId()
    withGaugeAndCounterData(tenantId, {
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
          ],
          counter: []
      ]
      assertEquals(expected.size(), response.data.size())
      assertTrue(response.data.counter.isEmpty())
      assertEquals(expected.gauge.G1.size(), response.data.gauge.G1.size())
      assertNumericBucketEquals('The data for G1[0] does not match', expected.gauge.G1[0], response.data.gauge.G1[0])
      assertNumericBucketEquals('The data for G1[1] does not match', expected.gauge.G1[1], response.data.gauge.G1[1])
      assertEquals(expected.gauge.G3.size(), response.data.gauge.G3.size())
      assertNumericBucketEquals('The data for G3[0] does not match', expected.gauge.G3[0], response.data.gauge.G3[0])
      assertNumericBucketEquals('The data for G3[1] does not match', expected.gauge.G3[1], response.data.gauge.G3[1])
    })
  }

  @Test
  void fetchStatsFromCounters() {
    String tenantId = nextTenantId()
    withGaugeAndCounterData(tenantId, {
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
          gauge: [],
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
      printJson(response.data)
      assertEquals(expected.size(), response.data.size())
      assertEquals(expected.gauge.size(), response.data.gauge.size())

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
}