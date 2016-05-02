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

import com.datastax.driver.core.Cluster
import org.hawkular.metrics.core.service.DataAccess
import org.hawkular.metrics.core.service.DataAccessImpl
import org.hawkular.metrics.model.DataPoint
import org.hawkular.metrics.model.Metric
import org.hawkular.metrics.model.MetricId
import org.joda.time.DateTime
import org.junit.Test

import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit

import static org.hawkular.metrics.model.MetricType.GAUGE
import static org.joda.time.DateTime.now
import static org.joda.time.Days.days
import static org.junit.Assert.assertEquals
/**
 * @author jsanda
 */
class StressITest extends RESTTest {

  @Test
  void stressReads() {
    Cluster cluster = new Cluster.Builder().addContactPoint("127.0.0.1").build()
    def session = cluster.connect("hawkular_metrics_rest_tests")
    DataAccess dataAccess = new DataAccessImpl(session)
    DateTime start = now().minusYears(1).minusMonths(3)
    DateTime time = start
    DateTime end = now()
    def metricId = new MetricId<Double>("STRESS", GAUGE, "G1")
    def inflightRequests = new Semaphore(150)
    def dataPoints = 0
    def abort = false
    def exception = null

    def concurrentReads = 25
    def executors = Executors.newFixedThreadPool(concurrentReads)

    while (time.isBefore(end)) {
      if (abort) {
        throw exception
      }
      def metric = new Metric<>(metricId, [
          new DataPoint(time.millis, 3.14 as double),
          new DataPoint(time.plusSeconds(15).millis, 3.14 as double),
          new DataPoint(time.plusSeconds(30).millis, 3.14 as double),
          new DataPoint(time.plusSeconds(45).millis, 3.14 as double)
      ])
      inflightRequests.acquire()
      def result = dataAccess.insertGaugeData(metric, days(10).toStandardSeconds().seconds)
      time = time.plusMinutes(1)
      result.subscribe({ dataPoints++; inflightRequests.release(); }, {t -> abort = true; exception = t})
    }
    println "DATA POINTS WRITTEN = $dataPoints"

    def requests = new CountDownLatch(concurrentReads)
    concurrentReads.each {
      executors.execute({
        try {
          def response = hawkularMetrics.get(
              path: "gauges/G1/raw/streaming",
              headers: [(tenantHeaderName): 'STRESS'],
              query: [start: start.millis, end: end.millis])
          assertEquals(200, response.status)
          println "FINISHED READ"
          requests.countDown()
        } catch (Exception e) {
          e.printStackTrace()
          executors.shutdown()
        }
      })
    }
    requests.await(3, TimeUnit.MINUTES)
  }

}
