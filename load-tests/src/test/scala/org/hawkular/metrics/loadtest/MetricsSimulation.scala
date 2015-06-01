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
package org.hawkular.metrics.loadtest

import scala.concurrent.duration._

import io.gatling.core.Predef._
import io.gatling.http.Predef._
import io.gatling.jdbc.Predef._

class MetricsSimulation extends Simulation {

  // --------------------------- Options

  val baseURI = System.getProperty("baseURI", "http://localhost:8080")
  val tenant = System.getProperty("tenant", "default")

  // Number of concurrent clients (think of collectors on different machines)
  val clients = Integer.getInteger("clients", 1)
  // Delay before firing up another client
  val ramp  = java.lang.Long.getLong("ramp", 0L)

  // The number of loops for each client
  val loops = Integer.getInteger("loops", 10).toInt
  // Interval between metrics reports
  val interval = Integer.getInteger("interval", 1)

  // Number of metrics in a JSON report
  val metrics = Integer.getInteger("metrics", 10)
  // Number of data points for a metric
  val points = Integer.getInteger("points", 1)

  // ---------------------------

  val httpProtocol = http
    .baseURL(baseURI)
    .header("Hawkular-Tenant", tenant)
    .contentTypeHeader("application/json;charset=utf-8")

  val random = new util.Random
  val genReport = (m: Int, p: Int) => {
    val builder = new StringBuilder
    builder += '['
    for (i <- 1 to m) {
      builder ++= """{"id":"metrics.load.test."""
      builder.append(i)
      builder ++= """.value","data":["""
      for (j <- 1 to p) {
        builder ++= """{"timestamp":"""
        builder.append(System.currentTimeMillis)
        builder ++= ""","value":"""
        builder.append(random.nextDouble)
        builder += '}'
        if (j < p) builder += ','
      }
      builder ++= "]}"
      if (i < m) builder += ','
    }
    builder += ']'
    builder.toString
  }

  val simulation = repeat(loops, "n") {
    exec(http("Report ${n}")
      .post("/hawkular/metrics/gauges/data")
      .body(StringBody(session => genReport(metrics, points)))
    ).pause(interval)
  }

  val scn = scenario("MetricsSimulation").exec(simulation)

  setUp(scn.inject(rampUsers(clients) over (ramp seconds))).protocols(httpProtocol)
}

