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
package org.hawkular.metrics.core.service.metrics;

import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import static org.hawkular.metrics.model.MetricType.GAUGE;
import static org.joda.time.DateTime.now;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.jboss.logging.Logger;
import org.joda.time.DateTime;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

/**
 * @author jsanda
 */
public class CacheITest extends BaseMetricsITest {

    private static Logger logger = Logger.getLogger(CacheITest.class);

    MetricRegistry registry;
    Meter inserts;
    Timer insertTime;
    ConsoleReporter consoleReporter;

    @BeforeClass
    public void init() throws Exception {
        registry = new MetricRegistry();
        inserts = registry.meter("inserts");
        insertTime = registry.timer("insert-time");

        File logDir = new File("target");
        consoleReporter = ConsoleReporter.forRegistry(registry)
                .convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(MILLISECONDS)
                .outputTo(new PrintStream(new FileOutputStream(new File(logDir, "metrics.txt")))).build();
        consoleReporter.start(20, TimeUnit.SECONDS);
    }

    @AfterClass
    public void shutdown() {
        consoleReporter.stop();
    }

    @Test
    public void updateCache() {
        String tenant = "UpdateCacheTest";
        List<Metric<Double>> metrics = new ArrayList<>();
        for (int i = 0; i < 1000; ++i) {
            MetricId<Double> id = new MetricId<>(tenant, GAUGE, "G" + i);
            metrics.add(new Metric<>(id, singletonList(new DataPoint<>(System.currentTimeMillis(), 3.14))));
        }
        metrics.forEach(metric -> cacheService.update(metric));
    }

//    @Test
    public void populateCache() {
        String tenant = "CacheTest";
        List<Metric<Double>> metrics = new ArrayList<>();
        DateTime end = now().plusMinutes(1);

        while (now().isBefore(end)) {
            for (int i = 0; i < 1000; ++i) {
                MetricId<Double> id = new MetricId<>(tenant, GAUGE, "G" + i);
                metrics.add(new Metric<>(id, singletonList(new DataPoint<>(System.currentTimeMillis(), 3.14))));
            }
            insertMetrics(metrics);
        }
    }

    private void insertMetrics(List<Metric<Double>> metrics) {
        com.google.common.base.Stopwatch stopwatch = com.google.common.base.Stopwatch.createStarted();
        cacheService.putAll(metrics).subscribe(
                () -> {
                    stopwatch.stop();
                    inserts.mark(metrics.size());
                    insertTime.update(stopwatch.elapsed(MILLISECONDS), MILLISECONDS);
                },
                t -> logger.warn("Cache updates failed", t)
        );
    }

}
