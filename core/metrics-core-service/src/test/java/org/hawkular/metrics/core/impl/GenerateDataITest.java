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
package org.hawkular.metrics.core.impl;

import static java.util.Arrays.asList;

import static org.hawkular.metrics.model.MetricType.GAUGE;
import static org.testng.Assert.assertNull;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.hawkular.metrics.core.service.metrics.BaseMetricsITest;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.jboss.logging.Logger;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import rx.Observable;

/**
 * This test is disabled because it is really just a utility for generating a bunch of data.
 *
 * @author jsanda
 */
public class GenerateDataITest extends BaseMetricsITest {

    private static Logger logger = Logger.getLogger(GenerateDataITest.class);

    @Test(enabled = false)
    public void generateData() throws Exception {
        int numTenants = 50;
        int metricsPerTenant = 50;
        AtomicReference<Throwable> exceptionRef = new AtomicReference<>();
        DateTime end = DateTime.now();
        DateTime time = end.minusDays(3).minusHours(1);

        while (time.isBefore(end)) {
            CountDownLatch latch = new CountDownLatch(1);
            List<Observable<Void>> inserted = new ArrayList<>();
            for (int i = 0; i < numTenants; ++i) {
                List<Metric<Double>> metrics = new ArrayList<>();
                for (int j = 0; j < metricsPerTenant; ++j) {
                    List<DataPoint<Double>> dataPoints = asList(
                            new DataPoint<>(time.getMillis(), 3.14),
                            new DataPoint<>(time.plusSeconds(10).getMillis(), 3.14),
                            new DataPoint<>(time.plusSeconds(20).getMillis(), 3.14),
                            new DataPoint<>(time.plusSeconds(30).getMillis(), 3.14),
                            new DataPoint<>(time.plusSeconds(40).getMillis(), 3.14),
                            new DataPoint<>(time.plusSeconds(50).getMillis(), 3.14),
                            new DataPoint<>(time.plusSeconds(60).getMillis(), 3.14),
                            new DataPoint<>(time.plusSeconds(70).getMillis(), 3.14),
                            new DataPoint<>(time.plusSeconds(80).getMillis(), 3.14),
                            new DataPoint<>(time.plusSeconds(90).getMillis(), 3.14),
                            new DataPoint<>(time.plusSeconds(100).getMillis(), 3.14),
                            new DataPoint<>(time.plusSeconds(110).getMillis(), 3.14),
                            new DataPoint<>(time.plusSeconds(120).getMillis(), 3.14),
                            new DataPoint<>(time.plusSeconds(130).getMillis(), 3.14),
                            new DataPoint<>(time.plusSeconds(140).getMillis(), 3.14),
                            new DataPoint<>(time.plusSeconds(150).getMillis(), 3.14),
                            new DataPoint<>(time.plusSeconds(160).getMillis(), 3.14),
                            new DataPoint<>(time.plusSeconds(170).getMillis(), 3.14),
                            new DataPoint<>(time.plusSeconds(180).getMillis(), 3.14),
                            new DataPoint<>(time.plusSeconds(190).getMillis(), 3.14),
                            new DataPoint<>(time.plusSeconds(200).getMillis(), 3.14),
                            new DataPoint<>(time.plusSeconds(210).getMillis(), 3.14),
                            new DataPoint<>(time.plusSeconds(220).getMillis(), 3.14),
                            new DataPoint<>(time.plusSeconds(230).getMillis(), 3.14),
                            new DataPoint<>(time.plusSeconds(240).getMillis(), 3.14),
                            new DataPoint<>(time.plusSeconds(250).getMillis(), 3.14),
                            new DataPoint<>(time.plusSeconds(260).getMillis(), 3.14),
                            new DataPoint<>(time.plusSeconds(270).getMillis(), 3.14),
                            new DataPoint<>(time.plusSeconds(280).getMillis(), 3.14),
                            new DataPoint<>(time.plusSeconds(290).getMillis(), 3.14)
                    );
                    metrics.add(new Metric<>(new MetricId<>("T" + i, GAUGE, "M" + j), dataPoints));
                }
                inserted.add(metricsService.addDataPoints(GAUGE, Observable.from(metrics)));
            }
            Observable.merge(inserted).subscribe(
                    aVoid -> {},
                    t -> {
                        logger.error("Inserting data failed", t);
                        exceptionRef.set(t);
                        latch.countDown();
                    },
                    latch::countDown
            );
            latch.await();
            assertNull(exceptionRef.get());
            time = time.plusMinutes(5);
            logger.info("Current time is [" + time.toDate() + "]");
        }
    }

}
