/*
 * Copyright 2016 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.core.service;

import static java.util.Arrays.asList;

import static org.hawkular.metrics.model.MetricType.GAUGE;
import static org.joda.time.DateTime.now;
import static org.testng.Assert.fail;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.hawkular.metrics.datetime.DateTimeService;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.RateLimiter;

import rx.Observable;

/**
 * @author jsanda
 */
public class StressITest extends MetricsITest {

    @Test
    public void stress() {
        DataAccess dataAccess = new DataAccessImpl(session);
        DateTimeService dateTimeService = new DateTimeService();
        MetricsServiceImpl metricsService = new MetricsServiceImpl();
        metricsService.setDataAccess(dataAccess);
        metricsService.setTaskScheduler(new FakeTaskScheduler());
        metricsService.setDateTimeService(dateTimeService);
        metricsService.startUp(session, getKeyspace(), true, new MetricRegistry());
        metricsService.setDefaultTTL(365);

        RateLimiter rateLimiter = RateLimiter.create(70000);
        DateTime end = now();
        DateTime time = end.minusMonths(6);
        String tenantPrefix = "STRESS-" + System.currentTimeMillis();
        Random random = new Random();
        int numTenants = 1000;
        AtomicInteger pending = new AtomicInteger();

        while (time.isBefore(end)) {
            for (int i = 0; i < numTenants; ++i) {
                String tenantId = tenantPrefix + "-" + i;
                MetricId id1 = new MetricId<>(tenantId, GAUGE, "G1");
                MetricId id2 = new MetricId<>(tenantId, GAUGE, "G2");
                MetricId id3 = new MetricId<>(tenantId, GAUGE, "G3");

                rateLimiter.acquire();
                Observable<Void> observable = metricsService.addDataPoints(GAUGE, Observable.just(
                        new Metric<>(id1, asList(new DataPoint<>(time.getMillis(),
                                random.nextDouble()), new DataPoint<>(time.plusSeconds(30).getMillis(),
                                random.nextDouble()))),
                        new Metric<>(id2, asList(new DataPoint<>(time.getMillis(),
                                random.nextDouble()), new DataPoint<>(time.plusSeconds(30).getMillis(),
                                random.nextDouble()))),
                        new Metric<>(id3, asList(new DataPoint<>(time.getMillis(),
                                random.nextDouble()), new DataPoint<>(time.plusSeconds(30).getMillis(),
                                random.nextDouble())))
                ));
                observable.subscribe(
                        aVoid -> {},
                        t -> fail("Data insert failed!", t)
                );
            }

            time = time.plusMinutes(1);
        }
    }

}
