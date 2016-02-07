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
package org.hawkular.metrics.core.service;

import static java.util.Collections.singletonList;

import static org.hawkular.metrics.model.MetricType.COUNTER;
import static org.hawkular.metrics.model.MetricType.COUNTER_RATE;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.hawkular.metrics.core.service.log.CoreLogger;
import org.hawkular.metrics.core.service.log.CoreLogging;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.tasks.api.Task2;

import rx.Observable;
import rx.functions.Action1;

/**
 * Calculates and persists rates for all counter metrics of a single tenant.
 */
public class GenerateRate implements Action1<Task2> {
    private static final CoreLogger log = CoreLogging.getCoreLogger(GenerateRate.class);

    public static final String TASK_NAME = "generate-rates";

    private MetricsService metricsService;

    public GenerateRate(MetricsService metricsService) {
        this.metricsService = metricsService;
    }

    @Override
    public void call(Task2 task) {
        // TODO We need to make this fault tolerant. See HWKMETRICS-213 for details.
        log.debugf("Generating rate for %s", task);
        String tenant = task.getParameters().get("tenant");
        long start = task.getTrigger().getTriggerTime();
        long end = start + TimeUnit.MINUTES.toMillis(1);

        Observable<Metric<Double>> rates = metricsService.<Long> findMetrics(tenant, COUNTER)
                .flatMap(counter ->
                        metricsService.<Long> findDataPoints(counter.getMetricId(), start, end, 0, Order.DESC)
                        .take(1)
                        .map(dataPoint -> ((dataPoint.getValue().doubleValue() / (end - start) * 1000 * 60)))
                        .map(rate -> new Metric<>(
                                new MetricId<>(tenant, COUNTER_RATE, counter.getMetricId().getName()),
                                singletonList(new DataPoint<>(start, rate)))));
        Observable<Void> updates = metricsService.addDataPoints(COUNTER_RATE, rates);

        CountDownLatch latch = new CountDownLatch(1);

        updates.subscribe(
                aVoid -> {
                },
                t -> {
                    log.warnFailedToPersistRates(tenant, start, end, t);
                    latch.countDown();
                },
                () -> {
                    if (log.isDebugEnabled()) {
                        log.debug("Successfully persisted rate data for {tenant= " + tenant + ", start= " + start +
                                ", end= " + end + "}");
                    }
                    latch.countDown();
                }
        );

        // TODO We do not want to block but have to for now. See HWKMETRICS-214 for details.
        try {
            latch.await();
        } catch (InterruptedException ignored) {
        }
    }

}
