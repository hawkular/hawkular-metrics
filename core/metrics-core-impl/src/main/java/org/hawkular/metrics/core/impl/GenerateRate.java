/*
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
package org.hawkular.metrics.core.impl;

import static java.util.Collections.singletonList;
import static org.hawkular.metrics.core.api.MetricType.COUNTER;
import static org.hawkular.metrics.core.api.MetricType.COUNTER_RATE;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.hawkular.metrics.core.api.DataPoint;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.MetricsService;
import org.hawkular.metrics.tasks.api.Task2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Action1;

/**
 * @author jsanda
 */
public class GenerateRate implements Action1<Task2> {

    private static final Logger logger = LoggerFactory.getLogger(GenerateRate.class);

    private MetricsService metricsService;

    public GenerateRate(MetricsService metricsService) {
        this.metricsService = metricsService;
    }

    @Override
    public void call(Task2 task) {
        logger.info("Generating rate for {}", task);
        MetricId id = new MetricId(task.getParameters().get("tenantId"), COUNTER, task.getParameters().get("metricId"));
        long start = task.getTrigger().getTriggerTime();
        long end = start + TimeUnit.MINUTES.toMillis(1);

        CountDownLatch latch = new CountDownLatch(1);

        logger.debug("start = {}, end = {}", start, end);
        metricsService.findCounterData(id, start, end)
                .take(1)
                .doOnNext(dataPoint -> logger.debug("Data Point = {}", dataPoint))
                .map(dataPoint -> ((dataPoint.getValue().doubleValue() / (end - start) * 1000)))
                .map(rate -> new Metric<>(new MetricId(id.getTenantId(), COUNTER_RATE, id.getName()),
                        singletonList(new DataPoint<>(start, rate))))
                .flatMap(metric -> metricsService.addGaugeData(Observable.just(metric)))
                .subscribe(
                        aVoid -> {
                        },
                        t -> {
                            logger.warn("Failed to persist rate data", t);
                            latch.countDown();
                        },
                        () -> {
                            logger.debug("Successfully persisted rate data");
                            latch.countDown();
                        }
                );
        try {
            latch.await();
        } catch (InterruptedException e) {
        }
    }

}
