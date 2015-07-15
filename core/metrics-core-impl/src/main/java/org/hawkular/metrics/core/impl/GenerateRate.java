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
import static org.hawkular.metrics.core.api.MetricType.COUNTER_RATE;
import static org.joda.time.Duration.standardMinutes;
import static org.joda.time.Duration.standardSeconds;

import org.hawkular.metrics.core.api.DataPoint;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.MetricsService;
import org.hawkular.metrics.tasks.api.Task;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Action1;

/**
 * @author jsanda
 */
public class GenerateRate implements Action1<Task> {

    private static final Logger logger = LoggerFactory.getLogger(GenerateRate.class);

    private MetricsService metricsService;

    public GenerateRate(MetricsService metricsService) {
        this.metricsService = metricsService;
    }

    @Override
    public void call(Task task) {
        logger.info("Generating rate for {}", task);
        MetricId id = new MetricId(task.getSources().iterator().next());
        long start = task.getTimeSlice().getMillis();
        long end = task.getTimeSlice().plus(getDuration(task.getWindow())).getMillis();
        metricsService.findCounterData(task.getTenantId(), id, start, end)
                .take(1)
                .map(dataPoint -> dataPoint.getValue().doubleValue() / (end - start) * 1000)
                .map(rate -> new Metric<>(task.getTenantId(), COUNTER_RATE, id,
                        singletonList(new DataPoint<>(start, rate))))
                .flatMap(metric -> metricsService.addGaugeData(Observable.just(metric)))
                .subscribe(
                        aVoid -> {
                        },
                        t -> logger.warn("Failed to persist rate data", t),
                        () -> logger.debug("Successfully persisted rate data for {}", task)
                );
    }

    private Duration getDuration(int duration) {
        // This is somewhat of a temporary hack until HWKMETRICS-142 is done. The time units
        // for tasks are currently scheduled globally with the TaskServiceBuilder class. The
        // system property below is the only hook we have right now for specifying and
        // checking the time units used.
        if ("seconds".equals(System.getProperty("hawkular.scheduler.time-units", "minutes"))) {
            return standardSeconds(duration);
        }
        return standardMinutes(duration);
    }
}
