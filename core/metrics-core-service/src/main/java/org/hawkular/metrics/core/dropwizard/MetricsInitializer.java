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
package org.hawkular.metrics.core.dropwizard;

import static org.hawkular.metrics.model.MetricType.COUNTER;
import static org.hawkular.metrics.model.MetricType.GAUGE;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.hawkular.metrics.core.service.MetricsService;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.jboss.logging.Logger;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;

import rx.Completable;
import rx.Observable;

/**
 * Creates the metric definitions for internal metrics. By internal I mean those metrics that hawkular-metrics collects
 * about itself. Each metric has two tags - hostname and baseMetric. The hostname defaults to the hostname of the
 * local host address. The baseMetric tags is for grouping constituent metrics belonging to a DropWizard metric.
 * Hawkular Metrics does not have a meter metric type for example; so, each of the rates stores in a meter are persisted
 * as separate gauge metrics.
 *
 * @author jsanda
 */
public class MetricsInitializer {

    private static Logger logger = Logger.getLogger(MetricsInitializer.class);

    private MetricsService metricsService;

    private MetricRegistry metricRegistry;

    private MetricNameService metricNameService;

    public MetricsInitializer(MetricRegistry metricRegistry, MetricsService metricsService,
            MetricNameService metricNameService) {
        this.metricRegistry = metricRegistry;
        this.metricsService = metricsService;
        this.metricNameService = metricNameService;
    }

    public void run() {
        Stopwatch stopwatch = Stopwatch.createStarted();

        logger.info("Creating metrics");

        List<Metric<Double>> gaugeMetrics = new ArrayList<>();
        List<Metric<Long>> counterMetrics = new ArrayList<>();

        metricRegistry.getMeters(metricNameService).entrySet().forEach(entry -> {
            String tenantId = MetricNameService.TENANT_ID;
            String metricName = metricNameService.getMetricName(entry.getKey());
            Map<String, String> tags = getTags(metricName);

            gaugeMetrics.add(new Metric<>(new MetricId<>(tenantId, GAUGE, metricName + "-1min"), tags));
            gaugeMetrics.add(new Metric<>(new MetricId<>(tenantId, GAUGE, metricName + "-5min"), tags));
            gaugeMetrics.add(new Metric<>(new MetricId<>(tenantId, GAUGE, metricName + "-15min"), tags));
            gaugeMetrics.add(new Metric<>(new MetricId<>(tenantId, GAUGE, metricName + "-mean"), tags));

            counterMetrics.add(new Metric<>(new MetricId<>(tenantId, COUNTER, metricName), tags));
        });

        metricRegistry.getTimers(metricNameService).entrySet().forEach(entry -> {
            String tenantId = MetricNameService.TENANT_ID;
            String metricName = metricNameService.getMetricName(entry.getKey());
            Map<String, String> tags = getTags(metricName);

            gaugeMetrics.add(new Metric<>(new MetricId<>(tenantId, GAUGE, metricName + "-1min"), tags));
            gaugeMetrics.add(new Metric<>(new MetricId<>(tenantId, GAUGE, metricName + "-5min"), tags));
            gaugeMetrics.add(new Metric<>(new MetricId<>(tenantId, GAUGE, metricName + "-15min"), tags));
            gaugeMetrics.add(new Metric<>(new MetricId<>(tenantId, GAUGE, metricName + "-mean"), tags));

            counterMetrics.add(new Metric<>(new MetricId<>(tenantId, COUNTER, metricName), tags));

            gaugeMetrics.add(new Metric<>(new MetricId<>(tenantId, GAUGE, metricName + "-median"), tags));
            gaugeMetrics.add(new Metric<>(new MetricId<>(tenantId, GAUGE, metricName + "-max"), tags));
            gaugeMetrics.add(new Metric<>(new MetricId<>(tenantId, GAUGE, metricName + "-min"), tags));
            gaugeMetrics.add(new Metric<>(new MetricId<>(tenantId, GAUGE, metricName + "-stdDev"), tags));
            gaugeMetrics.add(new Metric<>(new MetricId<>(tenantId, GAUGE, metricName + "-75p"), tags));
            gaugeMetrics.add(new Metric<>(new MetricId<>(tenantId, GAUGE, metricName + "-95p"), tags));
            gaugeMetrics.add(new Metric<>(new MetricId<>(tenantId, GAUGE, metricName + "-99p"), tags));
            gaugeMetrics.add(new Metric<>(new MetricId<>(tenantId, GAUGE, metricName + "-999p"), tags));
        });

        Completable gaugesCreated = createMetrics(gaugeMetrics);
        Completable countersCreated = createMetrics(counterMetrics);

        Completable.merge(gaugesCreated, countersCreated).subscribe(
                () -> {
                    stopwatch.stop();
                    logger.infof("Finished creating metrics in %d ms", stopwatch.elapsed(TimeUnit.MILLISECONDS));
                },
                t -> logger.warn("Failed to create metrics", t)
        );
    }

    private Map<String, String> getTags(String metricName) {
        return ImmutableMap.of(
                "hostname", metricNameService.getHostName(),
                "baseMetric", metricName,
                "component", "org.hawkular.metrics"
        );
    }

    private <T> Completable createMetrics(List<Metric<T>> metrics) {
        return Observable.from(metrics)
                .flatMap(metric -> metricsService.createMetric(metric, true)
                        .doOnNext(aVoid -> logger.debugf("Created %s", metric)))
                .toCompletable();
    }

}
