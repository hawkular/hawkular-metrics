/*
 * Copyright 2014-2017 Red Hat, Inc. and/or its affiliates
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

import static java.util.Collections.singletonList;

import static org.hawkular.metrics.model.MetricType.COUNTER;
import static org.hawkular.metrics.model.MetricType.GAUGE;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import org.hawkular.metrics.core.service.MetricsService;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.MetricType;
import org.jboss.logging.Logger;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;

import rx.Observable;

/**
 * A scheduled reporter that persists internally collected metrics. Several of the DropWizard metric types are complex
 * types having multiple values, and they do not map directly to metric types in Hawkular Metrics. Numeric gauges and
 * counters map directly, but meters, times, and histograms do not. Only metrics having meta data will be persisted.
 *
 * @author jsanda
 */
public class DropWizardReporter extends ScheduledReporter {

    private static Logger logger = Logger.getLogger(DropWizardReporter.class);

    public static final String REPORTER_NAME = "hawkular-metrics-reporter";

    private HawkularMetricRegistry metricRegistry;

    private MetricNameService metricNameService;

    private MetricsService metricsService;

    public DropWizardReporter(HawkularMetricRegistry registry, MetricNameService metricNameService,
            MetricsService metricsService) {
        super(registry, REPORTER_NAME, MetricFilter.ALL, TimeUnit.SECONDS, TimeUnit.SECONDS);
        this.metricRegistry = registry;
        this.metricsService = metricsService;
        this.metricNameService = metricNameService;
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
            SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {
        long timestamp = System.currentTimeMillis();

        List<Metric<Long>> countersList = new ArrayList<>();
        List<Metric<Double>> gaugesList = new ArrayList<>();

        meters.entrySet()
                .stream()
                .filter(entry -> metricRegistry.getMetaData(entry.getKey()) != null)
                .forEach(entry -> {
                    MetricId<Double> gaugeId = getMetricId(entry.getKey(), GAUGE);
                    MetricId<Long> counterId = getMetricId(entry.getKey(), COUNTER);

                    gaugesList.add(new Metric<>(new MetricId<>(gaugeId.getTenantId(), GAUGE, gaugeId.getName() +
                            "-5min"), singletonList(new DataPoint<>(timestamp, entry.getValue().getFiveMinuteRate()))));
                    gaugesList.add(new Metric<>(new MetricId<>(gaugeId.getTenantId(), GAUGE, gaugeId.getName() +
                            "-15min"), singletonList(new DataPoint<>(timestamp,
                            entry.getValue().getFifteenMinuteRate()))));
                    gaugesList.add(new Metric<>(new MetricId<>(gaugeId.getTenantId(), GAUGE, gaugeId.getName() +
                            "-mean"), singletonList(new DataPoint<>(timestamp, entry.getValue().getMeanRate()))));

                    countersList.add(new Metric<>(counterId, singletonList(new DataPoint<>(timestamp,
                            entry.getValue().getCount()))));
                });

        timers.entrySet()
                .stream()
                .filter(entry -> metricRegistry.getMetaData(entry.getKey()) != null)
                .forEach(entry -> {
                    MetricId<Double> gaugeId = getMetricId(entry.getKey(), GAUGE);
                    MetricId<Long> counterId = getMetricId(entry.getKey(), COUNTER);

                    gaugesList.add(new Metric<>(new MetricId<>(gaugeId.getTenantId(), GAUGE, gaugeId.getName() +
                            "-5min"), singletonList(new DataPoint<>(timestamp,
                            entry.getValue().getFiveMinuteRate()))));
                    gaugesList.add(new Metric<>(new MetricId<>(gaugeId.getTenantId(), GAUGE, gaugeId.getName() +
                            "-15min"), singletonList(new DataPoint<>(timestamp,
                            entry.getValue().getFifteenMinuteRate()))));
                    gaugesList.add(new Metric<>(new MetricId<>(gaugeId.getTenantId(), GAUGE, gaugeId.getName() +
                            "-mean"), singletonList(new DataPoint<>(timestamp, entry.getValue().getMeanRate()))));

                    countersList.add(new Metric<>(counterId, singletonList(new DataPoint<>(timestamp,
                            entry.getValue().getCount()))));

                    gaugesList.add(new Metric<>(new MetricId<>(gaugeId.getTenantId(), GAUGE, gaugeId.getName() +
                            "-median"), singletonList(new DataPoint<>(timestamp,
                            entry.getValue().getSnapshot().getMedian()))));
                    gaugesList.add(new Metric<>(new MetricId<>(gaugeId.getTenantId(), GAUGE, gaugeId.getName() +
                            "-max"), singletonList(new DataPoint<>(timestamp,
                            (double) entry.getValue().getSnapshot().getMax()))));
                    gaugesList.add(new Metric<>(new MetricId<>(gaugeId.getTenantId(), GAUGE, gaugeId.getName() +
                            "-min"), singletonList(new DataPoint<>(timestamp,
                            (double) entry.getValue().getSnapshot().getMin()))));
                    gaugesList.add(new Metric<>(new MetricId<>(gaugeId.getTenantId(), GAUGE, gaugeId.getName() +
                            "-75p"), singletonList(new DataPoint<>(timestamp,
                            entry.getValue().getSnapshot().get75thPercentile()))));
                    gaugesList.add(new Metric<>(new MetricId<>(gaugeId.getTenantId(), GAUGE, gaugeId.getName() +
                            "-95p"), singletonList(new DataPoint<>(timestamp,
                            entry.getValue().getSnapshot().get95thPercentile()))));
                    gaugesList.add(new Metric<>(new MetricId<>(gaugeId.getTenantId(), GAUGE, gaugeId.getName() +
                            "-99p"), singletonList(new DataPoint<>(timestamp,
                            entry.getValue().getSnapshot().get99thPercentile()))));
                    gaugesList.add(new Metric<>(new MetricId<>(gaugeId.getTenantId(), GAUGE, gaugeId.getName() +
                            "-999p"), singletonList(new DataPoint<>(timestamp,
                            entry.getValue().getSnapshot().get999thPercentile()))));
                });

        gauges.entrySet()
                .stream()
                .filter(entry -> metricRegistry.getMetaData(entry.getKey()) != null)
                .forEach(entry -> {
                    MetricId<Double> gaugeId = getMetricId(entry.getKey(), GAUGE);
                    gaugesList.add(new Metric<>(gaugeId, singletonList(new DataPoint<>(timestamp,
                            Double.parseDouble(entry.getValue().getValue().toString())))));
                });

        counters.entrySet()
                .stream()
                .filter(entry -> metricRegistry.getMetaData(entry.getKey()) != null)
                .forEach(entry -> {
                    MetricId<Long> counterId = getMetricId(entry.getKey(), COUNTER);
                    countersList.add(new Metric<>(counterId, singletonList(new DataPoint<>(timestamp,
                            entry.getValue().getCount()))));
                });

        histograms.entrySet()
                .stream()
                .filter(entry -> metricRegistry.getMetaData(entry.getKey()) != null)
                .forEach(entry -> {
                    MetricId<Double> gaugeId = getMetricId(entry.getKey(), GAUGE);
                    MetricId<Long> counterId = getMetricId(entry.getKey(), COUNTER);

                    countersList.add(new Metric<>(counterId, singletonList(new DataPoint<>(timestamp,
                            entry.getValue().getCount()))));

                    gaugesList.add(new Metric<>(new MetricId<>(gaugeId.getTenantId(), GAUGE, gaugeId.getName() +
                            "-mean"), singletonList(new DataPoint<>(timestamp,
                            entry.getValue().getSnapshot().getMean()))));
                    gaugesList.add(new Metric<>(new MetricId<>(gaugeId.getTenantId(), GAUGE, gaugeId.getName() +
                            "-median"), singletonList(new DataPoint<>(timestamp,
                            entry.getValue().getSnapshot().getMedian()))));
                    gaugesList.add(new Metric<>(new MetricId<>(gaugeId.getTenantId(), GAUGE, gaugeId.getName() +
                            "-max"), singletonList(new DataPoint<>(timestamp,
                            (double) entry.getValue().getSnapshot().getMax()))));
                    gaugesList.add(new Metric<>(new MetricId<>(gaugeId.getTenantId(), GAUGE, gaugeId.getName() +
                            "-min"), singletonList(new DataPoint<>(timestamp,
                            (double) entry.getValue().getSnapshot().getMin()))));
                    gaugesList.add(new Metric<>(new MetricId<>(gaugeId.getTenantId(), GAUGE, gaugeId.getName() +
                            "-75p"), singletonList(new DataPoint<>(timestamp,
                            entry.getValue().getSnapshot().get75thPercentile()))));
                    gaugesList.add(new Metric<>(new MetricId<>(gaugeId.getTenantId(), GAUGE, gaugeId.getName() +
                            "-95p"), singletonList(new DataPoint<>(timestamp,
                            entry.getValue().getSnapshot().get95thPercentile()))));
                    gaugesList.add(new Metric<>(new MetricId<>(gaugeId.getTenantId(), GAUGE, gaugeId.getName() +
                            "-99p"), singletonList(new DataPoint<>(timestamp,
                            entry.getValue().getSnapshot().get99thPercentile()))));
                    gaugesList.add(new Metric<>(new MetricId<>(gaugeId.getTenantId(), GAUGE, gaugeId.getName() +
                            "-999p"), singletonList(new DataPoint<>(timestamp,
                            entry.getValue().getSnapshot().get999thPercentile()))));
        });

        // TODO add failover support

        Observable<Void> insertedGauges = metricsService.addDataPoints(GAUGE, Observable.from(gaugesList));
        Observable<Void> insertedCounters = metricsService.addDataPoints(COUNTER, Observable.from(countersList));

        Observable.merge(insertedGauges, insertedCounters).subscribe(
                aVoid -> {},
                t -> logger.warn("Persisting metrics failed", t),
                () -> logger.debug("Finished persisting metrics")
        );
    }

    private <T> MetricId<T> getMetricId(String metric, MetricType<T> type) {
        String tenantId = metricNameService.getTenantId();
        MetaData metaData = metricRegistry.getMetaData(metric);
        String fullyQualifiedName = metricNameService.createMetricName(metaData);
        return new MetricId<>(tenantId, type, fullyQualifiedName);
    }

}
