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
package org.hawkular.metrics.dropwizard;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import org.hawkular.metrics.client.common.http.HawkularHttpClient;
import org.hawkular.metrics.client.common.http.HawkularJson;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;

/**
 * Dropwizard Reporter used to report to a Hawkular Metrics data store
 */
public class HawkularReporter extends ScheduledReporter {

    private final Optional<String> prefix;
    private final Clock clock;
    private final HawkularHttpClient hawkularClient;
    private final MetricsDecomposer decomposer;
    private final MetricsTagger tagger;

    HawkularReporter(MetricRegistry registry,
                     HawkularHttpClient hawkularClient,
                     Optional<String> prefix,
                     MetricsDecomposer decomposer,
                     MetricsTagger tagger,
                     TimeUnit rateUnit,
                     TimeUnit durationUnit,
                     MetricFilter filter) {
        super(registry, "hawkular-reporter", filter, rateUnit, durationUnit);

        this.prefix = prefix;
        this.clock = Clock.defaultClock();
        this.hawkularClient = hawkularClient;
        this.decomposer = decomposer;
        this.tagger = tagger;
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges,
                       SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms,
                       SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers) {

        hawkularClient.manageFailover();

        if (gauges.isEmpty() && counters.isEmpty() && histograms.isEmpty() && meters.isEmpty() &&
                timers.isEmpty()) {
            return;
        }

        final long timestamp = clock.getTime();

        DataAccumulator accu = new DataAccumulator();
        processGauges(accu, gauges);
        processCounters(accu, counters);
        processMeters(accu, meters);
        processHistograms(accu, histograms);
        processTimers(accu, timers);

        if (!accu.getCounters().isEmpty() || !accu.getGauges().isEmpty()) {
            String json = HawkularJson.metricsToString(timestamp, accu.getCounters(), accu.getGauges());
            hawkularClient.postMetrics(json);
        }
    }

    private static void processGauges(DataAccumulator builder, Map<String, Gauge> gauges) {
        for (Map.Entry<String, Gauge> e : gauges.entrySet()) {
            builder.addGauge(e.getKey(), e.getValue().getValue());
        }
    }

    private static void processCounters(DataAccumulator builder, Map<String, Counter> counters) {
        for (Map.Entry<String, Counter> e : counters.entrySet()) {
            builder.addCounter(e.getKey(), e.getValue().getCount());
        }
    }

    private void processMeters(DataAccumulator builder, Map<String, Meter> meters) {
        for (Map.Entry<String, Meter> e : meters.entrySet()) {
            MetricsDecomposer.PartsStreamer streamer = decomposer.streamParts(e.getKey());
            streamer.countings().forEach(metricPart -> builder.addSubCounter(metricPart, e));
            streamer.metered().forEach(metricPart -> builder.addSubGauge(metricPart, e));
        }
    }

    private void processHistograms(DataAccumulator builder, Map<String, Histogram> histograms) {
        for (Map.Entry<String, Histogram> e : histograms.entrySet()) {
            MetricsDecomposer.PartsStreamer streamer = decomposer.streamParts(e.getKey());
            streamer.countings().forEach(metricPart -> builder.addSubCounter(metricPart, e));
            streamer.samplings().forEach(metricPart -> builder.addSubGauge(metricPart, e));
        }
    }

    private void processTimers(DataAccumulator builder, Map<String, Timer> timers) {
        for (Map.Entry<String, Timer> e : timers.entrySet()) {
            MetricsDecomposer.PartsStreamer streamer = decomposer.streamParts(e.getKey());
            streamer.countings().forEach(metricPart -> builder.addSubCounter(metricPart, e));
            streamer.metered().forEach(metricPart -> builder.addSubGauge(metricPart, e));
            streamer.samplings().forEach(metricPart -> builder.addSubGauge(metricPart, e));
        }
    }

    public Optional<String> getPrefix() {
        return prefix;
    }

    public Map<String, String> getGlobalTags() {
        return tagger.getGlobalTags();
    }

    public HawkularHttpClient getHawkularClient() {
        return hawkularClient;
    }

    public Map<String, String> getTagsForMetrics(String m) {
        return tagger.getTagsForMetrics(m);
    }

    public Optional<Collection<String>> getAllowedParts(String metricName) {
        return decomposer.getAllowedParts(metricName);
    }

    public void addTag(String m, String key, String value) {
        tagger.addTag(m, key, value);
    }

    public boolean isEnableTagComposition() {
        return tagger.isEnableTagComposition();
    }

    /**
     * Create a new builder for an {@link HawkularReporter}
     *
     * @param registry the Dropwizard Metrics registry
     * @param tenant   the Hawkular tenant ID
     */
    public static HawkularReporterBuilder builder(MetricRegistry registry, String tenant) {
        return new HawkularReporterBuilder(registry, tenant);
    }

    private class DataAccumulator {
        private Map<String, Double> gauges = new HashMap<>();
        private Map<String, Long> counters = new HashMap<>();

        private DataAccumulator() {
        }

        private Map<String, Double> getGauges() {
            return gauges;
        }

        private Map<String, Long> getCounters() {
            return counters;
        }

        private DataAccumulator addCounter(String name, long l) {
            String fullName = prefix.map(p -> p + name).orElse(name);
            counters.put(fullName, l);
            return this;
        }

        private DataAccumulator addGauge(String name, Object value) {
            String fullName = prefix.map(p -> p + name).orElse(name);
            if (value instanceof BigDecimal) {
                gauges.put(fullName, ((BigDecimal) value).doubleValue());
            } else if (value instanceof BigInteger) {
                gauges.put(fullName, ((BigInteger) value).doubleValue());
            } else if (value != null && value.getClass().isAssignableFrom(Double.class)) {
                if (!Double.isNaN((Double) value) && Double.isFinite((Double) value)) {
                    gauges.put(fullName, (Double) value);
                }
            } else if (value != null && value instanceof Number) {
                gauges.put(fullName, ((Number) value).doubleValue());
            }
            return this;
        }

        private <T> DataAccumulator addSubCounter(MetricPart<T, Long> metricPart,
                                                  Map.Entry<String, ? extends T> counterEntry) {
            String nameWithSuffix = metricPart.getMetricNameWithSuffix(counterEntry.getKey());
            String fullName = prefix.map(p -> p + nameWithSuffix).orElse(nameWithSuffix);
            counters.put(fullName, metricPart.getData(counterEntry.getValue()));
            return this;
        }

        private <T> DataAccumulator addSubGauge(MetricPart<T, Object> metricPart,
                                                Map.Entry<String, ? extends T> gaugeEntry) {
            String nameWithSuffix = metricPart.getMetricNameWithSuffix(gaugeEntry.getKey());
            String fullName = prefix.map(p -> p + nameWithSuffix).orElse(nameWithSuffix);
            Object value = metricPart.getData(gaugeEntry.getValue());
            if (value instanceof BigDecimal) {
                gauges.put(fullName, ((BigDecimal) value).doubleValue());
            } else if (value != null && value.getClass().isAssignableFrom(Double.class)
                    && !Double.isNaN((Double) value) && Double.isFinite((Double) value)) {
                gauges.put(fullName, (Double) value);
            }
            return this;
        }
    }
}
