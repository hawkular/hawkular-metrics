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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import org.hawkular.metrics.client.common.http.HawkularHttpClient;
import org.hawkular.metrics.client.common.http.HawkularJson;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricRegistryListener;
import com.codahale.metrics.Timer;

/**
 * @author Joel Takvorian
 */
class MetricsTagger implements MetricRegistryListener {

    static final String METRIC_TYPE_COUNTER = "counters";
    static final String METRIC_TYPE_GAUGE = "gauges";

    private final Optional<String> prefix;
    private final Map<String, String> globalTags;
    private final Map<String, Map<String, String>> perMetricTags;
    private final Collection<RegexContainer<Map<String, String>>> regexTags;
    private final boolean enableTagComposition;
    private final HawkularHttpClient hawkularClient;
    private final MetricFilter metricFilter;
    private final MetricsDecomposer metricsDecomposer;

    MetricsTagger(Optional<String> prefix,
                  Map<String, String> globalTags,
                  Map<String, Map<String, String>> perMetricTags,
                  Collection<RegexContainer<Map<String, String>>> regexTags,
                  boolean enableTagComposition,
                  MetricsDecomposer metricsDecomposer,
                  HawkularHttpClient hawkularClient,
                  MetricRegistry registry,
                  MetricFilter metricFilter) {
        this.prefix = prefix;
        this.globalTags = new HashMap<>(globalTags);
        this.perMetricTags = new HashMap<>(perMetricTags);
        this.regexTags = new ArrayList<>(regexTags);
        this.enableTagComposition = enableTagComposition;
        this.metricsDecomposer = metricsDecomposer;
        this.hawkularClient = hawkularClient;
        this.metricFilter = metricFilter;

        // Initialize with existing metrics
        registry.getGauges().forEach(this::onGaugeAdded);
        registry.getCounters().forEach(this::onCounterAdded);
        registry.getHistograms().forEach(this::onHistogramAdded);
        registry.getTimers().forEach(this::onTimerAdded);
        registry.getMeters().forEach(this::onMeterAdded);

        registry.addListener(this);
    }

    private void tagMetric(String baseName, MetricPart<?,?> metricPart, String tagKey) {
        String nameWithSuffix = metricPart.getMetricNameWithSuffix(baseName);
        String fullName = prefix.map(p -> p + nameWithSuffix).orElse(nameWithSuffix);
        Map<String, String> tags = new LinkedHashMap<>(globalTags);
        if (enableTagComposition) {
            tags.put(tagKey, metricPart.getSuffix());
        }
        // Don't use prefixed name for per-metric tagging
        tags.putAll(getTagsForMetrics(baseName));
        tags.putAll(getTagsForMetrics(nameWithSuffix));
        if (!tags.isEmpty()) {
            hawkularClient.putTags(metricPart.getMetricType(), fullName, HawkularJson.tagsToString(tags));
        }
    }

    private void tagMetric(String metricType, String baseName) {
        String fullName = prefix.map(p -> p + baseName).orElse(baseName);
        Map<String, String> tags = new LinkedHashMap<>(globalTags);
        // Don't use prefixed name for per-metric tagging
        tags.putAll(getTagsForMetrics(baseName));
        if (!tags.isEmpty()) {
            hawkularClient.putTags(metricType, fullName, HawkularJson.tagsToString(tags));
        }
    }

    Map<String, String> getTagsForMetrics(String name) {
        Map<String, String> tags = new LinkedHashMap<>();
        regexTags.forEach(reg -> reg.match(name).ifPresent(tags::putAll));
        if (perMetricTags.containsKey(name)) {
            tags.putAll(perMetricTags.get(name));
        }
        return tags;
    }

    @Override public void onGaugeAdded(String name, Gauge<?> gauge) {
        if (metricFilter.matches(name, gauge)) {
            tagMetric(METRIC_TYPE_GAUGE, name);
        }
    }

    @Override public void onGaugeRemoved(String name) {
    }

    @Override public void onCounterAdded(String name, Counter counter) {
        if (metricFilter.matches(name, counter)) {
            tagMetric(METRIC_TYPE_COUNTER, name);
        }
    }

    @Override public void onCounterRemoved(String name) {
    }

    @Override public void onHistogramAdded(String name, Histogram histogram) {
        if (metricFilter.matches(name, histogram)) {
            MetricsDecomposer.PartsStreamer streamer = metricsDecomposer.streamParts(name);
            streamer.countings().forEach(metricPart -> tagMetric(name, metricPart, "histogram"));
            streamer.samplings().forEach(metricPart -> tagMetric(name, metricPart, "histogram"));
        }
    }

    @Override public void onHistogramRemoved(String name) {
    }

    @Override public void onMeterAdded(String name, Meter meter) {
        if (metricFilter.matches(name, meter)) {
            MetricsDecomposer.PartsStreamer streamer = metricsDecomposer.streamParts(name);
            streamer.countings().forEach(metricPart -> tagMetric(name, metricPart, "meter"));
            streamer.metered().forEach(metricPart -> tagMetric(name, metricPart, "meter"));
        }
    }

    @Override public void onMeterRemoved(String name) {
    }

    @Override public void onTimerAdded(String name, Timer timer) {
        if (metricFilter.matches(name, timer)) {
            MetricsDecomposer.PartsStreamer streamer = metricsDecomposer.streamParts(name);
            streamer.countings().forEach(metricPart -> tagMetric(name, metricPart, "timer"));
            streamer.metered().forEach(metricPart -> tagMetric(name, metricPart, "timer"));
            streamer.samplings().forEach(metricPart -> tagMetric(name, metricPart, "timer"));
        }
    }

    @Override public void onTimerRemoved(String name) {
    }

    Map<String, String> getGlobalTags() {
        return globalTags;
    }

    boolean isEnableTagComposition() {
        return enableTagComposition;
    }

    void addTag(String m, String key, String value) {
        perMetricTags.computeIfAbsent(m, k -> new HashMap<>()).put(key, value);
    }
}
