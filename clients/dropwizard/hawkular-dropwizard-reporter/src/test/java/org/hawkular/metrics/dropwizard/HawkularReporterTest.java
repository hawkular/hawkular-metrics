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

import static java.util.stream.Collectors.toMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;

import org.apache.commons.lang3.tuple.Pair;
import org.assertj.core.api.iterable.Extractor;
import org.assertj.core.util.Lists;
import org.hawkular.metrics.client.common.http.HawkularHttpClient;
import org.hawkular.metrics.client.common.http.HawkularHttpResponse;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

/**
 * @author Joel Takvorian
 */
public class HawkularReporterTest {

    private final MetricRegistry registry = new MetricRegistry();
    private final Extractor<Object, String> idFromRoot = e -> ((JSONObject)e).getString("id");
    private final Extractor<Object, Integer> valueFromDataPoints = e -> ((JSONObject)e).getInt("value");
    private final Extractor<Object, Integer> valueFromRoot = e -> valueFromDataPoints.extract(((JSONObject)e)
            .getJSONArray("dataPoints").get(0));
    private final Extractor<Object, Double> dValueFromDataPoints = e -> ((JSONObject)e).getDouble("value");
    private final Extractor<Object, Double> dValueFromRoot = e -> dValueFromDataPoints.extract(((JSONObject)e)
            .getJSONArray("dataPoints").get(0));
    private final HttpClientMock client = new HttpClientMock();

    @Test
    public void shouldReportSimpleCounterWithoutTag() {
        HawkularReporter reporter = HawkularReporter.builder(registry, "unit-test").useHttpClient(uri -> client).build();

        final Counter counter = registry.counter("my.counter");
        counter.inc();
        reporter.report();

        assertThat(client.getMetricsRestCalls()).hasSize(1);
        JSONObject metrics = new JSONObject(client.getMetricsRestCalls().get(0));
        assertThat(metrics.keySet()).containsExactly("counters");
        JSONArray json = metrics.getJSONArray("counters");
        assertThat(json).extracting(idFromRoot).containsExactly("my.counter");
        assertThat(json).extracting(valueFromRoot).containsExactly(1);
        assertThat(client.getTagsRestCalls()).isEmpty();
    }

    @Test
    public void shouldReportCountersWithTags() {
        HawkularReporter reporter = HawkularReporter.builder(registry, "unit-test")
                .useHttpClient(uri -> client)
                .globalTags(Collections.singletonMap("global-tag", "abc"))
                .perMetricTags(Collections.singletonMap("my.second.counter",
                        Collections.singletonMap("metric-tag", "def")))
                .build();

        final Counter counter1 = registry.counter("my.first.counter");
        final Counter counter2 = registry.counter("my.second.counter");
        counter1.inc();
        counter2.inc();
        reporter.report();

        assertThat(client.getMetricsRestCalls()).hasSize(1);
        JSONObject metrics = new JSONObject(client.getMetricsRestCalls().get(0));
        assertThat(metrics.keySet()).containsExactly("counters");
        JSONArray json = metrics.getJSONArray("counters");
        assertThat(json).extracting(idFromRoot).containsOnly("my.first.counter", "my.second.counter");
        assertThat(json).extracting(valueFromRoot).containsExactly(1, 1);

        assertThat(client.getTagsRestCalls()).containsOnly(
                Pair.of("/counters/my.first.counter/tags", "{\"global-tag\":\"abc\"}"),
                Pair.of("/counters/my.second.counter/tags", "{\"global-tag\":\"abc\",\"metric-tag\":\"def\"}"));
    }

    @Test
    public void shouldReportHistogram() {
        HawkularReporter reporter = HawkularReporter.builder(registry, "unit-test").useHttpClient(uri -> client).build();

        final Histogram histogram = registry.histogram("my.histogram");
        histogram.update(3);
        histogram.update(8);
        histogram.update(7);
        histogram.update(1);
        histogram.update(8);
        histogram.update(4);
        reporter.report();

        assertThat(client.getMetricsRestCalls()).hasSize(1);
        JSONObject metrics = new JSONObject(client.getMetricsRestCalls().get(0));
        assertThat(metrics.keySet()).containsOnly("counters", "gauges");
        JSONArray countersJson = metrics.getJSONArray("counters");
        assertThat(countersJson).extracting(idFromRoot).containsExactly("my.histogram.count");
        assertThat(countersJson).extracting(valueFromRoot).containsExactly(6);

        JSONArray gaugesJson = metrics.getJSONArray("gauges");
        Map<String, Integer> values = StreamSupport.stream(gaugesJson.spliterator(), false)
                .collect(toMap(idFromRoot::extract, valueFromRoot::extract));
        // Note: we extract int values here for simplicity, but actual values are double. The goal is not to test
        // Dropwizard algorithm for metrics generation, so we don't bother with accuracy.
        assertThat(values).containsOnly(
                entry("my.histogram.mean", 5),
                entry("my.histogram.median", 7),
                entry("my.histogram.stddev", 2),
                entry("my.histogram.75perc", 8),
                entry("my.histogram.95perc", 8),
                entry("my.histogram.98perc", 8),
                entry("my.histogram.99perc", 8),
                entry("my.histogram.999perc", 8));

        assertThat(client.getTagsRestCalls()).containsOnly(
                Pair.of("/counters/my.histogram.count/tags", "{\"histogram\":\"count\"}"),
                Pair.of("/gauges/my.histogram.mean/tags", "{\"histogram\":\"mean\"}"),
                Pair.of("/gauges/my.histogram.min/tags", "{\"histogram\":\"min\"}"),
                Pair.of("/gauges/my.histogram.max/tags", "{\"histogram\":\"max\"}"),
                Pair.of("/gauges/my.histogram.stddev/tags", "{\"histogram\":\"stddev\"}"),
                Pair.of("/gauges/my.histogram.median/tags", "{\"histogram\":\"median\"}"),
                Pair.of("/gauges/my.histogram.75perc/tags", "{\"histogram\":\"75perc\"}"),
                Pair.of("/gauges/my.histogram.95perc/tags", "{\"histogram\":\"95perc\"}"),
                Pair.of("/gauges/my.histogram.98perc/tags", "{\"histogram\":\"98perc\"}"),
                Pair.of("/gauges/my.histogram.99perc/tags", "{\"histogram\":\"99perc\"}"),
                Pair.of("/gauges/my.histogram.999perc/tags", "{\"histogram\":\"999perc\"}"));
    }

    @Test
    public void shouldReportPartialHistogram() {
        HawkularReporter reporter = HawkularReporter.builder(registry, "unit-test")
                .setMetricComposition("my.histogram", Lists.newArrayList("mean", "median", "stddev"))
                .useHttpClient(uri -> client)
                .build();

        final Histogram histogram = registry.histogram("my.histogram");
        histogram.update(3);
        reporter.report();

        assertThat(client.getMetricsRestCalls()).hasSize(1);
        JSONObject metrics = new JSONObject(client.getMetricsRestCalls().get(0));
        assertThat(metrics.keySet()).containsOnly("gauges");

        JSONArray gaugesJson = metrics.getJSONArray("gauges");
        Map<String, Integer> values = StreamSupport.stream(gaugesJson.spliterator(), false)
                .collect(toMap(idFromRoot::extract, valueFromRoot::extract));
        // Note: we extract int values here for simplicity, but actual values are double. The goal is not to test
        // Dropwizard algorithm for metrics generation, so we don't bother with accuracy.
        assertThat(values).containsOnly(
                entry("my.histogram.mean", 3),
                entry("my.histogram.median", 3),
                entry("my.histogram.stddev", 0));

        assertThat(client.getTagsRestCalls()).containsOnly(
                Pair.of("/gauges/my.histogram.mean/tags", "{\"histogram\":\"mean\"}"),
                Pair.of("/gauges/my.histogram.stddev/tags", "{\"histogram\":\"stddev\"}"),
                Pair.of("/gauges/my.histogram.median/tags", "{\"histogram\":\"median\"}"));
    }

    @Test
    public void shouldReportPartialMetersWithRegex() {
        HawkularReporter reporter = HawkularReporter.builder(registry, "unit-test")
                .useHttpClient(uri -> client)
                .setRegexMetricComposition(Pattern.compile("meter\\.partial\\..+"), Lists.newArrayList("count", "meanrt"))
                .build();

        final Meter m1 = registry.meter("meter.partial.1");
        final Meter m2 = registry.meter("meter.partial.2");
        final Meter m3 = registry.meter("meter.full.3");
        m1.mark();
        m2.mark();
        m3.mark();
        reporter.report();

        assertThat(client.getMetricsRestCalls()).hasSize(1);
        JSONObject metrics = new JSONObject(client.getMetricsRestCalls().get(0));
        assertThat(metrics.keySet()).containsOnly("counters", "gauges");
        JSONArray counters = metrics.getJSONArray("counters");
        assertThat(counters).extracting(idFromRoot).containsOnly(
                "meter.partial.1.count",
                "meter.partial.2.count",
                "meter.full.3.count");
        assertThat(counters).extracting(valueFromRoot).containsExactly(1, 1, 1);

        JSONArray gauges = metrics.getJSONArray("gauges");
        assertThat(gauges).extracting(idFromRoot).containsOnly(
                "meter.partial.1.meanrt",
                "meter.partial.2.meanrt",
                "meter.full.3.1minrt",
                "meter.full.3.5minrt",
                "meter.full.3.15minrt",
                "meter.full.3.meanrt");
    }

    @Test
    public void shouldCreateRegexTags() {
        HawkularReporter reporter = HawkularReporter.builder(registry, "unit-test")
                .useHttpClient(uri -> client)
                .addRegexTag(Pattern.compile("my\\..*"), "owner", "me")
                .addMetricTag("my.first.counter", "type", "counter")
                .build();

        assertThat(reporter.getTagsForMetrics("my.first.gauge")).containsOnly(entry("owner", "me"));
        assertThat(reporter.getTagsForMetrics("my.first.counter")).containsOnly(
                entry("owner", "me"),
                entry("type", "counter"));
        assertThat(reporter.getTagsForMetrics("your.first.gauge")).isEmpty();
    }

    @Test
    public void shouldCreateRegexTagsFromString() {
        HawkularReporter reporter = HawkularReporter.builder(registry, "unit-test")
                .useHttpClient(uri -> client)
                .addMetricTag("/my\\..*/", "owner", "me")
                .addMetricTag("my.first.counter", "type", "counter")
                .build();

        assertThat(reporter.getTagsForMetrics("my.first.gauge")).containsOnly(entry("owner", "me"));
        assertThat(reporter.getTagsForMetrics("my.first.counter")).containsOnly(
                entry("owner", "me"),
                entry("type", "counter"));
        assertThat(reporter.getTagsForMetrics("your.first.gauge")).isEmpty();
    }

    @Test
    public void shouldDefaultBehaviourBeListedLast() {
        HawkularReporter reporter = HawkularReporter.builder(registry, "unit-test")
                .useHttpClient(uri -> client)
                .setMetricComposition("meter.1", Lists.newArrayList("count", "meanrt", "1minrt"))
                .setRegexMetricComposition(Pattern.compile("meter\\.2"), Lists.newArrayList("count", "meanrt", "15minrt"))
                // Here is the default behaviour => every regex beyond this point will be ignored
                .setRegexMetricComposition(Pattern.compile(".*"), Lists.newArrayList("count", "meanrt"))
                .setRegexMetricComposition(Pattern.compile("meter\\.3"), Lists.newArrayList("count", "meanrt", "15minrt"))
                .build();

        Optional<Collection<String>> parts = reporter.getAllowedParts("meter.1");
        assertThat(parts).hasValueSatisfying(col -> assertThat(col).containsOnly("count", "meanrt", "1minrt"));

        parts = reporter.getAllowedParts("meter.2");
        assertThat(parts).hasValueSatisfying(col -> assertThat(col).containsOnly("count", "meanrt", "15minrt"));

        parts = reporter.getAllowedParts("meter.3");
        assertThat(parts).hasValueSatisfying(col -> assertThat(col).containsOnly("count", "meanrt"));
    }

    @Test
    public void shouldReportDoubleGauge() {
        HawkularReporter reporter = HawkularReporter.builder(registry, "unit-test")
                .useHttpClient(uri -> client)
                .build();

        final Gauge<Double> gauge = () -> 1.5d;
        registry.register("gauge.double", gauge);
        reporter.report();

        assertThat(client.getMetricsRestCalls()).hasSize(1);
        JSONObject metrics = new JSONObject(client.getMetricsRestCalls().get(0));
        assertThat(metrics.keySet()).containsOnly("gauges");

        JSONArray gaugesJson = metrics.getJSONArray("gauges");
        Map<String, Double> values = StreamSupport.stream(gaugesJson.spliterator(), false)
                .collect(toMap(idFromRoot::extract, dValueFromRoot::extract));
        assertThat(values).containsOnly(entry("gauge.double", 1.5d));
    }

    @Test
    public void shouldNotFailOnInfinityOrNaNGauge() {
        HawkularReporter reporter = HawkularReporter.builder(registry, "unit-test")
                .useHttpClient(uri -> client)
                .build();

        final Gauge<Double> g1 = () -> Double.POSITIVE_INFINITY;
        final Gauge<Double> g2 = () -> Double.NaN;
        registry.register("gauge.infinity", g1);
        registry.register("gauge.nan", g2);
        reporter.report();

        assertThat(client.getMetricsRestCalls()).hasSize(0);
        // Infinity and NaN are not supported in Hawkular
    }

    @Test
    public void shouldReportBigDecimalGauge() {
        HawkularReporter reporter = HawkularReporter.builder(registry, "unit-test")
                .useHttpClient(uri -> client)
                .build();

        final Gauge<BigDecimal> gauge = () -> new BigDecimal("1.5");
        registry.register("gauge.bigd", gauge);
        reporter.report();

        assertThat(client.getMetricsRestCalls()).hasSize(1);
        JSONObject metrics = new JSONObject(client.getMetricsRestCalls().get(0));
        assertThat(metrics.keySet()).containsOnly("gauges");

        JSONArray gaugesJson = metrics.getJSONArray("gauges");
        Map<String, Double> values = StreamSupport.stream(gaugesJson.spliterator(), false)
                .collect(toMap(idFromRoot::extract, dValueFromRoot::extract));
        assertThat(values).containsOnly(entry("gauge.bigd", 1.5d));
    }

    @Test
    public void shouldReportIntegerGauges() {
        HawkularReporter reporter = HawkularReporter.builder(registry, "unit-test")
                .useHttpClient(uri -> client)
                .build();

        final Gauge<Integer> gauge = () -> 1;
        registry.register("gauge.integer", gauge);
        reporter.report();

        assertThat(client.getMetricsRestCalls()).hasSize(1);
        JSONObject metrics = new JSONObject(client.getMetricsRestCalls().get(0));
        assertThat(metrics.keySet()).containsOnly("gauges");

        JSONArray gaugesJson = metrics.getJSONArray("gauges");
        Map<String, Double> values = StreamSupport.stream(gaugesJson.spliterator(), false)
                .collect(toMap(idFromRoot::extract, dValueFromRoot::extract));
        assertThat(values).containsOnly(entry("gauge.integer", 1d));
    }

    @Test
    public void shouldReportLongGauges() {
        HawkularReporter reporter = HawkularReporter.builder(registry, "unit-test")
                .useHttpClient(uri -> client)
                .build();

        final Gauge<Long> gauge = () -> 101L;
        registry.register("gauge.long", gauge);
        reporter.report();

        assertThat(client.getMetricsRestCalls()).hasSize(1);
        JSONObject metrics = new JSONObject(client.getMetricsRestCalls().get(0));
        assertThat(metrics.keySet()).containsOnly("gauges");

        JSONArray gaugesJson = metrics.getJSONArray("gauges");
        Map<String, Double> values = StreamSupport.stream(gaugesJson.spliterator(), false)
                .collect(toMap(idFromRoot::extract, dValueFromRoot::extract));
        assertThat(values).containsOnly(entry("gauge.long", 101d));
    }

    @Test
    public void shouldReportBigIntegerGauge() {
        HawkularReporter reporter = HawkularReporter.builder(registry, "unit-test")
                .useHttpClient(uri -> client)
                .build();

        final Gauge<BigInteger> gauge = () -> new BigInteger("2");
        registry.register("gauge.bigi", gauge);
        reporter.report();

        assertThat(client.getMetricsRestCalls()).hasSize(1);
        JSONObject metrics = new JSONObject(client.getMetricsRestCalls().get(0));
        assertThat(metrics.keySet()).containsOnly("gauges");

        JSONArray gaugesJson = metrics.getJSONArray("gauges");
        Map<String, Double> values = StreamSupport.stream(gaugesJson.spliterator(), false)
                .collect(toMap(idFromRoot::extract, dValueFromRoot::extract));
        assertThat(values).containsOnly(entry("gauge.bigi", 2d));
    }

    private static class HttpClientMock implements HawkularHttpClient {
        private List<String> metricsRestCalls = new ArrayList<>();
        private List<Pair<String, String>> tagsRestCalls = new ArrayList<>();

        @Override public void addHeaders(Map<String, String> headers) {}

        @Override public HawkularHttpResponse postMetrics(String jsonBody) {
            metricsRestCalls.add(jsonBody);
            return null;
        }

        @Override public HawkularHttpResponse putTags(String type, String metricName, String jsonBody) {
            tagsRestCalls.add(Pair.of("/" + type + "/" + metricName + "/tags", jsonBody));
            return null;
        }

        @Override
        public void setFailoverOptions(Optional<Long> failoverCacheDuration, Optional<Integer> failoverCacheMaxSize) {
        }

        @Override public void manageFailover() {
        }

        List<String> getMetricsRestCalls() {
            return metricsRestCalls;
        }

        List<Pair<String, String>> getTagsRestCalls() {
            return tagsRestCalls;
        }
    }
}
