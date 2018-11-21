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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.RandomStringUtils;
import org.assertj.core.api.iterable.Extractor;
import org.assertj.core.util.DoubleComparator;
import org.hawkular.metrics.client.common.http.HawkularHttpResponse;
import org.hawkular.metrics.client.common.http.JdkHawkularHttpClient;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

/**
 * Note: these integration tests can be run in 2 ways:<br/>
 * - with maven install and profile "dropwizard-integration-tests" (it requires Cassandra running on host)<br/>
 * - without maven, you need to turn off profile "wildfly.deployment" and start hawkular-metrics on localhost:8080<br/>
 * @author Joel Takvorian
 */
public class HawkularReporterITest {

    private static final String BASE_URI;

    static {
        String baseUri = System.getProperty("hawkular-metrics.base-uri");
        if (baseUri == null || baseUri.trim().isEmpty()) {
            baseUri = "http://localhost:8080";
        }
        BASE_URI = baseUri;
    }

    private final MetricRegistry registry = new MetricRegistry();
    private final String defaultTenant = "unit-test";
    private final JdkHawkularHttpClient defaultClient = new JdkHawkularHttpClient(BASE_URI);
    private final Extractor<Object, Double> doubleExtractor = e -> (Double) ((JSONObject)e).get("value");
    private final Extractor<Object, Integer> intExtractor = e -> (Integer) ((JSONObject)e).get("value");

    @Before
    public void setup() {
        defaultClient.addHeaders(Collections.singletonMap("Hawkular-Tenant", defaultTenant));
    }

    @Test
    public void shouldReportCounter() throws IOException {
        String metricName = randomName();
        HawkularReporter reporter = HawkularReporter.builder(registry, defaultTenant)
                .uri(BASE_URI)
                .build();

        final Counter counter = registry.counter(metricName);
        counter.inc(5);
        reporter.report();

        HawkularHttpResponse response = defaultClient.readMetric("counters", metricName);

        assertThat(response.getResponseCode()).isEqualTo(200);
        JSONArray result = new JSONArray(response.getContent());
        assertThat(result).extracting(intExtractor).containsExactly(5);

        counter.inc(8);
        reporter.report();

        response = defaultClient.readMetric("counters", metricName);

        assertThat(response.getResponseCode()).isEqualTo(200);
        result = new JSONArray(response.getContent());
        assertThat(result).extracting(intExtractor).containsExactly(13, 5);
    }

    @Test
    public void shouldReportGauge() throws InterruptedException, IOException {
        String metricName = randomName();
        HawkularReporter reporter = HawkularReporter.builder(registry, defaultTenant)
                .uri(BASE_URI)
                .build();

        final AtomicReference<Double> gauge = new AtomicReference<>(10d);
        registry.register(metricName, (Gauge<Double>) gauge::get);
        reporter.report();
        gauge.set(7.1);
        Thread.sleep(50);
        reporter.report();
        gauge.set(13.4);
        Thread.sleep(50);
        reporter.report();

        HawkularHttpResponse response = defaultClient.readMetric("gauges", metricName);

        assertThat(response.getResponseCode()).isEqualTo(200);
        JSONArray result = new JSONArray(response.getContent());
        assertThat(result).extracting(doubleExtractor)
                .usingElementComparator(new DoubleComparator(0.001))
                .containsExactly(13.4, 7.1, 10d);
    }

    @Test
    public void shouldReportMeter() throws InterruptedException, IOException {
        String metricName = randomName();
        HawkularReporter reporter = HawkularReporter.builder(registry, defaultTenant)
                .uri(BASE_URI)
                .build();

        Meter meter = registry.meter(metricName);
        meter.mark(1000);
        Thread.sleep(100);
        meter.mark(1000);
        reporter.report();

        HawkularHttpResponse response = defaultClient.readMetric("gauges", metricName + ".meanrt");

        assertThat(response.getResponseCode()).isEqualTo(200);
        JSONArray result = new JSONArray(response.getContent());
        assertThat(result).hasSize(1);
        Double rate = doubleExtractor.extract(result.get(0));
        // Should be around 15000 ~ 18000, never more than 20000
        assertThat(rate).isBetween(3000d, 20000d);

        // It must also have posted a counter
        response = defaultClient.readMetric("counters", metricName + ".count");

        assertThat(response.getResponseCode()).isEqualTo(200);
        result = new JSONArray(response.getContent());
        assertThat(result).extracting(intExtractor).containsExactly(2000);
    }

    @Test
    public void shouldReportWithPrefix() throws IOException {
        String metricName = randomName();
        HawkularReporter reporter = HawkularReporter.builder(registry, defaultTenant)
                .uri(BASE_URI)
                .prefixedWith("prefix-")
                .build();

        registry.register(metricName, (Gauge<Double>) () -> 5d);
        reporter.report();

        HawkularHttpResponse response = defaultClient.readMetric("gauges", metricName);

        // Wrong metric name
        assertThat(response.getResponseCode()).isEqualTo(204);

        response = defaultClient.readMetric("gauges", "prefix-" + metricName);
        assertThat(response.getResponseCode()).isEqualTo(200);
    }

    @Test
    public void shouldUseFailoverDuration() throws IOException, InterruptedException {
        String metricName = randomName();
        HawkularReporter reporter = HawkularReporter.builder(registry, defaultTenant)
                .uri("http://invalid:999")
                .failoverCacheDuration(100)
                .build();
        JdkHawkularHttpClient client = (JdkHawkularHttpClient) reporter.getHawkularClient();

        Meter meter = registry.meter(metricName);
        meter.mark(1000);
        Thread.sleep(100);
        meter.mark(1000);
        reporter.report();

        // All 5 tags should have been evicted
        assertThat(client.getFailoverCacheSize()).isEqualTo(1);

        client.manageFailover();
        // No eviction
        assertThat(client.getFailoverCacheSize()).isEqualTo(1);

        Thread.sleep(100);
        client.manageFailover();
        // Everything should have been evicted
        assertThat(client.getFailoverCacheSize()).isEqualTo(0);
    }

    @Test
    public void shouldUseFailoverMaxRequests() throws IOException, InterruptedException {
        String metricName = randomName();
        HawkularReporter reporter = HawkularReporter.builder(registry, defaultTenant)
                .uri("http://invalid:999")
                .failoverCacheMaxSize(3)
                .build();

        Meter meter = registry.meter(metricName);
        meter.mark(1000);
        Thread.sleep(100);
        meter.mark(1000);
        reporter.report();

        // 6 requests (6 = 5 tags + 1 metric), but majored by 3
        assertThat(((JdkHawkularHttpClient) reporter.getHawkularClient()).getFailoverCacheSize()).isEqualTo(3);
    }

    @Test
    public void shouldUseFailoverWithoutRestriction() throws IOException, InterruptedException {
        String metricName = randomName();
        HawkularReporter reporter = HawkularReporter.builder(registry, defaultTenant)
                .uri("http://invalid:999")
                .build();

        Meter meter = registry.meter(metricName);
        meter.mark(1000);
        Thread.sleep(100);
        meter.mark(1000);
        reporter.report();

        // 6 = 5 tags + 1 metric
        assertThat(((JdkHawkularHttpClient) reporter.getHawkularClient()).getFailoverCacheSize()).isEqualTo(6);
    }

    private static String randomName() {
        return RandomStringUtils.randomAlphanumeric(8).toLowerCase();
    }
}
