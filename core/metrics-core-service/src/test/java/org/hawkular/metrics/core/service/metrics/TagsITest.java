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

package org.hawkular.metrics.core.service.metrics;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import static org.hawkular.metrics.model.AvailabilityType.DOWN;
import static org.hawkular.metrics.model.AvailabilityType.UP;
import static org.hawkular.metrics.model.MetricType.AVAILABILITY;
import static org.hawkular.metrics.model.MetricType.COUNTER;
import static org.hawkular.metrics.model.MetricType.COUNTER_RATE;
import static org.hawkular.metrics.model.MetricType.GAUGE;
import static org.joda.time.DateTime.now;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.hawkular.metrics.core.service.Order;
import org.hawkular.metrics.core.service.transformers.NumericDataPointCollector;
import org.hawkular.metrics.core.service.transformers.TaggedDataPointCollector;
import org.hawkular.metrics.model.AvailabilityType;
import org.hawkular.metrics.model.Buckets;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.NumericBucketPoint;
import org.hawkular.metrics.model.Percentile;
import org.hawkular.metrics.model.TaggedBucketPoint;
import org.hawkular.metrics.model.Tenant;
import org.hawkular.metrics.model.param.BucketConfig;
import org.hawkular.metrics.model.param.TimeRange;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import rx.Observable;
import rx.observers.TestSubscriber;

public class TagsITest extends BaseMetricsITest {

    @SuppressWarnings({ "rawtypes" })
    @Test
    public void createAndFindMetricsWithTags() throws Exception {

        String tenantId = "t1";

        List<Metric<?>> metricsToAdd = createTagMetrics(tenantId);

        // Check different scenarios..
        List<MetricId<Double>> gauges = metricsService
                .findMetricIdentifiersWithFilters(tenantId, GAUGE, "a1:*")
                .toList()
                .toBlocking().lastOrDefault(null);
        assertEquals(gauges.size(), 5, "Metrics m1-m5 should have been returned");

        gauges = metricsService.findMetricIdentifiersWithFilters(tenantId, GAUGE, "a1:*,a2:2")
                .toList().toBlocking().lastOrDefault(null);
        assertEquals(gauges.size(), 1, "Only metric m3 should have been returned");

        gauges = metricsService.findMetricIdentifiersWithFilters(tenantId, GAUGE, "a1:*,a2:2|3")
                .toList().toBlocking().lastOrDefault(null);
        assertEquals(gauges.size(), 2, "Metrics m3-m4 should have been returned");

        gauges = metricsService.findMetricIdentifiersWithFilters(tenantId, GAUGE, "a2:2|3")
                .toList().toBlocking().lastOrDefault(null);
        assertEquals(gauges.size(), 2, "Metrics m3-m4 should have been returned");

        gauges = metricsService.findMetricIdentifiersWithFilters(tenantId, GAUGE, "a1:*,a2:*")
                .toList().toBlocking().lastOrDefault(null);
        assertEquals(gauges.size(), 3, "Metrics m3-m5 should have been returned");

        gauges = metricsService.findMetricIdentifiersWithFilters(tenantId, GAUGE, "a1:*,a5:*")
                .toList().toBlocking().lastOrDefault(null);
        assertEquals(gauges.size(), 0, "No gauges should have been returned");

        gauges = metricsService.findMetricIdentifiersWithFilters(tenantId, GAUGE, "a4:*,a5:none")
                .toList().toBlocking().lastOrDefault(null);
        assertEquals(gauges.size(), 0, "No gauges should have been returned");

        List<MetricId<Object>> metrics = metricsService.findMetricIdentifiersWithFilters(tenantId, null, "a1:*")
                .toList().toBlocking().lastOrDefault(null);
        assertEquals(metrics.size(), 6, "Metrics m1-m5 and a1 should have been returned");

        // Test that we actually get correct gauges also, not just correct size
        gauges = metricsService.findMetricIdentifiersWithFilters(tenantId, GAUGE, "a1:2,a2:2")
                .toList().toBlocking().lastOrDefault(null);
        MetricId m3 = metricsToAdd.get(2).getMetricId();
        assertEquals(gauges.size(), 1, "Only metric m3 should have been returned");
        assertEquals(gauges.get(0), m3, "m3 did not match the original inserted metric");

        // Test for NOT operator
        gauges = metricsService.findMetricIdentifiersWithFilters(tenantId, GAUGE, "a2:!4")
                .toList().toBlocking().lastOrDefault(null);
        System.out.println(gauges);
        assertEquals(gauges.size(), 2, "Only gauges m3-m4 should have been returned");

        gauges = metricsService.findMetricIdentifiersWithFilters(tenantId, GAUGE, "a1:2,a2:!4")
                .toList().toBlocking().lastOrDefault(null);
        assertEquals(gauges.size(), 2, "Only gauges m3-m4 should have been returned");

        gauges = metricsService.findMetricIdentifiersWithFilters(tenantId, GAUGE, "a2:!4|3")
                .toList().toBlocking().lastOrDefault(null);
        assertEquals(gauges.size(), 1, "Only gauges m3 should have been returned");
        assertEquals(gauges.get(0), m3, "m3 did not match the original inserted metric");

        // What about incorrect query?
        try {
            metricsService.findMetricIdentifiersWithFilters(tenantId, GAUGE, "a2:**")
                    .toList().toBlocking().lastOrDefault(null);
            fail("Should have thrown an PatternSyntaxException");
        } catch (PatternSyntaxException ignored) {
        }

        // More regexp tests
        gauges = metricsService.findMetricIdentifiersWithFilters(tenantId, GAUGE, "hostname:web.*")
                .toList().toBlocking().lastOrDefault(null);
        assertEquals(gauges.size(), 2, "Only websrv01 and websrv02 should have been returned");

        gauges = metricsService.findMetricIdentifiersWithFilters(tenantId, GAUGE, "hostname:.*01")
                .toList().toBlocking().lastOrDefault(null);
        assertEquals(gauges.size(), 2, "Only websrv01 and backend01 should have been returned");

        gauges = metricsService.findMetricIdentifiersWithFilters(tenantId, GAUGE, "owner:h[e|a]de(s?)")
                .toList().toBlocking().lastOrDefault(null);
        assertEquals(gauges.size(), 2, "Both hede and hades should have been returned, but not 'had'");

        gauges = metricsService.findMetricIdentifiersWithFilters(tenantId, GAUGE, "owner:h[e|a]de(s?)")
                .filter(metricsService.idFilter(".F"))
                .toList().toBlocking().lastOrDefault(null);
        assertEquals(gauges.size(), 1, "Only hades should have been returned");

        // Not existing tags
        gauges = metricsService.<Double>findMetricIdentifiersWithFilters(tenantId, GAUGE, "!a2:*,a1:*")
                .doOnError(Throwable::printStackTrace)
                .toList().toBlocking().lastOrDefault(null);
        assertEquals(gauges.size(), 2, "Only metrics with a1, but without a2 and type GAUGE should have been found");

        gauges = metricsService.<Double>findMetricIdentifiersWithFilters(tenantId, GAUGE, "!a1:*")
                .doOnError(Throwable::printStackTrace)
                .toList().toBlocking().lastOrDefault(null);
        assertEquals(gauges.size(), 8, "Only metrics without a1 and type GAUGE should have been found");
    }

    @Test
    public void tagValueSearch() throws Exception {
        String tenantId = "t1tag";

        createTagMetrics(tenantId);

        // Test only tags value fetching
        Map<String, Set<String>> tagMap = metricsService
                .getTagValues(tenantId, null, ImmutableMap.of("hostname", "*"))
                .toBlocking()
                .lastOrDefault(null);

        Set<String> hostnameSet = tagMap.get("hostname");
        assertEquals(hostnameSet.size(), 4, "There should have been 4 hostname tag values");

        tagMap = metricsService.getTagValues(tenantId, null, ImmutableMap.of("a1", "*", "a2", "*"))
                .toBlocking()
                .lastOrDefault(null);

        assertEquals(tagMap.size(), 2, "We should have two keys, a1 and a2");
        assertEquals(tagMap.get("a1").size(), 1, "a1 should have only one valid value");
        assertEquals(tagMap.get("a2").size(), 3, "a2 should have three values");

        tagMap = metricsService.getTagValues(tenantId, AVAILABILITY, ImmutableMap.of("a1", "*"))
                .toBlocking()
                .lastOrDefault(null);
        assertEquals(tagMap.get("a1").size(), 1, "a1 should have only one valid value");
    }

    @Test
    public void tagNameSearch() throws Exception {
        String tenantId = "t1tag";
        createTagMetrics(tenantId);

        Observable<String> tagNames = metricsService.getTagNames(tenantId, null, null);
        TestSubscriber<String> testSubscriber = new TestSubscriber<>();
        tagNames.subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent(5, TimeUnit.SECONDS);
        testSubscriber.assertCompleted();
        testSubscriber.assertValueCount(5);

        tagNames = metricsService.getTagNames(tenantId, null, "host.*");
        testSubscriber = new TestSubscriber<>();
        tagNames.subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent(5, TimeUnit.SECONDS);
        testSubscriber.assertCompleted();
        testSubscriber.assertValueCount(1);

        tagNames = metricsService.getTagNames(tenantId, AVAILABILITY, null);
        testSubscriber = new TestSubscriber<>();
        tagNames.subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent(5, TimeUnit.SECONDS);
        testSubscriber.assertCompleted();
        testSubscriber.assertValueCount(1);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void doNotAllowInvalidMetricTags() {
        Map<String, String> invalidTagMap = ImmutableMap.of("key1", "value1", "", "value2");

        metricsService.addTags(new Metric<>(new MetricId<>("test", COUNTER_RATE, "counter-rate")), invalidTagMap)
                .toBlocking().lastOrDefault(null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void doNotAllowEmptyMetricTags() {
        metricsService.addTags(new Metric<>(new MetricId<>("test", COUNTER_RATE, "counter-rate")), null)
                .toBlocking()
                .lastOrDefault(null);
    }

    @Test
    public void updateMetricTags() throws Exception {
        Metric<Double> metric = new Metric<>(new MetricId<>("t1", GAUGE, "m1"),
                ImmutableMap.of("a1", "1", "a2", "2"), DEFAULT_TTL);
        metricsService.createMetric(metric, false).toBlocking().lastOrDefault(null);

        Map<String, String> additions = ImmutableMap.of("a2", "two", "a3", "3");
        metricsService.addTags(metric, additions).toBlocking().lastOrDefault(null);

        Set<String> deletions = ImmutableSet.of("a1");
        metricsService.deleteTags(metric, deletions).toBlocking()
                .lastOrDefault(null);

        Metric<?> updatedMetric = metricsService.findMetric(metric.getMetricId()).toBlocking()
                .last();

        assertEquals(updatedMetric.getTags(), ImmutableMap.of("a2", "two", "a3", "3"),
                "The updated meta data does not match the expected values");

        assertMetricIndexMatches(metric.getMetricId().getTenantId(), GAUGE, singletonList(updatedMetric));
    }

    @SuppressWarnings("unchecked")
    protected List<Metric<?>> createTagMetrics(String tenantId) throws Exception {
        ImmutableList<MetricId<?>> ids = ImmutableList.of(
                new MetricId<>(tenantId, GAUGE, "m1"),
                new MetricId<>(tenantId, GAUGE, "m2"),
                new MetricId<>(tenantId, GAUGE, "m3"),
                new MetricId<>(tenantId, GAUGE, "m4"),
                new MetricId<>(tenantId, GAUGE, "m5"),
                new MetricId<>(tenantId, GAUGE, "m6"),
                new MetricId<>(tenantId, GAUGE, "mA"),
                new MetricId<>(tenantId, GAUGE, "mB"),
                new MetricId<>(tenantId, GAUGE, "mC"),
                new MetricId<>(tenantId, GAUGE, "mD"),
                new MetricId<>(tenantId, GAUGE, "mE"),
                new MetricId<>(tenantId, GAUGE, "mF"),
                new MetricId<>(tenantId, GAUGE, "mG"),
                new MetricId<>(tenantId, AVAILABILITY, "a1"));

        ImmutableList<ImmutableMap<String, String>> maps = ImmutableList.of(
                ImmutableMap.of("a1", "1"),
                ImmutableMap.of("a1", "2", "a3", "3"),
                ImmutableMap.of("a1", "2", "a2", "2"),
                ImmutableMap.of("a1", "2", "a2", "3"),
                ImmutableMap.of("a1", "2", "a2", "4"),
                ImmutableMap.of("a2", "4"),
                ImmutableMap.of("hostname", "webfin01"),
                ImmutableMap.of("hostname", "webswe02"),
                ImmutableMap.of("hostname", "backendfin01"),
                ImmutableMap.of("hostname", "backendswe02"),
                ImmutableMap.of("owner", "hede"),
                ImmutableMap.of("owner", "hades"),
                ImmutableMap.of("owner", "had"),
                ImmutableMap.of("a1", "4"));
        assertEquals(ids.size(), maps.size(), "ids' size should equal to maps' size");

        // Create the metrics
        List<Metric<?>> metricsToAdd = new ArrayList<>(ids.size());
        for (int i = 0; i < ids.size(); i++) {
            if(ids.get(i).getType() == GAUGE) {
                metricsToAdd.add(new Metric<>((MetricId<Double>) ids.get(i), maps.get(i), 24, asList(
                        new DataPoint<>(System.currentTimeMillis(), 1.0))));
            } else {
                metricsToAdd.add(new Metric<>(ids.get(i), maps.get(i), 24));
            }
        }

        // Insert metrics
        Observable.from(metricsToAdd)
                .subscribe(m -> metricsService.createMetric(m, false).toBlocking().lastOrDefault(null));

        return metricsToAdd;
    }

    @Test
    public void addTaggedGaugeDataPoints() throws Exception {
        DateTime start = now().minusMinutes(30);
        DateTime end = start.plusMinutes(20);
        String tenantId = "tagged-data-points";

        Metric<Double> metric = new Metric<>(new MetricId<>(tenantId, GAUGE, "G1"), asList(
                new DataPoint<>(start.plusMinutes(4).getMillis(), 1.1, ImmutableMap.of("x", "1")),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 2.2, ImmutableMap.of("x", "2", "y", "5")),
                new DataPoint<>(start.getMillis(), 3.4)));

        doAction(() -> metricsService.addDataPoints(GAUGE, Observable.just(metric)));

        Observable<DataPoint<Double>> data = metricsService.findDataPoints(new MetricId<>(tenantId, GAUGE, "G1"),
                start.getMillis(), end.getMillis(), 0, Order.ASC);
        List<DataPoint<Double>> actual = toList(data);
        List<DataPoint<Double>> expected = asList(
                new DataPoint<>(start.getMillis(), 3.4),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 2.2, ImmutableMap.of("x", "2", "y", "5")),
                new DataPoint<>(start.plusMinutes(4).getMillis(), 1.1, ImmutableMap.of("x", "1")));

        assertEquals(actual, expected, "The tagged data does not match the expected values");
        assertMetricIndexMatches(tenantId, GAUGE, singletonList(new Metric<>(metric.getMetricId(),
                metric.getDataPoints(), 7)));

        data = metricsService.findDataPoints(new MetricId<>(tenantId, GAUGE, "G1"), start.getMillis(),
                end.getMillis(), 1, Order.ASC);
        actual = toList(data);
        expected = singletonList(new DataPoint<>(start.getMillis(), 3.4));

        assertEquals(actual, expected, "The tagged data does not match when limiting the number of data points");

        data = metricsService.findDataPoints(new MetricId<>(tenantId, GAUGE, "G1"), start.getMillis(),
                end.getMillis(), 0, Order.DESC);
        actual = toList(data);
        expected = asList(
                new DataPoint<>(start.plusMinutes(4).getMillis(), 1.1, ImmutableMap.of("x", "1")),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 2.2, ImmutableMap.of("x", "2", "y", "5")),
                new DataPoint<>(start.getMillis(), 3.4));

        assertEquals(actual, expected, "The tagg data does not match when the order is DESC");

        data = metricsService.findDataPoints(new MetricId<>(tenantId, GAUGE, "G1"), start.getMillis(),
                end.getMillis(), 1, Order.DESC);
        actual = toList(data);
        expected = singletonList(new DataPoint<>(start.plusMinutes(4).getMillis(), 1.1, ImmutableMap.of("x", "1")));

        assertEquals(actual, expected, "The tagged data does not match when the order is DESC and there is a limit");
    }

    @Test
    public void addTaggedAvailabilityDataPoints() throws Exception {
        DateTime start = now().minusMinutes(30);
        DateTime end = start.plusMinutes(20);
        String tenantId = "tagged-availability";

        Metric<AvailabilityType> metric = new Metric<>(new MetricId<>(tenantId, AVAILABILITY, "A1"), asList(
                new DataPoint<>(start.plusMinutes(4).getMillis(), UP, ImmutableMap.of("x", "1")),
                new DataPoint<>(start.plusMinutes(2).getMillis(), DOWN, ImmutableMap.of("x", "2", "y", "3")),
                new DataPoint<>(start.getMillis(), UP)));

        doAction(() -> metricsService.addDataPoints(AVAILABILITY, Observable.just(metric)));

        Observable<DataPoint<AvailabilityType>> data = null;
        try {
//            Observable<DataPoint<AvailabilityType>>
                    data = metricsService.findDataPoints(new MetricId<>(tenantId,
                    AVAILABILITY, "A1"), start.getMillis(), end.getMillis(), 0, Order.ASC);
        } catch(NullPointerException e) {
            e.printStackTrace();
        }
        List<DataPoint<AvailabilityType>> actual = toList(data);
        List<DataPoint<AvailabilityType>> expected = asList(
                new DataPoint<>(start.getMillis(), UP),
                new DataPoint<>(start.plusMinutes(2).getMillis(), DOWN, ImmutableMap.of("x", "2", "y", "3")),
                new DataPoint<>(start.plusMinutes(4).getMillis(), UP, ImmutableMap.of("x", "1")));

        assertEquals(actual, expected, "The tagged availability data points do not match");
        assertMetricIndexMatches(tenantId, AVAILABILITY, singletonList(new Metric<>(metric.getMetricId(),
                metric.getDataPoints(), 7)));

        data = metricsService.findDataPoints(new MetricId<>(tenantId, AVAILABILITY, "A1"), start.getMillis(),
                end.getMillis(), 1, Order.ASC);
        actual = toList(data);
        expected = singletonList(new DataPoint<>(start.getMillis(), UP));

        assertEquals(actual, expected, "The tagged availability data points do not match when there is a limit");

        data = metricsService.findDataPoints(new MetricId<>(tenantId, AVAILABILITY, "A1"), start.getMillis(),
                end.getMillis(), 0, Order.DESC);
        actual = toList(data);
        expected = asList(
                new DataPoint<>(start.plusMinutes(4).getMillis(), UP, ImmutableMap.of("x", "1")),
                new DataPoint<>(start.plusMinutes(2).getMillis(), DOWN, ImmutableMap.of("x", "2", "y", "3")),
                new DataPoint<>(start.getMillis(), UP));

        assertEquals(actual, expected, "The tagged availability data points do not match when the order is DESC");

        data = metricsService.findDataPoints(new MetricId<>(tenantId, AVAILABILITY, "A1"), start.getMillis(),
                end.getMillis(), 1, Order.DESC);
        actual = toList(data);
        expected = singletonList(new DataPoint<>(start.plusMinutes(4).getMillis(), UP, ImmutableMap.of("x", "1")));

        assertEquals(actual, expected, "The tagged availability data points do not match when the order is DESC and " +
                "there is a limit");
    }

    @Test
    public void addTaggedCounterDataPoints() throws Exception {
        DateTime start = now().minusMinutes(30);
        DateTime end = start.plusMinutes(20);
        String tenantId = "counters-tenant";

        doAction(() -> metricsService.createTenant(new Tenant(tenantId), false));

        Metric<Long> counter = new Metric<>(new MetricId<>(tenantId, COUNTER, "c1"), asList(
                new DataPoint<>(start.getMillis(), 10L, ImmutableMap.of("x", "1")),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 15L, ImmutableMap.of("x", "2", "y", "3")),
                new DataPoint<>(start.plusMinutes(4).getMillis(), 25L, ImmutableMap.of("z", "5")),
                new DataPoint<>(end.getMillis(), 45L)));

        doAction(() -> metricsService.addDataPoints(COUNTER, Observable.just(counter)));

        Observable<DataPoint<Long>> data = metricsService.findDataPoints(new MetricId<>(tenantId, COUNTER, "c1"),
                start.getMillis(), end.getMillis(), 0, Order.DESC);
        List<DataPoint<Long>> actual = toList(data);
        List<DataPoint<Long>> expected = asList(
                new DataPoint<>(start.plusMinutes(4).getMillis(), 25L, ImmutableMap.of("z", "5")),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 15L, ImmutableMap.of("x", "2", "y", "3")),
                new DataPoint<>(start.getMillis(), 10L, ImmutableMap.of("x", "1")));

        assertEquals(actual, expected, "The tagged counter data does not match the expected values");
        assertMetricIndexMatches(tenantId, COUNTER,
                singletonList(new Metric<>(counter.getMetricId(), counter.getDataPoints(), 7)));

        data = metricsService.findDataPoints(new MetricId<>(tenantId, COUNTER, "c1"), start.getMillis(),
                end.getMillis(), 1, Order.DESC);
        actual = toList(data);
        expected = singletonList(new DataPoint<>(start.plusMinutes(4).getMillis(), 25L, ImmutableMap.of("z", "5")));

        assertEquals(actual, expected, "The tagged counter data does not match when there is a limit");

        data = metricsService.findDataPoints(new MetricId<>(tenantId, COUNTER, "c1"), start.getMillis(),
                end.getMillis(), 0, Order.ASC);
        actual = toList(data);
        expected = asList(
                new DataPoint<>(start.getMillis(), 10L, ImmutableMap.of("x", "1")),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 15L, ImmutableMap.of("x", "2", "y", "3")),
                new DataPoint<>(start.plusMinutes(4).getMillis(), 25L, ImmutableMap.of("z", "5")));

        assertEquals(actual, expected, "The tagged counter data does not match when the order is ASC");

        data = metricsService.findDataPoints(new MetricId<>(tenantId, COUNTER, "c1"), start.getMillis(),
                end.getMillis(), 1, Order.ASC);
        actual = toList(data);
        expected = singletonList(new DataPoint<>(start.getMillis(), 10L, ImmutableMap.of("x", "1")));

        assertEquals(actual, expected, "The tagged counter data does not match when the order is ASC and there is a " +
                "limit");
    }

    @Test
    public void findCounterStats() {
        String tenantId = "counter-stats-test";
        long now = now().getMillis();

        Metric<Long> counter = new Metric<>(new MetricId<>(tenantId, COUNTER, "C1"), asList(
                new DataPoint<>((long) (now + 60_000 * 1.0), 0L),
                new DataPoint<>((long) (now + 60_000 * 1.5), 200L),
                new DataPoint<>((long) (now + 60_000 * 3.5), 400L),
                new DataPoint<>((long) (now + 60_000 * 5.0), 550L),
                new DataPoint<>((long) (now + 60_000 * 7.0), 950L),
                new DataPoint<>((long) (now + 60_000 * 7.5), 1000L)));

        doAction(() -> metricsService.addDataPoints(COUNTER, Observable.just(counter)));

        BucketConfig bucketConfig = mock(BucketConfig.class);
        when(bucketConfig.getTimeRange()).thenReturn(new TimeRange(now, now + 60_000*8));
        when(bucketConfig.getBuckets()).thenReturn(Buckets.fromStep(now + 60_000, now + 60_000 * 8, 60_000));
        List<NumericBucketPoint> actual = metricsService.findCounterStats(counter.getMetricId(),
                bucketConfig, asList(new Percentile("95.0"))).toBlocking().single();

        List<NumericBucketPoint> expected = new ArrayList<>();
        for (int i = 1; i < 8; i++) {
            NumericBucketPoint.Builder builder = new NumericBucketPoint.Builder(now + 60_000 * i, now + (60_000 * (i +
                    1)));
            switch (i) {
                case 1:
                    builder.setAvg(100D).setMax(200D).setMedian(0D).setMin(0D)
                            .setPercentiles(asList(new Percentile("95.0", 0D))).setSamples(2).setSum(200D);
                    break;
                case 2:
                case 4:
                case 6:
                    break;
                case 3:
                    builder.setAvg(400D).setMax(400D).setMedian(400D).setMin(400D)
                            .setPercentiles(asList(new Percentile("95.0", 400D))).setSamples(1).setSum(400D);
                    break;
                case 5:
                    builder.setAvg(550D).setMax(550D).setMedian(550D).setMin(550D)
                            .setPercentiles(asList(new Percentile("95.0", 550D))).setSamples(1).setSum(550D);
                    break;
                case 7:
                    builder.setAvg(975D).setMax(1000D).setMedian(950D).setMin(950D)
                            .setPercentiles(asList(new Percentile("95.0", 950D))).setSamples(2).setSum(1950D);
                    break;
            }
            expected.add(builder.build());
        }

        assertNumericBucketsEquals(actual, expected);
    }

    @Test
    public void findSimpleGaugeStatsByTags() {
        NumericDataPointCollector.createPercentile = InMemoryPercentileWrapper::new;

        String tenantId = "findGaugeStatsByTags";
        DateTime start = now().minusMinutes(10);

        Metric<Double> m1 = new Metric<>(new MetricId<>(tenantId, GAUGE, "M1"), asList(
                new DataPoint<>(start.getMillis(), 12.23),
                new DataPoint<>(start.plusMinutes(1).getMillis(), 9.745),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 14.01),
                new DataPoint<>(start.plusMinutes(3).getMillis(), 16.18),
                new DataPoint<>(start.plusMinutes(4).getMillis(), 18.94)));
        doAction(() -> metricsService.addDataPoints(GAUGE, Observable.just(m1)));
        doAction(() -> metricsService.addTags(m1, ImmutableMap.of("type", "cpu_usage", "node", "server1")));

        Metric<Double> m2 = new Metric<>(new MetricId<>(tenantId, GAUGE, "M2"), asList(
                new DataPoint<>(start.getMillis(), 15.47),
                new DataPoint<>(start.plusMinutes(1).getMillis(), 8.08),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 14.39),
                new DataPoint<>(start.plusMinutes(3).getMillis(), 17.76),
                new DataPoint<>(start.plusMinutes(4).getMillis(), 17.502)));
        doAction(() -> metricsService.addDataPoints(GAUGE, Observable.just(m2)));
        doAction(() -> metricsService.addTags(m2, ImmutableMap.of("type", "cpu_usage", "node", "server2")));

        Metric<Double> m3 = new Metric<>(new MetricId<>(tenantId, GAUGE, "M3"), asList(
                new DataPoint<>(start.getMillis(), 11.456),
                new DataPoint<>(start.plusMinutes(1).getMillis(), 18.32)));
        doAction(() -> metricsService.addDataPoints(GAUGE, Observable.just(m3)));
        doAction(() -> metricsService.addTags(m3, ImmutableMap.of("type", "cpu_usage", "node", "server3")));

        Buckets buckets = Buckets.fromCount(start.getMillis(), start.plusMinutes(5).getMillis(), 1);
        String tagFilters = "type:cpu_usage,node:server1|server2";

        List<List<NumericBucketPoint>> actual = getOnNextEvents(
                () -> metricsService.findMetricIdentifiersWithFilters(tenantId, GAUGE, tagFilters)
                        .toList()
                        .flatMap(ids -> metricsService.findNumericStats(ids, start.getMillis(), start.plusMinutes(5)
                                        .getMillis(), buckets, emptyList(), false, false)));

        assertEquals(actual.size(), 1);

        List<NumericBucketPoint> expected = Arrays.asList(createSingleBucket(
                Stream.concat(m1.getDataPoints().stream(), m2.getDataPoints().stream()).collect(Collectors.toList()),
                start, start.plusMinutes(5)));

        assertNumericBucketsEquals(actual.get(0), expected);
    }

    @Test
    public void findTaggedGaugeBucketPointsWithSimpleTagQuery() throws Exception {
        String tenantId = "tagged-bucket-points";
        DateTime start = now();
        DateTime end = start.plusHours(2);

        MetricId<Double> metricId = new MetricId<>(tenantId, GAUGE, "m1");
        Metric<Double> metric = new Metric<>(metricId, asList(
                new DataPoint<>(start.getMillis(), 27.43, ImmutableMap.of("x", "1")),
                new DataPoint<>(start.plusMinutes(5).getMillis(), 32.05, ImmutableMap.of("x", "2")),
                new DataPoint<>(start.plusMinutes(10).getMillis(), 34.14, ImmutableMap.of("x", "2")),
                new DataPoint<>(start.plusMinutes(15).getMillis(), 29.73, ImmutableMap.of("x", "1")),
                new DataPoint<>(start.plusMinutes(20).getMillis(), 28.44, ImmutableMap.of("x", "3")),
                new DataPoint<>(start.plusMinutes(25).getMillis(), 51.91, ImmutableMap.of("x", "3"))));
        doAction(() -> metricsService.addDataPoints(GAUGE, Observable.just(metric)));

        Map<String, TaggedBucketPoint> actual = getOnNextEvents(
                () -> metricsService.findGaugeStats(metricId, ImmutableMap.of("x", "*"), start.getMillis(),
                        end.getMillis(), emptyList())).get(0);

        assertEquals(actual.size(), 3, "The number of buckets do not match");

        TaggedDataPointCollector collector = new TaggedDataPointCollector(ImmutableMap.of("x", "1"), emptyList());
        collector.increment(metric.getDataPoints().get(0));
        collector.increment(metric.getDataPoints().get(3));
        assertEquals(actual.get("x:1"), collector.toBucketPoint());

        collector = new TaggedDataPointCollector(ImmutableMap.of("x", "2"), emptyList());
        collector.increment(metric.getDataPoints().get(1));
        collector.increment(metric.getDataPoints().get(2));
        assertEquals(actual.get("x:2"), collector.toBucketPoint());

        collector = new TaggedDataPointCollector(ImmutableMap.of("x", "3"), emptyList());
        collector.increment(metric.getDataPoints().get(4));
        collector.increment(metric.getDataPoints().get(5));
        assertEquals(actual.get("x:3"), collector.toBucketPoint());
    }

    @Test
    public void findTaggedGaugeBucketPointsWithMultipleTagFilters() throws Exception {
        String tenantId = "multiple-tag-filter-data-point-query";
        DateTime start = now();
        DateTime end = start.plusHours(2);

        MetricId<Double> metricId = new MetricId<>(tenantId, GAUGE, "m1");
        Metric<Double> metric = new Metric<>(metricId, asList(
                new DataPoint<>(start.getMillis(), 11.1, ImmutableMap.of("x", "1", "y", "1", "z", "1")),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 13.3, ImmutableMap.of("x", "2", "y", "2", "z", "2")),
                new DataPoint<>(start.plusMinutes(4).getMillis(), 14.4, ImmutableMap.of("x", "3", "y", "2", "z", "3")),
                new DataPoint<>(start.plusMinutes(6).getMillis(), 15.5,
                        ImmutableMap.of("x", "1", "y", "3", "z", "4"))));
        doAction(() -> metricsService.addDataPoints(GAUGE, Observable.just(metric)));

        Map<String, TaggedBucketPoint> actual = getOnNextEvents(
                () -> metricsService.findGaugeStats(metricId, ImmutableMap.of("x", "*", "y", "2", "z", "2|3"),
                        start.getMillis(), end.getMillis(), Collections.emptyList())).get(0);
        assertEquals(actual.size(), 2);

        TaggedDataPointCollector collector = new TaggedDataPointCollector(
                ImmutableMap.of("x", "2", "y", "2", "z", "2"),
                emptyList());
        collector.increment(metric.getDataPoints().get(1));
        assertEquals(actual.get("x:2,y:2,z:2"), collector.toBucketPoint());

        collector = new TaggedDataPointCollector(ImmutableMap.of("x", "3", "y", "2", "z", "3"), emptyList());
        collector.increment(metric.getDataPoints().get(2));
        assertEquals(actual.get("x:3,y:2,z:3"), collector.toBucketPoint());
    }

    @Test
    public void findTaggedCounterBucketPointsWithMultipleTagFilters() throws Exception {
        String tenantId = "multiple-tag-filter-counter-data-point-query";
        DateTime start = now();
        DateTime end = start.plusHours(2);

        MetricId<Long> metricId = new MetricId<>(tenantId, COUNTER, "C1");
        Metric<Long> metric = new Metric<>(metricId, asList(
                new DataPoint<>(start.getMillis(), 11L, ImmutableMap.of("x", "1", "y", "1", "z", "1")),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 13L, ImmutableMap.of("x", "2", "y", "2", "z", "2")),
                new DataPoint<>(start.plusMinutes(4).getMillis(), 14L, ImmutableMap.of("x", "3", "y", "2", "z", "3")),
                new DataPoint<>(start.plusMinutes(6).getMillis(), 15L,
                        ImmutableMap.of("x", "1", "y", "3", "z", "4"))));
        doAction(() -> metricsService.addDataPoints(COUNTER, Observable.just(metric)));

        Map<String, TaggedBucketPoint> actual = getOnNextEvents(
                () -> metricsService.findCounterStats(metricId, ImmutableMap.of("x", "*", "y", "2", "z", "2|3"),
                        start.getMillis(), end.getMillis(), Collections.emptyList())).get(0);
        assertEquals(actual.size(), 2);

        TaggedDataPointCollector collector = new TaggedDataPointCollector(
                ImmutableMap.of("x", "2", "y", "2", "z", "2"),
                emptyList());
        collector.increment(metric.getDataPoints().get(1));
        assertEquals(actual.get("x:2,y:2,z:2"), collector.toBucketPoint());

        collector = new TaggedDataPointCollector(ImmutableMap.of("x", "3", "y", "2", "z", "3"), emptyList());
        collector.increment(metric.getDataPoints().get(2));
        assertEquals(actual.get("x:3,y:2,z:3"), collector.toBucketPoint());
    }

}
