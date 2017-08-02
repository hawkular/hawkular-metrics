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
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;

import static org.hawkular.metrics.model.MetricType.GAUGE;
import static org.joda.time.DateTime.now;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.math3.stat.descriptive.summary.Sum;
import org.hawkular.metrics.core.service.Aggregate;
import org.hawkular.metrics.core.service.Order;
import org.hawkular.metrics.core.service.transformers.NumericDataPointCollector;
import org.hawkular.metrics.model.Buckets;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.NumericBucketPoint;
import org.hawkular.metrics.model.Tenant;
import org.joda.time.DateTime;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

import rx.Observable;

/**
 * @author John Sanda
 */
public class GaugeITest extends BaseMetricsITest {

    private String tenantId;

    @BeforeMethod
    public void initTest(Method method) {
        tenantId = method.getName();
    }

    @Test
    public void addAndFetchGaugeData() throws Exception {
        DateTime start = now();
        DateTime end = start.plusMinutes(50);

        doAction(() -> metricsService.createTenant(new Tenant(tenantId), false));

        Metric<Double> m1 = new Metric<>(new MetricId<>(tenantId, GAUGE, "m1"), asList(
                new DataPoint<>(start.getMillis(), 1.1),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 2.2),
                new DataPoint<>(start.plusMinutes(4).getMillis(), 3.3),
                new DataPoint<>(end.getMillis(), 4.4)));

        doAction(() -> metricsService.addDataPoints(GAUGE, Observable.just(m1)));

        Observable<DataPoint<Double>> observable = metricsService.findDataPoints(new MetricId<>(tenantId, GAUGE, "m1"),
                start.getMillis(), end.getMillis(), 0, Order.DESC);
        List<DataPoint<Double>> actual = toList(observable);
        List<DataPoint<Double>> expected = asList(
                new DataPoint<>(start.plusMinutes(4).getMillis(), 3.3),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 2.2),
                new DataPoint<>(start.getMillis(), 1.1));

        assertEquals(actual, expected, "The data does not match the expected values");
        assertMetricIndexMatches(tenantId, GAUGE, singletonList(new Metric<>(m1.getMetricId(), m1.getDataPoints(), 7)));
    }

    @Test
    public void addGaugeDataForMultipleMetrics() throws Exception {
        DateTime start = now().minusMinutes(10);
        DateTime end = start.plusMinutes(8);

        metricsService.createTenant(new Tenant(tenantId), false).toBlocking().lastOrDefault(null);

        Metric<Double> m1 = new Metric<>(new MetricId<>(tenantId, GAUGE, "m1"), asList(
                new DataPoint<>(start.plusSeconds(30).getMillis(), 11.2),
                new DataPoint<>(start.getMillis(), 11.1)));

        Metric<Double> m2 = new Metric<>(new MetricId<>(tenantId, GAUGE, "m2"), asList(
                new DataPoint<>(start.plusSeconds(30).getMillis(), 12.2),
                new DataPoint<>(start.getMillis(), 12.1)));

        Metric<Double> m3 = new Metric<>(new MetricId<>(tenantId, GAUGE, "m3"));

        Metric<Double> m4 = new Metric<>(new MetricId<>(tenantId, GAUGE, "m4"), emptyMap(), 24, asList(
                new DataPoint<>(start.plusSeconds(30).getMillis(), 55.5),
                new DataPoint<>(end.getMillis(), 66.6)));
        metricsService.createMetric(m4, false).toBlocking().lastOrDefault(null);

        metricsService.addDataPoints(GAUGE, Observable.just(m1, m2, m3, m4)).toBlocking().lastOrDefault(null);

        Observable<DataPoint<Double>> observable1 = metricsService.findDataPoints(m1.getMetricId(),
                start.getMillis(), end.getMillis(), 0, Order.DESC);
        assertEquals(toList(observable1), m1.getDataPoints(),
                "The gauge data for " + m1.getMetricId() + " does not match");

        Observable<DataPoint<Double>> observable2 = metricsService.findDataPoints(m2.getMetricId(),
                start.getMillis(), end.getMillis(), 0, Order.DESC);
        assertEquals(toList(observable2), m2.getDataPoints(),
                "The gauge data for " + m2.getMetricId() + " does not match");

        Observable<DataPoint<Double>> observable3 = metricsService.findDataPoints(m3.getMetricId(),
                start.getMillis(), end.getMillis(), 0, Order.DESC);
        assertTrue(toList(observable3).isEmpty(), "Did not expect to get back results for " + m3.getMetricId());

        Observable<DataPoint<Double>> observable4 = metricsService.findDataPoints(m4.getMetricId(),
                start.getMillis(), end.getMillis(), 0, Order.DESC);
        Metric<Double> expected = new Metric<>(new MetricId<>(tenantId, GAUGE, "m4"), emptyMap(), 24,
                singletonList(new DataPoint<>(start.plusSeconds(30).getMillis(), 55.5)));
        assertEquals(toList(observable4), expected.getDataPoints(),
                "The gauge data for " + m4.getMetricId() + " does not match");

        assertMetricIndexMatches(tenantId, GAUGE, asList(
                new Metric<>(m1.getMetricId(), m1.getDataPoints(), 7),
                new Metric<>(m2.getMetricId(), m2.getDataPoints(), 7),
                m4));
    }

    @Test
    public void getPeriodsAboveThreshold() throws Exception {
        DateTime start = now().minusMinutes(20);
        double threshold = 20.0;

        List<DataPoint<Double>> dataPoints = asList(
                new DataPoint<>(start.getMillis(), 14.0),
                new DataPoint<>(start.plusMinutes(1).getMillis(), 18.0),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 21.0),
                new DataPoint<>(start.plusMinutes(3).getMillis(), 23.0),
                new DataPoint<>(start.plusMinutes(3).getMillis(), 23.0),
                new DataPoint<>(start.plusMinutes(4).getMillis(), 20.0),
                new DataPoint<>(start.plusMinutes(5).getMillis(), 19.0),
                new DataPoint<>(start.plusMinutes(6).getMillis(), 22.0),
                new DataPoint<>(start.plusMinutes(6).getMillis(), 22.0),
                new DataPoint<>(start.plusMinutes(6).getMillis(), 22.0),
                new DataPoint<>(start.plusMinutes(7).getMillis(), 15.0),
                new DataPoint<>(start.plusMinutes(8).getMillis(), 31.0),
                new DataPoint<>(start.plusMinutes(9).getMillis(), 30.0),
                new DataPoint<>(start.plusMinutes(10).getMillis(), 31.0));
        Metric<Double> m1 = new Metric<>(new MetricId<>(tenantId, GAUGE, "m1"), dataPoints);

        metricsService.addDataPoints(GAUGE, Observable.just(m1))
                .doOnError(Throwable::printStackTrace)
                .toBlocking().lastOrDefault(null);

        List<long[]> actual = metricsService.getPeriods(m1.getMetricId(),
                value -> value > threshold, start.getMillis(), now().getMillis())
                .doOnError(Throwable::printStackTrace)
                .toBlocking().lastOrDefault(null);
        List<long[]> expected = asList(
                new long[] { start.plusMinutes(2).getMillis(), start.plusMinutes(3).getMillis() },
                new long[] { start.plusMinutes(6).getMillis(), start.plusMinutes(6).getMillis() },
                new long[] { start.plusMinutes(8).getMillis(), start.plusMinutes(10).getMillis() });

        assertEquals(actual.size(), expected.size(), "The number of periods is wrong");
        for (int i = 0; i < expected.size(); ++i) {
            assertArrayEquals(actual.get(i), expected.get(i), "The period does not match the expected value");
        }
    }

    @Test
    public void findStackedGaugeStatsByMetricNames() {
        NumericDataPointCollector.createPercentile = InMemoryPercentileWrapper::new;

        DateTime start = now().minusMinutes(10);

        MetricId<Double> m1Id = new MetricId<>(tenantId, GAUGE, "M1");
        Metric<Double> m1 = new Metric<>(m1Id, getDataPointList("M1", start));
        doAction(() -> metricsService.addDataPoints(GAUGE, Observable.just(m1)));

        MetricId<Double> m2Id = new MetricId<>(tenantId, GAUGE, "M2");
        Metric<Double> m2 = new Metric<>(m2Id, getDataPointList("M2", start));
        doAction(() -> metricsService.addDataPoints(GAUGE, Observable.just(m2)));

        MetricId<Double> m3Id = new MetricId<>(tenantId, GAUGE, "M3");
        Metric<Double> m3 = new Metric<>(m3Id, getDataPointList("M3", start));
        doAction(() -> metricsService.addDataPoints(GAUGE, Observable.just(m3)));

        Buckets buckets = Buckets.fromCount(start.getMillis(), start.plusMinutes(5).getMillis(), 1);

        List<List<NumericBucketPoint>> actual = getOnNextEvents(() -> metricsService.findNumericStats(asList(m1Id,
                m2Id), start.getMillis(), start.plusMinutes(5).getMillis(), buckets, emptyList(), true, false));

        assertEquals(actual.size(), 1);

        NumericBucketPoint collectorM1 = createSingleBucket(m1.getDataPoints(), start, start.plusMinutes(5));
        NumericBucketPoint collectorM2 = createSingleBucket(m2.getDataPoints(), start, start.plusMinutes(5));

        final Sum min = new Sum();
        final Sum average = new Sum();
        final Sum median = new Sum();
        final Sum max = new Sum();
        Observable.just(collectorM1, collectorM2).forEach(d -> {
            min.increment(d.getMin());
            max.increment(d.getMax());
            average.increment(d.getAvg());
            median.increment(d.getMedian());
        });

        assertEquals(actual.get(0).get(0).getMin(), min.getResult());
        assertEquals(actual.get(0).get(0).getMax(), max.getResult());
        assertEquals(actual.get(0).get(0).getMedian(), median.getResult());
        assertEquals(actual.get(0).get(0).getAvg(), average.getResult());
    }

    @Test
    public void findSimpleGaugeStatsByMetricNames() {
        NumericDataPointCollector.createPercentile = InMemoryPercentileWrapper::new;

        DateTime start = now().minusMinutes(10);

        MetricId<Double> m1Id = new MetricId<>(tenantId, GAUGE, "M1");
        Metric<Double> m1 = new Metric<>(m1Id, getDataPointList("M1", start));
        doAction(() -> metricsService.addDataPoints(GAUGE, Observable.just(m1)));

        MetricId<Double> m2Id = new MetricId<>(tenantId, GAUGE, "M2");
        Metric<Double> m2 = new Metric<>(m2Id, getDataPointList("M2", start));
        doAction(() -> metricsService.addDataPoints(GAUGE, Observable.just(m2)));

        MetricId<Double> m3Id = new MetricId<>(tenantId, GAUGE, "M3");
        Metric<Double> m3 = new Metric<>(m3Id, getDataPointList("M3", start));
        doAction(() -> metricsService.addDataPoints(GAUGE, Observable.just(m3)));

        Buckets buckets = Buckets.fromCount(start.getMillis(), start.plusMinutes(5).getMillis(), 1);

        List<List<NumericBucketPoint>> actual = getOnNextEvents(() -> metricsService.findNumericStats(
                asList(m1Id, m2Id), start.getMillis(), start.plusMinutes(5).getMillis(), buckets,
                emptyList(), false, false));

        assertEquals(actual.size(), 1);

        List<NumericBucketPoint> expected = Arrays.asList(createSingleBucket(
                Stream.concat(m1.getDataPoints().stream(), m2.getDataPoints().stream()).collect(Collectors.toList()),
                start, start.plusMinutes(5)));

        assertNumericBucketsEquals(actual.get(0), expected);
    }

    @Test
    public void findStackedGaugeStatsByTags() {
        NumericDataPointCollector.createPercentile = InMemoryPercentileWrapper::new;

        DateTime start = now().minusMinutes(10);

        MetricId<Double> m1Id = new MetricId<>(tenantId, GAUGE, "M1");
        Metric<Double> m1 = new Metric<>(m1Id, getDataPointList("M1", start));
        doAction(() -> metricsService.addDataPoints(GAUGE, Observable.just(m1)));
        doAction(() -> metricsService.addTags(m1, ImmutableMap.of("type", "cpu_usage", "node", "server1")));

        MetricId<Double> m2Id = new MetricId<>(tenantId, GAUGE, "M2");
        Metric<Double> m2 = new Metric<>(m2Id, getDataPointList("M2", start));
        doAction(() -> metricsService.addDataPoints(GAUGE, Observable.just(m2)));
        doAction(() -> metricsService.addTags(m2, ImmutableMap.of("type", "cpu_usage", "node", "server2")));

        MetricId<Double> m3Id = new MetricId<>(tenantId, GAUGE, "M3");
        Metric<Double> m3 = new Metric<>(m3Id, getDataPointList("M3", start));
        doAction(() -> metricsService.addDataPoints(GAUGE, Observable.just(m3)));
        doAction(() -> metricsService.addTags(m3, ImmutableMap.of("type", "cpu_usage", "node", "server3")));

        Buckets buckets = Buckets.fromCount(start.getMillis(), start.plusMinutes(5).getMillis(), 1);
        String tagFilters = "type:cpu_usage,node:server1|server2";

        List<List<NumericBucketPoint>> actual = getOnNextEvents(
                () -> metricsService.findMetricIdentifiersWithFilters(tenantId, GAUGE, tagFilters)
                        .toList()
                        .flatMap(metrics -> metricsService.findNumericStats(metrics, start.getMillis(), start
                                .plusMinutes(5).getMillis(), buckets, emptyList(), true, false)));

        assertEquals(actual.size(), 1);

        NumericBucketPoint collectorM1 = createSingleBucket(m1.getDataPoints(), start, start.plusMinutes(5));
        NumericBucketPoint collectorM2 = createSingleBucket(m2.getDataPoints(), start, start.plusMinutes(5));

        final Sum min = new Sum();
        final Sum average = new Sum();
        final Sum median = new Sum();
        final Sum max = new Sum();
        Observable.just(collectorM1, collectorM2).forEach(d -> {
            min.increment(d.getMin());
            max.increment(d.getMax());
            average.increment(d.getAvg());
            median.increment(d.getMedian());
        });

        assertEquals(actual.get(0).get(0).getMin(), min.getResult());
        assertEquals(actual.get(0).get(0).getMax(), max.getResult());
        assertEquals(actual.get(0).get(0).getMedian(), median.getResult());
        assertEquals(actual.get(0).get(0).getAvg(), average.getResult());
    }

    @Test
    public void addAndFetchGaugeDataAggregates() throws Exception {
        DateTime start = now();
        DateTime end = start.plusMinutes(50);

        metricsService.createTenant(new Tenant(tenantId), false).toBlocking().lastOrDefault(null);

        Metric<Double> m1 = new Metric<>(new MetricId<>(tenantId, GAUGE, "m1"), asList(
                new DataPoint<>(start.getMillis(), 10.0),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 20.0),
                new DataPoint<>(start.plusMinutes(4).getMillis(), 30.0),
                new DataPoint<>(end.getMillis(), 40.0)));

        Observable<Void> insertObservable = metricsService.addDataPoints(GAUGE, Observable.just(m1));
        insertObservable.toBlocking().lastOrDefault(null);

        @SuppressWarnings("unchecked")
        Observable<Double> observable = metricsService
                .findGaugeData(new MetricId<>(tenantId, GAUGE, "m1"), start.getMillis(), (end.getMillis() + 1000),
                        Aggregate.Min, Aggregate.Max, Aggregate.Average, Aggregate.Sum);
        List<Double> actual = toList(observable);
        List<Double> expected = asList(10.0, 40.0, 25.0, 100.0);

        assertEquals(actual, expected, "The data does not match the expected values");
        assertMetricIndexMatches(tenantId, GAUGE, singletonList(new Metric<>(m1.getMetricId(), m1.getDataPoints(), 7)));
    }

    @Test
    public void addDataForSingleGaugeAndFindWithLimit() throws Exception {
        // Limited copy of the similar method in rest-tests, as these cases not tested in our unit tests
        // This targets the MetricsServiceImpl filtering without compression (effectively out-of-order writes)
        metricsService.createTenant(new Tenant(tenantId), false).toBlocking().lastOrDefault(null);

        final DateTime start = now();
        long end = start.plusHours(1).getMillis();
        MetricId<Double> mId = new MetricId<>(tenantId, GAUGE, "m1");

        Function<DateTime, List<DataPoint<Double>>> metricsInserter = (startTime) -> {
            Observable<Metric<Double>> just = Observable.just(new Metric<>
                    (mId, asList(
                            new DataPoint<>(startTime.getMillis(), 100.1),
                            new DataPoint<>(startTime.plusMinutes(1).getMillis(), 200.2),
                            new DataPoint<>(startTime.plusMinutes(2).getMillis(), 300.3),
                            new DataPoint<>(startTime.plusMinutes(3).getMillis(), 400.4),
                            new DataPoint<>(startTime.plusMinutes(4).getMillis(), 500.5),
                            new DataPoint<>(startTime.plusMinutes(5).getMillis(), 600.6),
                            new DataPoint<>(now().plusSeconds(30).getMillis(), 750.7))));
            return toList(metricsService.addDataPoints(GAUGE, just)
                    .flatMap(aVoid -> metricsService.findDataPoints(new MetricId<>(tenantId,
                            GAUGE, "m1"), startTime.plusMinutes(1).getMillis(), end, 3, Order.ASC)));
        };

        Function<DateTime, List<DataPoint<Double>>> expectCreator = (startTime) -> asList(
                new DataPoint<>(startTime.plusMinutes(1).getMillis(), 200.2),
                new DataPoint<>(startTime.plusMinutes(2).getMillis(), 300.3),
                new DataPoint<>(startTime.plusMinutes(3).getMillis(), 400.4));

        List<DataPoint<Double>> actual = metricsInserter.apply(start);
        List<DataPoint<Double>> expected = expectCreator.apply(start);
        assertEquals(expected, actual);

        // Compress and repeat.. we should get the same results

        // We need new timestamp because MetricsServiceImpl does not look for compressed data when timestamp > now-2h
//        DateTime cStart = now().minusHours(7);
//
//        metricsInserter.apply(cStart);
//        expected = expectCreator.apply(cStart);
//
//        DateTime startSlice = DateTimeService.getTimeSlice(cStart, CompressData.DEFAULT_BLOCK_SIZE);
//        DateTime endSlice = startSlice.plus(CompressData.DEFAULT_BLOCK_SIZE);
//
//        Completable compressCompletable =
//                metricsService.compressBlock(Observable.just(mId), startSlice.getMillis(), endSlice.getMillis(),
//                        COMPRESSION_PAGE_SIZE, PublishSubject.create());
//
//        TestSubscriber<Void> testSubscriber = new TestSubscriber<>();
//        compressCompletable.subscribe(testSubscriber);
//        testSubscriber.awaitTerminalEvent(10, TimeUnit.SECONDS);
//        testSubscriber.assertCompleted();
//        testSubscriber.assertNoErrors();
//
//        actual = toList(metricsService.findDataPoints(new MetricId<>(tenantId, GAUGE, "m1"),
//                cStart.plusMinutes(1).getMillis(), end, 3, Order.ASC));
//
//        assertEquals(actual, expected);
    }

    @SuppressWarnings("unchecked")
    private <T> List<DataPoint<T>> getDataPointList(String name, DateTime start) {
        switch (name) {
            case "M1":
                return asList(
                        new DataPoint<T>(start.getMillis(), (T) Double.valueOf(12.23)),
                        new DataPoint<T>(start.plusMinutes(1).getMillis(), (T) Double.valueOf(9.745)),
                        new DataPoint<T>(start.plusMinutes(2).getMillis(), (T) Double.valueOf(14.01)),
                        new DataPoint<T>(start.plusMinutes(3).getMillis(), (T) Double.valueOf(16.18)),
                        new DataPoint<T>(start.plusMinutes(4).getMillis(), (T) Double.valueOf(18.94)));
            case "M2":
                return asList(
                        new DataPoint<T>(start.getMillis(), (T) Double.valueOf(15.47)),
                        new DataPoint<T>(start.plusMinutes(1).getMillis(), (T) Double.valueOf(8.08)),
                        new DataPoint<T>(start.plusMinutes(2).getMillis(), (T) Double.valueOf(14.39)),
                        new DataPoint<T>(start.plusMinutes(3).getMillis(), (T) Double.valueOf(17.76)),
                        new DataPoint<T>(start.plusMinutes(4).getMillis(), (T) Double.valueOf(17.502)));
            case "M3":
                return asList(
                        new DataPoint<T>(start.getMillis(), (T) Double.valueOf(11.456)),
                        new DataPoint<T>(start.plusMinutes(1).getMillis(), (T) Double.valueOf(18.32)));
            default:
                return emptyList();
        }
    }
}
