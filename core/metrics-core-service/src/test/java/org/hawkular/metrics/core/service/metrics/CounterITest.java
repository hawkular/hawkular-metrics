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

import static org.hawkular.metrics.model.MetricType.COUNTER;
import static org.hawkular.metrics.model.MetricType.COUNTER_RATE;
import static org.joda.time.DateTime.now;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.math3.stat.descriptive.rank.PSquarePercentile;
import org.apache.commons.math3.stat.descriptive.summary.Sum;
import org.hawkular.metrics.core.service.Order;
import org.hawkular.metrics.core.service.transformers.NumericDataPointCollector;
import org.hawkular.metrics.model.Buckets;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.NumericBucketPoint;
import org.hawkular.metrics.model.Percentile;
import org.hawkular.metrics.model.Retention;
import org.hawkular.metrics.model.Tenant;
import org.hawkular.metrics.model.exception.MetricAlreadyExistsException;
import org.hawkular.metrics.model.param.BucketConfig;
import org.hawkular.metrics.model.param.TimeRange;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import rx.Observable;
import rx.observers.TestSubscriber;

public class CounterITest extends BaseMetricsITest {

    @Test
    public void createBasicCounterMetric() throws Exception {
        String tenantId = "counter-tenant";
        String name = "basic-counter";
        MetricId<Long> id = new MetricId<>(tenantId, COUNTER, name);

        Metric<Long> counter = new Metric<>(id);
        metricsService.createMetric(counter, false).toBlocking().lastOrDefault(null);

        Metric<Long> actual = metricsService.<Long> findMetric(id).toBlocking().lastOrDefault(null);

        assertEquals(actual, new Metric<>(counter.getMetricId(), 7), "The counter metric does not match");
        assertMetricIndexMatches(tenantId, COUNTER, singletonList(new Metric<>(counter.getMetricId(), 7)));
    }

    @Test
    public void createCounterWithTags() throws Exception {
        String tenantId = "counter-tenant";
        String name = "tags-counter";
        MetricId<Long> id = new MetricId<>(tenantId, COUNTER, name);
        Map<String, String> tags = ImmutableMap.of("x", "1", "y", "2");

        Metric<Long> counter = new Metric<>(id, tags, null);
        metricsService.createMetric(counter, false).toBlocking().lastOrDefault(null);

        Metric<Long> actual = metricsService.findMetric(id).toBlocking().lastOrDefault(null);
        Metric<Long> ec = new Metric<>(counter.getMetricId(), counter.getTags(), 7);
        assertEquals(actual, ec, "The counter metric does not match");

        assertMetricIndexMatches(tenantId, COUNTER, singletonList(ec));
        assertMetricsTagsIndexMatches(tenantId, "x", singletonList(new MetricsTagsIndexEntry("1", id)));
    }

    @Test
    public void createCounterWithDataRetention() throws Exception {
        String tenantId = "counter-tenant";
        String name = "retention-counter";
        MetricId<Long> id = new MetricId<>(tenantId, COUNTER, name);
        Integer retention = 100;

        Metric<Long> counter = new Metric<>(id, emptyMap(), retention);
        metricsService.createMetric(counter, false).toBlocking().lastOrDefault(null);

        Metric<Long> actual = metricsService.<Long> findMetric(id).toBlocking().lastOrDefault(null);
        assertEquals(actual, counter, "The counter metric does not match");

        assertMetricIndexMatches(tenantId, COUNTER, singletonList(counter));
        assertDataRetentionsIndexMatches(tenantId, COUNTER, ImmutableSet.of(new Retention(id, retention)));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void doNotAllowCreationOfCounterRateMetric() {
        metricsService.createMetric(new Metric<>(new MetricId<>("test", COUNTER_RATE, "counter-rate")), false)
                .toBlocking()
                .lastOrDefault(null);
    }

    @Test
    public void createCounterMetricExists() throws Exception {
        String tenantId = "metric-exists";
        String name = "basic-metric";
        int dataRetention = 20;
        MetricId<Long> id = new MetricId<>(tenantId, COUNTER, name);

        Metric<Long> metric = new Metric<>(id, dataRetention);
        metricsService.createMetric(metric, false).toBlocking().lastOrDefault(null);

        Metric<Long> actual = metricsService.<Long> findMetric(id).toBlocking().last();

        assertEquals(actual, new Metric<>(metric.getMetricId(), dataRetention), "The counter metric does not match");
        assertMetricIndexMatches(tenantId, COUNTER, singletonList(new Metric<>(metric.getMetricId(), dataRetention)));

        try {
            metricsService.createMetric(metric, false).toBlocking().lastOrDefault(null);
            fail("Metrics should already be stored and not overwritten.");
        } catch (MetricAlreadyExistsException e) {

        }

        dataRetention = 100;
        Map<String, String> tags = new HashMap<String, String>();
        tags.put("test", "test2");
        metric = new Metric<>(id, tags, dataRetention);
        try {
            metricsService.createMetric(metric, true).toBlocking().lastOrDefault(null);
        } catch (MetricAlreadyExistsException e) {
            fail("Metrics should already be stored and be overwritten.");
        }

        actual = metricsService.<Long> findMetric(id).toBlocking().last();
        assertEquals(actual, new Metric<>(metric.getMetricId(), tags, dataRetention),
                "The counter metric does not match");
        assertMetricIndexMatches(tenantId, COUNTER,
                singletonList(new Metric<>(metric.getMetricId(), tags, dataRetention)));
    }

    @Test
    public void addAndFetchCounterData() throws Exception {
        DateTime start = now();
        DateTime end = start.plusMinutes(50);
        String tenantId = "counters-tenant";

        doAction(() -> metricsService.createTenant(new Tenant(tenantId), false));

        Metric<Long> counter = new Metric<>(new MetricId<>(tenantId, COUNTER, "c1"), asList(
                new DataPoint<>(start.getMillis(), 10L),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 15L),
                new DataPoint<>(start.plusMinutes(4).getMillis(), 25L),
                new DataPoint<>(end.getMillis(), 45L)));

        doAction(() -> metricsService.addDataPoints(COUNTER, Observable.just(counter)));

        Observable<DataPoint<Long>> data = metricsService.findDataPoints(new MetricId<>(tenantId, COUNTER, "c1"),
                start.getMillis(), end.getMillis(), 0, Order.DESC).doOnError(Throwable::printStackTrace);
        List<DataPoint<Long>> actual = toList(data);
        List<DataPoint<Long>> expected = asList(
                new DataPoint<>(start.plusMinutes(4).getMillis(), 25L),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 15L),
                new DataPoint<>(start.getMillis(), 10L));

        assertEquals(actual, expected, "The counter data does not match the expected values");
        assertMetricIndexMatches(tenantId, COUNTER,
                singletonList(new Metric<>(counter.getMetricId(), counter.getDataPoints(), 7)));
    }

    @Test
    public void testBucketPercentiles() {
        String tenantId = "counter-stats-test";

        long now = now().getMillis();

        int testSize = 100;
        List<DataPoint<Long>> counterList = new ArrayList<>(testSize);

        PSquarePercentile top = new PSquarePercentile(99.9);

        for (long i = 0; i < testSize; i++) {
            counterList.add(new DataPoint<>(now + 60000 + i, i));
            top.increment(i);
        }

        List<Percentile> percentiles = asList(new Percentile("50.0"), new Percentile("90.0"), new Percentile("99.0"),
                new Percentile("99.9"));

        Metric<Long> counter = new Metric<>(new MetricId<>(tenantId, COUNTER, "C1"), counterList);

        doAction(() -> metricsService.addDataPoints(COUNTER, Observable.just(counter)));

        BucketConfig bucketConfig = mock(BucketConfig.class);
        when(bucketConfig.getTimeRange()).thenReturn(new TimeRange(now, now + 60200));
        when(bucketConfig.getBuckets()).thenReturn(Buckets.fromStep(now + 60_000, now + 60_100, 60_100));
        Observable<List<NumericBucketPoint>> counterStatsObservable =
                metricsService.findCounterStats(counter.getMetricId(), bucketConfig,
                        percentiles).doOnError(Throwable::printStackTrace);
//                .toBlocking()
//                .single();

        TestSubscriber<List<NumericBucketPoint>> bucketPointTestSubscriber = new TestSubscriber<>();
        counterStatsObservable.subscribe(bucketPointTestSubscriber);
        bucketPointTestSubscriber.awaitTerminalEvent(5, TimeUnit.SECONDS);
        bucketPointTestSubscriber.assertNoErrors();
        bucketPointTestSubscriber.assertCompleted();
        bucketPointTestSubscriber.assertValueCount(1);
        List<NumericBucketPoint> actual = bucketPointTestSubscriber.getOnNextEvents().get(0);
        assertEquals(1, actual.size());

        NumericBucketPoint bucket = actual.get(0);

        assertEquals((Integer) testSize, bucket.getSamples());
        assertEquals(percentiles.size(), bucket.getPercentiles().size());
        assertEquals(top.getResult(), bucket.getPercentiles().get(3).getValue());
    }

//    @Test
//    public void addAndCompressData() throws Exception {
//        String tenantId = "t1-counter";
//        // Causes writes to go to compressed and one uncompressed row
//        DateTime dt = new DateTime(2016, 9, 2, 14, 15, DateTimeZone.UTC);
//        DateTimeUtils.setCurrentMillisFixed(dt.getMillis());
//
//        DateTime start = dt.minusMinutes(30);
//        DateTime end = start.plusMinutes(20);
//
//        MetricId<Long> mId = new MetricId<>(tenantId, COUNTER, "m1");
//
//        metricsService.createTenant(new Tenant(tenantId), false).toBlocking().lastOrDefault(null);
//
//        Metric<Long> m1 = new Metric<>(mId, asList(
//                new DataPoint<>(start.getMillis(), 1L),
//                new DataPoint<>(start.plusMinutes(2).getMillis(), 2L),
//                new DataPoint<>(start.plusMinutes(4).getMillis(), 3L),
//                new DataPoint<>(end.getMillis(), 4L)));
//
//        Observable<Void> insertObservable = metricsService.addDataPoints(COUNTER, Observable.just(m1));
//        insertObservable.toBlocking().lastOrDefault(null);
//
//        DateTime startSlice = DateTimeService.getTimeSlice(start, CompressData.DEFAULT_BLOCK_SIZE);
////        DateTime endSlice = startSlice.plus(CompressData.DEFAULT_BLOCK_SIZE);
//
//        Completable compressCompletable = metricsService.compressBlock(startSlice.getMillis(), COMPRESSION_PAGE_SIZE)
//                .doOnError(Throwable::printStackTrace);
//
////                metricsService.compressBlock(Observable.just(mId), startSlice.getMillis(), endSlice.getMillis(),
////                        COMPRESSION_PAGE_SIZE, PublishSubject.create())
//
//        TestSubscriber<Void> testSubscriber = new TestSubscriber<>();
//        compressCompletable.subscribe(testSubscriber);
//        testSubscriber.awaitTerminalEvent(10, TimeUnit.SECONDS);
//        testSubscriber.assertCompleted();
//        testSubscriber.assertNoErrors();
//
//        Observable<DataPoint<Long>> observable = metricsService.findDataPoints(mId, start.getMillis(),
//                end.getMillis() + 1, 0, Order.DESC);
//        List<DataPoint<Long>> actual = toList(observable);
//        List<DataPoint<Long>> expected = asList(
//                new DataPoint<>(end.getMillis(), 4L),
//                new DataPoint<>(start.plusMinutes(4).getMillis(), 3L),
//                new DataPoint<>(start.plusMinutes(2).getMillis(), 2L),
//                new DataPoint<>(start.getMillis(), 1L));
//
//        assertEquals(actual, expected, "The data does not match the expected values");
//        assertMetricIndexMatches(tenantId, COUNTER, singletonList(new Metric<>(m1.getMetricId(), m1.getDataPoints(),
//                7)));
//
//        observable = metricsService.findDataPoints(mId, start.getMillis(),
//                end.getMillis(), 0, Order.DESC);
//        actual = toList(observable);
//        assertEquals(3, actual.size(), "Last datapoint should be missing (<)");
//
//        DateTimeUtils.setCurrentMillisSystem();
//    }

    @Test
    public void findSimpleCounterStats() {
        //Setup the counter data
        NumericDataPointCollector.createPercentile = InMemoryPercentileWrapper::new;

        Random r = new Random(123);
        List<Long> randomList = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            randomList.add((long) r.nextInt(100));
        }
        Collections.sort(randomList);
        Iterator<Long> randoms = randomList.iterator();

        String tenantId = "findCounterStats";
        DateTime start = now().minusMinutes(10);

        Metric<Long> c1 = new Metric<>(new MetricId<>(tenantId, COUNTER, "C1"), asList(
                new DataPoint<>(start.getMillis(), 222L + randoms.next()),
                new DataPoint<>(start.plusMinutes(1).getMillis(), 224L + randoms.next()),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 226L + randoms.next()),
                new DataPoint<>(start.plusMinutes(3).getMillis(), 228L + randoms.next()),
                new DataPoint<>(start.plusMinutes(4).getMillis(), 229L + randoms.next())));
        doAction(() -> metricsService.addDataPoints(COUNTER, Observable.just(c1)));
        doAction(() -> metricsService.addTags(c1, ImmutableMap.of("type", "counter_cpu_usage", "node", "server1")));

        Metric<Long> c2 = new Metric<>(new MetricId<>(tenantId, COUNTER, "C2"), asList(
                new DataPoint<>(start.getMillis(), 150L),
                new DataPoint<>(start.plusMinutes(1).getMillis(), 153L + randoms.next()),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 156L + randoms.next()),
                new DataPoint<>(start.plusMinutes(3).getMillis(), 159L + randoms.next()),
                new DataPoint<>(start.plusMinutes(4).getMillis(), 162L + randoms.next())));
        doAction(() -> metricsService.addDataPoints(COUNTER, Observable.just(c2)));
        doAction(() -> metricsService.addTags(c2, ImmutableMap.of("type", "counter_cpu_usage", "node", "server2")));

        Metric<Long> c3 = new Metric<>(new MetricId<>(tenantId, COUNTER, "C3"), asList(
                new DataPoint<>(start.getMillis(), 11456L + randoms.next()),
                new DataPoint<>(start.plusMinutes(1).getMillis(), 183332L + randoms.next())));
        doAction(() -> metricsService.addDataPoints(COUNTER, Observable.just(c3)));
        doAction(() -> metricsService.addTags(c3, ImmutableMap.of("type", "counter_cpu_usage", "node", "server3")));

        Buckets buckets = Buckets.fromCount(start.getMillis(), start.plusMinutes(5).getMillis(), 1);
        String tagFilters = "type:counter_cpu_usage,node:server1|server2";

        List<DataPoint<Double>> c1Rate = getOnNextEvents(
                () -> metricsService.findRateData(c1.getMetricId(),
                        start.getMillis(), start.plusMinutes(5).getMillis(), 0, Order.ASC));

        List<DataPoint<Double>> c2Rate = getOnNextEvents(
                () -> metricsService.findRateData(c2.getMetricId(),
                        start.getMillis(), start.plusMinutes(5).getMillis(), 0, Order.ASC));

        //Test simple counter stats
        List<List<NumericBucketPoint>> actualCounterStatsByTag = getOnNextEvents(
                () -> metricsService.findMetricIdentifiersWithFilters(tenantId, COUNTER, tagFilters)
                        .toList()
                        .flatMap(ids -> metricsService.findNumericStats(ids, start.getMillis(),
                        start.plusMinutes(5).getMillis(), buckets, emptyList(), false, false)));
        assertEquals(actualCounterStatsByTag.size(), 1);

        List<List<NumericBucketPoint>> actualCounterStatsById = getOnNextEvents(
                () -> metricsService.findNumericStats(asList(c1.getMetricId(), c2.getMetricId()),
                        start.getMillis(), start.plusMinutes(5).getMillis(), buckets, emptyList(), false, false));
        assertEquals(actualCounterStatsById.size(), 1);

        List<NumericBucketPoint> expectedCounterStats = asList(createSingleBucket(
                Stream.concat(c1.getDataPoints().stream(), c2.getDataPoints().stream()).collect(Collectors.toList()),
                start, start.plusMinutes(5)));

        assertNumericBucketsEquals(actualCounterStatsByTag.get(0), expectedCounterStats);
        assertNumericBucketsEquals(actualCounterStatsById.get(0), expectedCounterStats);

        //Test stacked counter stats
        List<List<NumericBucketPoint>> actualStackedCounterStatsByTag = getOnNextEvents(
                () -> metricsService.findMetricIdentifiersWithFilters(tenantId, COUNTER, tagFilters)
                        .toList()
                        .flatMap(ids -> metricsService.findNumericStats(ids, start.getMillis(),
                            start.plusMinutes(5).getMillis(), buckets, emptyList(), true, false)));
        assertEquals(actualStackedCounterStatsByTag.size(), 1);

        List<List<NumericBucketPoint>> actualStackedCounterStatsById = getOnNextEvents(
                () -> metricsService.findNumericStats(asList(c1.getMetricId(), c2.getMetricId()),
                        start.getMillis(), start.plusMinutes(5).getMillis(), buckets, emptyList(), true, false));
        assertEquals(actualStackedCounterStatsByTag.size(), 1);

        NumericBucketPoint collectorC1 = createSingleBucket(c1.getDataPoints(), start, start.plusMinutes(5));
        NumericBucketPoint collectorC2 = createSingleBucket(c2.getDataPoints(), start, start.plusMinutes(5));

        final Sum min = new Sum();
        final Sum average = new Sum();
        final Sum median = new Sum();
        final Sum max = new Sum();
        final Sum sum = new Sum();
        Observable.just(collectorC1, collectorC2).forEach(d -> {
            min.increment(d.getMin());
            max.increment(d.getMax());
            average.increment(d.getAvg());
            median.increment(d.getMedian());
            sum.increment(d.getSum());
        });
        NumericBucketPoint expectedStackedRateBucketPoint = new NumericBucketPoint.Builder(start.getMillis(),
                start.plusMinutes(5).getMillis())
                        .setMin(min.getResult())
                        .setMax(max.getResult())
                        .setAvg(average.getResult())
                        .setMedian(median.getResult())
                        .setSum(sum.getResult())
                        .setSamples(2)
                        .build();
        List<NumericBucketPoint> expectedStackedCounterStatsList = new ArrayList<NumericBucketPoint>();
        expectedStackedCounterStatsList.add(expectedStackedRateBucketPoint);

        assertNumericBucketsEquals(actualStackedCounterStatsByTag.get(0), expectedStackedCounterStatsList);
        assertNumericBucketsEquals(actualStackedCounterStatsById.get(0), expectedStackedCounterStatsList);

        //Test simple counter rate stats
        List<List<NumericBucketPoint>> actualCounterRateStatsByTag = getOnNextEvents(
                () -> metricsService.findMetricIdentifiersWithFilters(tenantId, COUNTER, tagFilters)
                        .toList()
                        .flatMap(ids -> metricsService.findNumericStats(ids, start.getMillis(),
                            start.plusMinutes(5).getMillis(), buckets, emptyList(), false, true)));
        assertEquals(actualCounterRateStatsByTag.size(), 1);

        List<List<NumericBucketPoint>> actualCounterRateStatsById = getOnNextEvents(
                () -> metricsService.findNumericStats(asList(c1.getMetricId(), c2.getMetricId()),
                        start.getMillis(), start.plusMinutes(5).getMillis(), buckets, emptyList(), false, true));
        assertEquals(actualCounterRateStatsById.size(), 1);

        List<NumericBucketPoint> expectedCounterRateStats = Arrays.asList(createSingleBucket(
                Stream.concat(c1Rate.stream(), c2Rate.stream()).collect(Collectors.toList()),
                start, start.plusMinutes(5)));

        assertNumericBucketsEquals(actualCounterRateStatsByTag.get(0), expectedCounterRateStats);
        assertNumericBucketsEquals(actualCounterRateStatsById.get(0), expectedCounterRateStats);

        //Test stacked counter rate stats
        List<List<NumericBucketPoint>> actualStackedCounterRateStatsByTag = getOnNextEvents(
                () -> metricsService.findMetricIdentifiersWithFilters(tenantId, COUNTER, tagFilters)
                        .toList()
                        .flatMap(ids -> metricsService.findNumericStats(ids, start.getMillis(),
                            start.plusMinutes(5).getMillis(), buckets, emptyList(), true, true)));
        assertEquals(actualStackedCounterStatsByTag.size(), 1);

        List<List<NumericBucketPoint>> actualStackedCounterRateStatsById = getOnNextEvents(
                () -> metricsService.findNumericStats(asList(c1.getMetricId(), c2.getMetricId()),
                        start.getMillis(), start.plusMinutes(5).getMillis(), buckets, emptyList(), true, true));
        assertEquals(actualStackedCounterStatsByTag.size(), 1);

        NumericBucketPoint collectorC1Rate = createSingleBucket(c1Rate, start, start.plusMinutes(5));
        NumericBucketPoint collectorC2Rate = createSingleBucket(c2Rate, start, start.plusMinutes(5));

        final Sum counterRateMin = new Sum();
        final Sum counterRateMax = new Sum();
        final Sum counterRateAverage = new Sum();
        final Sum counterRateMedian = new Sum();
        final Sum counterRateSum = new Sum();
        Observable.just(collectorC1Rate, collectorC2Rate).forEach(d -> {
            counterRateMin.increment(d.getMin());
            counterRateMax.increment(d.getMax());
            counterRateAverage.increment(d.getAvg());
            counterRateMedian.increment(d.getMedian());
            counterRateSum.increment(d.getSum());
        });
        NumericBucketPoint expectedStackedCounterRateBucketPoint = new NumericBucketPoint.Builder(start.getMillis(),
                start.plusMinutes(5).getMillis())
                        .setMin(counterRateMin.getResult())
                        .setMax(counterRateMax.getResult())
                        .setAvg(counterRateAverage.getResult())
                        .setMedian(counterRateMedian.getResult())
                        .setSum(counterRateSum.getResult())
                        .setSamples(2)
                        .build();
        List<NumericBucketPoint> expectedStackedCounterRateStatsList = new ArrayList<NumericBucketPoint>();
        expectedStackedCounterRateStatsList.add(expectedStackedCounterRateBucketPoint);

        assertNumericBucketsEquals(actualStackedCounterRateStatsByTag.get(0), expectedStackedCounterRateStatsList);
        assertNumericBucketsEquals(actualStackedCounterRateStatsById.get(0), expectedStackedCounterRateStatsList);
    }
}
