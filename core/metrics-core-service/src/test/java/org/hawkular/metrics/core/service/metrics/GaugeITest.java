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

package org.hawkular.metrics.core.service.metrics;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;

import static org.hawkular.metrics.model.MetricType.GAUGE;
import static org.joda.time.DateTime.now;
import static org.joda.time.Duration.standardDays;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
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
import org.hawkular.metrics.model.MetricType;
import org.hawkular.metrics.model.NumericBucketPoint;
import org.hawkular.metrics.model.Percentile;
import org.hawkular.metrics.model.Tenant;
import org.hawkular.metrics.model.param.BucketConfig;
import org.hawkular.metrics.model.param.TimeRange;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.google.common.collect.ImmutableMap;

import rx.Completable;
import rx.Observable;

/**
 * @author John Sanda
 */
public class GaugeITest extends BaseMetricsITest {

    @Test
    public void addAndFetchGaugeData() throws Exception {
        DateTime start = now().minusMinutes(30);
        DateTime end = start.plusMinutes(20);
        String tenantId = "t1";

        metricsService.createTenant(new Tenant(tenantId), false).toBlocking().lastOrDefault(null);

        Metric<Double> m1 = new Metric<>(new MetricId<>(tenantId, GAUGE, "m1"), asList(
                new DataPoint<>(start.getMillis(), 1.1),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 2.2),
                new DataPoint<>(start.plusMinutes(4).getMillis(), 3.3),
                new DataPoint<>(end.getMillis(), 4.4)));

        Observable<Void> insertObservable = metricsService.addDataPoints(GAUGE, Observable.just(m1));
        insertObservable.toBlocking().lastOrDefault(null);

        Observable<DataPoint<Double>> observable = metricsService.findDataPoints(new MetricId<>(tenantId, GAUGE, "m1"),
                start.getMillis(), end.getMillis(), 0, Order.DESC);
        List<DataPoint<Double>> actual = toList(observable);
        List<DataPoint<Double>> expected = asList(
                new DataPoint<>(start.plusMinutes(4).getMillis(), 3.3),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 2.2),
                new DataPoint<>(start.getMillis(), 1.1));

        assertEquals(actual, expected, "The data does not match the expected values");
        assertMetricIndexMatches("t1", GAUGE, singletonList(new Metric<>(m1.getMetricId(), m1.getDataPoints(), 7)));
    }

    @Test
    public void addGaugeDataForMultipleMetrics() throws Exception {
        DateTime start = now().minusMinutes(10);
        DateTime end = start.plusMinutes(8);
        String tenantId = "test-tenant";

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
                new Metric<>(m3.getMetricId(), 7),
                m4));
    }

    @Test
    public void getPeriodsAboveThreshold() throws Exception {
        String tenantId = "test-tenant";
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

        metricsService.addDataPoints(GAUGE, Observable.just(m1)).toBlocking().lastOrDefault(null);

        List<long[]> actual = metricsService.getPeriods(m1.getMetricId(),
                value -> value > threshold, start.getMillis(), now().getMillis()).toBlocking().lastOrDefault(null);
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

        String tenantId = "findGaugeStatsByMetricNames";
        DateTime start = now().minusMinutes(10);

        Metric<Double> m1 = new Metric<>(new MetricId<>(tenantId, GAUGE, "M1"), getDataPointList("M1", start));
        doAction(() -> metricsService.addDataPoints(GAUGE, Observable.just(m1)));

        Metric<Double> m2 = new Metric<>(new MetricId<>(tenantId, GAUGE, "M2"), getDataPointList("M2", start));
        doAction(() -> metricsService.addDataPoints(GAUGE, Observable.just(m2)));

        Metric<Double> m3 = new Metric<>(new MetricId<>(tenantId, GAUGE, "M3"), getDataPointList("M3", start));
        doAction(() -> metricsService.addDataPoints(GAUGE, Observable.just(m3)));

        Buckets buckets = Buckets.fromCount(start.getMillis(), start.plusMinutes(5).getMillis(), 1);

        List<List<NumericBucketPoint>> actual = getOnNextEvents(() -> metricsService.findNumericStats(tenantId,
                MetricType.GAUGE, asList("M1", "M2"), start.getMillis(), start.plusMinutes(5).getMillis(), buckets,
                emptyList(), true));

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

        String tenantId = "findGaugeStatsByMetricNames";
        DateTime start = now().minusMinutes(10);

        Metric<Double> m1 = new Metric<>(new MetricId<>(tenantId, GAUGE, "M1"), getDataPointList("M1", start));
        doAction(() -> metricsService.addDataPoints(GAUGE, Observable.just(m1)));

        Metric<Double> m2 = new Metric<>(new MetricId<>(tenantId, GAUGE, "M2"), getDataPointList("M2", start));
        doAction(() -> metricsService.addDataPoints(GAUGE, Observable.just(m2)));

        Metric<Double> m3 = new Metric<>(new MetricId<>(tenantId, GAUGE, "M3"), getDataPointList("M3", start));
        doAction(() -> metricsService.addDataPoints(GAUGE, Observable.just(m3)));

        Buckets buckets = Buckets.fromCount(start.getMillis(), start.plusMinutes(5).getMillis(), 1);

        List<List<NumericBucketPoint>> actual = getOnNextEvents(() -> metricsService.findNumericStats(tenantId,
                MetricType.GAUGE, asList("M1", "M2"), start.getMillis(), start.plusMinutes(5).getMillis(), buckets,
                emptyList(), false));

        assertEquals(actual.size(), 1);

        List<NumericBucketPoint> expected = Arrays.asList(createSingleBucket(
                Stream.concat(m1.getDataPoints().stream(), m2.getDataPoints().stream()).collect(Collectors.toList()),
                start, start.plusMinutes(5)));

        assertNumericBucketsEquals(actual.get(0), expected);
    }

    @Test
    public void findStackedGaugeStatsByTags() {
        NumericDataPointCollector.createPercentile = InMemoryPercentileWrapper::new;

        String tenantId = "findGaugeStatsByTags";
        DateTime start = now().minusMinutes(10);

        Metric<Double> m1 = new Metric<>(new MetricId<>(tenantId, GAUGE, "M1"), getDataPointList("M1", start));
        doAction(() -> metricsService.addDataPoints(GAUGE, Observable.just(m1)));
        doAction(() -> metricsService.addTags(m1, ImmutableMap.of("type", "cpu_usage", "node", "server1")));

        Metric<Double> m2 = new Metric<>(new MetricId<>(tenantId, GAUGE, "M2"), getDataPointList("M2", start));
        doAction(() -> metricsService.addDataPoints(GAUGE, Observable.just(m2)));
        doAction(() -> metricsService.addTags(m2, ImmutableMap.of("type", "cpu_usage", "node", "server2")));

        Metric<Double> m3 = new Metric<>(new MetricId<>(tenantId, GAUGE, "M3"), getDataPointList("M3", start));
        doAction(() -> metricsService.addDataPoints(GAUGE, Observable.just(m3)));
        doAction(() -> metricsService.addTags(m3, ImmutableMap.of("type", "cpu_usage", "node", "server3")));

        Buckets buckets = Buckets.fromCount(start.getMillis(), start.plusMinutes(5).getMillis(), 1);
        Map<String, String> tagFilters = ImmutableMap.of("type", "cpu_usage", "node", "server1|server2");

        List<List<NumericBucketPoint>> actual = getOnNextEvents(() -> metricsService.findNumericStats(tenantId,
                MetricType.GAUGE, tagFilters, start.getMillis(), start.plusMinutes(5).getMillis(), buckets,
                emptyList(), true));

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
    public void findPrecomputedGaugeStats() {
        PreparedStatement insertDataPoint = session.prepare(
                "INSERT INTO rollup300 (tenant_id, metric, shard, time, min, max, avg, median, sum, samples, " +
                        "percentiles) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        String tenantId = "findPrecomputedGaugeStats";
        int retention = standardDays(1).toStandardSeconds().getSeconds();
        Metric<Double> metric = new Metric<>(new MetricId<>(tenantId, GAUGE, "G1"), retention);

        doAction(() -> metricsService.createMetric(metric, true));

        DateTime start = new DateTime(now()).minusDays(3);

        Observable<ResultSet> insert1 = rxSession.execute(insertDataPoint.bind(metric.getMetricId().getTenantId(),
                metric.getMetricId().getName(), 0L, start.toDate(), 1.0, 5.5, 4.4, 3.3, 6.6, 5,
                ImmutableMap.of(90.0f, 1.0, 95.0f, 1.4, 99.0f, 2.2)));
        Observable<ResultSet> insert2 = rxSession.execute(insertDataPoint.bind(metric.getMetricId().getTenantId(),
                metric.getMetricId().getName(), 0L, start.plusMinutes(5).toDate(), 6.0, 13.3, 9.4, 8.7, 21.2, 5,
                ImmutableMap.of(90.0f, 11.4, 95.0f, 9.6, 99.0f, 8.9)));

        Completable.merge(insert1.toCompletable(), insert2.toCompletable()).await(10, TimeUnit.SECONDS);

        List<List<NumericBucketPoint>> results = getOnNextEvents(() -> metricsService.findGaugeStats(
                metric.getMetricId(), new BucketConfig(null, null, new TimeRange(start.getMillis(),
                        start.plusMinutes(20).getMillis()), 300), emptyList()));

        assertEquals(results.get(0).size(), 2);

        List<NumericBucketPoint> actual = results.get(0);
        List<NumericBucketPoint> expected = asList(
                new NumericBucketPoint(
                        start.getMillis(),
                        start.plusMinutes(5).getMillis(),
                        1.0,
                        4.4,
                        3.3,
                        5.5,
                        6.6,
                        asList(new Percentile("90.0", 1.0), new Percentile("95.0", 1.4), new Percentile("99.0", 2.2)),
                        5),
                new NumericBucketPoint(
                        start.plusMinutes(5).getMillis(),
                        start.plusMinutes(10).getMillis(),
                        6.0,
                        9.4,
                        8.7,
                        13.3,
                        21.2,
                        asList(new Percentile("90.0", 11.4), new Percentile("95.0", 9.6), new Percentile("99.0", 8.9)),
                        5));

        assertNumericBucketsEquals(actual, expected);
    }

    @Test
    public void addAndFetchGaugeDataAggregates() throws Exception {
        DateTime start = now().minusMinutes(30);
        DateTime end = start.plusMinutes(20);
        String tenantId = "t1";

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
        assertMetricIndexMatches("t1", GAUGE, singletonList(new Metric<>(m1.getMetricId(), m1.getDataPoints(), 7)));
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
