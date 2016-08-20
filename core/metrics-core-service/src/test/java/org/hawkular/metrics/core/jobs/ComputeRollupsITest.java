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
package org.hawkular.metrics.core.jobs;

import static java.util.Arrays.asList;

import static org.hawkular.metrics.datetime.DateTimeService.currentHour;
import static org.hawkular.metrics.datetime.DateTimeService.currentMinute;
import static org.hawkular.metrics.model.MetricType.GAUGE;
import static org.joda.time.Duration.standardMinutes;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.rank.Max;
import org.apache.commons.math3.stat.descriptive.rank.Min;
import org.apache.commons.math3.stat.descriptive.summary.Sum;
import org.hawkular.metrics.core.service.BaseITest;
import org.hawkular.metrics.core.service.DataAccess;
import org.hawkular.metrics.core.service.DataAccessImpl;
import org.hawkular.metrics.core.service.MetricsServiceImpl;
import org.hawkular.metrics.core.service.PercentileWrapper;
import org.hawkular.metrics.core.service.cache.DataPointKey;
import org.hawkular.metrics.core.service.rollup.RollupServiceImpl;
import org.hawkular.metrics.core.service.transformers.NumericDataPointCollector;
import org.hawkular.metrics.datetime.DateTimeService;
import org.hawkular.metrics.model.Buckets;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.NumericBucketPoint;
import org.hawkular.metrics.model.Percentile;
import org.hawkular.metrics.scheduler.impl.TestScheduler;
import org.hawkular.metrics.sysconfig.ConfigurationService;
import org.infinispan.AdvancedCache;
import org.jboss.logging.Logger;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.codahale.metrics.MetricRegistry;

import rx.Completable;
import rx.Single;

/**
 * @author jsanda
 */
public class ComputeRollupsITest extends BaseITest {

    private static Logger logger = Logger.getLogger(ComputeRollupsITest.class);

    private MetricsServiceImpl metricsService;

    private ConfigurationService configurationService;

    private RollupServiceImpl rollupService;

    private TestScheduler jobScheduler;

    private JobsServiceImpl jobsService;

    private static AtomicInteger tenantCounter;

    @BeforeClass
    public void initClass() {
        NumericDataPointCollector.createPercentile = InMemoryPercentileWrapper::new;

        tenantCounter = new AtomicInteger();

        DataAccess dataAccess = new DataAccessImpl(session);

        configurationService = new ConfigurationService() ;
        configurationService.init(rxSession);

        metricsService = new MetricsServiceImpl();
        metricsService.setDataAccess(dataAccess);
        metricsService.setConfigurationService(configurationService);
        metricsService.setCacheService(cacheService);
        metricsService.startUp(session, getKeyspace(), true, new MetricRegistry());

        rollupService = new RollupServiceImpl(rxSession);
        rollupService.init();
    }

    private void initJobService(long timeStamp) {
        jobScheduler = new TestScheduler(rxSession);
        jobScheduler.advanceTimeTo(timeStamp);

        jobsService = new JobsServiceImpl();
        jobsService.setSession(rxSession);
        jobsService.setScheduler(jobScheduler);
        jobsService.setMetricsService(metricsService);
        jobsService.setConfigurationService(configurationService);
        jobsService.setCacheService(cacheService);
        jobsService.setRollupService(rollupService);

        jobScheduler.truncateTables(getKeyspace());
        jobsService.start();
    }

//    @BeforeMethod
//    public void initTest() {
//        jobScheduler.truncateTables(getKeyspace());
//        jobsService.start();
//    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        jobsService.shutdown();
    }

    @Test
    public void compute1MinuteRollupsForGauges() throws Exception {
        initJobService(DateTimeService.currentHour().getMillis());

        DateTime nextTimeSlice = currentMinute().plusMinutes(1);
        DateTime start = nextTimeSlice.minusMinutes(1);

        String tenant1 = nextTenantId();
        String tenant2 = nextTenantId();

        MetricId<Double> m1 = new MetricId<>(tenant1, GAUGE, "G1");
        MetricId<Double> m2 = new MetricId<>(tenant1, GAUGE, "G2");
        MetricId<Double> m3 = new MetricId<>(tenant2, GAUGE, "G1");
        MetricId<Double> m4 = new MetricId<>(tenant2, GAUGE, "G2");

        DataPoint<Double> d1 = new DataPoint<>(start.getMillis(), 1.1);
        DataPoint<Double> d2 = new DataPoint<>(start.plusSeconds(10).getMillis(), 1.5);
        DataPoint<Double> d3 = new DataPoint<>(start.getMillis(), 3.22);
        DataPoint<Double> d4 = new DataPoint<>(start.plusSeconds(10).getMillis(), 4.11);
        DataPoint<Double> d5 = new DataPoint<>(start.getMillis(), 27.03);
        DataPoint<Double> d6 = new DataPoint<>(start.plusSeconds(10).getMillis(), 25.55);
        DataPoint<Double> d7 = new DataPoint<>(start.getMillis(), 39.12);
        DataPoint<Double> d8 = new DataPoint<>(start.plusSeconds(10).getMillis(), 38.77);

        Completable c1 = Single.merge(cacheService.put(m1, d1), cacheService.put(m1, d2)).toCompletable();
        Completable c2 = Single.merge(cacheService.put(m2, d3), cacheService.put(m2, d4)).toCompletable();
        Completable c3 = Single.merge(cacheService.put(m3, d5), cacheService.put(m3, d6)).toCompletable();
        Completable c4 = Single.merge(cacheService.put(m4, d7), cacheService.put(m4, d8)).toCompletable();

        Completable.merge(c1, c2, c3, c4).await(10, TimeUnit.SECONDS);

        CountDownLatch latch = new CountDownLatch(1);
        jobScheduler.onTimeSliceFinished(finishedTimeSlice -> {
            if (finishedTimeSlice.equals(nextTimeSlice)) {
                latch.countDown();
            }
        });

        jobScheduler.advanceTimeTo(nextTimeSlice.getMillis());

        assertTrue(latch.await(30, TimeUnit.SECONDS));

        List<DataPoint<Double>> expectedDataPoints = asList(d1, d2);

        NumericBucketPoint actual = getDataPointFromDB(m1, start, 60);
        assertNumericBucketPointEquals(actual, getExpected1MinuteDataPoint(start, asList(d1, d2)));

        actual = getDataPointFromDB(m2, start, 60);
        assertNumericBucketPointEquals(actual, getExpected1MinuteDataPoint(start, asList(d3, d4)));

        actual = getDataPointFromDB(m3, start, 60);
        assertNumericBucketPointEquals(actual, getExpected1MinuteDataPoint(start, asList(d5, d6)));

        actual = getDataPointFromDB(m4, start, 60);
        assertNumericBucketPointEquals(actual, getExpected1MinuteDataPoint(start, asList(d7, d8)));

        AdvancedCache<DataPointKey, DataPoint<? extends Number>> cache = cacheService.getRawDataCache()
                .getAdvancedCache();
        assertTrue(cache.getGroup(Long.toString(start.getMillis())).isEmpty());
    }

    @Test
    public void compute1And5MinuteRollups() throws Exception {
        DateTime timeSlice = currentHour().plusMinutes(4);
        DateTime nextTimeSlice = timeSlice.plusMinutes(1);

        initJobService(timeSlice.minusMinutes(1).getMillis());

        String tenant1 = nextTenantId();
        String tenant2 = nextTenantId();

        MetricId<Double> m1 = new MetricId<>(tenant1, GAUGE, "G1");
        MetricId<Double> m2 = new MetricId<>(tenant1, GAUGE, "G2");
        MetricId<Double> m3 = new MetricId<>(tenant2, GAUGE, "G3");
        MetricId<Double> m4 = new MetricId<>(tenant2, GAUGE, "G4");

        DataPoint<Double> d1 = new DataPoint<>(timeSlice.minusMinutes(1).getMillis(), 2.75);
        DataPoint<Double> d2 = new DataPoint<>(timeSlice.minusMinutes(1).plusSeconds(15).getMillis(), 2.85);
        DataPoint<Double> d3 = new DataPoint<>(timeSlice.getMillis(), 1.1);
        DataPoint<Double> d4 = new DataPoint<>(timeSlice.plusSeconds(10).getMillis(), 1.1);

        Completable c1 = putInRawDataCache(m1, asList(d1, d2, d3, d4));

        DataPoint<Double> d5 = new DataPoint<>(timeSlice.minusMinutes(1).getMillis(), 3.079);
        DataPoint<Double> d6 = new DataPoint<>(timeSlice.minusMinutes(1).plusSeconds(15).getMillis(), 3.922);
        DataPoint<Double> d7 = new DataPoint<>(timeSlice.getMillis(), 3.22);
        DataPoint<Double> d8 = new DataPoint<>(timeSlice.plusSeconds(10).getMillis(), 4.11);

        Completable c2 = putInRawDataCache(m2, asList(d5, d6, d7, d8));

        DataPoint<Double> d9 = new DataPoint<>(timeSlice.minusMinutes(1).getMillis(), 29.76);
        DataPoint<Double> d10 = new DataPoint<>(timeSlice.minusMinutes(1).plusSeconds(15).getMillis(), 30.12);
        DataPoint<Double> d11 = new DataPoint<>(timeSlice.getMillis(), 27.03);
        DataPoint<Double> d12 = new DataPoint<>(timeSlice.plusSeconds(10).getMillis(), 25.55);

        Completable c3 = putInRawDataCache(m3, asList(d9, d10, d11, d12));

        DataPoint<Double> d13 = new DataPoint<>(timeSlice.minusMinutes(1).getMillis(), 44.57);
        DataPoint<Double> d14 = new DataPoint<>(timeSlice.minusMinutes(1).plusSeconds(15).getMillis(), 42.69);
        DataPoint<Double> d15 = new DataPoint<>(timeSlice.getMillis(), 39.12);
        DataPoint<Double> d16 = new DataPoint<>(timeSlice.plusSeconds(10).getMillis(), 38.77);

        Completable c4 = putInRawDataCache(m4, asList(d13, d14, d15, d16));

        Completable.merge(c1, c2, c3, c4).await(10, TimeUnit.SECONDS);

        CountDownLatch firstTimeSliceFinished = new CountDownLatch(1);
        CountDownLatch secondTimeSliceFinished = new CountDownLatch(1);

        jobScheduler.onTimeSliceFinished(finishedTimeSlice -> {
            logger.debug("Finished time slice " + finishedTimeSlice.toDate());
            if (finishedTimeSlice.equals(timeSlice)) {
                firstTimeSliceFinished.countDown();
            } else if (finishedTimeSlice.equals(timeSlice.plusMinutes(1))) {
                secondTimeSliceFinished.countDown();
            }
        });

        jobScheduler.advanceTimeTo(timeSlice.getMillis());

        assertTrue(firstTimeSliceFinished.await(10, TimeUnit.SECONDS));

        jobScheduler.advanceTimeTo(timeSlice.plusMinutes(1).getMillis());

        assertTrue(secondTimeSliceFinished.await(10, TimeUnit.SECONDS));

        assertNumericBucketPointEquals(getDataPointFromDB(m1, timeSlice.minusMinutes(1), 60),
                getExpected1MinuteDataPoint(timeSlice.minusMinutes(1), asList(d1, d2)));
        assertNumericBucketPointEquals(getDataPointFromDB(m2, timeSlice.minusMinutes(1), 60),
                getExpected1MinuteDataPoint(timeSlice.minusMinutes(1), asList(d5, d6)));
        assertNumericBucketPointEquals(getDataPointFromDB(m3, timeSlice.minusMinutes(1), 60),
                getExpected1MinuteDataPoint(timeSlice.minusMinutes(1), asList(d9, d10)));
        assertNumericBucketPointEquals(getDataPointFromDB(m4, timeSlice.minusMinutes(1), 60),
                getExpected1MinuteDataPoint(timeSlice.minusMinutes(1), asList(d13, d14)));

        assertNumericBucketPointEquals(getDataPointFromDB(m1, timeSlice, 60),
                getExpected1MinuteDataPoint(timeSlice, asList(d3, d4)));
        assertNumericBucketPointEquals(getDataPointFromDB(m2, timeSlice, 60),
                getExpected1MinuteDataPoint(timeSlice, asList(d7, d8)));
        assertNumericBucketPointEquals(getDataPointFromDB(m3, timeSlice, 60),
                getExpected1MinuteDataPoint(timeSlice, asList(d11, d12)));
        assertNumericBucketPointEquals(getDataPointFromDB(m4, timeSlice, 60),
                getExpected1MinuteDataPoint(timeSlice, asList(d15, d16)));

        assertNumericBucketPointEquals(getDataPointFromDB(m1, currentHour(), 300),
                getExpected5MinuteDataPoint(currentHour(), asList(d1, d2, d3, d4)));
        assertNumericBucketPointEquals(getDataPointFromDB(m2, currentHour(), 300),
                getExpected5MinuteDataPoint(currentHour(), asList(d5, d6, d7, d8)));
        assertNumericBucketPointEquals(getDataPointFromDB(m3, currentHour(), 300),
                getExpected5MinuteDataPoint(currentHour(), asList(d9, d10, d11, d12)));
        assertNumericBucketPointEquals(getDataPointFromDB(m4, currentHour(), 300),
                getExpected5MinuteDataPoint(currentHour(), asList(d13, d14, d15, d16)));

        AdvancedCache<DataPointKey, NumericDataPointCollector> rollupCache = cacheService.getRollupCache(300)
                .getAdvancedCache();
        assertTrue(rollupCache.getGroup(Long.toString(currentHour().getMillis())).isEmpty());
    }

    @Test
    public void compute1HourRollups() throws Exception {
        final DateTime timeSlice = currentHour().plusMinutes(53);

        initJobService(timeSlice.getMillis());

        String tenant1 = nextTenantId();
        MetricId<Double> m1 = new MetricId<>(tenant1, GAUGE, "G1");
        MetricId<Double> m2 = new MetricId<>(tenant1, GAUGE, "G2");

        DataPoint<Double> d1 = new DataPoint<>(timeSlice.getMillis(), 2.75);
        DataPoint<Double> d2 = new DataPoint<>(timeSlice.plusSeconds(15).getMillis(), 2.85);
        DataPoint<Double> d3 = new DataPoint<>(timeSlice.plusMinutes(1).getMillis(), 1.1);
        DataPoint<Double> d4 = new DataPoint<>(timeSlice.plusMinutes(1).plusSeconds(10).getMillis(), 1.6);
        DataPoint<Double> d5 = new DataPoint<>(timeSlice.plusMinutes(3).getMillis(), 4.33);
        DataPoint<Double> d6 = new DataPoint<>(timeSlice.plusMinutes(3).plusSeconds(15).getMillis(), 5.17);
        DataPoint<Double> d7 = new DataPoint<>(timeSlice.plusMinutes(6).getMillis(), 4.92);
        DataPoint<Double> d8 = new DataPoint<>(timeSlice.plusMinutes(6).plusSeconds(15).getMillis(), 4.65);

        DataPoint<Double> d9 = new DataPoint<>(timeSlice.getMillis(), 24.75);
        DataPoint<Double> d10 = new DataPoint<>(timeSlice.plusSeconds(15).getMillis(), 31.08);
        DataPoint<Double> d11 = new DataPoint<>(timeSlice.plusMinutes(1).getMillis(), 21.45);
        DataPoint<Double> d12 = new DataPoint<>(timeSlice.plusMinutes(1).plusSeconds(10).getMillis(), 26.77);
        DataPoint<Double> d13 = new DataPoint<>(timeSlice.plusMinutes(3).getMillis(), 31.32);
        DataPoint<Double> d14 = new DataPoint<>(timeSlice.plusMinutes(3).plusSeconds(15).getMillis(), 27.39);
        DataPoint<Double> d15 = new DataPoint<>(timeSlice.plusMinutes(6).getMillis(), 23.99);
        DataPoint<Double> d16 = new DataPoint<>(timeSlice.plusMinutes(6).plusSeconds(15).getMillis(), 39.06);

        Completable c1 = putInRawDataCache(m1, asList(d1, d2, d3, d4, d5, d6, d7, d8));
        Completable c2 = putInRawDataCache(m2, asList(d9, d10, d11, d12, d13, d14, d15, d16));
        Completable.merge(c1, c2).await(10, TimeUnit.SECONDS);

        advanceClockTo(timeSlice.plusMinutes(1));

        assertNumericBucketPointEquals(getDataPointFromDB(m1, timeSlice, 60),
                getExpected1MinuteDataPoint(timeSlice, asList(d1, d2)));
        assertNumericBucketPointEquals(getDataPointFromDB(m2, timeSlice, 60),
                getExpected1MinuteDataPoint(timeSlice, asList(d9, d10)));

        advanceClockTo(timeSlice.plusMinutes(2));

        assertNumericBucketPointEquals(getDataPointFromDB(m1, timeSlice.plusMinutes(1), 60),
                getExpected1MinuteDataPoint(timeSlice.plusMinutes(1), asList(d3, d4)));
        assertNumericBucketPointEquals(getDataPointFromDB(m2, timeSlice.plusMinutes(1), 60),
                getExpected1MinuteDataPoint(timeSlice.plusMinutes(1), asList(d11, d12)));

        assertNumericBucketPointEquals(getDataPointFromDB(m1, currentHour().plusMinutes(50), 300),
                getExpected5MinuteDataPoint(currentHour().plusMinutes(50), asList(d1, d2, d3, d4)));
        assertNumericBucketPointEquals(getDataPointFromDB(m2, currentHour().plusMinutes(50), 300),
                getExpected5MinuteDataPoint(currentHour().plusMinutes(50), asList(d9, d10, d11, d12)));

        advanceClockTo(timeSlice.plusMinutes(3));
        advanceClockTo(timeSlice.plusMinutes(4));

        assertNumericBucketPointEquals(getDataPointFromDB(m1, timeSlice.plusMinutes(3), 60),
                getExpected1MinuteDataPoint(timeSlice.plusMinutes(3), asList(d5, d6)));
        assertNumericBucketPointEquals(getDataPointFromDB(m2, timeSlice.plusMinutes(3), 60),
                getExpected1MinuteDataPoint(timeSlice.plusMinutes(3), asList(d13, d14)));

        advanceClockTo(timeSlice.plusMinutes(5));

        advanceClockTo(timeSlice.plusMinutes(6));
        advanceClockTo(timeSlice.plusMinutes(7));

        assertNumericBucketPointEquals(getDataPointFromDB(m1, timeSlice.plusMinutes(6), 60),
                getExpected1MinuteDataPoint(timeSlice.plusMinutes(6), asList(d7, d8)));
        assertNumericBucketPointEquals(getDataPointFromDB(m2, timeSlice.plusMinutes(6), 60),
                getExpected1MinuteDataPoint(timeSlice.plusMinutes(6), asList(d15, d16)));
        assertNumericBucketPointEquals(getDataPointFromDB(m1, currentHour().minusHours(1), 3600),
                getExpected1HourDataPoint(currentHour().minusHours(1), asList(d1, d2, d3, d4, d5, d6, d7, d8)));
        assertNumericBucketPointEquals(getDataPointFromDB(m2, currentHour().minusHours(1), 3600),
                getExpected1HourDataPoint(currentHour().minusHours(1), asList(d9, d10, d11, d12, d13, d14, d15, d16)));
    }

    private Completable putInRawDataCache(MetricId<Double> metricId, List<DataPoint<Double>> dataPoints) {
        if (dataPoints == null) {
            return Completable.complete();
        }
        return Completable.merge(dataPoints.stream().map(d -> cacheService.put(metricId, d).toCompletable())
                .collect(Collectors.toList()));
    }

    private String nextTenantId() {
        return "T" + tenantCounter.getAndIncrement();
    }

    private void advanceClockTo(DateTime time) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        jobScheduler.onTimeSliceFinished(timeSlice -> {
            if (time.equals(timeSlice)) {
                latch.countDown();
            }
        });
        jobScheduler.advanceTimeTo(time.getMillis());
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    private NumericBucketPoint getExpected1MinuteDataPoint(DateTime start, List<DataPoint<Double>> rawData) {
        return getExpectedDataPoint(start, standardMinutes(1), rawData);
    }

    private NumericBucketPoint getExpected5MinuteDataPoint(DateTime start, List<DataPoint<Double>> rawData) {
        return getExpectedDataPoint(start, standardMinutes(5), rawData);
    }

    private NumericBucketPoint getExpected1HourDataPoint(DateTime start, List<DataPoint<Double>> rawData) {
        return getExpectedDataPoint(start, standardMinutes(60), rawData);
    }

    private NumericBucketPoint getExpectedDataPoint(DateTime start, Duration step, List<DataPoint<Double>> rawData) {
        Buckets buckets = new Buckets(start.getMillis(), step.getMillis(), 1);
        Max max = new Max();
        Min min = new Min();
        Mean avg = new Mean();
        Sum sum = new Sum();
        AtomicInteger samples = new AtomicInteger();
        PercentileWrapper p50 = NumericDataPointCollector.createPercentile.apply(50.0);
        PercentileWrapper p90 = NumericDataPointCollector.createPercentile.apply(90.0);
        PercentileWrapper p95 = NumericDataPointCollector.createPercentile.apply(95.0);
        PercentileWrapper p99 = NumericDataPointCollector.createPercentile.apply(99.0);

        rawData.forEach(dataPoint -> {
            sum.increment(dataPoint.getValue());
            max.increment(dataPoint.getValue());
            min.increment(dataPoint.getValue());
            avg.increment(dataPoint.getValue());
            p50.addValue(dataPoint.getValue());
            p90.addValue(dataPoint.getValue());
            p95.addValue(dataPoint.getValue());
            p99.addValue(dataPoint.getValue());
            samples.incrementAndGet();
        });

        return new NumericBucketPoint(start.getMillis(), start.plus(step).getMillis(), min.getResult(), avg.getResult(),
                p50.getResult(), max.getResult(), sum.getResult(), asList(new Percentile("90.0", p90.getResult()),
                new Percentile("95.0", p95.getResult()), new Percentile("99.0", p99.getResult())), samples.get());
    }

    private NumericBucketPoint getDataPointFromDB(MetricId<Double> metricId, DateTime time, int rollup) {
        List<NumericBucketPoint> dataPoints = getOnNextEvents(() -> rollupService.find(metricId, time.getMillis(),
                time.plusSeconds(rollup).getMillis(), rollup));
        assertEquals(dataPoints.size(), 1);
        return dataPoints.get(0);
    }

    private void assertNumericBucketPointEquals(NumericBucketPoint actual, NumericBucketPoint expected) {
        assertEquals(actual.getStart(), expected.getStart());
        assertEquals(actual.getEnd(), expected.getEnd());
        assertDoubleEquals(actual.getMax(), expected.getMax());
        assertDoubleEquals(actual.getMin(), expected.getMin());
        assertDoubleEquals(actual.getAvg(), expected.getAvg());
        assertDoubleEquals(actual.getMedian(), expected.getMedian());
        assertDoubleEquals(actual.getSum(), expected.getSum());
        assertEquals(actual.getSamples(), expected.getSamples());
        assertPercentilesEquals(actual.getPercentiles(), expected.getPercentiles());
    }

    private void assertDoubleEquals(double actual, double expected) {
        assertEquals(actual, expected, 0.0001);
    }

    private void assertPercentilesEquals(List<Percentile> actual, List<Percentile> expected) {
        assertEquals(actual.size(), expected.size());
        for (Percentile expectedP : expected) {
            Percentile actualP = actual.stream().filter(p ->
                    p.getOriginalQuantile().equals(expectedP.getOriginalQuantile())).findFirst().orElseThrow(() ->
                    new RuntimeException("Failed to find " + expectedP.getOriginalQuantile() + " percentile"));
            assertDoubleEquals(actualP.getValue(), expectedP.getValue());
        }
    }

}
