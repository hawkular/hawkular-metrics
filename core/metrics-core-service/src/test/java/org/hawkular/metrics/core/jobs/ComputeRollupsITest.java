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
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;

import org.apache.commons.math3.stat.descriptive.rank.PSquarePercentile;
import org.hawkular.metrics.core.service.BaseITest;
import org.hawkular.metrics.core.service.DataAccess;
import org.hawkular.metrics.core.service.DataAccessImpl;
import org.hawkular.metrics.core.service.MetricsServiceImpl;
import org.hawkular.metrics.core.service.transformers.NumericDataPointCollector;
import org.hawkular.metrics.datetime.DateTimeService;
import org.hawkular.metrics.model.Buckets;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.NumericBucketPoint;
import org.hawkular.metrics.model.Percentile;
import org.hawkular.metrics.scheduler.impl.TestScheduler;
import org.hawkular.metrics.sysconfig.ConfigurationService;
import org.jboss.logging.Logger;
import org.joda.time.DateTime;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import rx.Completable;
import rx.Observable;

/**
 * @author jsanda
 */
public class ComputeRollupsITest extends BaseITest {

    private static Logger logger = Logger.getLogger(ComputeRollupsITest.class);

    private MetricsServiceImpl metricsService;

    private TestScheduler jobScheduler;

    private JobsServiceImpl jobsService;

    private static AtomicInteger tenantCounter;

    private PreparedStatement findDataPoint;

    @BeforeClass
    public void initClass() {
        tenantCounter = new AtomicInteger();

        DataAccess dataAccess = new DataAccessImpl(session);

        ConfigurationService configurationService = new ConfigurationService() ;
        configurationService.init(rxSession);

        metricsService = new MetricsServiceImpl();
        metricsService.setDataAccess(dataAccess);
        metricsService.setConfigurationService(configurationService);
        metricsService.startUp(session, getKeyspace(), true, new MetricRegistry());

        jobScheduler = new TestScheduler(rxSession);
        jobScheduler.advanceTimeTo(currentHour().getMillis());

        jobsService = new JobsServiceImpl();
        jobsService.setSession(rxSession);
        jobsService.setScheduler(jobScheduler);
        jobsService.setMetricsService(metricsService);

        findDataPoint = session.prepare("SELECT time, max, min, avg, median, samples, sum, percentiles " +
                "FROM rollup5min WHERE tenant_id = ? AND metric = ? AND shard = 0");
    }

    @BeforeMethod
    public void initTest() {
        CountDownLatch latch = new CountDownLatch(1);
        Completable truncations = Completable.merge(
                rxSession.execute("TRUNCATE data").toCompletable(),
                rxSession.execute("TRUNCATE rollup5min").toCompletable()
        );
        truncations.await();
        jobsService.start();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        jobsService.shutdown();
    }

    @Test
    public void compute5minRollupsForTenantsHavingOnlyGauges() throws Exception {
        DateTime nextTimeSlice = DateTimeService.getTimeSlice(currentMinute(), standardMinutes(5)).plusMinutes(5);
        DateTime start = nextTimeSlice.minusMinutes(5);

        String tenant1 = nextTenantId();
        String tenant2 = nextTenantId();

        Metric<Double> g1 = new Metric<>(new MetricId<>(tenant1, GAUGE, "G1"), asList(
                new DataPoint<>(start.getMillis(), 1.1),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 2.2)));
        Metric<Double> g2 = new Metric<>(new MetricId<>(tenant1, GAUGE, "G2"), asList(
                new DataPoint<>(start.getMillis(), 1.1),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 2.2)));

        doAction(() -> metricsService.addDataPoints(GAUGE, Observable.just(g1, g2)));

        Metric<Double> g3 = new Metric<>(new MetricId<>(tenant2, GAUGE, "G1"), asList(
                new DataPoint<>(start.getMillis(), 1.1),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 2.2)));
        Metric<Double> g4 = new Metric<>(new MetricId<>(tenant2, GAUGE, "G2"), asList(
                new DataPoint<>(start.getMillis(), 1.1),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 2.2)));

        doAction(() -> metricsService.addDataPoints(GAUGE, Observable.just(g3, g4)));

        CountDownLatch latch = new CountDownLatch(1);
        jobScheduler.onTimeSliceFinished(finishedTimeSlice -> {
            if (finishedTimeSlice.equals(nextTimeSlice)) {
                latch.countDown();
            }
        });

        jobScheduler.advanceTimeTo(nextTimeSlice.getMillis());

        assertTrue(latch.await(30, TimeUnit.SECONDS));

        NumericBucketPoint expected = getExpectedDataPoint(start.getMillis(), g1.getDataPoints());
        NumericBucketPoint actual = getDataPointFromDB(g1);
        assertNumericBucketPointEquals(actual, expected);

        expected = getExpectedDataPoint(start.getMillis(), g2.getDataPoints());
        actual = getDataPointFromDB(g2);
        assertNumericBucketPointEquals(actual, expected);

        expected = getExpectedDataPoint(start.getMillis(), g3.getDataPoints());
        actual = getDataPointFromDB(g3);
        assertNumericBucketPointEquals(actual, expected);

        expected = getExpectedDataPoint(start.getMillis(), g4.getDataPoints());
        actual = getDataPointFromDB(g4);
        assertNumericBucketPointEquals(actual, expected);
    }

    private String nextTenantId() {
        return "T" + tenantCounter.getAndIncrement();
    }

    private NumericBucketPoint getExpectedDataPoint(long start, List<DataPoint<Double>> rawData) {
        Buckets buckets = new Buckets(start, standardMinutes(5).getMillis(), 1);
        NumericDataPointCollector collector = new NumericDataPointCollector(buckets, 0,
                asList(new Percentile("90.0", 90.0), new Percentile("95.0", 95.0), new Percentile("99.0", 99.0)));
        rawData.forEach(collector::increment);
        return collector.toBucketPoint();
    }

    private NumericBucketPoint getDataPointFromDB(Metric<Double> metric) {
        ResultSet resultSet = session.execute(findDataPoint.bind(metric.getMetricId().getTenantId(),
                metric.getMetricId().getName()));
        List<Row> rows = resultSet.all();
        assertEquals(rows.size(), 1);
        Row row = rows.get(0);
        long start = row.getTimestamp(0).getTime();
        return new NumericBucketPoint(
                start,
                 start + standardMinutes(5).getMillis(),
                row.getDouble(2),
                row.getDouble(3),
                row.getDouble(4),
                row.getDouble(1),
                row.getDouble(6),
                getPercentiles(row.getMap(7, Float.class, Double.class)),
                row.getInt(5)
        );
    }

    private List<Percentile> getPercentiles(Map<Float, Double> map) {
        return map.entrySet().stream().map(entry -> new Percentile(entry.getKey().toString(), entry.getValue()))
                .collect(Collectors.toList());
    }

    private double median(double... values) {
        PSquarePercentile median = new PSquarePercentile(50.0);
        for (double value : values) {
            median.increment(value);
        }
        return median.getResult();
    }

    private double sum(double... values) {
        return DoubleStream.of(values).reduce((d1, d2) -> d1 + d2).orElseThrow(() ->
                new RuntimeException("No values supplied"));
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
