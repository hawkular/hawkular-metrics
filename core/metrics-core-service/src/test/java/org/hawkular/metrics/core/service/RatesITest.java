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

package org.hawkular.metrics.core.service;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.hawkular.metrics.model.MetricType.COUNTER;
import static org.joda.time.DateTime.now;
import static org.joda.time.Duration.standardMinutes;
import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.hawkular.metrics.model.Buckets;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.NumericBucketPoint;
import org.hawkular.metrics.model.Percentile;
import org.hawkular.metrics.model.Tenant;
import org.hawkular.metrics.tasks.api.AbstractTrigger;
import org.hawkular.metrics.tasks.impl.Queries;
import org.hawkular.metrics.tasks.impl.TaskSchedulerImpl;
import org.joda.time.DateTime;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import rx.Observable;
import rx.observers.TestSubscriber;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

/**
 * This class tests counter rates indirectly by running the task scheduler with a virtual
 * clock. {@link GenerateRateITest} tests directly without running a task scheduler.
 */
public class RatesITest extends BaseMetricsITest {

    private TaskSchedulerImpl taskScheduler;

    private DateTimeService dateTimeService;

    private TestScheduler tickScheduler;

    private Observable<Long> finishedTimeSlices;

    @BeforeClass
    public void initClass() {
        dateTimeService = new DateTimeService();

        DateTime startTime = dateTimeService.getTimeSlice(DateTime.now(), standardMinutes(1)).minusMinutes(20);

        tickScheduler = Schedulers.test();
        tickScheduler.advanceTimeTo(startTime.getMillis(), TimeUnit.MILLISECONDS);

        taskScheduler = new TaskSchedulerImpl(rxSession, new Queries(session));
        taskScheduler.setTickScheduler(tickScheduler);

        AbstractTrigger.now = tickScheduler::now;

        finishedTimeSlices = taskScheduler.getFinishedTimeSlices();
        taskScheduler.start();

        GenerateRate generateRate = new GenerateRate(metricsService);
        taskScheduler.subscribe(generateRate);
    }

    @Test
    public void generateRates() {
        String tenant = "rates-test";
        DateTime start = new DateTime(tickScheduler.now()).plusMinutes(1);

        Metric<Long> c1 = new Metric<>(new MetricId<>(tenant, COUNTER, "C1"));
        Metric<Long> c2 = new Metric<>(new MetricId<>(tenant, COUNTER, "C2"));
        Metric<Long> c3 = new Metric<>(new MetricId<>(tenant, COUNTER, "C3"));

        doAction(() -> metricsService.createTenant(new Tenant(tenant), false));

        doAction(() -> metricsService.createMetric(c1, false));
        doAction(() -> metricsService.createMetric(c2, false));
        doAction(() -> metricsService.createMetric(c3, false));

        doAction(() -> metricsService.addDataPoints(COUNTER, Observable.from(asList(
                new Metric<>(c1.getMetricId(), asList(new DataPoint<>(start.getMillis(), 10L),
                        new DataPoint<>(start.plusSeconds(30).getMillis(), 25L))),
                new Metric<>(c2.getMetricId(), asList(new DataPoint<>(start.getMillis(), 100L),
                        new DataPoint<>(start.plusSeconds(30).getMillis(), 165L))),
                new Metric<>(c3.getMetricId(), asList(new DataPoint<>(start.getMillis(), 42L),
                        new DataPoint<>(start.plusSeconds(30).getMillis(), 77L)))
        ))));

        TestSubscriber<Long> subscriber = new TestSubscriber<>();
        finishedTimeSlices.take(2).observeOn(Schedulers.immediate()).subscribe(subscriber);

        tickScheduler.advanceTimeBy(2, MINUTES);

        subscriber.awaitTerminalEvent(5, SECONDS);
        subscriber.assertNoErrors();
        subscriber.assertTerminalEvent();

        List<DataPoint<Double>> c1Rate = getOnNextEvents(() -> metricsService.findRateData(c1.getMetricId(),
                start.getMillis(), start.plusMinutes(1).getMillis(), 0, Order.ASC));
        List<DataPoint<Double>> c2Rate = getOnNextEvents(() -> metricsService.findRateData(c2.getMetricId(),
                start.getMillis(), start.plusMinutes(1).getMillis(), 0, Order.ASC));
        List<DataPoint<Double>> c3Rate = getOnNextEvents(() -> metricsService.findRateData(c3.getMetricId(),
                start.getMillis(), start.plusMinutes(1).getMillis(), 0, Order.ASC));

        assertEquals(c1Rate, singletonList(new DataPoint<>(start.getMillis(), calculateRate(25, start,
                start.plusMinutes(1)))));
        assertEquals(c2Rate, singletonList(new DataPoint<>(start.getMillis(), calculateRate(165, start,
                start.plusMinutes(1)))));
        assertEquals(c3Rate, singletonList(new DataPoint<>(start.getMillis(), calculateRate(77, start,
                start.plusMinutes(1)))));
    }

    @Test
    public void findRatesWhenNoDataIsFound() {
        DateTime start = now().minusHours(1);
        String tenantId = "counter-rate-test-no-data";

        Metric<Long> counter = new Metric<>(new MetricId<>(tenantId, COUNTER, "C1"), asList(
                new DataPoint<>(start.plusMinutes(1).getMillis(), 10L),
                new DataPoint<>(start.plusMinutes(1).plusSeconds(45).getMillis(), 17L),
                new DataPoint<>(start.plusMinutes(2).plusSeconds(10).getMillis(), 29L)));

        doAction(() -> metricsService.addDataPoints(COUNTER, Observable.just(counter)));

        List<DataPoint<Double>> actual = getOnNextEvents(() -> metricsService.findRateData(counter.getMetricId(),
                start.plusMinutes(3).getMillis(), start.plusMinutes(5).getMillis(), 0, Order.ASC));

        assertEquals(actual.size(), 0, "The rates do not match");
    }

    @Test
    public void findRates() {
        String tenantId = "counter-rate-test";

        Metric<Long> counter = new Metric<>(new MetricId<>(tenantId, COUNTER, "C1"), asList(
                new DataPoint<>((long) (60_000 * 1.0), 0L),
                new DataPoint<>((long) (60_000 * 1.5), 200L),
                new DataPoint<>((long) (60_000 * 3.5), 400L),
                new DataPoint<>((long) (60_000 * 5.0), 550L),
                new DataPoint<>((long) (60_000 * 7.0), 950L),
                new DataPoint<>((long) (60_000 * 7.5), 1000L)));

        doAction(() -> metricsService.addDataPoints(COUNTER, Observable.just(counter)));

        List<DataPoint<Double>> actual = getOnNextEvents(() -> {
            // Test order ASC with 4 points limit
            return metricsService.findRateData(counter.getMetricId(), 0, now().getMillis(), 4, Order.ASC);
        });
        List<DataPoint<Double>> expected = asList(
                new DataPoint<>((long) (60_000 * 1.5), 400D),
                new DataPoint<>((long) (60_000 * 3.5), 100D),
                new DataPoint<>((long) (60_000 * 5.0), 100D),
                new DataPoint<>((long) (60_000 * 7.0), 200D));

        assertEquals(actual, expected, "The rates do not match");

        actual = getOnNextEvents(() -> {
            // Test descending order with 3 points limit
            return metricsService.findRateData(counter.getMetricId(), 0, now().getMillis(), 3, Order.DESC);
        });
        expected = asList(
                new DataPoint<>((long) (60_000 * 7.5), 100D),
                new DataPoint<>((long) (60_000 * 7.0), 200D),
                new DataPoint<>((long) (60_000 * 5.0), 100D));

        assertEquals(actual, expected, "The rates do not match");
    }

    @Test
    public void findRatesWhenTherAreCounterResets() {
        String tenantId = "counter-reset-test";

        Metric<Long> counter = new Metric<>(new MetricId<>(tenantId, COUNTER, "C1"), asList(
                new DataPoint<>((long) (60_000 * 1.0), 1L),
                new DataPoint<>((long) (60_000 * 1.5), 2L),
                new DataPoint<>((long) (60_000 * 3.5), 3L),
                new DataPoint<>((long) (60_000 * 5.0), 1L),
                new DataPoint<>((long) (60_000 * 7.0), 2L),
                new DataPoint<>((long) (60_000 * 7.5), 3L),
                new DataPoint<>((long) (60_000 * 8.0), 1L),
                new DataPoint<>((long) (60_000 * 8.5), 2L),
                new DataPoint<>((long) (60_000 * 9.0), 3L)));
        doAction(() -> metricsService.addDataPoints(COUNTER, Observable.just(counter)));

        List<DataPoint<Double>> actual = getOnNextEvents(() -> metricsService.findRateData(counter.getMetricId(),
                0, now().getMillis(), 0, Order.ASC));
        List<DataPoint<Double>> expected = asList(
                new DataPoint<>((long) (60_000 * 1.5), 2D),
                new DataPoint<>((long) (60_000 * 3.5), 0.5),
                new DataPoint<>((long) (60_000 * 7.0), 0.5),
                new DataPoint<>((long) (60_000 * 7.5), 2D),
                new DataPoint<>((long) (60_000 * 8.5), 2D),
                new DataPoint<>((long) (60_000 * 9.0), 2D));

        assertEquals(actual, expected, "The rates do not match");
    }

    @Test
    public void findRateStats() {
        String tenantId = "counter-rate-stats-test";

        Metric<Long> counter = new Metric<>(new MetricId<>(tenantId, COUNTER, "C1"), asList(
                new DataPoint<>((long) (60_000 * 1.0), 0L),
                new DataPoint<>((long) (60_000 * 1.5), 200L),
                new DataPoint<>((long) (60_000 * 3.5), 400L),
                new DataPoint<>((long) (60_000 * 5.0), 550L),
                new DataPoint<>((long) (60_000 * 7.0), 950L),
                new DataPoint<>((long) (60_000 * 7.5), 1000L)));

        doAction(() -> metricsService.addDataPoints(COUNTER, Observable.just(counter)));

        List<NumericBucketPoint> actual = metricsService.findRateStats(counter.getMetricId(),
                0, now().getMillis(), Buckets.fromStep(60_000, 60_000 * 8, 60_000),
                asList(99.0)).toBlocking().single();
        List<NumericBucketPoint> expected = new ArrayList<>();
        for (int i = 1; i < 8; i++) {
            NumericBucketPoint.Builder builder = new NumericBucketPoint.Builder(60_000 * i, 60_000 * (i + 1));
            double val;
            switch (i) {
                case 1:
                    val = 400D;
                    builder.setAvg(val).setMax(val).setMedian(val).setMin(val)
                            .setPercentiles(asList(new Percentile(0.99, val))).setSamples(1).setSum(val);
                    break;
                case 3:
                    val = 100D;
                    builder.setAvg(val).setMax(val).setMedian(val).setMin(val)
                            .setPercentiles(asList(new Percentile(0.99, val))).setSamples(1).setSum(val);
                    break;
                case 5:
                    val = 100;
                    builder.setAvg(val).setMax(val).setMedian(val).setMin(val)
                            .setPercentiles(asList(new Percentile(0.99, val))).setSamples(1).setSum(val);
                case 6:
                    break;
                case 7:
                    builder.setAvg(150.0).setMax(200.0).setMin(100.0).setMedian(100.0).setPercentiles(
                            asList(new Percentile(0.99, 100.0))).setSamples(2).setSum(300.0);
                default:
                    break;
            }
            expected.add(builder.build());
        }

        assertNumericBucketsEquals(actual, expected);
    }

}
