/*
 * Copyright 2014-2015 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.core.impl;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hawkular.metrics.core.api.MetricType.COUNTER;
import static org.joda.time.Duration.standardMinutes;
import static org.testng.Assert.assertEquals;

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.codahale.metrics.MetricRegistry;
import org.hawkular.metrics.core.api.DataPoint;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.tasks.api.RepeatingTrigger;
import org.hawkular.metrics.tasks.impl.Queries;
import org.hawkular.metrics.tasks.impl.TaskSchedulerImpl;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import rx.Observable;
import rx.Observer;
import rx.observers.TestSubscriber;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

/**
 * @author jsanda
 */
public class RatesITest extends MetricsITest {

    private static Logger logger = LoggerFactory.getLogger(RatesITest.class);

    private MetricsServiceImpl metricsService;

    private TaskSchedulerImpl taskScheduler;

    private DateTimeService dateTimeService;

    private TestScheduler tickScheduler;

    private Observable<Long> finishedTimeSlices;

    @BeforeClass
    public void initClass() {
        initSession();

        System.setProperty("hawkular.scheduler.time-units", "seconds");

        dateTimeService = new DateTimeService();

        DateTime startTime = dateTimeService.getTimeSlice(DateTime.now(), standardMinutes(1)).minusMinutes(20);

        tickScheduler = Schedulers.test();
        tickScheduler.advanceTimeTo(startTime.getMillis(), TimeUnit.MILLISECONDS);

        taskScheduler = new TaskSchedulerImpl(rxSession, new Queries(session));
        taskScheduler.setTickScheduler(tickScheduler);

        RepeatingTrigger.now = tickScheduler::now;

        metricsService = new MetricsServiceImpl();
        metricsService.setTaskScheduler(taskScheduler);

        String keyspace = "hawkulartest";
        System.setProperty("keyspace", keyspace);

        finishedTimeSlices = taskScheduler.getFinishedTimeSlices();
        taskScheduler.start();

        GenerateRate generateRate = new GenerateRate(metricsService);
        taskScheduler.subscribe(generateRate);

        metricsService.startUp(session, keyspace, false, new MetricRegistry());
    }

    @BeforeMethod
    public void initMethod() {
        session.execute("TRUNCATE metrics_idx");
        session.execute("TRUNCATE data");
        session.execute("TRUNCATE leases");
        session.execute("TRUNCATE task_queue");
    }

    @AfterClass
    public void shutdown() {
//        taskScheduler.shutdown();
    }

    @Test
    public void generateRates() throws Exception {
        String tenantId = "generate-rates-test";
        MetricId id = new MetricId("c1");
        DateTime start = new DateTime(tickScheduler.now());
        DateTime end = start.plusMinutes(10);

        logger.debug("START TIME = {}", new Date(start.getMillis()));
        logger.debug("END TIME = {}", new Date(end.getMillis()));

        Metric<Long> counter = new Metric<>(tenantId, COUNTER, id, asList(
                new DataPoint<>(start.plusMinutes(1).plusMillis(50).getMillis(), 11L),
                new DataPoint<>(start.plusMinutes(1).plusSeconds(30).getMillis(), 17L),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 29L),
                new DataPoint<>(start.plusMinutes(2).plusSeconds(30).getMillis(), 46L),
                new DataPoint<>(start.plusMinutes(3).getMillis(), 69L),
                new DataPoint<>(start.plusMinutes(3).plusSeconds(30).getMillis(), 85L),
                new DataPoint<>(start.plusMinutes(4).getMillis(), 107L)
        ));

        insertBlocking(() -> metricsService.createMetric(counter));
        insertBlocking(() -> metricsService.addCounterData(Observable.just(counter)));

        Observer<Long> observer = new Observer<Long>() {
            @Override
            public void onCompleted() {
                logger.debug("Finished observing");
            }

            @Override
            public void onError(Throwable e) {

            }

            @Override
            public void onNext(Long timestamp) {
                logger.debug("Observing {}", new Date(timestamp));
            }
        };

        TestSubscriber<Long> subscriber = new TestSubscriber<>(observer);
//        finishedTimeSlices.takeUntil(time -> time > end.getMillis())
        finishedTimeSlices.takeUntil(time -> time >= start.plusMinutes(5).getMillis())
                .doOnNext(time -> logger.debug("NEXT TICK = {}", new Date(time)))
                .observeOn(Schedulers.immediate())
                .subscribe(subscriber);

        tickScheduler.advanceTimeTo(start.plusMinutes(5).getMillis(), TimeUnit.MILLISECONDS);
//        tickScheduler.advanceTimeTo(end.plusSeconds(2).getMillis(), TimeUnit.MILLISECONDS);

        subscriber.awaitTerminalEvent(10, SECONDS);
        subscriber.assertNoErrors();
        subscriber.assertTerminalEvent();

        List<DataPoint<Double>> actual = getOnNextEvents(() -> metricsService.findRateData(tenantId, id,
                start.plusMinutes(1).getMillis(), start.plusMinutes(2).getMillis()));
        List<DataPoint<Double>> expected = singletonList(new DataPoint<>(start.plusMinutes(1).getMillis(),
                calculateRate(17, start.plusMinutes(1), start.plusMinutes(2))));
        assertEquals(actual, expected, "The rate for " + start.plusMinutes(1) + " does not match the expected values");


        actual = getOnNextEvents(() -> metricsService.findRateData(tenantId, id, start.plusMinutes(2).getMillis(),
                start.plusMinutes(3).getMillis()));
        expected = singletonList(new DataPoint<>(start.plusMinutes(2).getMillis(),
                calculateRate(46, start.plusMinutes(2), start.plusMinutes(3))));
        assertEquals(actual, expected, "The rate for " + start.plusMinutes(2) + " does not match the expected values");

        actual = getOnNextEvents(() -> metricsService.findRateData(tenantId, id, start.plusMinutes(3).getMillis(),
                start.plusMinutes(4).getMillis()));
        expected = singletonList(new DataPoint<>(start.plusMinutes(3).getMillis(),
                calculateRate(85, start.plusMinutes(3), start.plusMinutes(4))));
        assertEquals(actual, expected, "The rate for " + start.plusMinutes(3) + " does not match the expected values");

//        actual = metricsService.findRateData(tenantId, id, start.plusSeconds(20).getMillis(),
//                start.plusSeconds(25).getMillis()).toBlocking().last();
//        expected = new DataPoint<>(start.plusSeconds(20).getMillis(), calculateRate(69, start.plusSeconds(20),
//                start.plusSeconds(25)));
//     assertEquals(actual, expected, "The rate for " + start.plusSeconds(20) + " does not match the expected value.");
    }

    private void insertBlocking(Supplier<Observable<Void>> fn) {
        TestSubscriber<Void> subscriber = new TestSubscriber<>();
        Observable<Void> observable = fn.get();
        observable.subscribe(subscriber);
        subscriber.awaitTerminalEvent(5, TimeUnit.SECONDS);
        subscriber.assertNoErrors();
        subscriber.assertCompleted();
    }

    private <T> List<T> getOnNextEvents(Supplier<Observable<T>> fn) {
        TestSubscriber<T> subscriber = new TestSubscriber<>();
        Observable<T> observable = fn.get();
        observable.subscribe(subscriber);
        subscriber.awaitTerminalEvent(5, SECONDS);
        subscriber.assertNoErrors();
        subscriber.assertCompleted();

        return subscriber.getOnNextEvents();
    }

    private double calculateRate(double value, DateTime startTime, DateTime endTime) {
        return (value / (endTime.getMillis() - startTime.getMillis())) * 1000;
    }

}
