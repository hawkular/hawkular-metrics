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
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.hawkular.metrics.core.api.MetricType.COUNTER;
import static org.joda.time.Duration.standardMinutes;
import static org.testng.Assert.assertEquals;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.hawkular.metrics.core.api.DataPoint;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.Tenant;
import org.hawkular.metrics.tasks.api.AbstractTrigger;
import org.hawkular.metrics.tasks.impl.Queries;
import org.hawkular.metrics.tasks.impl.TaskSchedulerImpl;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.codahale.metrics.MetricRegistry;

import rx.Observable;
import rx.observers.TestSubscriber;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

/**
 * This class tests counter rates indirectly by running the task scheduler with a virtual
 * clock. {@link GenerateRateITest} tests directly without running a task scheduler.
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

        dateTimeService = new DateTimeService();

        DateTime startTime = dateTimeService.getTimeSlice(DateTime.now(), standardMinutes(1)).minusMinutes(20);

        tickScheduler = Schedulers.test();
        tickScheduler.advanceTimeTo(startTime.getMillis(), TimeUnit.MILLISECONDS);

        taskScheduler = new TaskSchedulerImpl(rxSession, new Queries(session));
        taskScheduler.setTickScheduler(tickScheduler);

        AbstractTrigger.now = tickScheduler::now;

        metricsService = new MetricsServiceImpl();
        metricsService.setTaskScheduler(taskScheduler);
        metricsService.setDataAccess(new DataAccessImpl(session));
        metricsService.setDateTimeService(dateTimeService);

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
        session.execute("TRUNCATE tenants");
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
    public void generateRates() {
        String tenant = "rates-test";
        DateTime start = new DateTime(tickScheduler.now()).plusMinutes(1);

        Metric<Long> c1 = new Metric<>(new MetricId(tenant, COUNTER, "C1"));
        Metric<Long> c2 = new Metric<>(new MetricId(tenant, COUNTER, "C2"));
        Metric<Long> c3 = new Metric<>(new MetricId(tenant, COUNTER, "C3"));

        doAction(() -> metricsService.createTenant(new Tenant(tenant)));

        doAction(() -> metricsService.createMetric(c1));
        doAction(() -> metricsService.createMetric(c2));
        doAction(() -> metricsService.createMetric(c3));

        doAction(() -> metricsService.addCounterData(Observable.from(asList(
                new Metric<>(c1.getId(), asList(new DataPoint<>(start.getMillis(), 10L),
                        new DataPoint<>(start.plusSeconds(30).getMillis(), 25L))),
                new Metric<>(c2.getId(), asList(new DataPoint<>(start.getMillis(), 100L),
                        new DataPoint<>(start.plusSeconds(30).getMillis(), 165L))),
                new Metric<>(c3.getId(), asList(new DataPoint<>(start.getMillis(), 42L),
                        new DataPoint<>(start.plusSeconds(30).getMillis(), 77L)))
        ))));

        TestSubscriber<Long> subscriber = new TestSubscriber<>();
        finishedTimeSlices.take(2).observeOn(Schedulers.immediate()).subscribe(subscriber);

        tickScheduler.advanceTimeBy(2, MINUTES);

        subscriber.awaitTerminalEvent(5, SECONDS);
        subscriber.assertNoErrors();
        subscriber.assertTerminalEvent();

        List<DataPoint<Double>> c1Rate = getOnNextEvents(() -> metricsService.findRateData(c1.getId(),
                start.getMillis(), start.plusMinutes(1).getMillis()));
        List<DataPoint<Double>> c2Rate = getOnNextEvents(() -> metricsService.findRateData(c2.getId(),
                start.getMillis(), start.plusMinutes(1).getMillis()));
        List<DataPoint<Double>> c3Rate = getOnNextEvents(() -> metricsService.findRateData(c3.getId(),
                start.getMillis(), start.plusMinutes(1).getMillis()));

        assertEquals(c1Rate, singletonList(new DataPoint<>(start.getMillis(), calculateRate(25, start,
                start.plusMinutes(1)))));
        assertEquals(c2Rate, singletonList(new DataPoint<>(start.getMillis(), calculateRate(165, start,
                start.plusMinutes(1)))));
        assertEquals(c3Rate, singletonList(new DataPoint<>(start.getMillis(), calculateRate(77, start,
                start.plusMinutes(1)))));
    }

}
