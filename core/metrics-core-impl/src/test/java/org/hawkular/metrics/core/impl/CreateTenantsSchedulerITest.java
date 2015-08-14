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

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.joda.time.Duration.standardMinutes;
import static org.testng.Assert.assertEquals;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.hawkular.metrics.core.api.Tenant;
import org.hawkular.metrics.tasks.api.AbstractTrigger;
import org.hawkular.metrics.tasks.impl.Queries;
import org.hawkular.metrics.tasks.impl.TaskSchedulerImpl;
import org.joda.time.DateTime;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableSet;

import rx.Observable;
import rx.observers.TestSubscriber;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

/**
 * This class tests the tenant creation job indirectly by running the scheduler with a virtual clock.
 * {@link CreateTenantsITest} tests the job directly without running a scheduler.
 */
public class CreateTenantsSchedulerITest extends MetricsITest {

    private MetricsServiceImpl metricsService;

    private TaskSchedulerImpl taskScheduler;

    private DateTimeService dateTimeService;

    private TestScheduler tickScheduler;

    private Observable<Long> finishedTimeSlices;

    private DataAccess dataAccess;

    @BeforeClass
    public void initClass() {
        initSession();
        resetDB();

        dataAccess = new DataAccessImpl(session);
        dateTimeService = new DateTimeService();

        DateTime startTime = dateTimeService.getTimeSlice(DateTime.now(), standardMinutes(1)).minusMinutes(20);

        tickScheduler = Schedulers.test();
        tickScheduler.advanceTimeTo(startTime.getMillis(), TimeUnit.MILLISECONDS);

        taskScheduler = new TaskSchedulerImpl(rxSession, new Queries(session));
        taskScheduler.setTickScheduler(tickScheduler);

        AbstractTrigger.now = tickScheduler::now;

        metricsService = new MetricsServiceImpl();
        metricsService.setTaskScheduler(taskScheduler);
        metricsService.setDataAccess(dataAccess);

        finishedTimeSlices = taskScheduler.getFinishedTimeSlices();
        taskScheduler.start();

        metricsService.startUp(session, getKeyspace(), false, new MetricRegistry());

        CreateTenants job = new CreateTenants(metricsService, dataAccess);
        taskScheduler.getTasks().filter(task -> task.getName().equals(CreateTenants.TASK_NAME)).subscribe(job);
    }

    protected void resetDB() {
        session.execute("TRUNCATE tenants");
        session.execute("TRUNCATE tenants_by_time");
        session.execute("TRUNCATE leases");
        session.execute("TRUNCATE tasks");
        session.execute("TRUNCATE task_queue");
    }

    @Test
    public void executeJob() {
        DateTime start = new DateTime(tickScheduler.now()).plusMinutes(1);

        Tenant t1 = new Tenant("T1");
        doAction(() -> metricsService.createTenant(t1));
        doAction(() -> dataAccess.insertTenantId(start.getMillis(), "T2").flatMap(resultSet -> null));
        doAction(() -> dataAccess.insertTenantId(start.getMillis(), "T3").flatMap(resultSet -> null));

        TestSubscriber<Long> subscriber = new TestSubscriber<>();
        finishedTimeSlices.take(31).observeOn(Schedulers.immediate()).subscribe(subscriber);

        tickScheduler.advanceTimeBy(31, MINUTES);

        subscriber.awaitTerminalEvent(5, SECONDS);
        subscriber.assertNoErrors();
        subscriber.assertTerminalEvent();

        Set<Tenant> actual = ImmutableSet.copyOf(getOnNextEvents(metricsService::getTenants));
        Set<Tenant> expected = ImmutableSet.of(t1, new Tenant("T2"), new Tenant("T3"), new Tenant("$system"));

        assertEquals(actual, expected, "The tenants do not match");
    }

}
