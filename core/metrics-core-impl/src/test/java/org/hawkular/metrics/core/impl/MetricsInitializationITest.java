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

import static java.util.Collections.singletonList;

import static org.hawkular.metrics.core.impl.MetricsServiceImpl.SYSTEM_TENANT_ID;
import static org.joda.time.Duration.standardMinutes;
import static org.testng.Assert.assertEquals;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.hawkular.metrics.core.api.Tenant;
import org.hawkular.metrics.tasks.api.AbstractTrigger;
import org.hawkular.metrics.tasks.impl.Queries;
import org.hawkular.metrics.tasks.impl.TaskSchedulerImpl;
import org.joda.time.DateTime;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

/**
 * @author jsanda
 */
public class MetricsInitializationITest extends MetricsITest {

    private DataAccess dataAccess;

    private MetricsServiceImpl metricsService;

    private TaskSchedulerImpl taskScheduler;

    private DateTimeService dateTimeService;

    private TestScheduler tickScheduler;

    private String keyspace;

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

        taskScheduler.start();

        keyspace = System.getProperty("keyspace", "hawkulartest");
        metricsService.startUp(session, keyspace, false, new MetricRegistry());
    }

    protected void resetDB() {
        session.execute("TRUNCATE tenants");
        session.execute("TRUNCATE leases");
        session.execute("TRUNCATE task_queue");
        session.execute("TRUNCATE tasks");
    }

    @Test
    public void systemTenantShouldBeCreated() {
        List<Tenant> actual = getOnNextEvents(() ->
                metricsService.getTenants().filter(tenant -> tenant.getId().equals(SYSTEM_TENANT_ID)));
        List<Tenant> expected = singletonList(new Tenant(SYSTEM_TENANT_ID));

        assertEquals(actual, expected, "Expected to find only the system tenant");
    }

    @Test(dependsOnMethods = "systemTenantShouldBeCreated")
    public void tenantCreationJobShouldBeCreated() {
        ResultSet resultSet = session.execute("select group_key, name from tasks");
        List<Row> rows = resultSet.all();

        assertEquals(rows.size(), 1, "Expected to find one task");
        assertEquals(rows.get(0).getString(0), SYSTEM_TENANT_ID, "The job group key does not match");
        assertEquals(rows.get(0).getString(1), CreateTenants.TASK_NAME, "The job name does not match");
    }

    @Test(dependsOnMethods = "tenantCreationJobShouldBeCreated")
    public void doSystemInitializationOnlyOnce() {
        MetricsServiceImpl service = new MetricsServiceImpl();
        service.setTaskScheduler(taskScheduler);
        service.setDataAccess(dataAccess);
        service.startUp(session, keyspace, false, new MetricRegistry());

        List<Tenant> actual = getOnNextEvents(() ->
                service.getTenants().filter(tenant -> tenant.getId().equals(SYSTEM_TENANT_ID)));

        assertEquals(actual.size(), 1, "Expected to find only the system tenant but found " + actual);

        ResultSet resultSet = session.execute("select group_key, name from tasks");
        List<Row> rows = resultSet.all();

        assertEquals(rows.size(), 1, "There should only be one scheduled job");
    }

}
