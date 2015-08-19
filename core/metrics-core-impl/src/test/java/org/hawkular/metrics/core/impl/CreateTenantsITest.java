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

import static java.util.Collections.emptyMap;
import static java.util.UUID.randomUUID;

import static org.joda.time.Duration.standardMinutes;
import static org.testng.Assert.assertEquals;

import java.util.List;
import java.util.Set;

import org.hawkular.metrics.core.api.Tenant;
import org.hawkular.metrics.tasks.api.SingleExecutionTrigger;
import org.hawkular.metrics.tasks.api.Trigger;
import org.hawkular.metrics.tasks.impl.Task2Impl;
import org.joda.time.DateTime;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

/**
 * @author jsanda
 */
public class CreateTenantsITest extends MetricsITest {

    private MetricsServiceImpl metricsService;

    private DateTimeService dateTimeService;

    private DataAccess dataAccess;

    @BeforeClass
    public void initClass() {
        initSession();

        dataAccess = new DataAccessImpl(session);
        dateTimeService = new DateTimeService();

        metricsService = new MetricsServiceImpl();
        metricsService.setTaskScheduler(new FakeTaskScheduler());
        metricsService.setDataAccess(dataAccess);

        metricsService.startUp(session, getKeyspace(), false, new MetricRegistry());
    }

    @BeforeMethod
    public void initMethod() {
        session.execute("TRUNCATE tenants");
        session.execute("TRUNCATE tenants_by_time");
    }

    @Test
    public void executeWhenThereAreNoTenantsInBucket() {
        DateTime bucket = dateTimeService.getTimeSlice(DateTime.now(), standardMinutes(30));

        Tenant t1 = new Tenant("T1");
        doAction(() -> metricsService.createTenant(t1));

        Trigger trigger = new SingleExecutionTrigger(bucket.plusMinutes(30).getMillis());
        Task2Impl taskDetails = new Task2Impl(randomUUID(), "$system", 0, "create-tenants", emptyMap(), trigger);

        CreateTenants task = new CreateTenants(metricsService, metricsService.getDataAccess());
        task.call(taskDetails);

        List<Tenant> actual = getOnNextEvents(metricsService::getTenants);
        List<Tenant> expected = ImmutableList.of(t1);

        assertEquals(actual, expected, "No new tenants should have been created.");
        assertIsEmpty("The " + bucket.toDate() + " bucket should not exist",
                dataAccess.findTenantIds(bucket.getMillis()));
    }

    @Test
    public void executeWhenThereAreTenantsInBucket() {
        DateTime bucket = dateTimeService.getTimeSlice(DateTime.now(), standardMinutes(30));

        Tenant t1 = new Tenant("T1");
        doAction(() -> metricsService.createTenant(t1));
        doAction(() -> dataAccess.insertTenantId(bucket.getMillis(), "T2").flatMap(resultSet -> null));
        doAction(() -> dataAccess.insertTenantId(bucket.getMillis(), "T3").flatMap(resultSet -> null));

        Trigger trigger = new SingleExecutionTrigger(bucket.plusMinutes(30).getMillis());
        Task2Impl taskDetails = new Task2Impl(randomUUID(), "$system", 0, "create-tenants", emptyMap(), trigger);

        CreateTenants task = new CreateTenants(metricsService, metricsService.getDataAccess());
        task.call(taskDetails);

        Set<Tenant> actual = ImmutableSet.copyOf(getOnNextEvents(metricsService::getTenants));
        Set<Tenant> expected = ImmutableSet.of(t1, new Tenant("T2"), new Tenant("T3"));

        assertEquals(actual, expected, "Expected tenants T2 and T3 to be created");
        assertIsEmpty("The " + bucket.toDate() + " bucket should have been deleted",
                dataAccess.findTenantIds(bucket.getMillis()));
    }
}
