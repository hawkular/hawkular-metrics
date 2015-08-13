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

/**
 * @author jsanda
 */
public class CreateTenantsITest extends MetricsITest {

    private MetricsServiceImpl metricsService;

    private DateTimeService dateTimeService;

    @BeforeClass
    public void initClass() {
        initSession();

        dateTimeService = new DateTimeService();

        metricsService = new MetricsServiceImpl();
        metricsService.setTaskScheduler(new FakeTaskScheduler());

        metricsService.startUp(session, getKeyspace(), false, new MetricRegistry());
    }

    @BeforeMethod
    public void initMethod() {
        session.execute("TRUNCATE tenants");
        session.execute("TRUNCATE tenants_by_time");
    }

    @Test
    public void executeWhenThereAreNoTenantsForBucket() {
        DateTime bucket = dateTimeService.getTimeSlice(DateTime.now(), standardMinutes(1));

        Tenant t1 = new Tenant("T1");
        doAction(() -> metricsService.createTenant(t1));

        Trigger trigger = new SingleExecutionTrigger(bucket.getMillis());
        Task2Impl taskDetails = new Task2Impl(randomUUID(), "$system", 0, "create-tenants", emptyMap(), trigger);

        CreateTenants task = new CreateTenants(metricsService, metricsService.getDataAccess());
        task.call(taskDetails);

        List<Tenant> actual = getOnNextEvents(metricsService::getTenants);
        List<Tenant> expected = ImmutableList.of(t1);

        assertEquals(actual, expected, "No new tenants should have been created.");
    }
}
