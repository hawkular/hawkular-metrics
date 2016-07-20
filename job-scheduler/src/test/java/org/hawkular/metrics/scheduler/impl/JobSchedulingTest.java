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
package org.hawkular.metrics.scheduler.impl;

import static org.hawkular.metrics.datetime.DateTimeService.currentMinute;
import static org.testng.Assert.assertNotNull;

import java.util.Collections;
import java.util.Map;

import org.hawkular.metrics.scheduler.api.JobDetails;
import org.hawkular.metrics.scheduler.api.SingleExecutionTrigger;
import org.hawkular.metrics.scheduler.api.Trigger;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

/**
 * @author jsanda
 */
public class JobSchedulingTest extends JobSchedulerTest {

    @Test
    public void scheduleSingleExecutionJob() {
        DateTime activeQueue = currentMinute().plusMinutes(1);
        Trigger trigger = new SingleExecutionTrigger.Builder()
                .withTriggerTime(activeQueue.plusMinutes(5).getMillis())
                .build();
        String type = "Test-Job";
        String name = "Test Job";
        Map<String, String> params = ImmutableMap.of("x", "1", "y", "2");
        JobDetails details = jobScheduler.scheduleJob(type, name, params, trigger).toBlocking().value();

        assertNotNull(details);
        assertJobEquals(details);
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void doNotScheduleSingleExecutionJobWhenTriggerAlreadyFired() {
        DateTime activeQueue = currentMinute().plusMinutes(1);
        Trigger trigger = new SingleExecutionTrigger.Builder()
                .withTriggerTime(activeQueue.minusMinutes(2).getMillis())
                .build();
        String type = "Test-Job";
        String name = "Test Job";
        JobDetails details = jobScheduler.scheduleJob(type, name, Collections.emptyMap(), trigger).toBlocking().value();
    }

}
