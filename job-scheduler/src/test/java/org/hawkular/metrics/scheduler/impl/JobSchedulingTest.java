/*
 * Copyright 2014-2017 Red Hat, Inc. and/or its affiliates
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

import static java.util.Collections.emptyMap;

import static org.hawkular.metrics.datetime.DateTimeService.currentMinute;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.hawkular.metrics.scheduler.api.JobDetails;
import org.hawkular.metrics.scheduler.api.RepeatingTrigger;
import org.hawkular.metrics.scheduler.api.SingleExecutionTrigger;
import org.hawkular.metrics.scheduler.api.Trigger;
import org.joda.time.DateTime;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

import rx.Single;

/**
 * @author jsanda
 */
public class JobSchedulingTest extends JobSchedulerTest {

    private SchedulerImpl jobScheduler;

    @BeforeClass
    public void initClass() {
        try {
            jobScheduler = new SchedulerImpl(rxSession, InetAddress.getLocalHost().getHostName());
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeMethod(alwaysRun = true)
    public void initTest(Method method) throws Exception {
        resetSchema();
    }

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
        JobDetails details = jobScheduler.scheduleJob(type, name, emptyMap(), trigger).toBlocking().value();
    }

    @Test
    public void scheduleMultipleJobs() throws Exception {
        DateTime activeQueue = currentMinute().plusMinutes(1);
        Trigger trigger = new RepeatingTrigger.Builder()
                .withDelay(1, TimeUnit.MINUTES)
                .withInterval(5, TimeUnit.MINUTES)
                .build();
        CountDownLatch latch = new CountDownLatch(2);
        AtomicReference<Throwable> s1Error = new AtomicReference<>();
        AtomicReference<Throwable> s2Error = new AtomicReference<>();
        AtomicReference<JobDetails> details1 = new AtomicReference<>();
        AtomicReference<JobDetails> details2 = new AtomicReference<>();

        Single<? extends JobDetails> s1 = jobScheduler.scheduleJob("Test", "Test 1", emptyMap(), trigger);
        Single<? extends JobDetails> s2 = jobScheduler.scheduleJob("Test", "Test 2", emptyMap(), trigger);

        s1.subscribe(
                details -> {
                    details1.set(details);
                    latch.countDown();
                },
                t -> {
                    s1Error.set(t);
                    latch.countDown();
                });
        s2.subscribe(
                details -> {
                    details2.set(details);
                    latch.countDown();
                },
                t -> {
                    s2Error.set(t);
                    latch.countDown();
                });

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        if (s1Error.get() != null) {
            fail("Failed to schedule Test 1", s1Error.get());
        }
        if (s2Error.get() != null) {
            fail("Failed to schedule Test 2", s2Error.get());
        }
        assertNotNull(details1.get());
        assertJobEquals(details1.get());
        assertNotNull(details2.get());
        assertJobEquals(details2.get());
    }

    @Test
    public void doNotScheduleJobWhenLockCannotBeAcquired() throws Exception {
        Trigger trigger = new SingleExecutionTrigger.Builder().withDelay(1, TimeUnit.MINUTES).build();
        String lock = SchedulerImpl.QUEUE_LOCK_PREFIX + trigger.getTriggerTime();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<JobDetails> detailsRef = new AtomicReference<>();
        AtomicReference<Throwable> errorRef = new AtomicReference<>();

        session.execute("insert into locks (name, value) values ('" + lock + "', 'executing')");

        jobScheduler.scheduleJob("Test", "Test 1", emptyMap(), trigger).subscribe(
                details -> {
                    detailsRef.set(details);
                    latch.countDown();
                },
                t -> {
                    errorRef.set(t);
                    latch.countDown();
                }
        );

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertNull(detailsRef.get());
        assertNotNull(errorRef.get());
    }

}
