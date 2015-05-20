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
package org.hawkular.metrics.tasks.impl;

import static java.util.Collections.singletonList;
import static org.joda.time.DateTime.now;
import static org.joda.time.Duration.standardMinutes;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import com.datastax.driver.core.PreparedStatement;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.hawkular.metrics.tasks.BaseTest;
import org.hawkular.rx.cassandra.driver.RxSessionImpl;
import org.joda.time.DateTime;
import org.joda.time.Minutes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author jsanda
 */
public class LeaseServiceTest extends BaseTest {

    private static Logger logger = LoggerFactory.getLogger(LeaseServiceTest.class);

    private LeaseService leaseService;

    private PreparedStatement createdFinishedLease;

    @BeforeClass
    public void initClass() {
        createdFinishedLease = session.prepare(
                "INSERT INTO leases (time_slice, task_type, segment_offset, finished) VALUES (?, ?, ?, ?)");
    }

    @BeforeMethod
    public void init() {
        leaseService = new LeaseService(new RxSessionImpl(session), queries);
    }

    @Test
    public void findUnfinishedLeases() throws Exception {
        String taskType1 = "test-type-1";
        String taskType2 = "test-type-2";
        String taskType3 = "test-type-3";
        String taskType4 = "test-type-4";
        DateTime timeSlice = dateTimeService.getTimeSlice(now(), Minutes.ONE.toStandardDuration());

        session.execute(queries.createLease.bind(timeSlice.toDate(), taskType1, 0));
        session.execute(queries.createLease.bind(timeSlice.toDate(), taskType2, 0));
        session.execute(queries.createLease.bind(timeSlice.toDate(), taskType3, 0));
        session.execute(queries.createLease.bind(timeSlice.toDate(), taskType4, 0));

        session.execute(createdFinishedLease.bind(timeSlice.toDate(), taskType3, 0, true));
        session.execute(createdFinishedLease.bind(timeSlice.toDate(), taskType4, 0, true));

//        List<Lease> actual = ImmutableList.copyOf(leaseService.findUnfinishedLeases(timeSlice).toBlocking()
//                .toIterable());
        Set<Lease> actual = ImmutableSet.copyOf(leaseService.findUnfinishedLeases(timeSlice).toBlocking()
                .toIterable());
        Set<Lease> expected = ImmutableSet.of(
                new Lease(timeSlice, taskType1, 0, null, false),
                new Lease(timeSlice, taskType2, 0, null, false)
        );

        assertEquals(actual, expected, "The leases do not match. Found " + actual);
    }

    @Test
    public void acquireAvailableLease() throws Exception {
        DateTime timeSlice = dateTimeService.getTimeSlice(now(), Minutes.ONE.toStandardDuration());

        session.execute(queries.createLease.bind(timeSlice.toDate(), "test", 0));

        List<Lease> leases = ImmutableList.copyOf(leaseService.findUnfinishedLeases(timeSlice).toBlocking()
                .toIterable());
        assertEquals(leases.size(), 1, "Expected to find one lease");

        leases.get(0).setOwner("server1");
        Boolean acquired = leaseService.acquire(leases.get(0)).toBlocking().first();
        assertTrue(acquired, "Failed to acquire " + leases.get(0));
    }

    @Test
    public void doNotAcquireLeaseThatIsAlreadyOwned() throws Exception {
        DateTime timeSlice = dateTimeService.getTimeSlice(now(), Minutes.ONE.toStandardDuration());
        Lease lease = new Lease(timeSlice, "test", 0, null, false);

        session.execute(queries.createLease.bind(timeSlice.toDate(), "test", 0));

        lease.setOwner("server1");
        Boolean acquired = leaseService.acquire(lease).toBlocking().first();
        assertTrue(acquired, "Expected to acquire " + lease);

        // now try to acquire with a different owner
        lease.setOwner("server2");
        acquired = leaseService.acquire(lease).toBlocking().first();
        assertFalse(acquired, "Should have failed to acquire lease since it already has a different owner");
    }

    @Test
    public void acquireLeaseThatExpires() throws Exception {
        // This test demonstrates that if a lease is not renewed by its owner then it will
        // expire and a different owner can acquire the lease.
        DateTime timeSlice = dateTimeService.getTimeSlice(now(), Minutes.ONE.toStandardDuration());
        Lease lease = new Lease(timeSlice, "test", 0, null, false);

        session.execute(queries.createLease.bind(timeSlice.toDate(), "test", 0));

        lease.setOwner("server1");
        Boolean acquired = leaseService.acquire(lease, 1).toBlocking().first();
        assertTrue(acquired, "Expected to acquire " + lease);

        Thread.sleep(1000);

        // now try to acquire with a different owner
        lease.setOwner("server2");
        acquired = leaseService.acquire(lease).toBlocking().first();
        assertTrue(acquired, "Should have acquired the lease since it expired");
    }

    @Test
    public void renewLeaseBeforeItExpires() throws Exception {
        DateTime timeSlice = dateTimeService.getTimeSlice(now(), Minutes.ONE.toStandardDuration());
        Lease lease = new Lease(timeSlice, "test", 0, null, false);

        session.execute(queries.createLease.bind(timeSlice.toDate(), "test", 0));

        lease.setOwner("server1");
        Boolean acquired = leaseService.acquire(lease).toBlocking().first();
        assertTrue(acquired, "Expected to acquire " + lease);

        Boolean renewed = leaseService.renew(lease).toBlocking().first();
        assertTrue(renewed, "Expected lease to be renewed");
    }

    @Test
    public void autoRenewLease() throws Exception {
        int ttl = 3;
        int renewalRate = 1;

        DateTime timeSlice = dateTimeService.getTimeSlice(now(), standardMinutes(1));
        Lease lease = new Lease(timeSlice, "autoRenew", 0, null, false);

        session.execute(queries.createLease.bind(lease.getTimeSlice().toDate(), lease.getTaskType(),
                lease.getSegmentOffset()));

        leaseService.setTTL(ttl);
        leaseService.setRenewalRate(renewalRate);

        lease.setOwner("server1");
        Boolean acquired = leaseService.acquire(lease).toBlocking().first();
        assertTrue(acquired, "Expected to acquire " + lease);

        executeInNewThread(() -> {
            leaseService.autoRenew(lease);
            List<Lease> expectedLeases = singletonList(lease);
            try {
                for (int i = 0; i < 5; ++i) {
                    Thread.sleep(ttl * 1000);
                    List<Lease> remainingLeases = ImmutableList.copyOf(leaseService.findUnfinishedLeases(timeSlice)
                            .toBlocking().toIterable());
                    logger.info("Remaining leases = {}", remainingLeases);
                    assertTrue(remainingLeases.isEmpty(), "There should not be any available leases but found " +
                        remainingLeases);
//                    assertEquals(remainingLeases, expectedLeases,
//                            "The unfinished leases do not match expected values");
                }
                Boolean finished = leaseService.finish(lease).toBlocking().first();
                assertTrue(finished, "Expected " + lease + " to be finished");
            } catch (Exception e) {
                fail("There was an unexpected error", e);
            }
        }, 20000);

        List<Lease> unfinishedLeases = ImmutableList.copyOf(leaseService.findUnfinishedLeases(timeSlice).toBlocking()
                .toIterable());
        assertTrue(unfinishedLeases.isEmpty(), "There should not be any unfinished leases but found " +
                unfinishedLeases);
    }

    @Test
    public void failToRenewLease() throws Exception {
        int ttl = 2;
        int renewalRate = 10;
        DateTime timeSlice = dateTimeService.getTimeSlice(now(), standardMinutes(1));
        Lease lease = new Lease(timeSlice, "failToRenew", 0, null, false);

        session.execute(queries.createLease.bind(lease.getTimeSlice().toDate(), lease.getTaskType(),
                lease.getSegmentOffset()));

        leaseService.setTTL(ttl);
        leaseService.setRenewalRate(renewalRate);

        lease.setOwner("server1");
        Boolean acquired = leaseService.acquire(lease).toBlocking().first();
        assertTrue(acquired, "Expected to acquire " + lease);

        executeInNewThread(() -> {
            leaseService.autoRenew(lease);

            try {
                lease.setOwner("server2");
                Thread.sleep(2000);
                Boolean acquiredByNewOwner =  leaseService.acquire(lease).toBlocking().first();
                assertTrue(acquiredByNewOwner, "Expected " + lease + " to be acquired by new owner");
            } catch (Exception e) {
                fail("There was an unexpected error trying to acquire " + lease, e);
            }

            InterruptedException exception = null;
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                exception = e;
            }
            assertNotNull(exception, "Expected an " + InterruptedException.class.getSimpleName() +
                    " to be thrown.");
        }, 20000);
    }

    /**
     * Executes the runnable in a new thread. If an AssertionError or any other exception is
     * thrown in the new thread, it will be caught and rethrown in the calling thread as an
     * AssertionError.
     *
     * @param runnable
     * @param wait
     * @throws InterruptedException
     */
    private void executeInNewThread(Runnable runnable, long wait) throws InterruptedException {
        AtomicReference<AssertionError> errorRef = new AtomicReference<>();
        Runnable wrappedRunnable = () -> {
            try {
                runnable.run();
            } catch (AssertionError e) {
                errorRef.set(e);
            } catch (Throwable t) {
                errorRef.set(new AssertionError("There was an unexpected exception", t));
            }
        };
        Thread thread = new Thread(wrappedRunnable);
        thread.start();
        thread.join(wait);

        if (errorRef.get() != null) {
            throw errorRef.get();
        }
    }

}
