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

import static org.joda.time.DateTime.now;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.List;

import com.datastax.driver.core.PreparedStatement;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import org.hawkular.metrics.tasks.BaseTest;
import org.joda.time.DateTime;
import org.joda.time.Minutes;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @author jsanda
 */
public class LeaseManagerTest extends BaseTest {

//    private static final long FUTURE_TIMEOUT = 3;
//
//    private Session session;
//
//    private DateTimeService dateTimeService;
//
//    private Queries queries;
//
    private LeaseManager leaseManager;
//
    private PreparedStatement createdFinishedLease;
//
    @BeforeClass
    public void initClass() {
        leaseManager = new LeaseManager(session, queries);
        createdFinishedLease = session.prepare(
                "INSERT INTO leases (time_slice, task_type, segment_offset, finished) VALUES (?, ?, ?, ?)");
    }
//
//    @BeforeMethod
//    public void resetDB() {
//        session.execute("TRUNCATE leases");
//    }

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

        session.execute(createdFinishedLease.bind(timeSlice.toDate(), taskType3,  0, true));
        session.execute(createdFinishedLease.bind(timeSlice.toDate(), taskType4, 0, true));

        ListenableFuture<List<Lease>> future = leaseManager.findUnfinishedLeases(timeSlice);
        List<Lease> actual = getUninterruptibly(future);
        List<Lease> expected = ImmutableList.of(
                new Lease(timeSlice, taskType1, 0, null, false),
                new Lease(timeSlice, taskType2, 0, null, false)
        );

        assertEquals(actual, expected, "The leases do not match");
    }

    @Test
    public void acquireAvailableLease() throws Exception {
        DateTime timeSlice = dateTimeService.getTimeSlice(now(), Minutes.ONE.toStandardDuration());

        session.execute(queries.createLease.bind(timeSlice.toDate(), "test", 0));

        List<Lease> leases = getUninterruptibly(leaseManager.findUnfinishedLeases(timeSlice));
        assertEquals(leases.size(), 1, "Expected to find one lease");

        leases.get(0).setOwner("server1");
        Boolean acquired = getUninterruptibly(leaseManager.acquire(leases.get(0)));
        assertTrue(acquired, "Failed to acquire " + leases.get(0));
    }

    @Test
    public void doNotAcquireLeaseThatIsAlreadyOwned() throws Exception {
        DateTime timeSlice = dateTimeService.getTimeSlice(now(), Minutes.ONE.toStandardDuration());
        Lease lease = new Lease(timeSlice, "test", 0, null, false);

        session.execute(queries.createLease.bind(timeSlice.toDate(), "test", 0));

        lease.setOwner("server1");
        Boolean acquired = getUninterruptibly(leaseManager.acquire(lease));
        assertTrue(acquired, "Expected to acquire " + lease);

        // now try to acquire with a different owner
        lease.setOwner("server2");
        acquired = getUninterruptibly(leaseManager.acquire(lease));
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
        Boolean acquired = getUninterruptibly(leaseManager.acquire(lease, 1));
        assertTrue(acquired, "Expected to acquire " + lease);

        Thread.sleep(1000);

        // now try to acquire with a different owner
        lease.setOwner("server2");
        acquired = getUninterruptibly(leaseManager.acquire(lease));
        assertTrue(acquired, "Should have acquired the lease since it expired");
    }

    @Test
    public void renewLeaseBeforeItExpires() throws Exception {
        DateTime timeSlice = dateTimeService.getTimeSlice(now(), Minutes.ONE.toStandardDuration());
        Lease lease = new Lease(timeSlice, "test", 0, null, false);

        session.execute(queries.createLease.bind(timeSlice.toDate(), "test", 0));

        lease.setOwner("server1");
        Boolean acquired = getUninterruptibly(leaseManager.acquire(lease));
        assertTrue(acquired, "Expected to acquire " + lease);

        Boolean renewed = getUninterruptibly(leaseManager.renew(lease));
        assertTrue(renewed, "Expected lease to be renewed");
    }

}
