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

import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jsanda
 */
public class LeaseManager {

    private static final Logger logger = LoggerFactory.getLogger(LeaseManager.class);

    public static final int DEFAULT_LEASE_TTL = 180;

    public static final int DEFAULT_RENEWAL_RATE = 60;

    public static final Function<ResultSet, Void> TO_VOID = resultSet -> null;

    private Session session;

    private Queries queries;

    private ScheduledExecutorService renewals = Executors.newScheduledThreadPool(1);

    private int ttl = DEFAULT_LEASE_TTL;

    private int renewalRate = DEFAULT_RENEWAL_RATE;

    public LeaseManager(Session session, Queries queries) {
        this.session = session;
        this.queries = queries;
    }

    void setTTL(int ttl) {
        this.ttl = ttl;
    }

    void setRenewalRate(int renewalRate) {
        this.renewalRate = renewalRate;
    }

    public ListenableFuture<List<Lease>> findUnfinishedLeases(DateTime timeSlice) {
        ResultSetFuture future = session.executeAsync(queries.findLeases.bind(timeSlice.toDate()));
        return Futures.transform(future, (ResultSet resultSet) -> StreamSupport.stream(resultSet.spliterator(), false)
                .map(row->new Lease(timeSlice, row.getString(0), row.getInt(1), row.getString(2), row.getBool(3)))
                .filter(lease -> !lease.isFinished())
                .collect(toList()));
    }

    public ListenableFuture<Boolean> acquire(Lease lease) {
        ResultSetFuture future = session.executeAsync(queries.acquireLease.bind(ttl, lease.getOwner(),
                lease.getTimeSlice().toDate(), lease.getTaskType(), lease.getSegmentOffset()));
        return Futures.transform(future, ResultSet::wasApplied);
    }

    public ListenableFuture<Boolean> acquire(Lease lease, int ttl) {
        ResultSetFuture future = session.executeAsync(queries.acquireLease.bind(ttl, lease.getOwner(),
                lease.getTimeSlice().toDate(), lease.getTaskType(), lease.getSegmentOffset()));
        return Futures.transform(future, ResultSet::wasApplied);
    }

    public ListenableFuture<Boolean> renew(Lease lease) {
        ResultSetFuture future = session.executeAsync(queries.renewLease.bind(ttl, lease.getOwner(),
                lease.getTimeSlice().toDate(), lease.getTaskType(), lease.getSegmentOffset(),
                lease.getOwner()));
        return Futures.transform(future, ResultSet::wasApplied);
    }

    /**
     * Schedules the lease to be automatically renewed every {@link #DEFAULT_RENEWAL_RATE} seconds in a background
     * thread. Renewals will stop once the lease is set to finished. If the lease cannot be renewed, then the lease
     * owner, i.e., the calling thread, will be interrupted. It therefore important for lease owners to handle
     * InterruptedExceptions appropriately.
     */
    public void autoRenew(Lease lease) {
        autoRenew(lease, Thread.currentThread());
    }

    private void autoRenew(Lease lease, Thread leaseOwner) {
        renewals.schedule(createRenewRunnable(lease, leaseOwner), renewalRate, TimeUnit.SECONDS);
    }

    private Runnable createRenewRunnable(Lease lease, Thread leaseOwner) {
        return () -> {
            if (lease.isFinished()) {
                return;
            }
            ListenableFuture<Boolean> renewedFuture = renew(lease);
            Futures.addCallback(renewedFuture, new FutureCallback<Boolean>() {
                @Override
                public void onSuccess(Boolean renewed) {
                    if (renewed) {
                        autoRenew(lease, leaseOwner);
                    } else {
                        logger.info("Failed to renew " + lease);
                        leaseOwner.interrupt();
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    logger.warn("Failed to renew " + lease + " for " + leaseOwner);
                    // TODO figure out what to do in this scenario
                }
            });
        };
    }

    public ListenableFuture<Boolean> renew(Lease lease, int ttl) {
        ResultSetFuture future = session.executeAsync(queries.renewLease.bind(ttl, lease.getOwner(),
                lease.getTimeSlice().toDate(), lease.getTaskType(), lease.getSegmentOffset(),
                lease.getOwner()));
        return Futures.transform(future, ResultSet::wasApplied);
    }

    public ListenableFuture<Boolean> finish(Lease lease) {
        ResultSetFuture future = session.executeAsync(queries.finishLease.bind(lease.getTimeSlice().toDate(),
                lease.getTaskType(), lease.getSegmentOffset(), lease.getOwner()));
        return Futures.transform(future, ResultSet::wasApplied);
    }

    public ListenableFuture<Void> deleteLeases(DateTime timeSlice) {
        ResultSetFuture future = session.executeAsync(queries.deleteLeases.bind(timeSlice.toDate()));
        return Futures.transform(future, TO_VOID);
    }

}
