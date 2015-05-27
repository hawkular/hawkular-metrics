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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.ResultSet;
import org.hawkular.rx.cassandra.driver.RxSession;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

/**
 * @author jsanda
 */
public class LeaseService {

    private static final Logger logger = LoggerFactory.getLogger(LeaseService.class);

    public static final int DEFAULT_LEASE_TTL = 180;

    public static final int DEFAULT_RENEWAL_RATE = 60;

    private RxSession session;

    private Queries queries;

    private ScheduledExecutorService renewals = Executors.newScheduledThreadPool(1);

    private int ttl = DEFAULT_LEASE_TTL;

    private int renewalRate = DEFAULT_RENEWAL_RATE;

    public LeaseService(RxSession session, Queries queries) {
        this.session = session;
        this.queries = queries;
    }

    public void shutdown() {
        logger.info("Shutting down");
        renewals.shutdownNow();
    }

    void setTTL(int ttl) {
        this.ttl = ttl;
    }

    void setRenewalRate(int renewalRate) {
        this.renewalRate = renewalRate;
    }

    public Observable<? extends List<Lease>> loadLeases(DateTime timeSlice) {
        return session.execute(queries.findLeases.bind(timeSlice.toDate()))
                .flatMap(Observable::from)
                .map(row -> new Lease(timeSlice, row.getString(0), row.getInt(1), row.getString(2), row.getBool(3)))
                .filter(lease -> !lease.isFinished())
                .collect(ArrayList::new, ArrayList::add);
    }

    public Observable<Lease> findUnfinishedLeases(DateTime timeSlice) {
        // TODO Add logic to "refresh" the leases
        // We need to keep polling for leases until either we get back an empty result set
        // or all leases are marked finished. And to make things more interesting, we do
        // not want to poll again until there is at least one worker thread (i.e., one of
        // the threads executing tasks) free.
        return Observable.create(subscriber ->
                        loadLeases(timeSlice)
                                .flatMap(Observable::from)
                                .filter(lease -> lease.getOwner() == null)
                                .flatMap(lease ->
                                        acquire(lease).map(acquired ->
                                                acquired ? lease : null))
                                .subscribe(subscriber::onNext, subscriber::onError, subscriber::onCompleted)
        );
    }


    public Observable<Boolean> acquire(Lease lease) {
        return session.execute(queries.acquireLease.bind(ttl, lease.getOwner(), lease.getTimeSlice().toDate(),
                lease.getTaskType(), lease.getSegmentOffset())).map(ResultSet::wasApplied);
    }

    public Observable<Boolean> acquire(Lease lease, int ttl) {
        return session.execute(queries.acquireLease.bind(ttl, lease.getOwner(), lease.getTimeSlice().toDate(),
                lease.getTaskType(), lease.getSegmentOffset())).map(ResultSet::wasApplied);
    }

    public Observable<Boolean> renew(Lease lease) {
        return session.execute(queries.renewLease.bind(ttl, lease.getOwner(), lease.getTimeSlice().toDate(),
                lease.getTaskType(), lease.getSegmentOffset(), lease.getOwner())).map(ResultSet::wasApplied);
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
            renew(lease).subscribe(
                    renewed -> {
                        if (renewed) {
                            autoRenew(lease, leaseOwner);
                        } else {
                            logger.info("Failed to renew " + lease + " for " + leaseOwner);
                            leaseOwner.interrupt();
                        }
                    },
                    t -> {
                        logger.warn("Failed to renew " + lease + " for " + leaseOwner);
                        // TODO figure out what to do in this scenario
                    });
        };
    }

    public Observable<Boolean> finish(Lease lease) {
        return session.execute(queries.finishLease.bind(lease.getTimeSlice().toDate(), lease.getTaskType(),
                lease.getSegmentOffset(), lease.getOwner())).map(ResultSet::wasApplied);
    }

    public Observable<Void> deleteLeases(DateTime timeSlice) {
        return session.execute(queries.deleteLeases.bind(timeSlice.toDate())).flatMap(resultSet -> null);
    }

}
