/*
 *
 *  * Copyright 2014-2015 Red Hat, Inc. and/or its affiliates
 *  * and other contributors as indicated by the @author tags.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */
package org.hawkular.metrics.tasks;

import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.stream.StreamSupport;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.joda.time.DateTime;

/**
 * @author jsanda
 */
public class LeaseManager {

    public static final int DEFAULT_LEASE_TTL = 180;

    private Session session;

    private Queries queries;

    public LeaseManager(Session session, Queries queries) {
        this.session = session;
        this.queries = queries;
    }

    public ListenableFuture<List<Lease>> findUnfinishedLeases(DateTime timeSlice) {
        ResultSetFuture future = session.executeAsync(queries.findLeases.bind(timeSlice.toDate()));
        return Futures.transform(future, (ResultSet resultSet) -> StreamSupport.stream(resultSet.spliterator(), false)
                .map(row->new Lease(timeSlice, row.getString(0), row.getInt(1), row.getString(2), row.getBool(3)))
                .filter(lease -> !lease.isFinished())
                .collect(toList()));
    }

    public ListenableFuture<Boolean> acquire(Lease lease) {
        ResultSetFuture future = session.executeAsync(queries.acquireLease.bind(DEFAULT_LEASE_TTL, lease.getOwner(),
                lease.getTimeSlice().toDate(), lease.getTaskType(), lease.getSegmentOffset()));
        return Futures.transform(future, ResultSet::wasApplied);
    }

    public ListenableFuture<Boolean> acquire(Lease lease, int ttl) {
        ResultSetFuture future = session.executeAsync(queries.acquireLease.bind(ttl, lease.getOwner(),
                lease.getTimeSlice().toDate(), lease.getTaskType(), lease.getSegmentOffset()));
        return Futures.transform(future, ResultSet::wasApplied);
    }

    public ListenableFuture<Boolean> renew(Lease lease) {
        ResultSetFuture future = session.executeAsync(queries.renewLease.bind(DEFAULT_LEASE_TTL, lease.getOwner(),
                lease.getTimeSlice().toDate(), lease.getTaskType(), lease.getSegmentOffset(),
                lease.getOwner()));
        return Futures.transform(future, ResultSet::wasApplied);
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

}
