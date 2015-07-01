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

import java.util.Objects;

/**
 * Tasks are associated with a lease. Clients must acquire a lease before they can start executing tasks associated
 * with it; however, leases are not directly exposed to clients. The actions of acquiring, renewing, and finishing a
 * lease are handled internally allowing clients to focus on their tasks.
 *
 * @author jsanda
 */
public class Lease {

    private long timeSlice;

    private int shard;

    private String owner;

    private boolean finished;

    public Lease(long timeSlice, int shard) {
        this.timeSlice = timeSlice;
        this.shard = shard;
    }

    public Lease(long timeSlice, int shard, String owner, boolean finished) {
        this.timeSlice = timeSlice;
        this.shard = shard;
        this.owner = owner;
        this.finished = finished;
    }

    public long getTimeSlice() {
        return timeSlice;
    }

    public int getShard() {
        return shard;
    }

    public String getOwner() {
        return owner;
    }

    public boolean isFinished() {
        return finished;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Lease lease = (Lease) o;
        return Objects.equals(timeSlice, lease.timeSlice) &&
                Objects.equals(shard, lease.shard) &&
                Objects.equals(finished, lease.finished) &&
                Objects.equals(owner, lease.owner);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timeSlice, shard, owner, finished);
    }

    @Override
    public String toString() {
        return "Lease{" +
                "timeSlice=" + timeSlice +
                ", shard=" + shard +
                ", owner='" + owner + '\'' +
                ", finished=" + finished +
                '}';
    }
}
