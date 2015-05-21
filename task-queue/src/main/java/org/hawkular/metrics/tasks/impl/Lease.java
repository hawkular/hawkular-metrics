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

import org.joda.time.DateTime;

/**
 * Tasks are associated with a lease. Clients must acquire a lease before they can start executing tasks associated
 * with it; however, leases are not directly exposed to clients. The actions of acquiring, renewing, and finishing a
 * lease are handled internally allowing clients to focus on their tasks.
 *
 * @author jsanda
 */
class Lease {

    public static final Lease NOT_ACQUIRED = new Lease(null, null, 0, null, false);

    private DateTime timeSlice;

    private String taskType;

    private int segmentOffset;

    private String owner;

    private boolean finished;

    public Lease(DateTime timeSlice, String taskType, int segmentOffset, String owner, boolean finished) {
        this.timeSlice = timeSlice;
        this.taskType = taskType;
        this.segmentOffset = segmentOffset;
        this.owner = owner;
        this.finished = finished;
    }

    /**
     * The time slice for which the lease has been allocated. For example, if a client schedules a task to aggregate
     * data every hour and if the current time is 13:33, then a lease will be created and persisted for 14:00.
     */
    public DateTime getTimeSlice() {
        return timeSlice;
    }

    /**
     *
     * @return The {@link org.hawkular.metrics.tasks.api.TaskType task type} name
     */
    public String getTaskType() {
        return taskType;
    }

    /**
     * Specifies how many tasks in terms of segments are associated with this lease. Suppose there are 100 segments and
     * use a segment offset size of 10. There will be 10 task segments per lease. When a client acquires this lease,
     * all tasks in each of the 10 segments assigned to this lease will be executed.
     */
    public int getSegmentOffset() {
        return segmentOffset;
    }

    /**
     * The current lease owner or null if there is no owner.
     */
    public String getOwner() {
        return owner;
    }

    public Lease setOwner(String owner) {
        this.owner = owner;
        return this;
    }

    /**
     * @return True if all tasks associated with the lease are finished, false otherwise.
     */
    public boolean isFinished() {
        return finished;
    }

    public Lease setFinished(boolean finished) {
        this.finished = finished;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Lease lease = (Lease) o;
        return Objects.equals(segmentOffset, lease.segmentOffset) &&
                Objects.equals(finished, lease.finished) &&
                Objects.equals(timeSlice, lease.timeSlice) &&
                Objects.equals(taskType, lease.taskType) &&
                Objects.equals(owner, lease.owner);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timeSlice, taskType, segmentOffset, owner, finished);
    }

    @Override
    public String toString() {
        return com.google.common.base.Objects.toStringHelper(this)
                .add("timeSlice", timeSlice)
                .add("taskType", taskType)
                .add("segmentOffset", segmentOffset)
                .add("owner", owner)
                .add("finished", finished)
                .toString();
    }
}
