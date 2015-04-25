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
package org.hawkular.metrics.tasks;

import java.util.Objects;

import org.joda.time.DateTime;

/**
 * @author jsanda
 */
public class Lease {

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

    public DateTime getTimeSlice() {
        return timeSlice;
    }

    public String getTaskType() {
        return taskType;
    }

    public int getSegmentOffset() {
        return segmentOffset;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public boolean isFinished() {
        return finished;
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
