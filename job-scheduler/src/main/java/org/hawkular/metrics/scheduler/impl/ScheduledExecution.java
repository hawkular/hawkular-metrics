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

import java.util.Date;
import java.util.Objects;

import org.hawkular.metrics.scheduler.api.JobDetails;

import com.google.common.base.MoreObjects;

/**
 * @author jsanda
 */
class ScheduledExecution {

    private final Date timeSlice;
    private final JobDetailsImpl jobDetails;

    public ScheduledExecution(Date timeSlice, JobDetailsImpl jobDetails) {
        this.timeSlice = timeSlice;
        this.jobDetails = jobDetails;
    }

    public Date getTimeSlice() {
        return timeSlice;
    }

    public JobDetails getJobDetails() {
        return jobDetails;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ScheduledExecution that = (ScheduledExecution) o;
        return Objects.equals(timeSlice, that.timeSlice) &&
                Objects.equals(jobDetails, that.jobDetails);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timeSlice, jobDetails);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("timeSlice", timeSlice)
                .add("jobDetails", jobDetails)
                .toString();
    }
}
