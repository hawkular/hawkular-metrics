/*
 * Copyright 2014-2016 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.scheduler.api;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import com.google.common.base.MoreObjects;

/**
 * @author jsanda
 */
public class JobDetails {

    private UUID jobId;

    private String jobType;

    private String jobName;

    private Map<String, String> parameters;

    private Trigger trigger;

    private JobStatus status;

    public JobDetails(UUID jobId, String jobType, String jobName, Map<String, String> parameters, Trigger trigger) {
        this.jobId = jobId;
        this.jobType = jobType;
        this.jobName = jobName;
        this.parameters = Collections.unmodifiableMap(parameters);
        this.trigger = trigger;
        status = JobStatus.NONE;
    }

    public JobDetails(UUID jobId, String jobType, String jobName, Map<String, String> parameters, Trigger trigger,
            JobStatus status) {
        this.jobId = jobId;
        this.jobType = jobType;
        this.jobName = jobName;
        this.parameters = Collections.unmodifiableMap(parameters);
        this.trigger = trigger;
        this.status = status;
    }

    public UUID getJobId() {
        return jobId;
    }

    public String getJobType() {
        return jobType;
    }

    public String getJobName() {
        return jobName;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    public Trigger getTrigger() {
        return trigger;
    }

    public JobStatus getStatus() {
        return status;
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JobDetails details = (JobDetails) o;
        return Objects.equals(jobId, details.jobId) &&
                Objects.equals(jobType, details.jobType) &&
                Objects.equals(jobName, details.jobName) &&
                Objects.equals(parameters, details.parameters) &&
                Objects.equals(trigger, details.trigger) &&
                Objects.equals(status, details.status);
    }

    @Override public int hashCode() {
        return Objects.hash(jobId, jobType, jobName, parameters, trigger, status);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("jobId", jobId)
                .add("jobType", jobType)
                .add("jobName", jobName)
                .add("parameters", parameters)
                .add("trigger", trigger)
                .add("status", status)
                .omitNullValues()
                .toString();
    }
}
