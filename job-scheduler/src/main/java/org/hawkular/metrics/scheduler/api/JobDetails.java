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
import java.util.UUID;

/**
 * @author jsanda
 */
public class JobDetails {

    private UUID jobId;

    private String jobType;

    private String jobName;

    private Map<String, String> parameters;

    private Trigger trigger;

    public JobDetails(UUID jobId, String jobType, String jobName, Map<String, String> parameters, Trigger trigger) {
        this.jobId = jobId;
        this.jobType = jobType;
        this.jobName = jobName;
        this.parameters = Collections.unmodifiableMap(parameters);
        this.trigger = trigger;
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

    @Override public String toString() {
        return "JobDetails{" +
                "jobId=" + jobId +
                ", jobType='" + jobType + '\'' +
                ", jobName='" + jobName + '\'' +
                ", parameters=" + parameters +
                ", trigger=" + trigger +
                '}';
    }
}
