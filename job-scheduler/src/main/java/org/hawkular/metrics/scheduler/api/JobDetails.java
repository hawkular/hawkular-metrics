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
package org.hawkular.metrics.scheduler.api;

import java.util.UUID;

/**
 * Provides information about scheduled jobs.
 *
 * @author jsanda
 */
public interface JobDetails {

    /**
     * A unique identifier that the scheduler uses to query Cassandra for the job details
     */
    UUID getJobId();

    /**
     * Every job has a type. The scheduler uses the type to determine who is responsible for the job execution.
     * @see  Scheduler#register(String, rx.functions.Func1)
     * @see  Scheduler#register(String, rx.functions.Func1, rx.functions.Func2)
     */
    String getJobType();

    /**
     * Note that thee job name does not have to be unique.
     */
    String getJobName();

    /**
     * The job {@link JobParameters parameters} which are mutable
     */
    JobParameters getParameters();

    /**
     * The {@link Trigger trigger} specifies when the job will execute.
     */
    Trigger getTrigger();

    /**
     * This is primarily for internal use by the scheduler.
     */
    JobStatus getStatus();
}
