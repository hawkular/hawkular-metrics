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

import java.util.Map;

import rx.Completable;
import rx.Single;
import rx.functions.Func1;
import rx.functions.Func2;

/**
 * @author jsanda
 */
public interface Scheduler {

    /**
     * Schedules a job for execution. A unique id is created and is persisted along with the specified job details.
     * If the execution time has already passed, the job details will not be persisted, and the returned Single
     * will call subscribers onError method.
     *
     * @param type
     * @param name
     * @param parameters
     * @param trigger
     * @return A Single that emits the job details
     */
    Single<? extends JobDetails> scheduleJob(String type, String name, Map<String, String> parameters, Trigger trigger);

    /**
     * Deletes all the scheduled execution for a job id.
     *
     * @param jobId
     * @return Completable instance
     */
    Completable unscheduleJobById(String jobId);

    Completable unscheduleJobByTypeAndName(String jobType, String jobName);

    /**
     * Register a function that produces a job of the specified type. This method should be called prior to scheduling
     * any jobs of the specified type.
     *
     * @param jobType
     * @param jobProducer
     */
    void register(String jobType, Func1<JobDetails, Completable> jobProducer);

    /**
     * Registers two functions. The first produces a job of the specfied type. The second function returns a retry
     * policy that is used with non-repeating jobs when the fail.
     *
     * @param jobType
     * @param jobProducer
     * @param retryFunction
     */
    void register(String jobType, Func1<JobDetails, Completable> jobProducer,
            Func2<JobDetails, Throwable, RetryPolicy> retryFunction);

    /**
     * Start executing jobs.
     */
    void start();

    /**
     * Shut down thread pools and stop executing jobs. Jobs that are running may be interrupted and might not finish.
     */
    void shutdown();

}
