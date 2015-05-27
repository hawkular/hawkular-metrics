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
package org.hawkular.metrics.tasks.api;

import org.joda.time.DateTime;
import rx.Observable;

/**
 * The primary API for task scheduling and execution. See {@link TaskServiceBuilder} for details on creating and
 * configuring a TaskService. Note that there is currently only support for repeating tasks. Support will be added for
 * non-repeated, single execution tasks.
 *
 * @author jsanda
 */
public interface TaskService {
    /**
     * Starts the internal scheduler which takes care of running jobs to process the task queue. This method should be
     * called before any calls to {@link #scheduleTask(DateTime, Task)}.
     */
    void start();

    /**
     * Shuts down the internal scheduler.
     */
    void shutdown();

    /**
     * <p>
     * Schedules the task for repeated execution starting in the next time slice after <code>time</code>. If the task
     * scheduler runs jobs for executing tasks every minute and if <code>time</code> is 13:01:22, then the earliest
     * that the task can first get scheduled to execute is at 13:02 and every minute there after. Execution times are
     * determined by {@link Task#getInterval()}. If the task has an interval of 30 minutes, then it would be scheduled
     * to run at 13:30, 14:00, 14:30, and so on.
     * </p>
     * <p>
     * The task is guaranteed to execute at its  scheduled time or later, and only one instance of the task will be
     * executed at a time for a given time slice. In other words, if there are 5 machines running TaskService, only one
     * of those TaskService instances will execute the task for a particular time slice.
     * </p>
     * <p>
     * If task execution fails for whatever reason, it will be rescheduled for execution in the next time slice. If the
     * task runs at 13:30 and fails with some unhandled exception, then TaskService will run the task twice at 14:00.
     * It will first run the task for the 13:30 time slice and then for the 14:00 time slice.
     * </p>
     *
     * @param time The starting time from which the initial execution is scheduled. The scheduled execution time depends
     *             on {@link Task#getInterval()}. Tasks are scheduled to execute on fixed time boundaries. For example,
     *             if the task has an interval of 15 minutes, then it will get scheduled to run at 14:00, 14:15, 14:30,
     *             14:45, 15:00, and so on.
     * @param task The task to schedule for execution
     * @return The task with its {@link Task#getTimeSlice() scheduled execution time} set
     */
    Observable<Task> scheduleTask(DateTime time, Task task);
}
