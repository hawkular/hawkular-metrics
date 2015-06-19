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

import java.util.Set;

import org.joda.time.DateTime;

/**
 * Represents a unit of work to be performed by a user supplied function. All tasks are considered repeating and are
 * rescheduled on fixed intervals.

 * @author jsanda
 */
public interface Task {


    TaskType getTaskType();

    String getTenantId();

    /**
     * This is a key or identifier of the time series that is associated with the task. Consider aggregating metrics or
     * events as an example. Let's say there is a task for computing a 5 minute rollup from raw
     * data. This property should be the key or identifier of the 5 minute rollup time series.
     */
    String getTarget();

    /**
     * The keys or identifiers of the time series associated with the source data being operated on. There can be one or
     * more sources. Consider aggregating metrics or events as an example. There is a task for computing a 5 minute
     * rollup from raw data. This property identifies the time series of the raw data to be aggregated.
     */
    Set<String> getSources();

    /**
     * The frequency of the task execution, e.g., five minutes.
     */
    int getInterval();

    /**
     * Specifies the amount of data to be included. For an aggregation task that runs every 5 minutes with a window of
     * 15 minutes, the task should query for data from the past 15 minutes using {@link #getTimeSlice() timeSlice} as
     * the end time.
     */
    int getWindow();

    /**
     * The end time of the time slice for which the task is scheduled. If a task has an interval of an hour, then the
     * time slice would be 9:00 - 10:00, 10:00 - 11:00, etc. The task is guaranteed to run no earlier than this time.
     */
    DateTime getTimeSlice();

}
