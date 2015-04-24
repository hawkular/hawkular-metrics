/*
 *
 *  * Copyright 2014-2015 Red Hat, Inc. and/or its affiliates
 *  * and other contributors as indicated by the @author tags.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */
package org.hawkular.metrics.tasks;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;

/**
 * @author jsanda
 */
public class ExecutionResults {

    private DateTime timeSlice;

    private TaskType taskType;

    private int segment;

    private List<ExecutedTask> executedTasks = new ArrayList<>();

    public ExecutionResults(DateTime timeSlice, TaskType taskType, int segment) {
        this.timeSlice = timeSlice;
        this.taskType = taskType;
        this.segment = segment;
    }

    public DateTime getTimeSlice() {
        return timeSlice;
    }

    public TaskType getTaskType() {
        return taskType;
    }

    public int getSegment() {
        return segment;
    }

    public void add(ExecutedTask task) {
        executedTasks.add(task);
    }

    public List<ExecutedTask> getExecutedTasks() {
        return executedTasks;
    }

}
