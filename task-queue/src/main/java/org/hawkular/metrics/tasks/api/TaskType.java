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

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.hawkular.metrics.tasks.impl.TaskImpl;

/**
 * Tasks are grouped by type, and execution order is also determined by type. If there are types A, B, C, then for
 * a given time slice all tasks of type A are executed followed by tasks of type B and finally tasks of type C are
 * executed. The task type specifies the granularity of partitioning of tasks in the database. It also provides a
 * factory function that produces a function to execute tasks.
 *
 * @author jsanda
 */
public class TaskType {

    private String name;

    // TODO This should be an implementation detail of the taks service
    private int segments;

    // TODO This should be an implementation detail of the taks service
    private int segmentOffsets;

    private int interval;

    private int window;

    /**
     * The task type name which must be unique among task types.
     */
    public String getName() {
        return name;
    }

    public TaskType setName(String name) {
        this.name = name;
        return this;
    }

    /**
     * The number of partitions in the database to use for storing tasks. The segment is determined by,
     * <code>task.target.hashCode() % segments</code>
     */
    public int getSegments() {
        return segments;
    }

    public TaskType setSegments(int segments) {
        this.segments = segments;
        return this;
    }

    /**
     * Tasks are associated with leases. A client needs to acquire a lease before it can execute the tasks associated
     * with the lease. This property specifies how many tasks in terms of segments are associated with a lease. Let's
     * say that we have 100 segments and 10 segment offsets. This means that there are 10 task segments per lease.
     * When a client acquires a lease, the client will then execute the tasks in each of those segments.
     * <br><br>
     * TODO Come up with a more descriptive name
     */
    public int getSegmentOffsets() {
        return segmentOffsets;
    }

    public TaskType setSegmentOffsets(int segmentOffsets) {
        this.segmentOffsets = segmentOffsets;
        return this;
    }

    /**
     * The default frequency of execution for tasks of this type. The time unit is configured globally with
     * {@link TaskServiceBuilder#withTimeUnit(TimeUnit)}.
     */
    public int getInterval() {
        return interval;
    }

    public TaskType setInterval(int interval) {
        this.interval = interval;
        return this;
    }

    /**
     * The default amount of data to include for tasks of this type, expressed as a duration. The time unit is
     * configured globally with {@link TaskServiceBuilder#withTimeUnit(TimeUnit)}.
     */
    public int getWindow() {
        return window;
    }

    public TaskType setWindow(int window) {
        this.window = window;
        return this;
    }

    /**
     * A factory method for creating tasks of this type.
     *
     * @param target Identifier or key of the time series associated with the task
     * @param source Identifier or key of the time series that provides input data for the task
     *
     * @return A {@link Task}
     */
    public Task createTask(String tenantId, String target, String source) {
        return createTask(tenantId, target, source, interval, window);
    }

    /**
     * A factory method for creating tasks of this type. Note that the time units for <code>interval</code> and
     * <code>window</code> is configured globally with {@link TaskServiceBuilder#withTimeUnit(TimeUnit)}.
     *
     * @param target Identifier or key of the time series associated with the task
     * @param source Identifier or key of the time series that provides input data for the task
     * @param interval Specifies the frequency of execution
     * @param window Specifies the amount of data to include, expressed as a duration
     * @return A {@link Task}
     */
    public Task createTask(String tenantId, String target, String source, int interval, int window) {
        return new TaskImpl(this, tenantId, null, target, source, interval, window);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TaskType taskType = (TaskType) o;
        return Objects.equals(name, taskType.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
