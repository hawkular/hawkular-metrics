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
package org.hawkular.metrics.tasks.impl;

import java.util.Objects;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.hawkular.metrics.tasks.api.Task;
import org.hawkular.metrics.tasks.api.TaskType;
import org.joda.time.DateTime;

/**
 * @author jsanda
 */
public class TaskImpl implements Task {

    private TaskType taskType;

    private String tenantId;

    private String target;

    private Set<String> sources;

    private int interval;

    private int window;

    private DateTime timeSlice;

    public TaskImpl(TaskType taskType, String tenantId, DateTime timeSlice, String target, String source, int interval,
            int window) {
        this.taskType = taskType;
        this.tenantId = tenantId;
        this.timeSlice = timeSlice;
        this.target = target;
        this.sources = ImmutableSet.of(source);
        this.interval = interval;
        this.window = window;
    }

    public TaskImpl(TaskType taskType, String tenantId, DateTime timeSlice, String target, Set<String> sources,
            int interval, int window) {
        this.taskType = taskType;
        this.tenantId = tenantId;
        this.timeSlice = timeSlice;
        this.target = target;
        this.sources = sources;
        this.interval = interval;
        this.window = window;
    }

    @Override
    public TaskType getTaskType() {
        return taskType;
    }

    @Override
    public String getTenantId() {
        return tenantId;
    }

    @Override
    public String getTarget() {
        return target;
    }

    @Override
    public Set<String> getSources() {
        return sources;
    }

    @Override
    public int getInterval() {
        return interval;
    }

    @Override
    public int getWindow() {
        return window;
    }

    @Override
    public DateTime getTimeSlice() {
        return timeSlice;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Task)) return false;
        Task that = (Task) o;
        return Objects.equals(taskType, that.getTaskType()) &&
                Objects.equals(tenantId, that.getTenantId()) &&
                Objects.equals(target, that.getTarget()) &&
                Objects.equals(sources, that.getSources()) &&
                Objects.equals(interval, that.getInterval()) &&
                Objects.equals(window, that.getWindow()) &&
                Objects.equals(timeSlice, that.getTimeSlice());
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskType, tenantId, target, sources, interval, window, timeSlice);
    }

    @Override
    public String toString() {
        return com.google.common.base.Objects.toStringHelper(TaskImpl.class)
                .add("taskType", taskType.getName())
                .add("tenantId", tenantId)
                .add("timeSlice", timeSlice)
                .add("target", target)
                .add("sources", sources)
                .add("interval", interval)
                .add("window", window)
                .toString();
    }
}
