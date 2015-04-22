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

import static org.joda.time.Minutes.minutes;

import java.util.Objects;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.joda.time.Duration;

/**
 * @author jsanda
 */
public class Task {

    private TaskType taskType;

    private String target;

    private Set<String> sources;

    private Duration interval;

    private Duration window;

    public Task() {
    }

    public Task(TaskType taskType, String target, Set<String> sources, int interval, int window) {
        this.taskType = taskType;
        this.target = target;
        this.sources = sources;
        this.interval = minutes(interval).toStandardDuration();
        this.window = minutes(window).toStandardDuration();
    }

    public Task(TaskType taskType, String target, String source, int interval, int window) {
        this.taskType = taskType;
        this.target = target;
        this.sources = ImmutableSet.of(source);
        this.interval = minutes(interval).toStandardDuration();
        this.window = minutes(window).toStandardDuration();
    }

    public TaskType getTaskType() {
        return taskType;
    }

    public String getTarget() {
        return target;
    }

    public Set<String> getSources() {
        return sources;
    }

    public Duration getInterval() {
        return interval;
    }

    public Duration getWindow() {
        return window;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Task task = (Task) o;
        return Objects.equals(taskType, task.taskType) &&
                Objects.equals(target, task.target) &&
                Objects.equals(sources, task.sources) &&
                Objects.equals(interval, task.interval) &&
                Objects.equals(window, task.window);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskType, target, sources, interval, window);
    }

    @Override
    public String toString() {
        return com.google.common.base.Objects.toStringHelper(Task.class)
                .add("taskDef", taskType.getName())
                .add("target", target)
                .add("sources", sources)
                .add("interval", interval.toStandardMinutes())
                .add("window", window.toStandardMinutes())
                .toString();
    }
}
