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

import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.TreeSet;
import java.util.function.Consumer;

import org.hawkular.metrics.tasks.api.TaskType;
import org.hawkular.metrics.tasks.api.Task;
import org.joda.time.DateTime;
import org.joda.time.Duration;

/**
 * @author jsanda
 */
class TaskContainer implements Iterable<Task> {

    private TaskType taskType;

    private String target;

    private Set<String> sources;

    private Duration interval;

    private Duration window;

    private DateTime timeSlice;

    private int segment;

    private SortedSet<DateTime> failedTimeSlices = new TreeSet<>();

    /**
     * Creates a copy of the task container, excluding its {@link #getFailedTimeSlices() failedTimeSlices}.
     */
    public static TaskContainer copyWithoutFailures(TaskContainer container) {
        TaskContainer newContainer = new TaskContainer();
        newContainer.taskType = container.taskType;
        newContainer.timeSlice = container.timeSlice;
        newContainer.target = container.target;
        newContainer.sources = container.sources;
        newContainer.interval = container.interval;
        newContainer.window = container.window;
        newContainer.segment = container.segment;

        return container;
    }

    private TaskContainer() {
    }

    public TaskContainer(TaskType taskType, DateTime timeSlice, int segment, String target, Set<String> sources, int
            interval, int window, Set<DateTime> failedTimeSlices) {
        this.taskType = taskType;
        this.timeSlice = timeSlice;
        this.segment = segment;
        this.target = target;
        this.sources = sources;
        this.interval = Duration.standardMinutes(interval);
        this.window = Duration.standardMinutes(window);
        this.failedTimeSlices.addAll(failedTimeSlices);
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

    public SortedSet<DateTime> getFailedTimeSlices() {
        return failedTimeSlices;
    }

    public DateTime getTimeSlice() {
        return timeSlice;
    }

    public int getSegment() {
        return segment;
    }

    public Lease getLease() {
        return null;
    }

    @Override
    public Iterator<Task> iterator() {

        return new Iterator<Task>() {

            Iterator<DateTime> timeSliceIterator = getAllTimeSlices().iterator();

            public boolean hasNext() {
                return timeSliceIterator.hasNext();
            }

            @Override
            public Task next() {
                timeSlice = timeSliceIterator.next();
                return new TaskImpl(taskType, timeSlice, target, sources, interval, window);
            }
        };
    }

    @Override
    public void forEach(Consumer<? super Task> action) {
        getAllTimeSlices().forEach(time -> {
            timeSlice = time;
            action.accept(new TaskImpl(taskType, time, target, sources, interval, window));
        });
    }

    @Override
    public Spliterator<Task> spliterator() {
        return Spliterators.spliterator(iterator(), getAllTimeSlices().size(), Spliterator.ORDERED);
    }

    private Set<DateTime> getAllTimeSlices() {
        SortedSet<DateTime> set = new TreeSet<>();
        set.addAll(failedTimeSlices);
        set.add(timeSlice);

        return set;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TaskContainer task = (TaskContainer) o;
        return Objects.equals(taskType, task.taskType) &&
                Objects.equals(target, task.target) &&
                Objects.equals(sources, task.sources) &&
                Objects.equals(interval, task.interval) &&
                Objects.equals(window, task.window) &&
                Objects.equals(failedTimeSlices, task.failedTimeSlices) &&
                Objects.equals(timeSlice, task.timeSlice);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskType, target, sources, interval, window, failedTimeSlices,
                timeSlice);
    }

    @Override
    public String toString() {
        return com.google.common.base.Objects.toStringHelper(TaskContainer.class)
                .add("taskDef", taskType.getName())
                .add("target", target)
                .add("sources", sources)
                .add("interval", interval.toStandardMinutes())
                .add("window", window.toStandardMinutes())
                .add("failedTimeSlices", failedTimeSlices)
                .add("timeSlice", timeSlice)
                .add("segment", segment)
                .toString();
    }
}
