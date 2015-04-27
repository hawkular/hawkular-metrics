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
package org.hawkular.metrics.tasks;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * @author jsanda
 */
public class TaskType {

    private String name;

    private Supplier<Consumer<Task>> factory;

    private int segments;

    private int segmentOffsets;

    public String getName() {
        return name;
    }

    public TaskType setName(String name) {
        this.name = name;
        return this;
    }

    public Supplier<Consumer<Task>> getFactory() {
        return factory;
    }

    public TaskType setFactory(Supplier<Consumer<Task>> factory) {
        this.factory = factory;
        return this;
    }

    public int getSegments() {
        return segments;
    }

    public TaskType setSegments(int segments) {
        this.segments = segments;
        return this;
    }

    public int getSegmentOffsets() {
        return segmentOffsets;
    }

    public TaskType setSegmentOffsets(int segmentOffsets) {
        this.segmentOffsets = segmentOffsets;
        return this;
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
