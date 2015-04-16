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

import java.util.Objects;

import com.google.common.base.Supplier;

/**
 * @author jsanda
 */
public class TaskDef {

    private String name;

    private Supplier<Runnable> factory;

    private int segments;

    private int segmentOffsets;

    public String getName() {
        return name;
    }

    public TaskDef setName(String name) {
        this.name = name;
        return this;
    }

    public Supplier<Runnable> getFactory() {
        return factory;
    }

    public TaskDef setFactory(Supplier<Runnable> factory) {
        this.factory = factory;
        return this;
    }

    public int getSegments() {
        return segments;
    }

    public TaskDef setSegments(int segments) {
        this.segments = segments;
        return this;
    }

    public int getSegmentOffsets() {
        return segmentOffsets;
    }

    public TaskDef setSegmentOffsets(int segmentOffsets) {
        this.segmentOffsets = segmentOffsets;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TaskDef taskDef = (TaskDef) o;
        return Objects.equals(name, taskDef.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
