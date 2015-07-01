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

import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;
import org.hawkular.metrics.tasks.api.RepeatingTrigger;
import org.hawkular.metrics.tasks.api.Task2;

/**
 * @author jsanda
 */
public class Task2Impl implements Task2 {

    private UUID id;

    private int shard;

    private String name;

    private ImmutableMap<String, String> parameters;

    private RepeatingTrigger trigger;

    public Task2Impl(UUID id, int shard, String name, Map<String, String> parameters, RepeatingTrigger trigger) {
        this.id = id;
        this.shard = shard;
        this.name = name;
        this.parameters = ImmutableMap.copyOf(parameters);
        this.trigger = trigger;
    }

    @Override
    public UUID getId() {
        return id;
    }

    public int getShard() {
        return shard;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Map<String, String> getParameters() {
        return parameters;
    }

    @Override
    public RepeatingTrigger getTrigger() {
        return trigger;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Task2Impl task2 = (Task2Impl) o;
        return Objects.equals(shard, task2.shard) &&
                Objects.equals(id, task2.id) &&
                Objects.equals(name, task2.name) &&
                Objects.equals(parameters, task2.parameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, shard, name, parameters);
    }

    @Override
    public String toString() {
        return "Task2Impl{" +
                "id=" + id +
                ", shard=" + shard +
                ", name='" + name + '\'' +
                ", parameters=" + parameters +
                '}';
    }
}
