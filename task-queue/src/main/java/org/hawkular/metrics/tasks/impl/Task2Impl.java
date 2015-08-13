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

import org.hawkular.metrics.tasks.api.Task2;
import org.hawkular.metrics.tasks.api.Trigger;

import com.google.common.collect.ImmutableMap;

/**
 * @author jsanda
 */
public class Task2Impl implements Task2 {

    private UUID id;

    private String groupKey;

    private int order;

    private String name;

    private ImmutableMap<String, String> parameters;

    private Trigger trigger;

    public Task2Impl(UUID id, String groupKey, int order, String name, Map<String, String> parameters,
            Trigger trigger) {
        this.id = id;
        this.groupKey = groupKey;
        this.order = order;
        this.name = name;
        this.parameters = ImmutableMap.copyOf(parameters);
        this.trigger = trigger;
    }

    @Override
    public UUID getId() {
        return id;
    }

    @Override
    public String getGroupKey() {
        return groupKey;
    }

    @Override
    public int getOrder() {
        return order;
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
    public Trigger getTrigger() {
        return trigger;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Task2Impl task2 = (Task2Impl) o;
        return Objects.equals(order, task2.order) &&
                Objects.equals(id, task2.id) &&
                Objects.equals(groupKey, task2.groupKey) &&
                Objects.equals(name, task2.name) &&
                Objects.equals(parameters, task2.parameters) &&
                Objects.equals(trigger, task2.trigger);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, groupKey, order, name, parameters, trigger);
    }

    @Override
    public String toString() {
        return "Task2Impl{" +
                "id=" + id +
                ", groupKey='" + groupKey + '\'' +
                ", order=" + order +
                ", name='" + name + '\'' +
                ", parameters=" + parameters +
                ", trigger=" + trigger +
                '}';
    }
}
