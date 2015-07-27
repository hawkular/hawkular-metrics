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

import java.util.Map;
import java.util.UUID;

/**
 * @author jsanda
 */
public interface Task2 {

    /**
     * This is essentially a primary key that uniquely identifies a task.
     */
    UUID getId();

    /**
     * <p>
     * The group key provides a way to logically and physically group tasks. In conjunction with the
     * {@link #getOrder() priority} it can be used to control the order of execution of tasks which is desirable
     * when there are interdependencies between tasks. For example, suppose we have a task, T1, that aggregates data
     * from multiple time series to produce a new time series. Then we have a task, T2, that aggregates the data from
     * the time series produced by T2. T2 in effect depends on T1. They should use the same group key.
     * </p>
     * <p>
     * For Hawkular the group key will generally be the tenant ID. It is a good choice because tenants and all of their
     * data are isolated from one another.
     * </p>
     * <p>
     * In terms of implementation, all tasks with the same group key will be stored in the same queue shard, which
     * means that they will all be stored within the same physical partition. This should be taken into consideration
     * when choosing a group key because you do not want to wind up with queue shards/partitions that are excessively
     * large.
     * </p>
     */
    String getGroupKey();

    /**
     * Identifies the type of task. Multiple tasks can have the same name.
     */
    String getName();

    /**
     * Defines the order of execution of tasks within a group. Lower values are executed first. Note that this
     * ordering only applies to tasks within the same group, which is defined by the {@link #getGroupKey() group key}.
     */
    int getOrder();

    /**
     * An optional, arbitrary set of key/value parameters that the task receives upon execution.
     */
    Map<String, String> getParameters();

    /**
     * Defines the execution policy of the task, like when the task is scheduled to execute, how many times it should
     * execute, etc.
     */
    Trigger getTrigger();

}
