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
package org.hawkular.metrics.core.impl;

import org.hawkular.metrics.tasks.api.TaskType;

/**
 * <p>
 * Computing rates is the first and only scheduled task that we support at the moment. Task types serve a couple
 * purposes. They provide some configuration for tasks, like the execution interval. They also determine the order of
 * execution of tasks. This class provides some defaults.
 * </p>
 * <p>
 * The way in which tasks are configured, created, etc. is very like to change as the task schedule service undergoes
 * continued refactored as it gets used more.
 * </p>
 */
public class TaskTypes {

    private TaskTypes() {
    }

    public static TaskType COMPUTE_RATE = new TaskType()
            .setName("counter-rate")
            .setSegments(10)
            .setSegmentOffsets(10)
            .setInterval(5)
            .setWindow(5);

}
