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

import java.util.Set;

import org.hawkular.metrics.tasks.api.Task;
import org.hawkular.metrics.tasks.api.TaskService;
import org.hawkular.metrics.tasks.api.TaskType;
import org.joda.time.DateTime;
import rx.Observable;
import rx.Subscription;
import rx.functions.Action1;

/**
 * @author jsanda
 */
public class FakeTaskService implements TaskService {

    @Override
    public void start() {
    }

    @Override
    public void shutdown() {
    }

    @Override
    public Observable<Task> scheduleTask(DateTime time, Task task) {
        return Observable.just(new Task() {
            @Override
            public TaskType getTaskType() {
                return task.getTaskType();
            }

            @Override
            public String getTenantId() {
                return task.getTenantId();
            }

            @Override
            public String getTarget() {
                return task.getTarget();
            }

            @Override
            public Set<String> getSources() {
                return task.getSources();
            }

            @Override
            public int getInterval() {
                return task.getInterval();
            }

            @Override
            public int getWindow() {
                return task.getWindow();
            }

            @Override
            public DateTime getTimeSlice() {
                return time;
            }
        });
    }

    @Override
    public Subscription subscribe(TaskType taskType, Action1<? super Task> onNext) {
        return null;
    }
}
