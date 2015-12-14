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

import org.hawkular.metrics.tasks.impl.Lease;

import rx.Observable;
import rx.Subscriber;
import rx.Subscription;
import rx.functions.Action1;

/**
 * @author jsanda
 */
public interface TaskScheduler {

    Subscription subscribe(Action1<Task2> onNext);

    Subscription subscribe(Subscriber<Task2> subscriber);

    Observable<Task2> getTasks();

    Observable<Lease> start();

//    Observable<Task2> createTask(String name, Map<String, String> parameters, Trigger trigger);

    Observable<Task2> scheduleTask(String name, String groupKey, int executionOrder, Map<String, String> parameters,
            Trigger trigger);

    void shutdown();

    Observable<Long> getFinishedTimeSlices();

    boolean isRunning();

}
