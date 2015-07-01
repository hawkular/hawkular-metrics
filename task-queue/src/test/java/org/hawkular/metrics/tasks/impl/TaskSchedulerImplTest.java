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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.hawkular.metrics.tasks.BaseTest;
import org.hawkular.metrics.tasks.api.RepeatingTrigger;
import org.hawkular.metrics.tasks.api.Task2;
import org.testng.annotations.Test;
import rx.Observable;
import rx.observers.TestSubscriber;

/**
 * @author jsanda
 */
public class TaskSchedulerImplTest extends BaseTest {

    @Test
    public void createTask() {
        TaskSchedulerImpl scheduler = new TaskSchedulerImpl(rxSession, queries);
        String taskName = "test";
        Map<String, String> params = ImmutableMap.of("x", "1");

        Observable<Task2> observable = scheduler.createTask(taskName, params, new RepeatingTrigger(1000));
        TestSubscriber<Task2> subscriber = new TestSubscriber<>();
        observable.subscribe(subscriber);

        subscriber.awaitTerminalEvent();
        subscriber.assertNoErrors();

        assertEquals(subscriber.getOnNextEvents().size(), 1, "Expected the observable to emit one task");
        Task2 task = subscriber.getOnNextEvents().get(0);
        assertEquals(task.getName(), taskName, "The task name does not match");
        assertEquals(task.getParameters(), params, "The task parameters do not match");
        assertNotNull(task.getId(), "The task id should not be null");
    }

    @Test
    public void createAndFindTask() {
        TaskSchedulerImpl scheduler = new TaskSchedulerImpl(rxSession, queries);
        String taskName = "test";
        Map<String, String> params = ImmutableMap.of("x", "1");

        Observable<Task2> observable = scheduler.createTask(taskName, params, new RepeatingTrigger(1000));
        TestSubscriber<Task2> createSubscriber = new TestSubscriber<>();
        observable.subscribe(createSubscriber);

        createSubscriber.awaitTerminalEvent();
        createSubscriber.assertNoErrors();
        assertEquals(createSubscriber.getOnNextEvents().size(), 1,
                "Expected the create task observable to emit one event");

        Task2 expected = createSubscriber.getOnNextEvents().get(0);

        Observable<Task2> findObservable = scheduler.findTask(expected.getId());
        TestSubscriber<Task2> findSubscriber = new TestSubscriber<>();
        findObservable.subscribe(findSubscriber);

        findSubscriber.awaitTerminalEvent();
        findSubscriber.assertNoErrors();
        assertEquals(findSubscriber.getOnNextEvents().size(), 1, "Expected find observable to emit one event");

        Task2 actual = findSubscriber.getOnNextEvents().get(0);

        assertEquals(actual.getId(), expected.getId(), "The task id does not match");
        assertEquals(actual.getName(), expected.getName(), "The task name does not match");
        assertEquals(actual.getParameters(), expected.getParameters(), "The parameters do not match");
        assertEquals(actual.getTrigger(), expected.getTrigger(), "The trigger does not match");
    }

}
