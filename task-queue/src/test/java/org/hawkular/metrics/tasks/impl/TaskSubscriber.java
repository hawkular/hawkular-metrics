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

import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.hawkular.metrics.tasks.api.Task2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.functions.Action1;
import rx.observers.TestSubscriber;

/**
 * @author jsanda
 */
public class TaskSubscriber extends TestSubscriber<Task2> {

    private static Logger logger = LoggerFactory.getLogger(TaskSubscriber.class);

    private Random random = new Random();

    private int numberOfOnNextEvents;

    private CountDownLatch onNextEventsLatch = new CountDownLatch(0);

    private Action1<Task2> onNext = task -> {};

    public void setOnNext(Action1<Task2> onNext) {
        this.onNext = onNext;
    }

    @Override
    public void assertReceivedOnNext(List<Task2> tasks) {
        if (getOnNextEvents().size() != tasks.size()) {
            throw new AssertionError("Number of items does not match. Provided: " + tasks.size() + "  Actual: " +
                    getOnNextEvents().size());
        }

        for (int i = 0; i < tasks.size(); i++) {
            if (tasks.get(i) == null) {
                throw new AssertionError("tasks[" + i + "] is null");
            }
            assertTaskEquals(getOnNextEvents().get(i), tasks.get(i), i);
        }
    }

    private void assertTaskEquals(Task2 actual, Task2 expected, int index) {
//        String msg = "Task at index " + index
        if (actual == null) {
            throw new AssertionError("actual[" + index + "] is null");
        }
        boolean hasErrors = false;
        StringBuilder errors = new StringBuilder("The task at index [" + index + "] does not match the expected " +
                "value\n");
        if (!actual.getId().equals(expected.getId())) {
            hasErrors = true;
            errors.append("Expected tasks[" + index + "].id to be [" + expected.getId() + "] but was: [" +
                actual.getId() + "]\n");
        }
        if (!actual.getName().equals(expected.getName())) {
            hasErrors = true;
            errors.append("Expected tasks[" + index + "].name expected to be [" + expected.getName() + "] but was: [" +
                    actual.getName() + "]\n");
        }
        if (!actual.getParameters().equals(expected.getParameters())) {
            hasErrors = true;
            errors.append("Expected tasks[" + index + "].parameters expected to be [" + expected.getParameters() +
                    "] but was: [" + actual.getParameters() + "]\n");
        }
        if (!actual.getTrigger().equals(expected.getTrigger())) {
            hasErrors = true;
            errors.append("Expected tasks[" + index + "].trigger expected to be [" + expected.getTrigger() +
                    "] but was: [" + actual.getTrigger() + "]\n");
        }

        if (hasErrors) {
            throw new AssertionError(errors);
        }
    }

    public void awaitOnNextEvents(int count) {
        if (count < 1) {
            throw new IllegalArgumentException("count must be positive");
        }
        numberOfOnNextEvents = count;
        onNextEventsLatch = new CountDownLatch(1);
        try {
            onNextEventsLatch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted waiting for onNext events", e);
        } finally {
            unsubscribe();
        }
    }

    public void awaitOnNextEvents(int count, long timeout, TimeUnit unit) {
        if (count < 1) {
            throw new IllegalArgumentException("count must be positive");
        }
        if (getOnNextEvents().size() >= count) {
            unsubscribe();
        } else {
            numberOfOnNextEvents = count;
            onNextEventsLatch = new CountDownLatch(1);
            try {
                onNextEventsLatch.await(timeout, unit);
            } catch (InterruptedException e) {
                throw new RuntimeException("Interrupted waiting for onNext events", e);
            } finally {
                unsubscribe();
            }
        }
    }

    @Override
    public synchronized void onNext(Task2 task2) {
//        try {
//            long duration = Math.abs(random.nextLong()) % 5000;
//            logger.debug("Sleeping {} ms for execution of {}", duration, task2.getName());
//            Thread.sleep(duration);
//        } catch (InterruptedException e) {
//        }
//        super.onNext(task2);
//        if (numberOfOnNextEvents > 0 && getOnNextEvents().size() >= numberOfOnNextEvents) {
//            onNextEventsLatch.countDown();
//        }
        logger.debug("Executing {}", task2);
        onNext.call(task2);
        super.onNext(task2);
    }

}
