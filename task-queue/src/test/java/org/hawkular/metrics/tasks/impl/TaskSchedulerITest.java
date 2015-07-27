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

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.hawkular.metrics.tasks.impl.TaskSchedulerImpl.getTriggerValue;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.datastax.driver.core.ResultSet;
import org.hawkular.metrics.tasks.BaseTest;
import org.hawkular.metrics.tasks.api.SingleExecutionTrigger;
import org.hawkular.metrics.tasks.api.Task2;
import org.hawkular.metrics.tasks.api.Trigger;
import org.hawkular.rx.cassandra.driver.RxSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import rx.Observable;
import rx.observers.TestSubscriber;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

/**
 * @author jsanda
 */
public class TaskSchedulerITest extends BaseITest {

    private static Logger logger = LoggerFactory.getLogger(TaskSchedulerTest.class);

    private TestTaskScheduler scheduler;

    private TestScheduler tickScheduler;

    private long startTime;

    private Observable<Lease> leaseObservable;

    private Observable<Long> finishedTimeSlices;

    private class TestTaskScheduler extends TaskSchedulerImpl {

        private Function<String, Integer> defaultComputeShard = super::computeShard;

        private Function<String, Integer> computeShard = super::computeShard;

        public TestTaskScheduler(RxSession session, Queries queries) {
            super(session, queries);
            computeShard = defaultComputeShard;
        }

        public void setComputeShardFn(Function<String, Integer> computeShard) {
            this.computeShard = computeShard;
        }

        public void resetComputeShardFn() {
            computeShard = defaultComputeShard;
        }

        @Override
        int computeShard(String key) {
            return computeShard.apply(key);
        }
    }

    @BeforeClass
    public void initClass() {
        startTime = System.currentTimeMillis();
        scheduler = new TestTaskScheduler(rxSession, queries);
        tickScheduler = Schedulers.test();
        tickScheduler.advanceTimeTo(startTime, TimeUnit.MILLISECONDS);
        scheduler.setTickScheduler(tickScheduler);
        finishedTimeSlices = scheduler.getFinishedTimeSlices();
        leaseObservable = scheduler.start();
    }

    @BeforeMethod
    public void initMethod() {
        scheduler.resetComputeShardFn();
    }

    /**
     * This test exercises the simple scenario of a task that only executes once. No other tasks are scheduled for
     * execution during this time.
     */
    @Test
    public void executeSingleTask() {
        String group = "group-1";
        int order = 100;
        SingleExecutionTrigger trigger = new SingleExecutionTrigger(tickScheduler.now() + SECONDS.toMillis(1));
        Date timeSlice = new Date(trigger.getTriggerTime());
        Task2Impl task = new Task2Impl(randomUUID(), group, order, "task1", emptyMap(), trigger);

        setUpTasksForExecution(timeSlice, task);

        TestSubscriber<Long> timeSlicesSubscriber = new TestSubscriber<>();
        finishedTimeSlices.take(1).observeOn(Schedulers.immediate()).subscribe(timeSlicesSubscriber);

        TaskSubscriber taskSubscriber = new TaskSubscriber();
        scheduler.subscribe(taskSubscriber);

        tickScheduler.advanceTimeBy(1, SECONDS);

        timeSlicesSubscriber.awaitTerminalEvent(5, SECONDS);
        timeSlicesSubscriber.assertNoErrors();
        timeSlicesSubscriber.assertTerminalEvent();
        taskSubscriber.assertValueCount(1);

        taskSubscriber.assertReceivedOnNext(singletonList(task));

        // verify that the lease and queue have been deleted
        assertLeasesDoNotExist(trigger.getTriggerTime());
        assertQueueDoesNotExist(trigger.getTriggerTime(), group);
    }

    /**
     * In this tests all tasks belong to the same group, and each task defines a unique ordering. We need to verify
     * that the execution honors that ordering. A {@link SingleExecutionTrigger single execution trigger} is used for
     * all of the tasks, which means that each task should only be emitted once. The tasks should not be rescheduled.
     */
    @Test
    public void executeMultipleTasksFromSameGroup() {
        int numTasks = 10;
        String group = "group-1";
        SingleExecutionTrigger trigger = new SingleExecutionTrigger(tickScheduler.now() + SECONDS.toMillis(1));

        Observable<Task2> tasks = createTasks(numTasks, group, trigger).cache();
        setUpTasksForExecution(tasks);

        TestSubscriber<Long> timeSlicesSubscriber = new TestSubscriber<>();
        finishedTimeSlices.take(1).observeOn(Schedulers.immediate()).subscribe(timeSlicesSubscriber);

        TaskSubscriber taskSubscriber = new TaskSubscriber();
        scheduler.subscribe(taskSubscriber);

        tickScheduler.advanceTimeBy(1, SECONDS);
        timeSlicesSubscriber.awaitTerminalEvent(5, SECONDS);
        timeSlicesSubscriber.assertNoErrors();
        timeSlicesSubscriber.assertCompleted();

        taskSubscriber.assertValueCount(numTasks);
        taskSubscriber.assertReceivedOnNext(tasks.toList().toBlocking().first());


        // verify that the lease and queue have been deleted
        assertLeasesDoNotExist(trigger.getTriggerTime());
        assertQueueDoesNotExist(trigger.getTriggerTime(), group);
    }

    /**
     * In this test there are two groups. Tasks define a unique ordering in each group. We need to verify that the
     * execution honors that ordering within each group. Each task should only be emitted once since a
     * {@link SingleExecutionTrigger} is used. Tasks should not be rescheduled.
     */
    @Test
    public void executeMultipleTasksFromMultipleGroupsInDifferentQueues() {
        int numTasks = 10;
        final String group1 = "group-one";
        final String group2 = "group-two";
        SingleExecutionTrigger trigger = new SingleExecutionTrigger(tickScheduler.now() + SECONDS.toMillis(1));

        scheduler.setComputeShardFn(group -> {
            switch (group) {
                case group1:
                    return 1;
                case group2:
                    return 2;
                default:
                    throw new IllegalArgumentException(group + " is not a recognized group key");
            }
        });

        Observable<Task2> group1Tasks = createTasks(numTasks, group1, trigger).cache();
        Observable<Task2> group2Tasks = createTasks(numTasks, group2, trigger).cache();

        setUpTasksForExecution(group1Tasks.concatWith(group2Tasks));

        TestSubscriber<Long> timeSlicesSubscriber = new TestSubscriber<>();
        finishedTimeSlices.takeUntil(time -> time > trigger.getTriggerTime() + SECONDS.toMillis(1))
                .observeOn(Schedulers.immediate())
                .subscribe(timeSlicesSubscriber);

        TaskSubscriber taskSubscriber = new TaskSubscriber();
        scheduler.subscribe(taskSubscriber);

        tickScheduler.advanceTimeBy(3, SECONDS);
        timeSlicesSubscriber.awaitTerminalEvent(5, SECONDS);
        timeSlicesSubscriber.assertNoErrors();
        timeSlicesSubscriber.assertCompleted();

        taskSubscriber.assertValueCount(numTasks * 2);
        List<Task2> actualGroup1Tasks = taskSubscriber.getOnNextEvents().stream()
                .filter(t -> t.getGroupKey().equals(group1)).collect(toList());
        List<Task2> actualGroup2Tasks = taskSubscriber.getOnNextEvents().stream()
                .filter(t -> t.getGroupKey().equals(group2)).collect(toList());

        assertEquals(actualGroup1Tasks, group1Tasks.toList().toBlocking().first(), group1 + " tasks do not match");
        assertEquals(actualGroup2Tasks, group2Tasks.toList().toBlocking().first(), group2 + " tasks do not match");

        // verify that the leases and queues have been deleted
        assertLeasesDoNotExist(trigger.getTriggerTime());
        assertQueueDoesNotExist(trigger.getTriggerTime(), group1);
        assertQueueDoesNotExist(trigger.getTriggerTime(), group2);
    }

    @Test
    public void executeMultipleTasksFromMultipleGroupsInSameQueue() {
        int numTasks = 10;
        final String group1 = "group-one";
        final String group2 = "group-two";
        final int shard = 1;
        SingleExecutionTrigger trigger = new SingleExecutionTrigger(tickScheduler.now() + SECONDS.toMillis(1));

        scheduler.setComputeShardFn(group -> shard);

        Observable<Task2> group1Tasks = createTasks(numTasks, group1, trigger).cache();
        Observable<Task2> group2Tasks = createTasks(numTasks, group2, trigger).cache();

        setUpTasksForExecution(group1Tasks.concatWith(group2Tasks));

        TestSubscriber<Long> timeSlicesSubscriber = new TestSubscriber<>();
        finishedTimeSlices.takeUntil(time -> time > trigger.getTriggerTime() + SECONDS.toMillis(1))
                .observeOn(Schedulers.immediate())
                .subscribe(timeSlicesSubscriber);

        TaskSubscriber taskSubscriber = new TaskSubscriber();
        scheduler.subscribe(taskSubscriber);

        tickScheduler.advanceTimeBy(3, SECONDS);
        timeSlicesSubscriber.awaitTerminalEvent(5, SECONDS);
        timeSlicesSubscriber.assertNoErrors();
        timeSlicesSubscriber.assertCompleted();

        taskSubscriber.assertValueCount(numTasks * 2);
        List<Task2> actualGroup1Tasks = taskSubscriber.getOnNextEvents().stream()
                .filter(t -> t.getGroupKey().equals(group1)).collect(toList());
        List<Task2> actualGroup2Tasks = taskSubscriber.getOnNextEvents().stream()
                .filter(t -> t.getGroupKey().equals(group2)).collect(toList());

        assertEquals(actualGroup1Tasks, group1Tasks.toList().toBlocking().first(), group1 + " tasks do not match");
        assertEquals(actualGroup2Tasks, group2Tasks.toList().toBlocking().first(), group2 + " tasks do not match");

        // verify that the leases and queues have been deleted
        assertLeasesDoNotExist(trigger.getTriggerTime());
        assertQueueDoesNotExist(trigger.getTriggerTime(), group1);
        assertQueueDoesNotExist(trigger.getTriggerTime(), group2);
    }

    private Observable<Task2> createTasks(int count, String group, Trigger trigger) {
        return Observable.range(1, count).map(i -> new Task2Impl(randomUUID(), group, i * 10, "task-" + i, emptyMap(),
                trigger));
    }

    /**
     * Inserts the tasks into the time slice queue and creates the leases. The method then
     * blocks until all writes have completed and asserts that there are no errors.
     *
     * @param timeSlice The time slice for which tasks and leases will be created
     * @param tasks The tasks to create
     */
    private void setUpTasksForExecution(Date timeSlice, Task2Impl... tasks) {
        List<Observable<ResultSet>> resultSets = new ArrayList<>();
        for (Task2Impl t : tasks) {
            int shard = scheduler.computeShard(t.getGroupKey());
            resultSets.add(rxSession.execute(queries.insertIntoQueue.bind(timeSlice, shard, t.getId(), t.getGroupKey(),
                    t.getOrder(), t.getName(), t.getParameters(), getTriggerValue(rxSession, t.getTrigger()))));
            resultSets.add(rxSession.execute(queries.createLease.bind(timeSlice, shard)));
        }
        TestSubscriber<ResultSet> subscriber = new TestSubscriber<>();
        Observable.merge(resultSets).subscribe(subscriber);
        subscriber.awaitTerminalEvent(5, SECONDS);
        subscriber.assertCompleted();
        subscriber.assertNoErrors();
    }

    private void setUpTasksForExecution(Observable<Task2> tasks) {
        Observable<ResultSet> resultSets = tasks.flatMap(t -> Observable.concat(insertIntoQueue(t), createLease(t)));
        TestSubscriber<ResultSet> subscriber = new TestSubscriber<>();
        resultSets.subscribe(subscriber);
        subscriber.awaitTerminalEvent(5, SECONDS);
        subscriber.assertNoErrors();
        subscriber.assertCompleted();
    }

    private Observable<ResultSet> insertIntoQueue(Task2 task) {
        return rxSession.execute(queries.insertIntoQueue.bind(
                new Date(task.getTrigger().getTriggerTime()),
                scheduler.computeShard(task.getGroupKey()),
                task.getId(),
                task.getGroupKey(),
                task.getOrder(),
                task.getName(),
                task.getParameters(),
                getTriggerValue(rxSession, task.getTrigger())));
    }

    private Observable<ResultSet> createLease(Task2 task) {
        return rxSession.execute(queries.createLease.bind(new Date(task.getTrigger().getTriggerTime()),
                scheduler.computeShard(task.getGroupKey())));
    }

    private void assertLeasesDoNotExist(long time) {
        Date timeSlice = new Date(time);
        ResultSet resultSet = session.execute(queries.findLeases.bind(timeSlice));
        assertTrue(resultSet.isExhausted(), "Did not expect to find any leases for " + timeSlice + " but found " +
                + resultSet.all().size() + " lease(s)");
    }

    private void assertQueueDoesNotExist(long time, String groupKey) {
        Date timeSlice = new Date(time);
        int shard = scheduler.computeShard(groupKey);
        ResultSet resultSet = session.execute(queries.getTasksFromQueue.bind(timeSlice, shard));
        assertTrue(resultSet.isExhausted(), "Found TaskQueue{timeSlice=" + time + ", shard=" + shard + "} but " +
                "did not expect it to exist");
    }

}
