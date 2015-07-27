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
import static org.hawkular.metrics.tasks.impl.TaskSchedulerImpl.getTriggerValue;
import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.ResultSet;
import org.hawkular.metrics.tasks.BaseTest;
import org.hawkular.metrics.tasks.api.SingleExecutionTrigger;
import org.hawkular.metrics.tasks.api.Task2;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import rx.Observable;
import rx.observers.TestSubscriber;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

/**
 * @author jsanda
 */
public class TaskSchedulerITest extends BaseITest {

    private TaskSchedulerImpl scheduler;

    private TestScheduler tickScheduler;

    private long startTime;

    private Observable<Lease> leaseObservable;

    @BeforeClass
    public void initClass() {
        startTime = System.currentTimeMillis();
        scheduler = new TaskSchedulerImpl(rxSession, queries);
        tickScheduler = Schedulers.test();
        tickScheduler.advanceTimeTo(startTime, TimeUnit.MILLISECONDS);
        scheduler.setTickScheduler(tickScheduler);
        leaseObservable = scheduler.start();
    }

    @Test
    public void executeSingleTask() {
        String group = "group-1";
        int order = 100;
        SingleExecutionTrigger trigger = new SingleExecutionTrigger(tickScheduler.now() + TimeUnit.SECONDS.toMillis(1));
        Date timeSlice = new Date(trigger.getTriggerTime());
        Task2Impl task = new Task2Impl(randomUUID(), group, order, "task1", emptyMap(), trigger);

        setUpTasksForExecution(timeSlice, task);

        TestSubscriber<Lease> leaseSubscriber = new TestSubscriber<>();
        leaseObservable.take(1).observeOn(Schedulers.immediate()).subscribe(leaseSubscriber);

        TaskSubscriber taskSubscriber = new TaskSubscriber();
        scheduler.subscribe(taskSubscriber);

        tickScheduler.advanceTimeBy(2, TimeUnit.SECONDS);

        leaseSubscriber.awaitTerminalEvent(5, TimeUnit.SECONDS);
        leaseSubscriber.assertTerminalEvent();
        taskSubscriber.assertValueCount(1);

        taskSubscriber.assertReceivedOnNext(singletonList(task));
    }

    @Test
    public void executeMultipleTasksFromSameGroup() {
        int numTasks = 10;
        String group = "group-1";
        SingleExecutionTrigger trigger = new SingleExecutionTrigger(tickScheduler.now() + TimeUnit.SECONDS.toMillis(1));

        Observable<Task2> tasks = Observable.range(1, numTasks).map(i -> new Task2Impl(randomUUID(), group, i * 10,
                "task-" + i, emptyMap(), trigger)).cache().cast(Task2.class);
        setUpTasksForExecution(tasks);

        TestSubscriber<Lease> leaseSubscriber = new TestSubscriber<>();
        leaseObservable.take(1).observeOn(Schedulers.immediate()).subscribe(leaseSubscriber);

        TaskSubscriber taskSubscriber = new TaskSubscriber();
        scheduler.subscribe(taskSubscriber);

        tickScheduler.advanceTimeBy(2, TimeUnit.SECONDS);
        leaseSubscriber.awaitTerminalEvent(5, TimeUnit.SECONDS);
        leaseSubscriber.assertTerminalEvent();
        taskSubscriber.assertValueCount(numTasks);

        assertEquals(taskSubscriber.getOnNextEvents().size(), numTasks);
        taskSubscriber.assertReceivedOnNext(tasks.toList().toBlocking().first());
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
        subscriber.awaitTerminalEvent(5, TimeUnit.SECONDS);
        subscriber.assertCompleted();
        subscriber.assertNoErrors();
    }

    private void setUpTasksForExecution(Observable<Task2> tasks) {
        Observable<ResultSet> resultSets = tasks.flatMap(t -> Observable.concat(insertIntoQueue(t), createLease(t)));
        TestSubscriber<ResultSet> subscriber = new TestSubscriber<>();
        resultSets.subscribe(subscriber);
        subscriber.awaitTerminalEvent(5, TimeUnit.SECONDS);
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

}
