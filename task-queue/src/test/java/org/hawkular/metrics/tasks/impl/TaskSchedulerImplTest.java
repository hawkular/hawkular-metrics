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

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.UUID.randomUUID;
import static org.hawkular.metrics.tasks.impl.TaskSchedulerImpl.getTriggerValue;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.UDTValue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.hawkular.metrics.tasks.BaseTest;
import org.hawkular.metrics.tasks.api.RepeatingTrigger;
import org.hawkular.metrics.tasks.api.Task2;
import org.hawkular.metrics.tasks.api.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import rx.Observable;
import rx.observers.TestSubscriber;
import rx.schedulers.Schedulers;

/**
 * @author jsanda
 */
public class TaskSchedulerImplTest extends BaseTest {

    private static Logger logger = LoggerFactory.getLogger(TaskSchedulerImpl.class);

    private PreparedStatement findTasks;

    private PreparedStatement insertTask;

    private PreparedStatement findQueueEntries;

    private TaskSchedulerImpl scheduler;

    private Observable<Lease> leaseObservable;

    @BeforeClass
    public void initClass() {
        findTasks = session.prepare("select id, shard, name, params, trigger from tasks");
        insertTask = session.prepare("insert into tasks (id, shard, name, params, trigger) values (?, ?, ?, ?, ?)");
        findQueueEntries = session.prepare("select task_id from task_queue where time_slice = ? and shard = ?");

        scheduler = new TaskSchedulerImpl(rxSession, queries);
        leaseObservable = scheduler.start();
    }

//    @Test
    public void createTask() {
        TaskSchedulerImpl scheduler = new TaskSchedulerImpl(rxSession, queries);
        String taskName = "test";
        Map<String, String> params = ImmutableMap.of("x", "1");
        Trigger trigger = new RepeatingTrigger.Builder().withInterval(1, TimeUnit.SECONDS).build();

        Observable<Task2> observable = scheduler.createTask(taskName, params, trigger);
        TaskSubscriber subscriber = new TaskSubscriber();
        observable.subscribe(subscriber);

        subscriber.awaitTerminalEvent();
        subscriber.assertNoErrors();
        subscriber.assertReceivedOnNext(getTasksFromDB());
    }

//    @Test
    public void findTask() {
        TaskSchedulerImpl scheduler = new TaskSchedulerImpl(rxSession, queries);
        String taskName = "test";
        int shard = 10;
        Map<String, String> params = ImmutableMap.of("x", "1");
        Trigger trigger = new RepeatingTrigger.Builder()
                .withInterval(1, TimeUnit.SECONDS)
                .withDelay(5, TimeUnit.SECONDS)
                .build();
        Task2Impl task = new Task2Impl(randomUUID(), shard, taskName, params, trigger);

        insertTaskIntoDB(task);

        Observable<Task2> observable = scheduler.findTask(task.getId());
        TaskSubscriber subscriber = new TaskSubscriber();
        observable.subscribe(subscriber);

        subscriber.awaitTerminalEvent();
        subscriber.assertNoErrors();
        subscriber.assertReceivedOnNext(singletonList(task));
    }

//    @Test
    public void scheduleTask() {
        TaskSchedulerImpl scheduler = new TaskSchedulerImpl(rxSession, queries);
        String taskName = "test";
        Map<String, String> params = ImmutableMap.of("x", "1", "y", "2");
        Trigger trigger = new RepeatingTrigger.Builder().withInterval(1, TimeUnit.MINUTES).build();

        Observable<Task2> observable = scheduler.scheduleTask(taskName, params, trigger);
        TaskSubscriber subscriber = new TaskSubscriber();
        observable.subscribe(subscriber);

        subscriber.awaitTerminalEvent();
        subscriber.assertNoErrors();
        // verify that the task received is persisted in the database
        subscriber.assertReceivedOnNext(getTasksFromDB());

        Task2Impl task = (Task2Impl) subscriber.getOnNextEvents().get(0);
        long timeSlice = task.getTrigger().getTriggerTime();
        int shard = task.getShard();

        // verify that the queue has been updated
        assertEquals(getTaskIdsFromQueue(timeSlice, shard), singletonList(task.getId()),
                "The task ids for queue{timeSlice: " + timeSlice + ", shard: " + shard + "} do not match");

        // verify that the lease has been created
        assertEquals(getLeasesFromDB(timeSlice), singletonList(new Lease(timeSlice, shard)),
                "The leases for time slice [" + timeSlice + "] do not match");
    }

//    @Test
    public void scheduleAndExecuteTaskWithNoParams() throws Exception {
        String taskName = "SimpleTaskWithNoParams";
        Trigger trigger = new RepeatingTrigger.Builder()
                .withInterval(1, TimeUnit.SECONDS)
                .withDelay(2, TimeUnit.SECONDS)
                .build();

        TaskSubscriber executeSubscriber = new TaskSubscriber();
        scheduler.subscribe(executeSubscriber);

        Observable<Task2> scheduled = scheduler.scheduleTask(taskName, emptyMap(), trigger);
        TaskSubscriber scheduledSubscriber = new TaskSubscriber();
        scheduled.subscribe(scheduledSubscriber);

        scheduledSubscriber.awaitTerminalEvent();
        assertEquals(scheduledSubscriber.getOnNextEvents().size(), 1, "Expected to receive 1 task");

        TestSubscriber<Lease> leaseSubscriber = new TestSubscriber<>();
        leaseObservable.take(2).observeOn(Schedulers.immediate()).subscribe(leaseSubscriber);

        leaseSubscriber.awaitTerminalEvent(10, TimeUnit.SECONDS);
        leaseSubscriber.assertCompleted();

        Task2Impl scheduledTask = (Task2Impl) scheduledSubscriber.getOnNextEvents().get(0);
        UUID id = scheduledTask.getId();
        int shard = scheduledTask.getShard();

        assertEquals(executeSubscriber.getOnNextEvents().size(), 2, "Expected two task executions for two time " +
                "slices");

        List<Task2> expected = asList(
                new Task2Impl(id, shard, taskName, emptyMap(), trigger),
                new Task2Impl(id, shard, taskName, emptyMap(), trigger.nextTrigger())
        );
        List<Task2> actual = executeSubscriber.getOnNextEvents();

        assertEquals(actual, expected, "The tasks do not match");

        assertQueueDoesNotExist(trigger.getTriggerTime(), shard);
        assertQueueDoesNotExist(trigger.nextTrigger().getTriggerTime(), shard);

        assertLeasesDoNotExist(trigger.getTriggerTime());
        assertLeasesDoNotExist(trigger.nextTrigger().getTriggerTime());
    }

//    @Test
    public void scheduleAndExecuteTaskWithParams() throws Exception {
        String taskName = "SimpleTaskWithParams";
        Map<String, String> params = ImmutableMap.of("x", "1", "y", "2", "z", "3");
        Trigger trigger = new RepeatingTrigger.Builder()
                .withInterval(1, TimeUnit.SECONDS)
                .withDelay(2, TimeUnit.SECONDS)
                .build();

        TaskSubscriber executeSubscriber = new TaskSubscriber();
        scheduler.subscribe(executeSubscriber);

        Observable<Task2> scheduled = scheduler.scheduleTask(taskName, params, trigger);
        TaskSubscriber scheduledSubscriber = new TaskSubscriber();
        scheduled.subscribe(scheduledSubscriber);

        scheduledSubscriber.awaitTerminalEvent();
        assertEquals(scheduledSubscriber.getOnNextEvents().size(), 1, "Expected to receive 1 task");

        TestSubscriber<Lease> leaseSubscriber = new TestSubscriber<>();
        leaseObservable.take(2).observeOn(Schedulers.immediate()).subscribe(leaseSubscriber);

        leaseSubscriber.awaitTerminalEvent(10, TimeUnit.SECONDS);
        leaseSubscriber.assertCompleted();

        Task2Impl scheduledTask = (Task2Impl) scheduledSubscriber.getOnNextEvents().get(0);
        UUID id = scheduledTask.getId();
        int shard = scheduledTask.getShard();

        assertEquals(executeSubscriber.getOnNextEvents().size(), 2, "Expected two task executions for two time " +
                "slices");

        List<Task2> expected = asList(
                new Task2Impl(id, shard, taskName, params, trigger),
                new Task2Impl(id, shard, taskName, params, trigger.nextTrigger())
        );
        List<Task2> actual = executeSubscriber.getOnNextEvents();

        assertEquals(actual, expected, "The tasks do not match");

        assertQueueDoesNotExist(trigger.getTriggerTime(), shard);
        assertQueueDoesNotExist(trigger.nextTrigger().getTriggerTime(), shard);

        assertLeasesDoNotExist(trigger.getTriggerTime());
        assertLeasesDoNotExist(trigger.nextTrigger().getTriggerTime());
    }

    @Test
    public void executeMultipleTasksAcrossDifferentShards() throws Exception {
        Trigger trigger = new RepeatingTrigger.Builder()
                .withInterval(1, TimeUnit.SECONDS)
                .withDelay(2, TimeUnit.SECONDS)
                .build();
        UDTValue triggerUDT = getTriggerValue(rxSession, trigger);
        Date timeSlice = new Date(trigger.getTriggerTime());
        Task2Impl task1 = new Task2Impl(randomUUID(), 0, "task1", emptyMap(), trigger);
        Task2Impl task2 = new Task2Impl(randomUUID(), 1, "task2", emptyMap(), trigger);

        logger.debug("The first trigger is {}", trigger);

        TestSubscriber<ResultSet> resultSetSubscriber = new TestSubscriber<>();

        TaskSubscriber taskSubscriber = new TaskSubscriber();
        scheduler.subscribe(taskSubscriber);

        Observable<ResultSet> resultSets = Observable.merge(
                rxSession.execute(queries.insertIntoQueue.bind(timeSlice, task1.getShard(), task1.getId(),
                        task1.getName(), emptyMap(), triggerUDT)),
                rxSession.execute(queries.insertIntoQueue.bind(timeSlice, task2.getShard(), task2.getId(),
                        task2.getName(), emptyMap(), triggerUDT)),
                rxSession.execute(queries.createLease.bind(timeSlice, task1.getShard())),
                rxSession.execute(queries.createLease.bind(timeSlice, task2.getShard()))
        );

        resultSets.subscribe(resultSetSubscriber);
        resultSetSubscriber.awaitTerminalEvent(5, TimeUnit.SECONDS);
        resultSetSubscriber.assertCompleted();
        resultSetSubscriber.assertNoErrors();

        TestSubscriber<Lease> leaseSubscriber = new TestSubscriber<>();
        leaseObservable.take(4).observeOn(Schedulers.immediate()).subscribe(leaseSubscriber);

        leaseSubscriber.awaitTerminalEvent(10, TimeUnit.SECONDS);
        leaseSubscriber.assertCompleted();

        List<Lease> actualLeases = leaseSubscriber.getOnNextEvents();
        assertEquals(actualLeases.size(), 4, "Expected to receive four leases");

        List<Task2> onNextEvents = taskSubscriber.getOnNextEvents();
        assertTrue(taskSubscriber.getOnNextEvents().size() >= 4, "Expected to receive at least four task events but " +
                "received " + onNextEvents.size() + " events: " + onNextEvents);

        List<Task2> actual = onNextEvents.subList(0, 4);

        Set<Task2Impl> expectedValuesFor1stTrigger = ImmutableSet.of(task1, task2);
        Set<Task2Impl> expectedValuesFor2ndTrigger = ImmutableSet.of(
                new Task2Impl(task1.getId(), task1.getShard(), task1.getName(), task1.getParameters(),
                        trigger.nextTrigger()),
                new Task2Impl(task2.getId(), task2.getShard(), task2.getName(), task2.getParameters(),
                        trigger.nextTrigger())
        );

        // The scheduler guarantees that tasks are executed in order with respect to time
        // There are no guarantees though around execution order for tasks scheduled for
        // the same execution time. The first two events/tasks should have the first trigger
        // time, and the latter two tasks should have the next trigger time.
        assertEquals(ImmutableSet.copyOf(actual.subList(0, 2)), expectedValuesFor1stTrigger, "The tasks for the " +
            "first trigger " + trigger + " do not match expected values - ");
        assertEquals(ImmutableSet.copyOf(actual.subList(2, 4)), expectedValuesFor2ndTrigger, "The tasks for the " +
                "second trigger " + trigger.nextTrigger() + " do not match expected values - ");

        assertQueueDoesNotExist(trigger.getTriggerTime(), task1.getShard());
        assertQueueDoesNotExist(trigger.getTriggerTime(), task2.getShard());
        assertQueueDoesNotExist(trigger.nextTrigger().getTriggerTime(), task1.getShard());
        assertQueueDoesNotExist(trigger.nextTrigger().getTriggerTime(), task2.getShard());

        assertLeasesDoNotExist(trigger.getTriggerTime());
        assertLeasesDoNotExist(trigger.nextTrigger().getTriggerTime());
    }

//    @Test
    public void executeMulitpleTasksInSameShard() {
        int shard = 1;

        UUID task1Id = randomUUID();
        String task1Name = "task-1.1";

        UUID task2Id = randomUUID();
        String task2Name = "task-1.2";

        UUID task3Id = randomUUID();
        String task3Name = "task-1.3";

        UUID task4Id = randomUUID();
        String task4Name = "task-1.4";

        Trigger trigger = new RepeatingTrigger.Builder()
                .withInterval(1, TimeUnit.SECONDS)
                .withDelay(2, TimeUnit.SECONDS)
                .build();
        Date timeSlice = new Date(trigger.getTriggerTime());

        Map<String, String> params = emptyMap();
        UDTValue triggerValue = getTriggerValue(rxSession, trigger);

        session.execute(queries.insertIntoQueue.bind(timeSlice, shard, task1Id, task1Name, params, triggerValue));
        session.execute(queries.insertIntoQueue.bind(timeSlice, shard, task2Id, task2Name, params, triggerValue));
        session.execute(queries.insertIntoQueue.bind(timeSlice, shard, task3Id, task3Name, params, triggerValue));
        session.execute(queries.insertIntoQueue.bind(timeSlice, shard, task4Id, task4Name, params, triggerValue));
        session.execute(queries.createLease.bind(timeSlice, shard));

        session.execute(queries.insertIntoQueue.bind(timeSlice, 2, randomUUID(), "task-2.1", params, triggerValue));
        session.execute(queries.insertIntoQueue.bind(timeSlice, 2, randomUUID(), "task-2.2", params, triggerValue));
        session.execute(queries.insertIntoQueue.bind(timeSlice, 2, randomUUID(), "task-2.3", params, triggerValue));
        session.execute(queries.createLease.bind(timeSlice, 2));

        TaskSubscriber executeSubscriber = new TaskSubscriber();

        scheduler.subscribe(executeSubscriber);
//        scheduler.start();
        scheduler.start().subscribe(
                lease -> logger.debug("Finished processing {}", lease),
                t -> logger.warn("There was an error observing leases", t),
                () -> {
                    logger.debug("Finished observing leases");
                }
        );

        executeSubscriber.awaitTerminalEventAndUnsubscribeOnTimeout(15, TimeUnit.SECONDS);
        assertTrue(executeSubscriber.getOnNextEvents().size() > 1, "Expected at least two task executions but there " +
                "were " + executeSubscriber.getOnNextEvents().size());
    }

    private void assertLeasesDoNotExist(long time) {
        Date timeSlice = new Date(time);
        ResultSet resultSet = session.execute(queries.findLeases.bind(timeSlice));
        assertTrue(resultSet.isExhausted(), "Did not expect to find any leases for " + timeSlice + " but found " +
            + resultSet.all().size() + " lease(s)");
    }

    private void assertQueueDoesNotExist(long time, int shard) {
        Date timeSlice = new Date(time);
        ResultSet resultSet = session.execute(queries.getTasksFromQueue.bind(timeSlice, shard));
        assertTrue(resultSet.isExhausted(), "Found TaskQueue{timeSlice=" + time + ", shard=" + shard + "} but " +
            "did not expect it to exist");
    }

    private void assertTaskEquals(Task2 actual, Task2 expected) {
        assertEquals(actual.getId(), expected.getId(), "The task id does not match");
        assertEquals(actual.getName(), expected.getName(), "The task name does not match");
        assertEquals(actual.getParameters(), expected.getParameters(), "The parameters do not match");
        assertEquals(actual.getTrigger(), expected.getTrigger(), "The trigger does not match");
    }

    private List<Task2> getTasksFromDB() {
        ResultSet resultSet = session.execute(findTasks.bind());
        List<Task2> tasks = new ArrayList<>();
        for (Row row : resultSet) {
            tasks.add(new Task2Impl(row.getUUID(0), row.getInt(1), row.getString(2),
                    row.getMap(3, String.class, String.class), TaskSchedulerImpl.getTrigger(row.getUDTValue(4))));
        }
        return tasks;
    }

    private void insertTaskIntoDB(Task2Impl task) {
        session.execute(insertTask.bind(task.getId(), task.getShard(), task.getName(), task.getParameters(),
                getTriggerValue(rxSession, task.getTrigger())));
    }

    private List<UUID> getTaskIdsFromQueue(long timeSlice, int shard) {
        ResultSet resultSet = session.execute(findQueueEntries.bind(new Date(timeSlice), shard));
        List<UUID> taskIds = new ArrayList<>();
        for (Row row : resultSet) {
            taskIds.add(row.getUUID(0));
        }
        return taskIds;
    }

    private List<Lease> getLeasesFromDB(long timeSlice) {
        ResultSet resultSet = session.execute(queries.findLeases.bind(new Date(timeSlice)));
        List<Lease> leases = new ArrayList<>();
        for (Row row : resultSet) {
            leases.add(new Lease(timeSlice, row.getInt(0), row.getString(1), row.getBool(2)));
        }
        return leases;
    }

}
