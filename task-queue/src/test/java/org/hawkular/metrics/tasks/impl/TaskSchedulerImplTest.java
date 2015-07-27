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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
import org.testng.annotations.BeforeMethod;
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

    private AtomicInteger ids = new AtomicInteger();

    @BeforeClass
    public void initClass() {
        findTasks = session.prepare("select id, group_key, exec_order, name, params, trigger from tasks");
        insertTask = session.prepare(
                "insert into tasks (id, group_key, exec_order, name, params, trigger) " +
                "values (?, ?, ?, ?, ?, ?)");
        findQueueEntries = session.prepare("select task_id from task_queue where time_slice = ? and shard = ?");

        scheduler = new TaskSchedulerImpl(rxSession, queries);
        leaseObservable = scheduler.start();
    }

    @BeforeMethod
    public void beforeTestMethod(Method method) {
        logger.debug("Preparing to execute {}", method.getName());
    }

//    @Test
    public void findTask() {
        TaskSchedulerImpl scheduler = new TaskSchedulerImpl(rxSession, queries);
        String group = "test-group";
        int order = 100;
        String taskName = "test";
        Map<String, String> params = ImmutableMap.of("x", "1");
        Trigger trigger = new RepeatingTrigger.Builder()
                .withInterval(1, TimeUnit.SECONDS)
                .withDelay(5, TimeUnit.SECONDS)
                .build();
        Task2Impl task = new Task2Impl(randomUUID(), group, order, taskName, params, trigger);

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
        String group = "test-group";
        int order = 100;
        Map<String, String> params = ImmutableMap.of("x", "1", "y", "2");
        Trigger trigger = new RepeatingTrigger.Builder().withInterval(1, TimeUnit.MINUTES).build();

        Observable<Task2> observable = scheduler.scheduleTask(taskName, group, order, params, trigger);
        TaskSubscriber subscriber = new TaskSubscriber();
        observable.subscribe(subscriber);

        subscriber.awaitTerminalEvent();
        subscriber.assertNoErrors();
        // verify that the task received is persisted in the database
        subscriber.assertReceivedOnNext(getTasksFromDB());

        Task2Impl task = (Task2Impl) subscriber.getOnNextEvents().get(0);
        long timeSlice = task.getTrigger().getTriggerTime();
        int shard = scheduler.computeShard(task.getGroupKey());

        // verify that the queue has been updated
        assertEquals(getTaskIdsFromQueue(timeSlice, shard), singletonList(task.getId()),
                "The task ids for queue{timeSlice: " + timeSlice + ", shard: " + shard + "} do not match");

        // verify that the lease has been created
        assertEquals(getLeasesFromDB(timeSlice), singletonList(new Lease(timeSlice, shard)),
                "The leases for time slice [" + timeSlice + "] do not match");
    }

    @Test
    public void scheduleAndExecuteTaskWithNoParams() throws Exception {
        String group = "test-group";
        int order = 100;
        String taskName = "SimpleTaskWithNoParams";
        Trigger trigger = new RepeatingTrigger.Builder()
                .withInterval(1, TimeUnit.SECONDS)
                .withDelay(2, TimeUnit.SECONDS)
                .build();

        TaskSubscriber executeSubscriber = new TaskSubscriber();
        scheduler.subscribe(executeSubscriber);

        Observable<Task2> scheduled = scheduler.scheduleTask(taskName, group, order, emptyMap(), trigger);
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
        int shard = scheduler.computeShard(group);

        assertEquals(executeSubscriber.getOnNextEvents().size(), 2, "Expected two task executions for two time " +
                "slices");

        List<Task2> expected = asList(
                new Task2Impl(id, group, order, taskName, emptyMap(), trigger),
                new Task2Impl(id, group, order, taskName, emptyMap(), trigger.nextTrigger())
        );
        List<Task2> actual = executeSubscriber.getOnNextEvents();

        assertEquals(actual, expected, "The tasks do not match");

        assertQueueDoesNotExist(trigger.getTriggerTime(), shard);
        assertQueueDoesNotExist(trigger.nextTrigger().getTriggerTime(), shard);

        assertLeasesDoNotExist(trigger.getTriggerTime());
        assertLeasesDoNotExist(trigger.nextTrigger().getTriggerTime());
    }

    @Test
    public void scheduleAndExecuteTaskWithParams() throws Exception {
        String group = "test-group";
        int order = 100;
        String taskName = "SimpleTaskWithParams";
        Map<String, String> params = ImmutableMap.of("x", "1", "y", "2", "z", "3");
        Trigger trigger = new RepeatingTrigger.Builder()
                .withInterval(1, TimeUnit.SECONDS)
                .withDelay(2, TimeUnit.SECONDS)
                .build();

        TaskSubscriber executeSubscriber = new TaskSubscriber();
        scheduler.subscribe(executeSubscriber);

        Observable<Task2> scheduled = scheduler.scheduleTask(taskName, group, order, params, trigger);
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
        int shard = scheduler.computeShard(group);

        assertEquals(executeSubscriber.getOnNextEvents().size(), 2, "Expected two task executions for two time " +
                "slices");

        List<Task2> expected = asList(
                new Task2Impl(id, group, order, taskName, params, trigger),
                new Task2Impl(id, group, order, taskName, params, trigger.nextTrigger())
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
        String group1 = "group-1";
        String group2 = "group-2";
        int order = 100;
        Trigger trigger = new RepeatingTrigger.Builder()
                .withInterval(1, TimeUnit.SECONDS)
                .withDelay(2, TimeUnit.SECONDS)
                .build();
        Date timeSlice = new Date(trigger.getTriggerTime());
        Task2Impl task1 = new Task2Impl(randomUUID(), group1, order, "task1", emptyMap(), trigger);
        Task2Impl task2 = new Task2Impl(randomUUID(), group2, order, "task2", emptyMap(), trigger);

        TaskSubscriber taskSubscriber = new TaskSubscriber();
        scheduler.subscribe(taskSubscriber);

        setUpTasksForExecution(timeSlice, task1, task2);

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
                new Task2Impl(task1.getId(), group1, order, task1.getName(), task1.getParameters(),
                        trigger.nextTrigger()),
                new Task2Impl(task2.getId(), group2, order, task2.getName(), task2.getParameters(),
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

        assertQueueDoesNotExist(trigger.getTriggerTime(), scheduler.computeShard(task1.getGroupKey()));
        assertQueueDoesNotExist(trigger.getTriggerTime(), scheduler.computeShard(task2.getGroupKey()));
        assertQueueDoesNotExist(trigger.nextTrigger().getTriggerTime(), scheduler.computeShard(task1.getGroupKey()));
        assertQueueDoesNotExist(trigger.nextTrigger().getTriggerTime(), scheduler.computeShard(task2.getGroupKey()));

        assertLeasesDoNotExist(trigger.getTriggerTime());
        assertLeasesDoNotExist(trigger.nextTrigger().getTriggerTime());
    }

    @Test
    public void executeTaskThatThrowsException() {
        String group1 = "group-1";
        String group2 = "group-2";
        int order = 100;
        Trigger trigger = new RepeatingTrigger.Builder()
                .withInterval(1, TimeUnit.SECONDS)
                .withDelay(2, TimeUnit.SECONDS)
                .build();
        Date timeSlice = new Date(trigger.getTriggerTime());
        Task2Impl task1 = new Task2Impl(randomUUID(), group1, order, "task1", emptyMap(), trigger);
        Task2Impl task2 = new Task2Impl(randomUUID(), group2, order, "task2", emptyMap(), trigger);

        TaskSubscriber taskSubscriber = new TaskSubscriber();
        taskSubscriber.setOnNext(task -> {
            if (task.getName().equals(task1.getName()) && task.getTrigger().equals(trigger)) {
                logger.debug("Failing execution of {}", task);
                throw new RuntimeException("Execution of " + task + " failed!");
            }

        });
        scheduler.subscribe(taskSubscriber);

        setUpTasksForExecution(timeSlice, task1, task2);

        TestSubscriber<Lease> leaseSubscriber = new TestSubscriber<>();
        leaseObservable.take(4).observeOn(Schedulers.immediate()).subscribe(leaseSubscriber);

        leaseSubscriber.awaitTerminalEvent(10, TimeUnit.SECONDS);
        leaseSubscriber.assertCompleted();

        List<Lease> actualLeases = leaseSubscriber.getOnNextEvents();
        assertEquals(actualLeases.size(), 4, "Expected to receive four leases");

        List<Task2> onNextEvents = taskSubscriber.getOnNextEvents();
        assertTrue(taskSubscriber.getOnNextEvents().size() >= 3, "Expected to receive at least three task events but "
                + "received " + onNextEvents.size() + " events: " + onNextEvents);

        List<Task2> actual = onNextEvents.subList(0, 3);

        Set<Task2Impl> expectedValuesFor1stTrigger = ImmutableSet.of(task2);
        Set<Task2Impl> expectedValuesFor2ndTrigger = ImmutableSet.of(
                new Task2Impl(task1.getId(), task1.getGroupKey(), task1.getOrder(), task1.getName(),
                        task1.getParameters(), trigger.nextTrigger()),
                new Task2Impl(task2.getId(), task2.getGroupKey(), task2.getOrder(), task2.getName(),
                        task2.getParameters(), trigger.nextTrigger())
        );

        // The scheduler guarantees that tasks are executed in order with respect to time
        // There are no guarantees though around execution order for tasks scheduled for
        // the same execution time. The first two events/tasks should have the first trigger
        // time, and the latter two tasks should have the next trigger time.
        assertEquals(ImmutableSet.copyOf(actual.subList(0, 1)), expectedValuesFor1stTrigger, "The tasks for the " +
                "first trigger " + trigger + " do not match expected values - ");
        assertEquals(ImmutableSet.copyOf(actual.subList(1, 3)), expectedValuesFor2ndTrigger, "The tasks for the " +
                "second trigger " + trigger.nextTrigger() + " do not match expected values - ");

        assertQueueDoesNotExist(trigger.getTriggerTime(), scheduler.computeShard(task1.getGroupKey()));
        assertQueueDoesNotExist(trigger.getTriggerTime(), scheduler.computeShard(task2.getGroupKey()));
        assertQueueDoesNotExist(trigger.nextTrigger().getTriggerTime(), scheduler.computeShard(task1.getGroupKey()));
        assertQueueDoesNotExist(trigger.nextTrigger().getTriggerTime(), scheduler.computeShard(task2.getGroupKey()));

        assertLeasesDoNotExist(trigger.getTriggerTime());
        assertLeasesDoNotExist(trigger.nextTrigger().getTriggerTime());
    }

    @Test
    public void executeLongRunningTask() {
        String group1 = "group-1";
        String group2 = "group-2";
        int order = 100;
        Trigger trigger = new RepeatingTrigger.Builder()
                .withInterval(1, TimeUnit.SECONDS)
                .withDelay(2, TimeUnit.SECONDS)
                .withRepeatCount(2)
                .build();
        Date timeSlice = new Date(trigger.getTriggerTime());
        Task2Impl task1 = new Task2Impl(randomUUID(), group1, order, "taskLongRunning-1", emptyMap(), trigger);
        Task2Impl task2 = new Task2Impl(randomUUID(), group2, order, "taskLongRunning-2", emptyMap(), trigger);

        TaskSubscriber taskSubscriber = new TaskSubscriber();
        taskSubscriber.setOnNext(task -> {
            if (task.getName().equals(task1.getName())) {
                logger.debug("Executing long running task {}", task);
                try {
                    Thread.sleep(1500);
                } catch (InterruptedException e) {
                }
            }
        });
        scheduler.subscribe(taskSubscriber);

        setUpTasksForExecution(timeSlice, task1, task2);

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
                new Task2Impl(task1.getId(), task1.getGroupKey(), task1.getOrder(), task1.getName(),
                        task1.getParameters(), trigger.nextTrigger()),
                new Task2Impl(task2.getId(), task2.getGroupKey(), task2.getOrder(), task2.getName(),
                        task2.getParameters(), trigger.nextTrigger())
        );

        // The scheduler guarantees that tasks are executed in order with respect to time
        // There are no guarantees though around execution order for tasks scheduled for
        // the same execution time. The first two events/tasks should have the first trigger
        // time, and the latter two tasks should have the next trigger time.
        assertEquals(ImmutableSet.copyOf(actual.subList(0, 2)), expectedValuesFor1stTrigger, "The tasks for the " +
                "first trigger " + trigger + " do not match expected values - ");
        assertEquals(ImmutableSet.copyOf(actual.subList(2, 4)), expectedValuesFor2ndTrigger, "The tasks for the " +
                "second trigger " + trigger.nextTrigger() + " do not match expected values - ");

        assertQueueDoesNotExist(trigger.getTriggerTime(), scheduler.computeShard(task1.getGroupKey()));
        assertQueueDoesNotExist(trigger.getTriggerTime(), scheduler.computeShard(task2.getGroupKey()));
        assertQueueDoesNotExist(trigger.nextTrigger().getTriggerTime(), scheduler.computeShard(task1.getGroupKey()));
        assertQueueDoesNotExist(trigger.nextTrigger().getTriggerTime(), scheduler.computeShard(task2.getGroupKey()));

        assertLeasesDoNotExist(trigger.getTriggerTime());
        assertLeasesDoNotExist(trigger.nextTrigger().getTriggerTime());
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

    private int nextId() {
        return ids.getAndIncrement();
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
            tasks.add(new Task2Impl(row.getUUID(0), row.getString(1), row.getInt(2), row.getString(3),
                    row.getMap(4, String.class, String.class), TaskSchedulerImpl.getTrigger(row.getUDTValue(5))));
        }
        return tasks;
    }

    private void insertTaskIntoDB(Task2Impl task) {
        session.execute(insertTask.bind(task.getId(), task.getGroupKey(), task.getOrder(), task.getName(),
                task.getParameters(), getTriggerValue(rxSession, task.getTrigger())));
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
