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
package org.hawkular.metrics.tasks;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.joda.time.DateTime.now;
import static org.joda.time.Duration.standardMinutes;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.StreamSupport;

import com.datastax.driver.core.ResultSet;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @author jsanda
 */
public class TaskServiceTest extends BaseTest {

    protected LeaseManager leaseManager;

    @BeforeClass
    public void initClass() {
        leaseManager = new LeaseManager(session, queries);
    }

    @Test
    public void scheduleTask() throws Exception {
        String type = "test1";
        List<TaskType> taskTypes = asList(new TaskType()
                .setName(type)
                .setSegments(100)
                .setSegmentOffsets(10));

        TaskService taskService = new TaskService(session, queries, leaseManager, standardMinutes(1), taskTypes);

        Task task = new Task(taskTypes.get(0), "my.metric.5min", "my.metric", 5, 15);
        DateTime timeSlice =  getUninterruptibly(taskService.scheduleTask(now(), task));

        int segment = Math.abs(task.getTarget().hashCode() % task.getTaskType().getSegments());

        List<Task> tasks = getUninterruptibly(taskService.findTasks(type, timeSlice, segment));
        assertEquals(tasks, asList(task), "Failed to retrieve schedule task");

        int segmentOffset = (segment / 10) * 10;
        List<Lease> leases = getUninterruptibly(leaseManager.findUnfinishedLeases(timeSlice));
        assertEquals(leases, asList(new Lease(timeSlice, type, segmentOffset, null, false)), "Failed to find lease");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void doNotScheduleTaskHavingInvalidType() throws Exception {

        TaskService taskService = new TaskService(session, queries, leaseManager, standardMinutes(1), asList(
                new TaskType().setName("test").setSegments(1).setSegmentOffsets(1)));
        TaskType invalidType = new TaskType().setName("invalid").setSegments(1).setSegmentOffsets(1);
        Task task = new Task(invalidType, "my.metric", "my.metric", 5, 15);
        taskService.scheduleTask(now(), task);
    }

    @Test
    public void executeTasksOfOneType() throws Exception {
        DateTime timeSlice = dateTimeService.getTimeSlice(now().minusMinutes(1), standardMinutes(1));
        String type = "test";
        int interval = 5;
        int window = 15;
        int segment0 = 0;
        int segment1 = 1;
        int segmentOffset = 0;

        TaskType taskType = new TaskType().setName("test").setSegments(5).setSegmentOffsets(1);

        String metric1 = "metric1.5min";
        String metric2 = "metric2.5min";

        int metric1Segment = Math.abs(metric1.hashCode() % taskType.getSegments());
        int metric2Segment = Math.abs(metric2.hashCode() % taskType.getSegments());

        Map<Task, Boolean> executedTasks = new HashMap<>();
        executedTasks.put(new Task(taskType, metric1, "metric1", interval, window), false);
        executedTasks.put(new Task(taskType, metric2, "metric2", interval, window), false);

        taskType.setFactory(task -> () -> executedTasks.put(task, true));

        session.execute(queries.createTask.bind(type, timeSlice.toDate(), metric1Segment, metric1,
                ImmutableSet.of("metric1"), interval, window));
        session.execute(queries.createTask.bind(type, timeSlice.toDate(), metric2Segment, metric2,
                ImmutableSet.of("metric2"), interval, window));
        session.execute(queries.createLease.bind(timeSlice.toDate(), type, segmentOffset));

        TaskService taskService = new TaskService(session, queries, leaseManager, standardMinutes(1), asList(taskType));
        taskService.executeTasks(timeSlice);

        // verify that the tasks were executed
        executedTasks.forEach((task, executed) -> assertTrue(executedTasks.get(task),
                "Expected " + task + " to be executed"));

        assertTasksPartitionDeleted(type, timeSlice, metric1Segment);
        assertTasksPartitionDeleted(type, timeSlice, metric2Segment);

        assertLeasePartitionDeleted(timeSlice);

        DateTime nextTimeSlice = timeSlice.plusMinutes(interval);

        assertLeasesCreated(nextTimeSlice, newLease(taskType, segmentOffset));

        assertTasksRescheduled(taskType, nextTimeSlice, segment0, new Task(taskType, metric1, "metric1",
                interval, window));
        assertTasksRescheduled(taskType, nextTimeSlice, segment1, new Task(taskType, metric2, "metric2",
                interval, window));
    }

    @Test
    public void executeTasksOfMultipleTypes() throws Exception {
        DateTime timeSlice = dateTimeService.getTimeSlice(now().minusMinutes(1), standardMinutes(1));
        String type1 = "test1";
        String type2 = "test2";
        int interval = 5;
        int window = 15;
        int segmentOffset = 0;
        TaskExecutionHistory executionHistory = new TaskExecutionHistory();
        Function<Task, Runnable> taskFactory = task -> () -> executionHistory.add(task.getTarget());

        TaskType taskType1 = new TaskType().setName(type1).setSegments(2).setSegmentOffsets(1).setFactory(taskFactory);
        TaskType taskType2 = new TaskType().setName(type2).setSegments(2).setSegmentOffsets(1).setFactory(taskFactory);

        String metric1 = "test1.metric1.5min";
        String metric2 = "test1.metric2.5min";
        String metric3 = "test2.metric1.5min";
        String metric4 = "test2.metric2.5min";

        int metric1Segment = metric1.hashCode() % taskType1.getSegments();
        int metric2Segment = metric2.hashCode() % taskType1.getSegments();
        int metric3Segment = metric3.hashCode() % taskType2.getSegments();
        int metric4Segment = metric4.hashCode() % taskType2.getSegments();

        session.execute(queries.createTask.bind(type1, timeSlice.toDate(), metric1Segment, metric1,
                ImmutableSet.of("test1.metric1"), interval, window));
        session.execute(queries.createTask.bind(type1, timeSlice.toDate(), metric2Segment, metric2,
                ImmutableSet.of("test1.metric2"), interval, window));
        session.execute(queries.createLease.bind(timeSlice.toDate(), type1, segmentOffset));

        session.execute(queries.createTask.bind(type2, timeSlice.toDate(), metric3Segment, metric3,
                ImmutableSet.of("test2.metric1"), interval, window));
        session.execute(queries.createTask.bind(type2, timeSlice.toDate(), metric4Segment, metric4,
                ImmutableSet.of("test2.metric2"), interval, window));
        session.execute(queries.createLease.bind(timeSlice.toDate(), type2, segmentOffset));

        TaskService taskService = new TaskService(session, queries, leaseManager, standardMinutes(1),
                asList(taskType1, taskType2));
        taskService.executeTasks(timeSlice);

        // We need to verify that tasks are executed. We also need to verify that they are
        // executed in the correct order with respect to type.
        assertEquals(executionHistory.getExecutedTasks().size(), 4, "Expected 4 tasks to be executed");
        assertTaskExecuted(metric1, 0, 1, executionHistory);
        assertTaskExecuted(metric2, 0, 1, executionHistory);
        assertTaskExecuted(metric3, 2, 3, executionHistory);
        assertTaskExecuted(metric4, 2, 3, executionHistory);

        assertTasksPartitionDeleted(type1, timeSlice, metric1Segment);
        assertTasksPartitionDeleted(type1, timeSlice, metric2Segment);
        assertTasksPartitionDeleted(type2, timeSlice, metric3Segment);
        assertTasksPartitionDeleted(type2, timeSlice, metric4Segment);

        assertLeasePartitionDeleted(timeSlice);

        DateTime nextTimeSlice = timeSlice.plusMinutes(interval);

        assertLeasesCreated(nextTimeSlice, newLease(taskType1, segmentOffset), newLease(taskType2, segmentOffset));

        assertTasksRescheduled(taskType1, nextTimeSlice, metric1Segment, new Task(taskType1, metric1,
                "test1.metric1", interval, window));
        assertTasksRescheduled(taskType1, nextTimeSlice, metric2Segment, new Task(taskType1, metric2,
                "test1.metric2", interval, window));
        assertTasksRescheduled(taskType2, nextTimeSlice, metric3Segment, new Task(taskType2, metric3,
                "test2.metric1", interval, window));
        assertTasksRescheduled(taskType2, nextTimeSlice, metric4Segment, new Task(taskType2, metric4,
                "test2.metric2", interval, window));
    }

    /**
     * This test exercises the scenario in which other clients have already acquired all
     * of the leases.
     */
    @Test
    public void tryToExecuteTasksWhenAllLeasesAreAlreadyReserved() throws Exception {
        DateTime timeSlice = dateTimeService.getTimeSlice(now().minusMinutes(1), standardMinutes(1));
        String type1 = "test1";
        int interval = 5;
        int window = 15;
        int segment0 = 0;
        int segment1 = 1;
        int segmentOffset = 0;

        TaskExecutionHistory executionHistory = new TaskExecutionHistory();
        Function<Task, Runnable> taskFactory = task -> () -> executionHistory.add(task.getTarget());

        TaskType taskType1 = new TaskType().setName(type1).setSegments(5).setSegmentOffsets(1).setFactory(taskFactory);

        session.execute(queries.createTask.bind(type1, timeSlice.toDate(), segment0, "test1.metric1.5min",
                ImmutableSet.of("test1.metric1"), interval, window));
        session.execute(queries.createTask.bind(type1, timeSlice.toDate(), segment1, "test1.metric2.5min",
                ImmutableSet.of("test1.metric2"), interval, window));
        session.execute(queries.createLease.bind(timeSlice.toDate(), type1, segmentOffset));

        String owner = "host2";
        Lease lease = new Lease(timeSlice, type1, segmentOffset, owner, false);
        boolean acquired = getUninterruptibly(leaseManager.acquire(lease));
        assertTrue(acquired, "Should have acquired lease");

        TaskService taskService = new TaskService(session, queries, leaseManager, standardMinutes(1),
                asList(taskType1));

        Thread t1 = new Thread(() -> taskService.executeTasks(timeSlice));
        t1.start();

        AtomicReference<Exception> executionErrorRef = new AtomicReference<>();
        Runnable executeTasks = () -> {
            try {
                Thread.sleep(1000);
                session.execute(queries.deleteTasks.bind(lease.getTaskType(), lease.getTimeSlice().toDate(), segment0));
                session.execute(queries.deleteTasks.bind(lease.getTaskType(), lease.getTimeSlice().toDate(), segment1));
                getUninterruptibly(leaseManager.finish(lease));
            } catch (Exception e) {
                executionErrorRef.set(e);
            }
        };
        Thread t2 = new Thread(executeTasks);
        t2.start();

        t2.join(10000);
        t1.join(10000);

        assertNull(executionErrorRef.get(), "Did not expect an exception to be thrown from other client, but " +
                "got: " + executionErrorRef.get());

        assertTrue(executionHistory.getExecutedTasks().isEmpty(), "Did not expect this task service to execute any " +
                "tasks but it executed " + executionHistory.getExecutedTasks());
        assertTasksPartitionDeleted(type1, timeSlice, segment0);
        assertTasksPartitionDeleted(type1, timeSlice, segment1);
        assertLeasePartitionDeleted(timeSlice);

        // We do not need to verify that tasks are rescheduled or that new leases are
        // created since the "other" clients would be doing that.
    }

    @Test
    public void executeTaskThatFails() throws Exception {
        DateTime timeSlice = dateTimeService.getTimeSlice(now().minusMinutes(1), standardMinutes(1));
        String type = "test";
        TaskType taskType = new TaskType().setName(type).setSegments(1).setSegmentOffsets(1)
                .setFactory(task -> () -> {
                    throw new RuntimeException();
                });
        String metric = "metric1.5min";
        int interval = 5;
        int window = 15;
        int segment = metric.hashCode() % taskType.getSegments();
        int segmentOffset = 0;


        session.execute(queries.createTask.bind(type, timeSlice.toDate(), segment, metric, ImmutableSet.of("metric1"),
                interval, window));
        session.execute(queries.createLease.bind(timeSlice.toDate(), type, segmentOffset));

        TaskService taskService = new TaskService(session, queries, leaseManager, standardMinutes(1), asList(taskType));
        taskService.executeTasks(timeSlice);

        assertTasksPartitionDeleted(type, timeSlice, segment);
        assertLeasePartitionDeleted(timeSlice);

        // now verify that the task is rescheduled
        DateTime nextTimeSlice = timeSlice.plusMinutes(interval);
        assertTasksRescheduled(taskType, nextTimeSlice, segment, new Task(taskType, metric, "metric1", interval, window,
                asList(timeSlice)));
    }

    private void assertTasksPartitionDeleted(String taskType, DateTime timeSlice, int segment) {
        ResultSet resultSet = session.execute(queries.findTasks.bind(taskType, timeSlice.toDate(), segment));
        assertTrue(resultSet.isExhausted(), "Expected task partition {taskType: " + taskType + ", timeSlice: " +
                timeSlice + ", segment: " + segment + "} to be deleted.");
    }

    private void assertLeasePartitionDeleted(DateTime timeSlice) {
        ResultSet leasesResultSet = session.execute(queries.findLeases.bind(timeSlice.toDate()));
        assertTrue(leasesResultSet.isExhausted(), "Expected lease partition for time slice " + timeSlice +
                " to be empty");
    }

    /**
     * Returns a function that is bound to taskType and segmentOffset and returns a new
     * Lease for a specified time slice.
     */
    private Function<DateTime, Lease> newLease(TaskType taskType, Integer segmentOffset) {
        return timeSlice -> new Lease(timeSlice, taskType.getName(), segmentOffset, null, false);
    }

    @SafeVarargs
    private final void assertLeasesCreated(DateTime timeSlice, Function<DateTime, Lease>... leaseFns) throws Exception {
        Lease[] leases = Arrays.stream(leaseFns)
                .map((Function<DateTime, Lease> fn) -> fn.apply(timeSlice))
                .toArray(Lease[]::new);
        assertLeasesEquals(timeSlice, leases);
    }

    private void assertLeasesEquals(DateTime timeSlice, Lease... leases) throws Exception {
        ResultSet resultSet = session.execute(queries.findLeases.bind(timeSlice.toDate()));
        List<Lease> actual = StreamSupport.stream(resultSet.spliterator(), false)
                .map(row -> new Lease(timeSlice, row.getString(0), row.getInt(1), row.getString(2), row.getBool(3)))
                .collect(toList());
        List<Lease> expected = asList(leases);

        assertEquals(actual, expected, "The leases for time slice " + timeSlice + " do not match the expected values");
    }

    private void assertTaskExecuted(String taskId, int lowerBound, int upperBound,
            TaskExecutionHistory executionHistory) {
        int index = executionHistory.getExecutedTasks().indexOf(taskId);
        assertTrue(index > -1, "Failed to execute " + taskId);
        assertTrue(index >= lowerBound && index <= upperBound, taskId + " was executed out of order. Its index in " +
                "the execution history is " + index);
    }

    private void assertTasksRescheduled(TaskType type, DateTime timeSlice, int segment, Task... expected) {
        ResultSet resultSet = session.execute(queries.findTasks.bind(type.getName(), timeSlice.toDate(), segment));
        Set<Task> actualTasks = StreamSupport.stream(resultSet.spliterator(), false)
                .map(row -> new Task(type, row.getString(0), row.getSet(1, String.class), row.getInt(2),
                        row.getInt(3), row.getSet(4, Date.class).stream().map(DateTime::new).collect(toList())))
                .collect(toSet());
        Set<Task> expectedTasks = ImmutableSet.copyOf(expected);

        assertEquals(actualTasks, expectedTasks, "The rescheduled tasks do not match the expected values");
    }

    private static class TaskExecutionHistory {
        private List<String> executedTasks = new ArrayList<>();

        public synchronized void add(String task) {
            executedTasks.add(task);
        }

        public List<String> getExecutedTasks() {
            return executedTasks;
        }
    }

}
