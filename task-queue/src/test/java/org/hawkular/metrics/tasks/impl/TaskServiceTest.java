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
import static java.util.Collections.singletonList;
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
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;

import com.datastax.driver.core.ResultSet;
import com.google.common.collect.ImmutableSet;
import org.hawkular.metrics.tasks.BaseTest;
import org.hawkular.metrics.tasks.api.Task;
import org.hawkular.metrics.tasks.api.TaskType;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author jsanda
 */
public class TaskServiceTest extends BaseTest {

    private static final Logger logger = LoggerFactory.getLogger(TaskServiceTest.class);

    protected LeaseService leaseService;

    @BeforeMethod
    public void setup() {
        leaseService = new LeaseService(rxSession, queries);
    }

    @Test
    public void scheduleTask() throws Exception {
        String type = "test1";
        TaskType taskType = new TaskType().setName(type).setSegments(100).setSegmentOffsets(10);
        int interval = 5;
        int window = 15;
        Task task = taskType.createTask("metric.5min", "metric", interval, window);

        TaskServiceImpl taskService = new TaskServiceImpl(rxSession, queries, leaseService, singletonList(taskType));

        DateTime expectedTimeSlice = dateTimeService.getTimeSlice(now(), standardMinutes(5)).plusMinutes(5);
        taskService.scheduleTask(now(), task).toBlocking().first();

        int segment = Math.abs(task.getTarget().hashCode() % task.getTaskType().getSegments());

        assertTasksScheduled(taskType, expectedTimeSlice, segment, new TaskImpl(taskType, expectedTimeSlice,
                "metric.5min", "metric", 5, 15));
        int segmentOffset = (segment / 10) * 10;
        assertLeasesCreated(expectedTimeSlice, newLease(taskType, segmentOffset));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void doNotScheduleTaskHavingInvalidType() throws Exception {

        TaskServiceImpl taskService = new TaskServiceImpl(rxSession, queries, leaseService,
                singletonList(new TaskType().setName("test").setSegments(1).setSegmentOffsets(1)));
        TaskType invalidType = new TaskType().setName("invalid").setSegments(1).setSegmentOffsets(1);
        Task task = invalidType.createTask("metric.5min", "metric", 5, 15);
        taskService.scheduleTask(now(), task);
    }

    @Test
    public void executeTasksOfOneType() throws Exception {
        DateTime timeSlice = dateTimeService.getTimeSlice(now().minusMinutes(1), standardMinutes(1));
        String type = "singleType-test";
        int interval = 5;
        int window = 15;
        int segment0 = 0;
        int segment1 = 1;
        int segmentOffset = 0;

        TaskType taskType = new TaskType().setName(type).setSegments(5).setSegmentOffsets(1);

        String metric1 = "metric1.5min";
        String metric2 = "metric2.5min";

        int metric1Segment = Math.abs(metric1.hashCode() % taskType.getSegments());
        int metric2Segment = Math.abs(metric2.hashCode() % taskType.getSegments());

        Task task1 = new TaskImpl(taskType, timeSlice, metric1, "metric1", interval, window);
        Task task2 = new TaskImpl(taskType, timeSlice, metric2, "metric2", interval, window);

        Map<Task, Boolean> executedTasks = new HashMap<>();
        executedTasks.put(task1, false);
        executedTasks.put(task2, false);

        taskType.setFactory(() -> task -> executedTasks.put(task, true));

        session.execute(queries.createTask.bind(type, timeSlice.toDate(), metric1Segment, metric1,
                ImmutableSet.of("metric1"), interval, window));
        session.execute(queries.createTask.bind(type, timeSlice.toDate(), metric2Segment, metric2,
                ImmutableSet.of("metric2"), interval, window));
        session.execute(queries.createLease.bind(timeSlice.toDate(), type, segmentOffset));

        LeaseService leaseService = new LeaseService(rxSession, queries);
        TaskServiceImpl taskService = new TaskServiceImpl(rxSession, queries, leaseService, singletonList(taskType));
        taskService.executeTasks(timeSlice);

        // verify that the tasks were executed
        executedTasks.forEach((task, executed) -> assertTrue(executedTasks.get(task),
                "Expected " + task + " to be executed"));

        assertTasksPartitionDeleted(type, timeSlice, metric1Segment);
        assertTasksPartitionDeleted(type, timeSlice, metric2Segment);

        assertLeasePartitionDeleted(timeSlice);

        DateTime nextTimeSlice = timeSlice.plusMinutes(interval);

        assertLeasesCreated(nextTimeSlice, newLease(taskType, segmentOffset));

        assertTasksScheduled(taskType, nextTimeSlice, segment0, new TaskImpl(taskType, nextTimeSlice, metric1,
                "metric1", interval, window));
        assertTasksScheduled(taskType, nextTimeSlice, segment1, new TaskImpl(taskType, nextTimeSlice, metric2,
                "metric2", interval, window));
    }

    @Test
    public void executeTasksOfMultipleTypes() throws Exception {
        DateTime timeSlice = dateTimeService.getTimeSlice(now().minusMinutes(1), standardMinutes(1));
        String type1 = "multiType-test1";
        String type2 = "multiType-test2";
        int interval = 5;
        int window = 15;
        int segmentOffset = 0;
        TaskExecutionHistory executionHistory = new TaskExecutionHistory();
        Supplier<Consumer<Task>> taskFactory = () -> task -> executionHistory.add(task.getTarget());

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

        LeaseService leaseService = new LeaseService(rxSession, queries);
        TaskServiceImpl taskService = new TaskServiceImpl(rxSession, queries, leaseService, asList(taskType1,
                taskType2));
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

        assertTasksScheduled(taskType1, nextTimeSlice, metric1Segment, new TaskImpl(taskType1, nextTimeSlice, metric1,
                "test1.metric1", interval, window));
        assertTasksScheduled(taskType1, nextTimeSlice, metric2Segment, new TaskImpl(taskType1, nextTimeSlice, metric2,
                "test1.metric2", interval, window));
        assertTasksScheduled(taskType2, nextTimeSlice, metric3Segment, new TaskImpl(taskType2, nextTimeSlice, metric3,
                "test2.metric1", interval, window));
        assertTasksScheduled(taskType2, nextTimeSlice, metric4Segment, new TaskImpl(taskType2, nextTimeSlice, metric4,
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
        Supplier<Consumer<Task>> taskFactory = () -> task -> executionHistory.add(task.getTarget());

        TaskType taskType1 = new TaskType().setName(type1).setSegments(5).setSegmentOffsets(1).setFactory(taskFactory);

        session.execute(queries.createTask.bind(type1, timeSlice.toDate(), segment0, "test1.metric1.5min",
                ImmutableSet.of("test1.metric1"), interval, window));
        session.execute(queries.createTask.bind(type1, timeSlice.toDate(), segment1, "test1.metric2.5min",
                ImmutableSet.of("test1.metric2"), interval, window));
        session.execute(queries.createLease.bind(timeSlice.toDate(), type1, segmentOffset));

        String owner = "host2";
        Lease lease = new Lease(timeSlice, type1, segmentOffset, owner, false);
        boolean acquired = leaseService.acquire(lease).toBlocking().first();
        assertTrue(acquired, "Should have acquired lease");

        LeaseService leaseService = new LeaseService(rxSession, queries);
        TaskServiceImpl taskService = new TaskServiceImpl(rxSession, queries, leaseService, singletonList(taskType1));

        Thread t1 = new Thread(() -> taskService.executeTasks(timeSlice));
        t1.start();

        AtomicReference<Exception> executionErrorRef = new AtomicReference<>();
        Runnable executeTasks = () -> {
            try {
                Thread.sleep(1000);
                session.execute(queries.deleteTasks.bind(lease.getTaskType(), lease.getTimeSlice().toDate(), segment0));
                session.execute(queries.deleteTasks.bind(lease.getTaskType(), lease.getTimeSlice().toDate(), segment1));
                leaseService.finish(lease).toBlocking().first();
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
        String type = "fails-test";
        TaskType taskType = new TaskType().setName(type).setSegments(1).setSegmentOffsets(1)
                .setFactory(() -> task -> {
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

        LeaseService leaseService = new LeaseService(rxSession, queries);
        TaskServiceImpl taskService = new TaskServiceImpl(rxSession, queries, leaseService, singletonList(taskType));
        taskService.executeTasks(timeSlice);

        assertTasksPartitionDeleted(type, timeSlice, segment);
        assertLeasePartitionDeleted(timeSlice);

        // now verify that the task is rescheduled
        DateTime nextTimeSlice = timeSlice.plusMinutes(interval);
        assertTasksScheduled(taskType, nextTimeSlice, segment,
                new TaskImpl(taskType, timeSlice, metric, "metric1", interval, window),
                new TaskImpl(taskType, nextTimeSlice, metric, "metric1", interval, window));
    }

    @Test
    public void executeTaskThatPreviouslyFailed() throws Exception {
        DateTime timeSlice = dateTimeService.getTimeSlice(now().minusMinutes(2), standardMinutes(1));
        String type = "previouslyFailed-test";
        TaskType taskType = new TaskType().setName(type).setSegments(1).setSegmentOffsets(1);
        String metric = "metric.5min";
        int interval = 5;
        int window = 15;
        int segment = metric.hashCode() % taskType.getSegments();
        int segmentOffset = 0;
        DateTime previousTimeSlice = timeSlice.minusMinutes(interval);

        List<Task> executedTasks = new ArrayList<>();
        taskType.setFactory(() -> executedTasks::add);

        session.execute(queries.createTaskWithFailures.bind(type, timeSlice.toDate(), segment, metric,
                ImmutableSet.of("metric"), interval, window, ImmutableSet.of(previousTimeSlice.toDate())));
        session.execute(queries.createLease.bind(timeSlice.toDate(), type, segmentOffset));

        LeaseService leaseService = new LeaseService(rxSession, queries);
        TaskServiceImpl taskService = new TaskServiceImpl(rxSession, queries, leaseService, singletonList(taskType));
        taskService.executeTasks(timeSlice);

        List<Task> expectedExecutedTasks = asList(
                new TaskImpl(taskType, previousTimeSlice, metric, "metric", interval, window),
                new TaskImpl(taskType, timeSlice, metric, "metric", interval, window)
        );

        assertEquals(executedTasks, expectedExecutedTasks, "The executed tasks do not match expected values");

        assertTasksPartitionDeleted(type, timeSlice, segment);
        assertLeasePartitionDeleted(timeSlice);
    }

    private void assertTasksPartitionDeleted(String taskType, DateTime timeSlice, int segment) {
        ResultSet resultSet = session.execute(queries.findTasks.bind(taskType, timeSlice.toDate(), segment));
        assertTrue(resultSet.isExhausted(), "Expected task partition {taskType: " + taskType + ", timeSlice: " +
                timeSlice + ", segment: " + segment + "} to be deleted.");
    }

    private void assertLeasePartitionDeleted(DateTime timeSlice) {
        ResultSet leasesResultSet = session.execute(queries.findLeases.bind(timeSlice.toDate()));
        assertTrue(leasesResultSet.isExhausted(), "Expected lease partition for time slice " + timeSlice +
                " to be empty but found " + StreamSupport.stream(leasesResultSet.spliterator(), false)
                .map(row -> new Lease(timeSlice, row.getString(0), row.getInt(1), row.getString(2), row.getBool(3)))
                .collect(toList()));
    }

    /**
     * Returns a function that is bound to taskType and segmentOffset and returns a new
     * Lease for a specified time slice.
     */
    private Function<DateTime, Lease> newLease(TaskType taskType, Integer segmentOffset) {
        return timeSlice -> new Lease(timeSlice, taskType.getName(), segmentOffset, null, false);
    }

    /**
     * The compiler requires that a method with @SafeVarargs be either static or final.
     * checkstyle fails when making a private method final, so this method is protected to
     * avoid any more wasted time with checkstyle
     *
     * @param timeSlice
     * @param leaseFns
     * @throws Exception
     */
    @SafeVarargs
    protected final void assertLeasesCreated(DateTime timeSlice, Function<DateTime, Lease>... leaseFns)
            throws Exception {
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

    private void assertTasksScheduled(TaskType type, DateTime timeSlice, int segment, Task... expected) {
        Set<Task> actualTasks = getTasks(type, timeSlice, segment);
        Set<Task> expectedTasks = ImmutableSet.copyOf(expected);

        assertEquals(actualTasks, expectedTasks, "The rescheduled tasks do not match the expected values");
    }

    private Set<Task> getTasks(TaskType type, DateTime timeSlice, int segment) {
        ResultSet resultSet = session.execute(queries.findTasks.bind(type.getName(), timeSlice.toDate(), segment));
        Set<TaskContainer> containers = StreamSupport.stream(resultSet.spliterator(), false)
                .map(row -> new TaskContainer(type, timeSlice, segment, row.getString(0), row.getSet(1, String.class),
                        row.getInt(2), row.getInt(3),
                        row.getSet(4, Date.class).stream().map(DateTime::new).collect(toSet())))
                .collect(toSet());
        return containers.stream()
                .flatMap(container -> StreamSupport.stream(container.spliterator(), false))
                .collect(toSet());
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
