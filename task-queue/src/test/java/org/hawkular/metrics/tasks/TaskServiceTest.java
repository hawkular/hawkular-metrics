/*
 *
 *  * Copyright 2014-2015 Red Hat, Inc. and/or its affiliates
 *  * and other contributors as indicated by the @author tags.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */
package org.hawkular.metrics.tasks;

import static java.util.Arrays.asList;
import static org.joda.time.DateTime.now;
import static org.joda.time.Duration.standardMinutes;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import com.datastax.driver.core.ResultSet;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @author jsanda
 */
public class TaskServiceTest extends BaseTest {

    private LeaseManager leaseManager;

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

        int segment = task.getTarget().hashCode() % task.getTaskType().getSegments();

        List<Task> tasks = getUninterruptibly(taskService.findTasks(type, timeSlice, segment));
        assertEquals(tasks, asList(task), "Failed to retrieve schedule task");

        int segmentOffset = segment / taskTypes.get(0).getSegmentOffsets();
        List<Lease> leases = getUninterruptibly(leaseManager.findUnfinishedLeases(timeSlice));
        assertEquals(leases, asList(new Lease(timeSlice, type, segmentOffset, null, false)), "Failed to find lease");
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

        Map<Task, Boolean> executedTasks = new HashMap<>();
        executedTasks.put(new Task(taskType, "metric1.5min", "metric1", interval, window), false);
        executedTasks.put(new Task(taskType, "metric2.5min", "metric2", interval, window), false);

        taskType.setFactory(task -> () -> executedTasks.put(task, true));

        session.execute(queries.createTask.bind(type, timeSlice.toDate(), segment0, "metric1.5min",
                ImmutableSet.of("metric1"), interval, window));
        session.execute(queries.createTask.bind(type, timeSlice.toDate(), segment1, "metric2.5min",
                ImmutableSet.of("metric2"), interval, window));
        session.execute(queries.createLease.bind(timeSlice.toDate(), type, segmentOffset));

        TaskService taskService = new TaskService(session, queries, leaseManager, standardMinutes(1), asList(taskType));
        taskService.executeTasks(timeSlice);

        // verify that the tasks were executed
        executedTasks.forEach((task, executed) -> assertTrue(executedTasks.get(task),
                "Expected " + task + " to be executed"));

        assertTasksPartitionDeleted(type, timeSlice, segment0);
        assertTasksPartitionDeleted(type, timeSlice, segment1);

        assertLeasePartitionDeleted(timeSlice);
    }

    @Test
    public void executeTasksOfMultipleTypes() throws Exception {
        DateTime timeSlice = dateTimeService.getTimeSlice(now().minusMinutes(1), standardMinutes(1));
        String type1 = "test1";
        String type2 = "test2";
        int interval = 5;
        int window = 15;
        int segment0 = 0;
        int segment1 = 1;
        int segmentOffset = 0;
        TaskExecutionHistory executionHistory = new TaskExecutionHistory();
        Function<Task, Runnable> taskFactory = task -> () -> executionHistory.add(task.getTarget());

        TaskType taskType1 = new TaskType().setName(type1).setSegments(5).setSegmentOffsets(1).setFactory(taskFactory);
        TaskType taskType2 = new TaskType().setName(type2).setSegments(5).setSegmentOffsets(1).setFactory(taskFactory);

        session.execute(queries.createTask.bind(type1, timeSlice.toDate(), segment0, "test1.metric1.5min",
                ImmutableSet.of("test1.metric1"), interval, window));
        session.execute(queries.createTask.bind(type1, timeSlice.toDate(), segment1, "test1.metric2.5min",
                ImmutableSet.of("test1.metric2"), interval, window));
        session.execute(queries.createLease.bind(timeSlice.toDate(), type1, segmentOffset));

        session.execute(queries.createTask.bind(type2, timeSlice.toDate(), segment0, "test2.metric1.5min",
                ImmutableSet.of("test2.metric1"), interval, window));
        session.execute(queries.createTask.bind(type2, timeSlice.toDate(), segment1, "test2.metric2.5min",
                ImmutableSet.of("test2.metric2"), interval, window));
        session.execute(queries.createLease.bind(timeSlice.toDate(), type2, segmentOffset));

        TaskService taskService = new TaskService(session, queries, leaseManager, standardMinutes(1),
                asList(taskType1, taskType2));
        taskService.executeTasks(timeSlice);

        // We need to verify that tasks are executed. We also need to verify that they are
        // executed in the correct order with respect to type.
        assertEquals(executionHistory.getExecutedTasks().size(), 4, "Expected 4 tasks to be executed");
        assertTaskExecuted("test1.metric1.5min", 0, 1, executionHistory);
        assertTaskExecuted("test1.metric2.5min", 0, 1, executionHistory);
        assertTaskExecuted("test2.metric1.5min", 2, 3, executionHistory);
        assertTaskExecuted("test2.metric2.5min", 2, 3, executionHistory);
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

    private void assertTaskExecuted(String taskId, int lowerBound, int upperBound,
            TaskExecutionHistory executionHistory) {
        int index = executionHistory.getExecutedTasks().indexOf(taskId);
        assertTrue(index > -1, "Failed to execute " + taskId);
        assertTrue(index >= lowerBound && index <= upperBound, taskId + " was executed out of order. Its index in " +
            "the execution history is " + index);
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
