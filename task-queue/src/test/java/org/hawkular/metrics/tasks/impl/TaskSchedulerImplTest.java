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

import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.collect.ImmutableMap;
import org.hawkular.metrics.tasks.BaseTest;
import org.hawkular.metrics.tasks.api.RepeatingTrigger;
import org.hawkular.metrics.tasks.api.Task2;
import org.hawkular.metrics.tasks.api.Trigger;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import rx.Observable;

/**
 * @author jsanda
 */
public class TaskSchedulerImplTest extends BaseTest {

    private PreparedStatement findTasks;

    private PreparedStatement insertTask;

    private PreparedStatement findQueueEntries;

    @BeforeClass
    public void initClass() {
        System.out.println("SESSION = " + session);
        findTasks = session.prepare("select id, shard, name, params, trigger from tasks");
        insertTask = session.prepare("insert into tasks (id, shard, name, params, trigger) values (?, ?, ?, ?, ?)");
        findQueueEntries = session.prepare("select task_id from task_queue where time_slice = ? and shard = ?");
    }

    @Test
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

    @Test
    public void findTask() {
        TaskSchedulerImpl scheduler = new TaskSchedulerImpl(rxSession, queries);
        String taskName = "test";
        int shard = 10;
        Map<String, String> params = ImmutableMap.of("x", "1");
        Trigger trigger = new RepeatingTrigger.Builder()
                .withInterval(1, TimeUnit.SECONDS)
                .withDelay(5, TimeUnit.SECONDS)
                .build();
        Task2Impl task = new Task2Impl(UUID.randomUUID(), shard, taskName, params, trigger);

        insertTaskIntoDB(task);

        Observable<Task2> observable = scheduler.findTask(task.getId());
        TaskSubscriber subscriber = new TaskSubscriber();
        observable.subscribe(subscriber);

        subscriber.awaitTerminalEvent();
        subscriber.assertNoErrors();
        subscriber.assertReceivedOnNext(singletonList(task));
    }

    @Test
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
                TaskSchedulerImpl.getTriggerValue(rxSession, task.getTrigger())));
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
