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

import static java.util.stream.Collectors.toList;
import static org.joda.time.DateTime.now;

import java.util.List;
import java.util.stream.StreamSupport;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.joda.time.DateTime;
import org.joda.time.Duration;

/**
 * @author jsanda
 */
public class TaskService {

    private Duration timeSliceDuration;

    private Session session;

    private Queries queries;

    private List<TaskDef> taskDefs;

    private DateTimeService dateTimeService = new DateTimeService();

    public TaskService(Session session, Queries queries, Duration timeSliceDuration, List<TaskDef> taskDefs) {
        this.session = session;
        this.queries = queries;
        this.timeSliceDuration = timeSliceDuration;
        this.taskDefs = taskDefs;
    }

    public ListenableFuture<List<Task>> findTasks(String type, DateTime timeSlice, int segment) {
        ResultSetFuture future = session.executeAsync(queries.findTasks.bind(type, timeSlice.toDate(), segment));
        TaskDef taskDef = taskDefs.stream()
                .filter(t->t.getName().equals(type))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(type + " is not a recognized task type"));
        return Futures.transform(future, (ResultSet resultSet) -> StreamSupport.stream(resultSet.spliterator(), false)
                .map(row -> new Task(taskDef, row.getString(0), row.getSet(1, String.class), row.getInt(2),
                        row.getInt(3)))
                .collect(toList()));
    }

    public ListenableFuture<DateTime> scheduleTask(Task task) {
        DateTime currentTimeSlice = dateTimeService.getTimeSlice(now(), timeSliceDuration);
        DateTime timeSlice = currentTimeSlice.plus(task.getInterval());
        int segment = task.getTaskDef().getName().hashCode() % task.getTaskDef().getSegments();
        int segmentOffset = segment / task.getTaskDef().getSegmentOffsets();

        ResultSetFuture queueFuture = session.executeAsync(queries.createTask.bind(task.getTaskDef().getName(),
                timeSlice.toDate(), segment, task.getTarget(), task.getSources(), (int) task.getInterval()
                        .getStandardMinutes(), (int) task.getWindow().getStandardMinutes()));
        ResultSetFuture leaseFuture = session.executeAsync(queries.createLease.bind(timeSlice.toDate(),
                task.getTaskDef().getName(), segmentOffset));
        ListenableFuture<List<ResultSet>> futures = Futures.allAsList(queueFuture, leaseFuture);

        return Futures.transform(futures, (List<ResultSet> resultSets) -> timeSlice);
    }

}
