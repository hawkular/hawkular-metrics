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
import static org.joda.time.Minutes.minutes;
import static org.testng.Assert.assertEquals;

import java.util.List;

import org.joda.time.DateTime;
import org.testng.annotations.Test;

/**
 * @author jsanda
 */
public class TaskServiceTest extends BaseTest {

    @Test
    public void scheduleTask() throws Exception {
        String type = "test1";
        List<TaskDef> taskDefs = asList(new TaskDef()
                .setName(type)
                .setSegments(100)
                .setSegmentOffsets(10));

        TaskService taskService = new TaskService(session, queries, minutes(1).toStandardDuration(), taskDefs);
        LeaseManager leaseManager = new LeaseManager(session, queries);

        Task task = new Task(taskDefs.get(0), "my.metric.5min", "my.metric", 5, 15);
        DateTime timeSlice =  getUninterruptibly(taskService.scheduleTask(task));

        int segment = task.getTaskDef().getName().hashCode() % task.getTaskDef().getSegments();

        List<Task> tasks = getUninterruptibly(taskService.findTasks(type, timeSlice, segment));
        assertEquals(tasks, asList(task), "Failed to retrieve schedule task");

        int segmentOffset = segment / taskDefs.get(0).getSegmentOffsets();
        List<Lease> leases = getUninterruptibly(leaseManager.findUnfinishedLeases(timeSlice));
        assertEquals(leases, asList(new Lease(timeSlice, type, segmentOffset, null, false)), "Failed to find lease");
    }

}
