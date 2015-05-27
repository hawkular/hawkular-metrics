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
import static org.joda.time.DateTime.now;
import static org.joda.time.Duration.standardSeconds;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.hawkular.metrics.tasks.BaseTest;
import org.hawkular.metrics.tasks.api.TaskType;
import org.hawkular.rx.cassandra.driver.RxSessionImpl;
import org.joda.time.DateTime;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @author jsanda
 */
public class TaskSchedulerTest extends BaseTest {

    private LeaseService leaseService;

    @BeforeClass
    public void initClass() {
        leaseService = new LeaseService(new RxSessionImpl(session), queries);
    }

    @Test
    public void startScheduler() throws Exception {
        List<TaskType> taskTypes = singletonList(new TaskType().setName("test").setSegments(1).setSegmentOffsets(1));

        List<DateTime> actualTimeSlices = new ArrayList<>();
        List<DateTime> expectedTimeSlices = asList(
                dateTimeService.getTimeSlice(now().plusSeconds(2), standardSeconds(1)),
                dateTimeService.getTimeSlice(now().plusSeconds(3), standardSeconds(1)),
                dateTimeService.getTimeSlice(now().plusSeconds(4), standardSeconds(1))
        );

        TaskServiceImpl taskService = new TaskServiceImpl(rxSession, queries, leaseService, taskTypes) {
            @Override
            public void executeTasks(DateTime timeSlice) {
                actualTimeSlices.add(timeSlice);
            }
        };
        taskService.setTimeUnit(TimeUnit.SECONDS);
        taskService.start();

        Thread.sleep(5000);
        taskService.shutdown();

        assertTrue(actualTimeSlices.size() >= 3, "Expected task execution to be scheduled at least 3 times but it " +
                "was scheduled " + actualTimeSlices.size() + " times.");
        assertTrue(actualTimeSlices.size() <= 6, "Expected no more that 6 task executions to be scheduled since " +
            "shutdown was called, but it was scheduled " + actualTimeSlices.size() + " times.");
        assertTaskExecutionScheduleForTimeSlices(actualTimeSlices, expectedTimeSlices);
    }

    private void assertTaskExecutionScheduleForTimeSlices(List<DateTime> actualTimeSlices,
            List<DateTime> expectedTimeSlices) {
        expectedTimeSlices.forEach(timeSlice -> assertTrue(actualTimeSlices.contains(timeSlice),
                "Expected task execution to be scheduled for time slice " + timeSlice));
    }

}
