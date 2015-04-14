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

import static org.joda.time.DateTime.now;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.joda.time.DateTime;
import org.joda.time.Seconds;

/**
 * @author jsanda
 */
public class Scheduler {

    private static final Comparator<TaskType> TASK_TYPE_COMPARATOR = Comparator.comparing(TaskType::getPriority);

    private int numWorkers;

    private ListeningExecutorService workersPool;

    private ScheduledExecutorService executor;

    private DateTimeService dateTimeService;

    private Set<TaskType> taskTypes;

    public Scheduler(int numWorkers, Set<TaskType> taskTypes) {
        this.numWorkers = numWorkers;
        workersPool = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(numWorkers));
        executor = Executors.newSingleThreadScheduledExecutor();
        dateTimeService =  new DateTimeService();
        this.taskTypes = taskTypes;
    }

    public void start() {
        executor.scheduleAtFixedRate(() -> {
            for (int i = 0; i < numWorkers; ++i) {
                DateTime timeSlice = dateTimeService.getTimeSlice(now(), Seconds.ONE.toStandardDuration());
                PriorityQueue<TaskType> typesQueue = new PriorityQueue<>(TASK_TYPE_COMPARATOR);
                typesQueue.addAll(taskTypes);
                workersPool.submit(new Worker(timeSlice, typesQueue));
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    public void stop() {
        executor.shutdown();
        workersPool.shutdown();
    }

}
