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

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jsanda
 */
public class Worker implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(Worker.class);

    private DateTime timeSlice;

    private Queue<TaskType> taskTypes;

    private LeaseManager leaseManager;

    private TaskService taskService;

    public Worker(TaskService taskService, LeaseManager leaseManager, DateTime timeSlice, Queue<TaskType> taskTypes) {
        this.taskService = taskService;
        this.leaseManager = leaseManager;
        this.timeSlice = timeSlice;
        this.taskTypes = taskTypes;
    }

    @Override
    public void run() {
        ListenableFuture<List<Lease>> leasesFuture = leaseManager.findUnfinishedLeases(timeSlice);
        Futures.addCallback(leasesFuture, new FutureCallback<List<Lease>>() {
            @Override
            public void onSuccess(List<Lease> leases) {
                Queue<Lease> leaseQueue = new ArrayDeque<>(leases);

                for (Lease lease : leases) {
                    ListenableFuture<Boolean> acquiredFuture = leaseManager.acquire(lease);
                }
            }

            @Override
            public void onFailure(Throwable t) {

            }
        });
    }

    private FutureCallback<Boolean> leaseAcquiredCallback() {
        return new FutureCallback<Boolean>() {
            @Override
            public void onSuccess(Boolean acquired) {
                if (acquired) {
                    // fetch tasks
                } else {
                    // try next lease
                }
            }

            @Override
            public void onFailure(Throwable t) {

            }
        };
    }
}
