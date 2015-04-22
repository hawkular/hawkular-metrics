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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.StreamSupport;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Uninterruptibles;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jsanda
 */
public class TaskService {

    private static final Logger logger = LoggerFactory.getLogger(TaskService.class);

    private Duration timeSliceDuration;

    private Session session;

    private Queries queries;

    private List<TaskType> taskTypes;

    private DateTimeService dateTimeService = new DateTimeService();

    private LeaseManager leaseManager;

    private ListeningExecutorService threadPool = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(4));

    private Semaphore permits = new Semaphore(1);

    private String owner;

    public TaskService(Session session, Queries queries, LeaseManager leaseManager, Duration timeSliceDuration,
            List<TaskType> taskTypes) {
        this.session = session;
        this.queries = queries;
        this.leaseManager = leaseManager;
        this.timeSliceDuration = timeSliceDuration;
        this.taskTypes = taskTypes;
        try {
            owner = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException("Failed to initialize owner name", e);
        }
    }

    public ListenableFuture<List<Task>> findTasks(String type, DateTime timeSlice, int segment) {
        ResultSetFuture future = session.executeAsync(queries.findTasks.bind(type, timeSlice.toDate(), segment));
        TaskType taskType = findTaskType(type);
        return Futures.transform(future, (ResultSet resultSet) -> StreamSupport.stream(resultSet.spliterator(), false)
                .map(row -> new Task(taskType, row.getString(0), row.getSet(1, String.class), row.getInt(2),
                        row.getInt(3)))
                .collect(toList()));
    }

    private TaskType findTaskType(String type) {
        return taskTypes.stream()
                .filter(t->t.getName().equals(type))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(type + " is not a recognized task type"));
    }

    public ListenableFuture<DateTime> scheduleTask(DateTime time, Task task) {
        DateTime currentTimeSlice = dateTimeService.getTimeSlice(time, timeSliceDuration);
        DateTime timeSlice = currentTimeSlice.plus(task.getInterval());
        int segment = task.getTarget().hashCode() % task.getTaskType().getSegments();
        int segmentOffset = segment / task.getTaskType().getSegmentOffsets();

        ResultSetFuture queueFuture = session.executeAsync(queries.createTask.bind(task.getTaskType().getName(),
                timeSlice.toDate(), segment, task.getTarget(), task.getSources(), (int) task.getInterval()
                        .getStandardMinutes(), (int) task.getWindow().getStandardMinutes()));
        ResultSetFuture leaseFuture = session.executeAsync(queries.createLease.bind(timeSlice.toDate(),
                task.getTaskType().getName(), segmentOffset));
        ListenableFuture<List<ResultSet>> futures = Futures.allAsList(queueFuture, leaseFuture);

        return Futures.transform(futures, (List<ResultSet> resultSets) -> timeSlice);
    }

    public void executeTasks(DateTime timeSlice) {
        try {
            List<Lease> leases = Uninterruptibles.getUninterruptibly(leaseManager.findUnfinishedLeases(timeSlice));
            AtomicReference<CountDownLatch> latchRef = new AtomicReference<>(new CountDownLatch(leases.size()));
            while (!leases.isEmpty() && leases.stream().anyMatch(lease -> !lease.isFinished())) {
                for (final Lease lease : leases) {
                    if (lease.getOwner() == null) {
                        permits.acquire();
                        lease.setOwner(owner);
                        ListenableFuture<Boolean> acquiredFuture = leaseManager.acquire(lease);
                        Futures.addCallback(acquiredFuture, new FutureCallback<Boolean>() {
                            @Override
                            public void onSuccess(Boolean acquired) {
                                latchRef.get().countDown();
                                if (acquired) {
                                    TaskType taskType = findTaskType(lease.getTaskType());
                                    for (int i = lease.getSegmentOffset(); i < taskType.getSegments(); ++i) {
                                        ListenableFuture<List<Task>> tasksFuture = findTasks(lease.getTaskType(),
                                                timeSlice, i);
                                        Futures.addCallback(tasksFuture, executeTasksCallback(lease, i), threadPool);
                                    }
                                } else {
                                    // someone else has the lease so return the permit and try to
                                    // acquire another lease
                                    permits.release();
                                }
                            }

                            @Override
                            public void onFailure(Throwable t) {
                                logger.warn("There was an error trying to acquire a lease", t);
                                latchRef.get().countDown();
                            }
                        }, threadPool);
                    } else {
                        latchRef.get().countDown();
                    }
                }
                latchRef.get().await();
                leases = Uninterruptibles.getUninterruptibly(leaseManager.findUnfinishedLeases(timeSlice));
                latchRef.set(new CountDownLatch(leases.size()));
            }

            Uninterruptibles.getUninterruptibly(leaseManager.deleteLeases(timeSlice));
        } catch (ExecutionException e) {
            logger.warn("Failed to load leases for time slice " + timeSlice, e);
        } catch (InterruptedException e) {
            logger.warn("There was an interrupt", e);
        }
    }

    private FutureCallback<List<Task>> executeTasksCallback(Lease lease, int segment) {
        return new FutureCallback<List<Task>>() {
            @Override
            public void onSuccess(List<Task> tasks) {
                tasks.forEach(task -> {
                    TaskType taskType = task.getTaskType();
                    Runnable taskRunner = taskType.getFactory().apply(task);
                    taskRunner.run();
                });

                logger.info("deleting tasks for time slice " + lease.getTimeSlice());

                session.execute(queries.deleteTasks.bind(lease.getTaskType(), lease.getTimeSlice().toDate(), segment));
                ListenableFuture<Boolean> leaseFinished = leaseManager.finish(lease);
                Futures.addCallback(leaseFinished, new FutureCallback<Boolean>() {
                    @Override
                    public void onSuccess(Boolean result) {
                        permits.release();
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        logger.warn("Failed to mark lease finished", t);
                    }
                }, threadPool);
            }

            @Override
            public void onFailure(Throwable t) {

            }
        };
    }

}
