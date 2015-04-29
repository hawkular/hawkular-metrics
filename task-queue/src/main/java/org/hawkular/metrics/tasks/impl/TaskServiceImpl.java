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

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.joda.time.DateTime.now;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.StreamSupport;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.google.common.base.Function;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Uninterruptibles;
import org.hawkular.metrics.tasks.DateTimeService;
import org.hawkular.metrics.tasks.api.TaskExecutionException;
import org.hawkular.metrics.tasks.api.Task;
import org.hawkular.metrics.tasks.api.TaskType;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jsanda
 */
public class TaskServiceImpl implements org.hawkular.metrics.tasks.api.TaskService {

    private static final Logger logger = LoggerFactory.getLogger(TaskServiceImpl.class);

    private Duration timeSliceDuration;

    private Session session;

    private Queries queries;

    private List<TaskType> taskTypes;

    private LeaseManager leaseManager;

    /**
     * The ticker thread pool is responsible for scheduling task execution every tick on the scheduler thread pool.
     */
    private ScheduledExecutorService ticker = Executors.newScheduledThreadPool(1);

    /**
     * Thread pool that schedules or kicks of task execution. Task execution runs on the workers thread pool. The
     * scheduler blocks though until task execution for a time slice is finished.
     */
    private ExecutorService scheduler = Executors.newSingleThreadExecutor();

    private ListeningExecutorService workers = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(4));

    /**
     * Used to limit the amount of leases we try to acquire concurrently to the same number of worker threads available
     * for processing tasks.
     */
    private Semaphore permits = new Semaphore(4);

    private String owner;

    private DateTimeService dateTimeService;

    private TimeUnit tickerUnit = TimeUnit.MINUTES;

    public TaskServiceImpl(Session session, Queries queries, LeaseManager leaseManager, Duration timeSliceDuration,
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
        dateTimeService = new DateTimeService();
    }

    void setTickerUnit(TimeUnit tickerUnit) {
        this.tickerUnit = tickerUnit;
    }

    @Override
    public void start() {
        Runnable runnable = () -> {
            DateTime timeSlice = dateTimeService.getTimeSlice(now(), timeSliceDuration);
            scheduler.submit(() -> executeTasks(timeSlice));
        };
        ticker.scheduleAtFixedRate(runnable, 0, 1, tickerUnit);
    }

    @Override
    public void shutdown() {
//        ticker.shutdownNow();
//        scheduler.shutdownNow();
//        List<Runnable> droppedTasks = workers.shutdownNow()
    }

    public ListenableFuture<List<TaskContainer>> findTasks(String type, DateTime timeSlice, int segment) {
        ResultSetFuture future = session.executeAsync(queries.findTasks.bind(type, timeSlice.toDate(), segment));
        TaskType taskType = findTaskType(type);
        return Futures.transform(future, (ResultSet resultSet) -> StreamSupport.stream(resultSet.spliterator(), false)
                .map(row -> new TaskContainer(taskType, timeSlice, row.getString(0), row.getSet(1, String.class),
                        row.getInt(2), row.getInt(3), row.getSet(4, Date.class).stream()
                        .map(DateTime::new).collect(toSet()))).collect(toList()));
    }

    private TaskType findTaskType(String type) {
        return taskTypes.stream()
                .filter(t->t.getName().equals(type))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(type + " is not a recognized task type"));
    }

    @Override
    public ListenableFuture<Task> scheduleTask(DateTime time, Task task) {
        TaskType taskType = findTaskType(task.getTaskType().getName());

        DateTime currentTimeSlice = dateTimeService.getTimeSlice(time, task.getInterval());
        DateTime timeSlice = currentTimeSlice.plus(task.getInterval());

        ListenableFuture<DateTime> timeFuture = scheduleTaskAt(timeSlice, task);
        return Futures.transform(timeFuture, (DateTime scheduledTime) -> new TaskImpl(task.getTaskType(),
                scheduledTime,
                task.getTarget(), task.getSources(), task.getInterval(), task.getWindow()));
    }

    private ListenableFuture<DateTime> rescheduleTask(TaskContainer taskContainer) {
        TaskType taskType = taskContainer.getTaskType();
        DateTime nextTimeSlice = taskContainer.getTimeSlice().plus(taskContainer.getInterval());
        int segment = Math.abs(taskContainer.getTarget().hashCode() % taskType.getSegments());
        int segmentsPerOffset = taskType.getSegments() / taskType.getSegmentOffsets();
        int segmentOffset = (segment / segmentsPerOffset) * segmentsPerOffset;
        ResultSetFuture queueFuture;

        if (taskContainer.getFailedTimeSlices().isEmpty()) {
            queueFuture = session.executeAsync(queries.createTask.bind(taskType.getName(), nextTimeSlice.toDate(),
                    segment, taskContainer.getTarget(), taskContainer.getSources(),
                    taskContainer.getInterval().toStandardMinutes().getMinutes(),
                    taskContainer.getWindow().toStandardMinutes().getMinutes()));
        } else {
            queueFuture = session.executeAsync(queries.createTaskWithFailures.bind(taskType.getName(),
                    nextTimeSlice.toDate(), segment, taskContainer.getTarget(), taskContainer.getSources(),
                    taskContainer.getInterval().toStandardMinutes().getMinutes(),
                    taskContainer.getWindow().toStandardMinutes().getMinutes(), toDates(
                            taskContainer.getFailedTimeSlices())));
        }
        ResultSetFuture leaseFuture = session.executeAsync(queries.createLease.bind(nextTimeSlice.toDate(),
                taskType.getName(), segmentOffset));
        ListenableFuture<List<ResultSet>> futures = Futures.allAsList(queueFuture, leaseFuture);

        return Futures.transform(futures, (List<ResultSet> resultSets) -> nextTimeSlice);
    }

    private Set<Date> toDates(Set<DateTime> times) {
        return times.stream().map(DateTime::toDate).collect(toSet());
    }

    private ListenableFuture<DateTime> scheduleTaskAt(DateTime time, Task task) {
        TaskType taskType = task.getTaskType();
        int segment = Math.abs(task.getTarget().hashCode() % taskType.getSegments());
        int segmentsPerOffset = taskType.getSegments() / taskType.getSegmentOffsets();
        int segmentOffset = (segment / segmentsPerOffset) * segmentsPerOffset;

        ResultSetFuture queueFuture = session.executeAsync(queries.createTask.bind(taskType.getName(),
                time.toDate(), segment, task.getTarget(), task.getSources(), (int) task.getInterval()
                        .getStandardMinutes(), (int) task.getWindow().getStandardMinutes()));
        ResultSetFuture leaseFuture = session.executeAsync(queries.createLease.bind(time.toDate(),
                taskType.getName(), segmentOffset));
        ListenableFuture<List<ResultSet>> futures = Futures.allAsList(queueFuture, leaseFuture);

        return Futures.transform(futures, (List<ResultSet> resultSets) -> time);
    }

    public void executeTasks(DateTime timeSlice) {
        try {
            // Execute tasks in order of task types. Once all of the tasks are executed, we delete the lease partition.
            taskTypes.forEach(taskType -> executeTasks(timeSlice, taskType));
            Uninterruptibles.getUninterruptibly(leaseManager.deleteLeases(timeSlice));
        } catch (ExecutionException e) {
            logger.warn("Failed to delete lease partition for time slice " + timeSlice);
        }
    }

    /**
     * This method does not return until all tasks of the specified type have been executed.
     *
     * @param timeSlice
     * @param taskType
     */
    private void executeTasks(DateTime timeSlice, TaskType taskType) {
        try {
            List<Lease> leases = Uninterruptibles.getUninterruptibly(leaseManager.findUnfinishedLeases(timeSlice))
                    .stream().filter(lease -> lease.getTaskType().equals(taskType.getName())).collect(toList());

            // A CountDownLatch is used to let us know when to query again for leases. We do not want to query (again)
            // for leases until we have gone through each one. If a lease already has an owner, then we just count
            // down the latch and move on. If the lease does not have an owner, we attempt to acquire it. When the
            // result from trying to acquire the lease are available, we count down the latch.
            AtomicReference<CountDownLatch> latchRef = new AtomicReference<>(new CountDownLatch(leases.size()));


            // Keep checking for and process leases as long as the query returns leases and there is at least one
            // that is not finished. When these conditions do not hold, then the leases for the current time slice
            // are finished.
            while (!(leases.isEmpty() || leases.stream().allMatch(Lease::isFinished))) {
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
                                    List<ListenableFuture<ResultSet>> deleteFutures = new ArrayList<>();
                                    TaskType taskType = findTaskType(lease.getTaskType());
                                    for (int i = lease.getSegmentOffset(); i < taskType.getSegments(); ++i) {
                                        ListenableFuture<List<TaskContainer>> tasksFuture =
                                                findTasks(lease.getTaskType(), timeSlice, i);
                                        ListenableFuture<List<TaskContainer>> resultsFuture =
                                                Futures.transform(tasksFuture, executeTasksSegment, workers);
                                        ListenableFuture<List<DateTime>> nextExecutionsFuture = Futures.transform(
                                                resultsFuture, scheduleNextExecution, workers);
                                        ListenableFuture<ResultSet> deleteFuture = Futures.transform(
                                                nextExecutionsFuture, deleteTaskSegment(timeSlice, taskType, i));
                                        deleteFutures.add(deleteFuture);
                                    }
                                    ListenableFuture<List<ResultSet>> deletesFuture =
                                            Futures.allAsList(deleteFutures);
                                    ListenableFuture<Boolean> leaseFinishedFuture = Futures.transform(deletesFuture,
                                            (List<ResultSet> resultSets) -> leaseManager.finish(lease), workers);
                                    Futures.addCallback(leaseFinishedFuture, leaseFinished(lease), workers);
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
                        }, workers);
                    } else {
                        latchRef.get().countDown();
                    }
                }
                latchRef.get().await();
                leases = Uninterruptibles.getUninterruptibly(leaseManager.findUnfinishedLeases(timeSlice))
                        .stream().filter(lease -> lease.getTaskType().equals(taskType.getName())).collect(toList());
                latchRef.set(new CountDownLatch(leases.size()));
            }

        } catch (ExecutionException e) {
            logger.warn("Failed to load leases for time slice " + timeSlice, e);
        } catch (InterruptedException e) {
            logger.warn("There was an interrupt", e);
        }
    }

    /**
     * Wraps {@link TaskType#getFactory()} with a delegating function that catches any exception thrown by the task
     * being executed. The exception will be wrapped and rethrown as a TaskExecutionException.
     */
    private Function<Consumer<Task>, Consumer<Task>> wrapTaskRunner = taskRunner -> task -> {
        try {
            taskRunner.accept(task);
        } catch (Throwable t) {
            throw new TaskExecutionException(task, t);
        }
    };

    /**
     * This function takes as input a list of task containers to execute. A task container represents one or more
     * tasks. The function returns an updated copy of the list which reflects any failed executions. When a failure
     * occurs for a container with multiple tasks, execution is aborted immediately, and those tasks will have to be
     * rescheduled.
     */
    private Function<List<TaskContainer>, List<TaskContainer>> executeTasksSegment = taskContainers -> {
        List<TaskContainer> results = new ArrayList<>(taskContainers.size());
        taskContainers.forEach(taskContainer -> {
            TaskContainer executedTasks = TaskContainer.copyWithoutFailures(taskContainer);
            Consumer<Task> taskRunner = taskContainer.getTaskType().getFactory().get();
            Consumer<Task> wrappedTaskedRunner = wrapTaskRunner.apply(taskRunner);
            try {
                taskContainer.forEach(task -> {
                    wrappedTaskedRunner.accept(task);
                    taskContainer.getFailedTimeSlices().remove(task.getTimeSlice());
                });
            } catch (TaskExecutionException e) {
                logger.warn("Failed to execute " + e.getFailedTask());
                executedTasks.getFailedTimeSlices().add(e.getFailedTask().getTimeSlice());
            } catch (Throwable t) {
                // TODO log details about the specific time slice that failed
                logger.error("There was an unexpected error during task execution. This is likely a bug!", t);
            }
            results.add(executedTasks);
        });
        return results;
    };

    private AsyncFunction<List<TaskContainer>, List<DateTime>> scheduleNextExecution = results ->
        Futures.allAsList(results.stream().map(this::rescheduleTask).collect(toList()));

    private AsyncFunction<List<DateTime>, ResultSet> deleteTaskSegment(DateTime timeSlice, TaskType taskType,
            int segment) {
        return nextExecutions -> session.executeAsync(queries.deleteTasks.bind(taskType.getName(), timeSlice.toDate(),
                segment));
    }

    private FutureCallback<Boolean> leaseFinished(Lease lease) {
        return new FutureCallback<Boolean>() {
            @Override
            public void onSuccess(Boolean finished) {
                if (!finished) {
                    logger.warn("All tasks for {} have completed but unable to set it to finished", lease);
                }
                permits.release();
            }

            @Override
            public void onFailure(Throwable t) {
                // We can wind up in the onFailure callback when either any of the task segment deletions fail or when
                // marking the lease finished fails with an error. In order to determine what exactly failed, we need
                // to register additional callbacks on each future with either Futures.addCallback or
                // Futures.withFallback. Neither is particular appealing as it makes the code more complicated. This is
                // one of a growing number of reasons I want to prototype a solution using RxJava.

                logger.warn("There was an error either while deleting one or more task segments or while attempting "
                        + "to mark " + lease + " finished", t);
                permits.release();
            }
        };
    }

}
