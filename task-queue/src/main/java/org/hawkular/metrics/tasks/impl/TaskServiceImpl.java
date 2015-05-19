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

import static java.util.stream.Collectors.toSet;
import static org.joda.time.DateTime.now;
import static org.joda.time.Duration.standardMinutes;
import static org.joda.time.Duration.standardSeconds;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.hawkular.metrics.schema.SchemaManager;
import org.hawkular.metrics.tasks.DateTimeService;
import org.hawkular.metrics.tasks.api.Task;
import org.hawkular.metrics.tasks.api.TaskExecutionException;
import org.hawkular.metrics.tasks.api.TaskService;
import org.hawkular.metrics.tasks.api.TaskType;
import org.hawkular.rx.cassandra.driver.RxSession;
import org.hawkular.rx.cassandra.driver.RxSessionImpl;
import org.hawkular.rx.cassandra.driver.RxUtil;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Func1;

/**
 * @author jsanda
 */
public class TaskServiceImpl implements TaskService {

    private static final Logger logger = LoggerFactory.getLogger(TaskServiceImpl.class);

    private Session session;

    private RxSession rxSession;

    private Queries queries;

    private List<TaskType> taskTypes;

    private LeaseService leaseService;

    /**
     * The ticker thread pool is responsible for scheduling task execution every tick on the scheduler thread pool.
     */
    private ScheduledExecutorService ticker = Executors.newScheduledThreadPool(1);

    /**
     * Thread pool that schedules or kicks off task execution. Task execution runs on the workers thread pool. The
     * scheduler blocks though until task execution for a time slice is finished.
     */
    private ExecutorService scheduler = Executors.newSingleThreadExecutor();

    /**
     * Thread pool for executing tasks.
     */
    private ListeningExecutorService workers = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(4));

    /**
     * Used to limit the amount of leases we try to acquire concurrently to the same number of worker threads available
     * for processing tasks.
     */
    private Semaphore permits = new Semaphore(4);

    private String owner;

    private DateTimeService dateTimeService;

    /**
     * The duration of a time slice for tasks.
     */
    private Duration timeSliceDuration = standardMinutes(1);

    /**
     * The time units to use for the ticker. This determines the frequency at which jobs are submitted to the scheduler.
     */
    private TimeUnit timeUnit = TimeUnit.MINUTES;

    public TaskServiceImpl(Session session, Queries queries, LeaseService leaseService, List<TaskType> taskTypes) {
        try {
            this.session = session;
            this.queries = queries;
            this.leaseService = leaseService;
            this.taskTypes = taskTypes;
            dateTimeService = new DateTimeService();
            owner = InetAddress.getLocalHost().getHostName();
            rxSession = new RxSessionImpl(session);

            SchemaManager schemaManager = new SchemaManager(session);
            String keyspace = System.getProperty("keyspace", "hawkular_metrics");
            schemaManager.createSchema(keyspace);
        } catch (UnknownHostException e) {
            throw new RuntimeException("Failed to initialize owner name", e);
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize schema", e);
        }
    }

    /**
     * <p>
     * The time unit determines a couple things. First, it determines the frequency for scheduling jobs. If
     * {@link TimeUnit#MINUTES} is used for instance, then jobs are scheduled every minute. In this context, a job
     * refers to finding and executing tasks in the queue for a particular time slice, which brings up the second
     * thing that <code>timeUnit</code> determines - time slice interval. Time slices are set along fixed intervals,
     * e.g., 13:00, 13:01, 13:02, etc.
     * </p>
     * <p>
     * <strong>Note:</strong> This should only be called prior to calling {@link #start()}.
     * </p>
     */
    public void setTimeUnit(TimeUnit timeUnit) {
        switch (timeUnit) {
            case SECONDS:
                this.timeUnit = TimeUnit.SECONDS;
                timeSliceDuration = standardSeconds(1);
                break;
            case MINUTES:
                this.timeUnit = TimeUnit.MINUTES;
                timeSliceDuration = standardMinutes(1);
                break;
            case HOURS:
                this.timeUnit = TimeUnit.HOURS;
                timeSliceDuration = standardMinutes(60);
                break;
            default:
                throw new IllegalArgumentException(timeUnit + " is not a supported time unit");
        }
    }

    @Override
    public void start() {
        Runnable runnable = () -> {
            DateTime timeSlice = dateTimeService.getTimeSlice(now(), timeSliceDuration);
            scheduler.submit(() -> executeTasks(timeSlice));
        };
        ticker.scheduleAtFixedRate(runnable, 0, 1, timeUnit);
    }

    @Override
    public void shutdown() {
        logger.info("Shutting down");

        leaseService.shutdown();
        ticker.shutdownNow();
        scheduler.shutdownNow();
        workers.shutdown();
        try {
            logger.debug("Waiting for active jobs to finish");
            workers.awaitTermination(1, timeUnit);
        } catch (InterruptedException e) {
            logger.info("The shutdown process has been interrupted. Attempting to forcibly terminate active jobs.");
            workers.shutdownNow();
        }

    }

    public Observable<TaskContainer> findTasks(String type, DateTime timeSlice, int segment) {
        TaskType taskType = findTaskType(type);
        return rxSession.execute(queries.findTasks.bind(type, timeSlice.toDate(), segment))
                .flatMap(Observable::from)
                .map(row -> new TaskContainer(taskType, timeSlice, segment, row.getString(0),
                        row.getSet(1, String.class), row.getInt(2), row.getInt(3), row.getSet(4, Date.class).stream()
                        .map(DateTime::new).collect(toSet())));
    }

    public Observable<TaskContainer> findTasks(Lease lease, TaskType taskType) {
        int start = lease.getSegmentOffset();
        int end = start + taskType.getSegments();
        return Observable.range(start, end).flatMap(i -> findTasks(lease.getTaskType(), lease.getTimeSlice(), i));
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

    private Observable<TaskContainer> rescheduleTask(TaskContainer taskContainer) {
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
        ListenableFuture<TaskContainer> resultFuture = Futures.transform(futures, (List<ResultSet> resultSets) ->
                taskContainer);

        return RxUtil.from(resultFuture, workers);
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
            leaseService.deleteLeases(timeSlice).toBlocking().last();
//            Uninterruptibles.getUninterruptibly(leaseService.deleteLeases(timeSlice));
        } catch (Exception e) {
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
            List<Lease> leases = ImmutableList.copyOf(leaseService.findUnfinishedLeases(timeSlice)
                    .filter(lease -> lease.getTaskType().equals(taskType.getName()))
                    .toBlocking()
                    .toIterable());

            // A CountDownLatch is used to let us know when to query again for leases. We do not want to query (again)
            // for leases until we have gone through each one. If a lease already has an owner, then we just count
            // down the latch and move on. If the lease does not have an owner, we attempt to acquire it. When the
            // result from trying to acquire the lease are available, we count down the latch.
            AtomicReference<CountDownLatch> latchRef = new AtomicReference<>(new CountDownLatch(leases.size()));


            // Keep checking for and process leases as long as the query returns leases and there is at least one
            // that is not finished. When these conditions do not hold, then the leases for the current time slice
            // are finished.
            while (!(leases.isEmpty() || leases.stream().allMatch(Lease::isFinished))) {
                if (Thread.currentThread().isInterrupted()) {
                    logger.info("Execution of {} tasks for time slice {} has been interrupted.", taskType.getName(),
                            timeSlice);
                    break;
                }

                for (final Lease lease : leases) {
                    if (lease.getOwner() == null) {
                        permits.acquire();
                        lease.setOwner(owner);
                        Observable<Boolean> acquiredObservable = leaseService.acquire(lease);

                        acquiredObservable.subscribe(
                                acquired -> {
                                    latchRef.get().countDown();
                                    if (acquired) {
                                        findTasks(lease, taskType)
                                                .map(executeTasksSegment)
                                                .flatMap(this::rescheduleTask)
                                                .flatMap(taskContainer -> rxSession.execute(queries.deleteTasks.bind(
                                                        taskContainer.getTaskType().getName(), timeSlice.toDate(),
                                                        taskContainer.getSegment())))
                                                .subscribe(
                                                        resultSet -> {},
                                                        t -> {
                                                            logger.warn("Failed to delete task segment", t);
                                                            permits.release();
                                                        },
                                                        () -> {
                                                            leaseService.finish(lease).subscribe(
                                                                    finished -> {
                                                                        if (!finished) {
                                                                            logger.warn("All tasks for {} have " +
                                                                                "completed but unable set it to " +
                                                                                "finished", lease);
                                                                        }
                                                                        permits.release();
                                                                    },
                                                                    t -> {
                                                                        logger.warn("Failed to finish lease", t);
                                                                        permits.release();
                                                                    });
                                                        });
                                    } else {
                                        // someone else has the lease so return the permit and try to
                                        // acquire another lease
                                        permits.release();
                                    }
                                },
                                t -> {
                                    logger.warn("There was an error trying to acquire a lease", t);
                                    latchRef.get().countDown();
                                }
                        );
                    } else {
                        latchRef.get().countDown();
                    }
                }
                latchRef.get().await();
                leases = ImmutableList.copyOf(leaseService.findUnfinishedLeases(timeSlice)
                        .filter(lease -> lease.getTaskType().equals(taskType.getName()))
                        .toBlocking()
                        .toIterable());
                latchRef.set(new CountDownLatch(leases.size()));
            }

        } catch (InterruptedException e) {
            logger.info("Execution of " + taskType.getName() + " tasks for time slice " + timeSlice +
                    "was interrupted.", e);
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
    private Func1<TaskContainer, TaskContainer> executeTasksSegment = taskContainer -> {
        if (Thread.currentThread().isInterrupted()) {
            // An interrupt could be due to loss of lease ownership or something else like JVM shutdown. Either
            // way, we need to respond to the interrupt which means cancelling task execution.
            throw new RuntimeException(Thread.currentThread().getName() + " has been interrupted. Cancelling " +
                    "task execution");
        }
        TaskContainer executedTasks = TaskContainer.copyWithoutFailures(taskContainer);
        Consumer<Task> taskRunner = taskContainer.getTaskType().getFactory().get();
        Consumer<Task> wrappedTaskedRunner = wrapTaskRunner.apply(taskRunner);
        try {
            taskContainer.forEach(wrappedTaskedRunner::accept);
        } catch (TaskExecutionException e) {
            logger.warn("Failed to execute " + e.getFailedTask());
            executedTasks.getFailedTimeSlices().add(e.getFailedTask().getTimeSlice());
        }
        return executedTasks;
    };

//    private Function<List<TaskContainer>, List<TaskContainer>> executeTasksSegment = taskContainers -> {
//        List<TaskContainer> results = new ArrayList<>(taskContainers.size());
//        taskContainers.forEach(taskContainer -> {
//            if (Thread.currentThread().isInterrupted()) {
//                // An interrupt could be due to loss of lease ownership or something else like JVM shutdown. Either
//                // way, we need to respond to the interrupt which means cancelling task execution.
//                throw new RuntimeException(Thread.currentThread().getName() + " has been interrupted. Cancelling " +
//                    "task execution");
//            }
//            TaskContainer executedTasks = TaskContainer.copyWithoutFailures(taskContainer);
//            Consumer<Task> taskRunner = taskContainer.getTaskType().getFactory().get();
//            Consumer<Task> wrappedTaskedRunner = wrapTaskRunner.apply(taskRunner);
//            try {
//                taskContainer.forEach(wrappedTaskedRunner::accept);
//            } catch (TaskExecutionException e) {
//                logger.warn("Failed to execute " + e.getFailedTask());
//                executedTasks.getFailedTimeSlices().add(e.getFailedTask().getTimeSlice());
//            }
//            results.add(executedTasks);
//        });
//        return results;
//    };

//    private AsyncFunction<List<TaskContainer>, List<DateTime>> scheduleNextExecution = results ->
//        Futures.allAsList(results.stream().map(this::rescheduleTask).collect(toList()));

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
                // There are multiple failure paths including losing lease ownership, failure to delete task segment,
                // or failure to mark lease finished. We do not have a good way of determining the exact cause without
                // do something like registering additional callbacks with Futures.addCallback or Futures.withFallback.
                // Neither is particular appealing as it makes the code more complicated. This is one of a growing
                // number of reasons I want to prototype a solution using RxJava.
                logger.warn("Failed to process tasks for " + lease, t);
                permits.release();
            }
        };
    }

}
