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
import static org.joda.time.Duration.standardHours;
import static org.joda.time.Duration.standardMinutes;
import static org.joda.time.Duration.standardSeconds;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.ResultSet;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.hawkular.metrics.schema.SchemaManager;
import org.hawkular.metrics.tasks.DateTimeService;
import org.hawkular.metrics.tasks.api.Task;
import org.hawkular.metrics.tasks.api.TaskService;
import org.hawkular.metrics.tasks.api.TaskType;
import org.hawkular.rx.cassandra.driver.RxSession;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func2;
import rx.subjects.PublishSubject;

/**
 * @author jsanda
 */
public class TaskServiceImpl implements TaskService {

    private static final Logger logger = LoggerFactory.getLogger(TaskServiceImpl.class);

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

    private Map<TaskType, PublishSubject<Task>> subjects = new HashMap<>();

    public TaskServiceImpl(RxSession session, Queries queries, LeaseService leaseService, List<TaskType> taskTypes) {
        try {
            this.rxSession = session;
            this.queries = queries;
            this.leaseService = leaseService;
            this.taskTypes = taskTypes;
            dateTimeService = new DateTimeService();
            owner = InetAddress.getLocalHost().getHostName();

            taskTypes.forEach(taskType -> subjects.put(taskType, PublishSubject.<Task>create()));

            SchemaManager schemaManager = new SchemaManager(session.getSession());
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
        logger.info("Using time unit of {}", timeUnit);
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

    public Subscription subscribe(TaskType taskType, final Action1<? super Task> onNext,
            final Action1<Throwable> onError, final Action0 onComplete) {
        PublishSubject<Task> subject = subjects.get(taskType);
        if (subject == null) {
            throw new IllegalArgumentException(taskType + " is not a recognized task type");
        }
        return subject.subscribe(onNext, onError, onComplete);
    }

    @Override
    public Subscription subscribe(TaskType taskType, Action1<? super Task> onNext) {
        PublishSubject<Task> subject = subjects.get(taskType);
        if (subject == null) {
            throw new IllegalArgumentException(taskType + " is not a recognized task type");
        }
        return subject.subscribe(onNext);
    }

    public Observable<TaskContainer> findTasks(String type, DateTime timeSlice, int segment) {
        TaskType taskType = findTaskType(type);
        return rxSession.execute(queries.findTasks.bind(type, timeSlice.toDate(), segment))
                .flatMap(Observable::from)
                .map(row -> new TaskContainer(taskType, row.getString(0), timeSlice, segment, row.getString(1),
                        row.getSet(2, String.class), row.getInt(3), row.getInt(4), row.getSet(5, Date.class).stream()
                        .map(DateTime::new).collect(toSet())));
    }

    public Observable<TaskContainer> findTasks(Lease lease, TaskType taskType) {
//        int start = lease.getSegmentOffset();
//        int end = start + taskType.getSegments();
//        return Observable.range(start, end).flatMap(i -> findTasks(lease.getTaskType(), lease.getTimeSlice(), i));
        return null;
    }

    private TaskType findTaskType(String type) {
        return taskTypes.stream()
                .filter(t->t.getName().equals(type))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(type + " is not a recognized task type"));
    }

    @Override
    public Observable<Task> scheduleTask(DateTime time, Task task) {
        TaskType taskType = findTaskType(task.getTaskType().getName());

        DateTime currentTimeSlice = dateTimeService.getTimeSlice(time, getDuration(task.getInterval()));
        DateTime timeSlice = currentTimeSlice.plus(getDuration(task.getInterval()));

        return scheduleTaskAt(timeSlice, task).map(scheduledTime -> new TaskImpl(task.getTaskType(), task.getTenantId(),
                scheduledTime, task.getTarget(), task.getSources(), task.getInterval(), task.getWindow()));
    }

    private Observable<TaskContainer> rescheduleTask(TaskContainer taskContainer) {
        TaskType taskType = taskContainer.getTaskType();
        DateTime nextTimeSlice = taskContainer.getTimeSlice().plus(getDuration(taskContainer.getInterval()));
        int segment = Math.abs(taskContainer.getTarget().hashCode() % taskType.getSegments());
        int segmentsPerOffset = taskType.getSegments() / taskType.getSegmentOffsets();
        int segmentOffset = (segment / segmentsPerOffset) * segmentsPerOffset;
        Observable<ResultSet> queueObservable;

        if (taskContainer.getFailedTimeSlices().isEmpty()) {
            queueObservable = rxSession.execute(queries.createTask.bind(taskType.getName(), taskContainer.getTenantId(),
                    nextTimeSlice.toDate(), segment, taskContainer.getTarget(), taskContainer.getSources(),
                    taskContainer.getInterval(), taskContainer.getWindow()));
        } else {
            queueObservable = rxSession.execute(queries.createTaskWithFailures.bind(taskType.getName(),
                    taskContainer.getTenantId(), nextTimeSlice.toDate(), segment, taskContainer.getTarget(),
                    taskContainer.getSources(), taskContainer.getInterval(), taskContainer.getWindow(),
                    toDates(taskContainer.getFailedTimeSlices())));
        }
        Observable<ResultSet> leaseObservable = rxSession.execute(queries.createLease.bind(nextTimeSlice.toDate(),
                taskType.getName(), segmentOffset));

        return Observable.create(subscriber ->
                        queueObservable.concatWith(leaseObservable).subscribe(
                                resultSet -> {
                                },
                                subscriber::onError,
                                () -> {
                                    subscriber.onNext(taskContainer);
                                    subscriber.onCompleted();
                                })
        );
    }

    private Set<Date> toDates(Set<DateTime> times) {
        return times.stream().map(DateTime::toDate).collect(toSet());
    }

    private Observable<DateTime> scheduleTaskAt(DateTime time, Task task) {
        TaskType taskType = task.getTaskType();
        int segment = Math.abs(task.getTarget().hashCode() % taskType.getSegments());
        int segmentsPerOffset = taskType.getSegments() / taskType.getSegmentOffsets();
        int segmentOffset = (segment / segmentsPerOffset) * segmentsPerOffset;

        Observable<ResultSet> queueObservable = rxSession.execute(queries.createTask.bind(taskType.getName(),
                task.getTenantId(), time.toDate(), segment, task.getTarget(), task.getSources(), task.getInterval(),
                task.getWindow()));
        Observable<ResultSet> leaseObservable = rxSession.execute(queries.createLease.bind(time.toDate(),
                taskType.getName(), segmentOffset));

        return Observable.create(subscriber -> queueObservable.concatWith(leaseObservable).subscribe(
                        resultSet -> {
                        },
                        subscriber::onError,
                        () -> {
                            subscriber.onNext(time);
                            subscriber.onCompleted();
                        }
                )
        );
    }

    /**
     * This method is visible for testing. It is not part of the {@link TaskService} API. It runs on the scheduler
     * thread and is blocking. Task execution is done in parallel in the workers thread pool, but the scheduler thread
     * executing this method blocks until all tasks for the time slice are finished.
     *
     * @param timeSlice The time slice to process
     */
    void executeTasks(DateTime timeSlice) {
//        try {
//         // Execute tasks in order of task types. Once all of the tasks are executed, we delete the lease partition.
//            taskTypes.forEach(taskType -> executeTasks(timeSlice, taskType));
//            leaseService.deleteLeases(timeSlice).toBlocking().lastOrDefault(null);
//        } catch (Exception e) {
//            logger.warn("Failed to delete lease partition for time slice " + timeSlice, e);
//        }
    }

    /**
     * This method does not return until all tasks of the specified type have been executed.
     *
     * @param timeSlice
     * @param taskType
     */
    private void executeTasks(DateTime timeSlice, TaskType taskType) {
        logger.debug("Executing tasks for time slice {}", timeSlice);

        // I know, I know. We should not have to used CountDownLatch with RxJava. It is
        // left over from the original implementation and was/is used to ensure tasks of
        // one type finish executing before we start executing tasks of the next type.
        CountDownLatch latch = new CountDownLatch(1);

        // We need logic for error handling as there are a number of different failure
        // scenarios that we have to support including lease renewal failure, failure to
        // reschedule tasks, failure to delete task segments, and failure to mark leases
        // finished.

        // There can be multiple tasks per segment and multiple task segments per lease.
        // A row in the task_queue table represents one or more task executions. Once all
        // of the tasks for a segment have been executed, we delete the task_queue
        // partition. When all of the segments for a lease have processed, we set the
        // finished flag of the lease to true. When all of the leases for the time slice
        // being processed are "finished", we delete the leases partition. Then we are done.

//        leaseService.findUnfinishedLeases(timeSlice)
//                .filter(lease -> lease.getTaskType().equals(taskType.getName()))
//                .flatMap(lease -> findTasks(lease, taskType)
//                        .map(TaskContainer::copyWithoutFailures)
//                        .flatMap(container -> Observable.from(container)
//                                .reduce(container, executeTask))
//                        .flatMap(this::rescheduleTask)
//                        .flatMap(this::deleteTaskSegment)
//                        .flatMap(resultSet -> leaseService.finish(lease)
//                                .map(lease::setFinished)))
//                .subscribe(
//                        lease -> {
//                            // TODO should this be treated as a failure situation?
//                            if (!lease.isFinished()) {
//                                logger.warn("Failed to mark {} finished", lease);
//                            }
//                        },
//                        t -> logger.warn("Task execution failed", t),
//                        latch::countDown);
//
//
//        try {
//            latch.await();
//        } catch (InterruptedException e) {
//            logger.warn("There was an interrupt waiting for task execution of type " + taskType.getName() +
//                            "to complete for time slice " + timeSlice, e);
//        }
    }

    private Func2<TaskContainer, Task, TaskContainer> executeTask = (container, task) -> {
        PublishSubject<Task> subject = subjects.get(task.getTaskType());
        try {
            subject.onNext(task);
        } catch (Exception e) {
            logger.warn("Execution of " + task + " failed", e);
            container.getFailedTimeSlices().add(task.getTimeSlice());
        }
        return container;
    };

    private Observable<ResultSet> deleteTaskSegment(TaskContainer taskContainer) {
        return rxSession.execute(queries.deleteTasks.bind(taskContainer.getTaskType()
                .getName(), taskContainer.getTimeSlice().toDate(), taskContainer.getSegment()));
    }

    private Duration getDuration(int duration) {
        switch (timeUnit) {
            case SECONDS: return standardSeconds(duration);
            case MINUTES: return standardMinutes(duration);
            case HOURS: return standardHours(duration);
            default: throw new IllegalArgumentException(timeUnit + " is not a supported time unit");
        }
    }

}
