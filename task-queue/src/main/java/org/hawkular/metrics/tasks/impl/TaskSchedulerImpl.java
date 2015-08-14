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

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.hawkular.metrics.tasks.DateTimeService;
import org.hawkular.metrics.tasks.api.RepeatingTrigger;
import org.hawkular.metrics.tasks.api.SingleExecutionTrigger;
import org.hawkular.metrics.tasks.api.Task2;
import org.hawkular.metrics.tasks.api.TaskScheduler;
import org.hawkular.metrics.tasks.api.Trigger;
import org.hawkular.rx.cassandra.driver.RxSession;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import rx.Observable;
import rx.Scheduler;
import rx.Subscriber;
import rx.Subscription;
import rx.functions.Action1;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;

/**
 * @author jsanda
 */
public class TaskSchedulerImpl implements TaskScheduler {

    private static Logger logger = LoggerFactory.getLogger(TaskSchedulerImpl.class);

    public static final int DEFAULT_LEASE_TTL = 180;

    private int numShards = Integer.parseInt(System.getProperty("hawkular.scheduler.shards", "10"));

    private HashFunction hashFunction = Hashing.murmur3_128();

    private RxSession session;

    private Queries queries;

    /**
     * Runs a single job on a tight loop. The sole purpose of that job is to emit a tick on
     * the leases thread pool. Each tick represents a time slice to be processed. Emission
     * of ticks is very fast as it just involves queueing up the tick on the leases thread
     * pool. It needs to be fast and non-blocking to ensure that do not skip a time slice.
     */
    private ScheduledExecutorService tickExecutor;

    private Scheduler tickScheduler;

    /**
     * When a tick is emitted, a job is submitted onto the leases thread pool to process
     * leases for the corresponding time slice. The leases thread pool contains only a
     * single thread to ensure we process leases/tasks in order with respect to time.
     */
    private ExecutorService leaseExecutor;

    /**
     * The thread pool in which task execution is performed. An instance of
     * TaskSchedulerImpl will execute multiple tasks in parallel provided they all share
     * the same lease.
     */
    private ExecutorService tasksExecutor;

    private Scheduler tasksScheduler;

    private Scheduler leaseScheduler;

    private DateTimeService dateTimeService;

    private boolean running;

    /**
     * A subject to broadcast tasks that are to be executed. Other task scheduling libraries
     * and frameworks have a more tight coupling with the objects that perform the actual
     * task execution. The pub/sub style makes things more loosely coupled. There are a
     * couple benefits. First, it gives clients full control over the life cycle of the
     * objects doing the task execution. Secondly, it makes writing tests easier.
     */
    private PublishSubject<Task2> taskSubject;

    private PublishSubject<Long> tickSubject;

    private Subscription leasesSubscription;

    public TaskSchedulerImpl(RxSession session, Queries queries) {
        this.session = session;
        this.queries = queries;

        dateTimeService = new DateTimeService();

        tickExecutor = Executors.newScheduledThreadPool(1,
                new ThreadFactoryBuilder().setNameFormat("ticker-pool-%d").build());
        tickScheduler = Schedulers.from(tickExecutor);
        leaseExecutor = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat("lease-pool-%d").build());

        tasksExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(),
                new ThreadFactoryBuilder().setNameFormat("tasks-pool-%d").build());
        tasksScheduler = Schedulers.from(tasksExecutor);
        leaseScheduler = Schedulers.from(leaseExecutor);

        taskSubject = PublishSubject.create();
        tickSubject = PublishSubject.create();
    }

    public void setTickScheduler(Scheduler scheduler) {
        this.tickScheduler = scheduler;
    }

    private class SubscriberWrapper extends Subscriber<Task2> {

        private Subscriber<Task2> delegate;

        public SubscriberWrapper(Subscriber<Task2> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void onCompleted() {
            delegate.onCompleted();
        }

        @Override
        public void onError(Throwable e) {
            delegate.onError(e);
        }

        @Override
        public void onNext(Task2 task2) {
            try {
                delegate.onNext(task2);
            } catch (Exception e) {
                logger.warn("Execution of {} failed", task2);
            }
        }
    }

    /**
     * Subscribe a callback that will be responsible for executing tasks.
     *
     * @param onNext The task execution callback
     * @return A subscription which can be used to stop receiving task notifications.
     */
    @Override
    public Subscription subscribe(Action1<Task2> onNext) {
        return taskSubject.subscribe(onNext);
    }

    /**
     * Subscribe a callback that will be responsible for executing tasks.
     *
     * @param subscriber The callback
     * @return A subscription which can be used to stop receiving task notifications.
     */
    @Override
    public Subscription subscribe(Subscriber<Task2> subscriber) {
        return taskSubject.subscribe(new SubscriberWrapper(subscriber));
    }

    @Override
    public Observable<Long> getFinishedTimeSlices() {
        return tickSubject;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public Observable<Task2> getTasks() {
        return taskSubject;
    }

    /**
     * Starts the scheduler so that it starts emitting tasks for execution.
     *
     * @return An observable that emits leases that are processed by this TaskScheduler
     * object. The observable emits a lease only after it has been fully processed. This
     * means all tasks belonging to the task have been executed, and the lease has been
     * marked finished. Note that the observable is hot; in other words, it emits leases
     * regardless of wehther or not there are any subscriptions.
     */
    @Override
    public Observable<Lease> start() {
        Observable<Date> seconds = createTicks();
        Observable<Lease> leases = seconds.flatMap(this::getAvailableLeases);
        Observable<Lease> processedLeases = Observable.create(subscriber -> {
            leasesSubscription = leases.subscribe(
                    lease -> {
                        logger.debug("Loading tasks for {}", lease);
                        CountDownLatch latch = new CountDownLatch(1);
                        getQueue(lease)
                                .observeOn(tasksScheduler)
                                .groupBy(Task2Impl::getGroupKey)
                                .flatMap(group -> group.flatMap(this::execute).map(this::rescheduleTask))
                                .subscribe(
                                        task -> logger.debug("Finished executing {}", task),
                                        t -> logger.warn("There was an error observing tasks", t),
                                        () -> {
                                            Date timeSlice = new Date(lease.getTimeSlice());
                                            // TODO We need error handling here
                                            // We do not want to mark the lease finished if deleting the task partition
                                            // fails. If either delete fails, we probably want to employ some retry
                                            // policy. If the failures continue, then we probably need to shut down the
                                            // scheduler because Cassandra is unstable.
                                            Observable.merge(
                                                    session.execute(queries.deleteTasks.bind(timeSlice,
                                                                    lease.getShard()), tasksScheduler),
                                                    session.execute(queries.finishLease.bind(timeSlice,
                                                                    lease.getShard()), tasksScheduler)
                                            ).subscribe(
                                                    resultSet -> {},
                                                    t -> {
                                                        logger.warn("There was an error during post-task processing",
                                                                t);
                                                        subscriber.onError(t);
                                                    },
                                                    () -> {
                                                        logger.debug("Finished executing tasks for {}", lease);
                                                        latch.countDown();
                                                        subscriber.onNext(lease);
                                                    }
                                            );
                                        }
                                );
                        logger.debug("Started processing tasks for {}", lease);
                        try {
                            // While using a CountDownLatch might seem contrary to RxJava, we
                            // want to block here until all tasks have finished executing. We
                            // We only want to acquire another lease and execute its tasks after
                            // we are finished with the current lease. If we do not block here,
                            // then we immediately start polling for another lease. We do
                            // not want to try and acquire another lease until we have
                            // finished with the tasks for the current lease.
                            latch.await();
                            logger.debug("Done waiting!");
                        } catch (InterruptedException e) {
                            logger.warn("Interrupted waiting for task execution to complete", e);
                        }
                    },
                    t -> logger.warn("There was an error observing leases", t),
                    () -> {
                        logger.debug("Finished observing leases");
                        subscriber.onCompleted();
                    }
            );
        });
        // We emit leases using a subject in order to make our observable hot. We want to
        // process/emit leases regardless of whether or not there are any subscribers. Note
        // that having an observable emit leases helps facilitate testing, and that was the
        // primary motivation for having this method return a hot observable.
        PublishSubject<Lease> leasesSubject = PublishSubject.create();
        processedLeases.subscribe(leasesSubject);
        running = true;
        return leasesSubject;
    }

    /**
     * <p>
     * Returns an observable that emits "ticks" in the form of {@link Date} objects every
     * minute. Each tick represents a time slice to be processed.
     * </p>
     * <p>
     * <strong>Note:</strong> Ticks must be emitted on the tick scheduler and observed on
     * the lease scheduler. No other work should run on the tick scheduler. This is to help
     * ensure nothing blocks ticks from being emitted every second.
     * </p>
     */
    // TODO We probably still need a check in place to make sure we don't skip a second
    private Observable<Date> createTicks() {
        // TODO handle back pressure
        // The timer previously emitted ticks every second, but it has been changed to
        // emit every minute to avoid back pressure. Emitting ticks less frequently
        // should help a lot but it does not completely avoid the problem. It only
        // takes one really long running task to cause back pressure. We will need to
        // figure something out.
        return Observable.timer(0, 1, TimeUnit.MINUTES, tickScheduler)
                .map(tick -> currentTimeSlice())
                .takeUntil(d -> !running)
                .doOnNext(tick -> logger.debug("Tick {}", tick))
                .observeOn(leaseScheduler);
    }

    /**
     * <p>
     * Creates an observable that emits acquired leases for the specified time slice. The
     * observable queries for available leases and emits the first one it acquires. After
     * the lease has been emitted, the observable "refreshes" (i.e., reloads them from the
     * database) the set of available leases since they can and will change when there are
     * multiple TaskScheduler instances running. The suscriber's
     * {@link Subscriber#onCompleted() onCompleted} method is called when all leases for
     * the time slice have been processed.
     * </p>
     * <p>
     * <strong>Note:</strong> The observable returned from this method must run on the
     * leases scheduler to ensure that tasks are processed in order with respect to time.
     * </p>
     */
    private Observable<Lease> getAvailableLeases(Date timeSlice) {
        Observable<Lease> observable = Observable.create(subscriber -> {
            // This observable is intentionally blocking. The queries that it executes are
            // NOT async by design. We want to process lease serially, one at a time. Once
            // we acquire a lease, we execute all of the tasks for the lease. We check for
            // available leases again only after those tasks have completed.
            try {
                logger.debug("Loading leases for {}", timeSlice);
                logger.debug("Timestamp is {}", timeSlice.getTime());
                List<Lease> leases = findAvailableLeases(timeSlice);
                while (!leases.isEmpty()) {
                    for (Lease lease : leases) {
                        if (acquire(lease)) {
                            logger.debug("Acquired {}", lease);
                            subscriber.onNext(lease);
                            logger.debug("Finished with {}", lease);
                        }
                        break;
                    }
                    logger.debug("Looking for available leases");
                    leases = findAvailableLeases(timeSlice);
                }
                logger.debug("No more leases to process for {}", timeSlice);
                // TODO we do not want to perform a delete if there are no leases for the time slice
                session.execute(queries.deleteLeases.bind(timeSlice)).toBlocking().first();
                subscriber.onCompleted();
                tickSubject.onNext(timeSlice.getTime());
            } catch (Exception e) {
                subscriber.onError(e);
            }
        });
        return observable;//.observeOn(leaseScheduler);
    }

    /**
     * Returns leases for the specified time slice that are not yet finished.
     */
    private List<Lease> findAvailableLeases(Date timeSlice) {
        // Normally our queries are async, but we want this to sync/blocking. This method
        // is called from the available leases observable which serializes its execution.
        return session.execute(queries.findLeases.bind(timeSlice))
                .flatMap(Observable::from)
                .map(row -> new Lease(timeSlice.getTime(), row.getInt(0), row.getString(1), row.getBool(2)))
                .filter(lease -> !lease.isFinished() && lease.getOwner() == null)
                .toList()
                .toBlocking()
                .firstOrDefault(Collections.<Lease>emptyList());
    }

    /**
     * Attempts to acquire a lease.
     */
    private boolean acquire(Lease lease) {
        return session.execute(queries.acquireLease.bind(DEFAULT_LEASE_TTL, "localhost", new Date(lease.getTimeSlice()),
                lease.getShard())).map(ResultSet::wasApplied).toBlocking().firstOrDefault(false);
    }

    /**
     * Loads the task queue for the specified lease. The returned observable emits tasks in
     * the queue. The observable should execute on the lease scheduler.
     */
    Observable<Task2Impl> getQueue(Lease lease) {
        logger.debug("Loading task queue for {}", lease);
        return session.execute(queries.getTasksFromQueue.bind(new Date(lease.getTimeSlice()), lease.getShard()),
                Schedulers.immediate())
                .flatMap(Observable::from)
                .map(row -> new Task2Impl(row.getUUID(2), row.getString(0), row.getInt(1), row.getString(3),
                        row.getMap(4, String.class, String.class), getTrigger(row.getUDTValue(5))));
    }

    /**
     * Creates an observable that emits a task that has been executed. Task execution is
     * accomplished by publishing the task. This method is somewhat of a hack because it
     * is really just for side effects. We want tasks from different groups to execute in
     * parallel. Execution of tasks within the same group should be serialized based on
     * their specified order. If the tasks have the same order, they can be executed in
     * parallel. The observable should run on the tasks scheduler.
     *
     * @param task The task to emit for execution
     * @return An observable that emits a task once it has been executed.
     */
    private Observable<Task2Impl> execute(Task2Impl task) {
        Observable<Task2Impl> observable = Observable.create(subscriber -> {
            logger.debug("Emitting {} for execution", task);
            // This onNext call is to perform the actual task execution
            taskSubject.onNext(task);
            // This onNext call is for data flow. After the task executes, we call
            // this onNext so that the task gets rescheduled.
            subscriber.onNext(task);
            subscriber.onCompleted();
            logger.debug("Finished executing {}", task);

        });
        // Subscribe on the same scheduler thread to make sure tasks within the same group
        // execute in order.
        return observable.subscribeOn(Schedulers.immediate());
    }

    @Override
    public void shutdown() {
        try {
            logger.debug("shutting down");
            running = false;

            if (leasesSubscription != null) {
                leasesSubscription.unsubscribe();
            }

            tasksExecutor.shutdown();
            tasksExecutor.awaitTermination(5, TimeUnit.SECONDS);

            leaseExecutor.shutdown();
            leaseExecutor.awaitTermination(5, TimeUnit.SECONDS);

            tickExecutor.shutdown();
            tickExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted during shutdown", e);
        }
    }

    private Date currentTimeSlice() {
//        return dateTimeService.getTimeSlice(now(), Duration.standardSeconds(1)).toDate();
        return dateTimeService.getTimeSlice(new DateTime(tickScheduler.now()), Duration.standardMinutes(1)).toDate();
    }

//    @Override
//    public Observable<Task2> createTask(String name, Map<String, String> parameters, Trigger trigger) {
//        UUID id = UUID.randomUUID();
//        int shard = computeShard(id);
//        UDTValue triggerUDT = getTriggerValue(session, trigger);
//
//        return Observable.create(subscriber ->
//                        session.execute(queries.createTask2.bind(id, shard, name, parameters, triggerUDT)).subscribe(
//                                resultSet -> subscriber.onNext(new Task2Impl(id, shard, name, parameters, trigger)),
//                                t -> subscriber.onError(new RuntimeException("Failed to create task", t)),
//                                subscriber::onCompleted
//                        )
//        );
//    }

    public Observable<Task2> findTask(UUID id) {
        return Observable.create(subscriber ->
            session.execute(queries.findTask.bind(id)).flatMap(Observable::from).subscribe(
                    row -> subscriber.onNext(new Task2Impl(id, row.getString(0), row.getInt(1), row.getString(2),
                            row.getMap(3, String.class, String.class), getTrigger(row.getUDTValue(4)))),
                    t -> subscriber.onError(new RuntimeException("Failed to find task with id " + id, t)),
                    subscriber::onCompleted
            )
        );
    }

    @Override
    public Observable<Task2> scheduleTask(String name, String groupKey, int executionOrder,
            Map<String, String> parameters, Trigger trigger) {

        UUID id = UUID.randomUUID();
        int shard = computeShard(groupKey);
        UDTValue triggerUDT = getTriggerValue(session, trigger);
        Date timeSlice = new Date(trigger.getTriggerTime());
        Task2Impl task = new Task2Impl(id, groupKey, executionOrder, name, parameters, trigger);

        logger.debug("Scheduling {}", task);

        Observable<ResultSet> createTask = session.execute(queries.createTask2.bind(id, groupKey, executionOrder, name,
                parameters, triggerUDT));
        Observable<ResultSet> updateQueue = session.execute(queries.insertIntoQueue.bind(timeSlice, shard, id, groupKey,
                executionOrder, name, parameters, triggerUDT));
        Observable<ResultSet> createLease = session.execute(queries.createLease.bind(timeSlice, shard));

        return Observable.create(subscriber ->
            Observable.merge(createTask, updateQueue, createLease).subscribe(
                    resultSet -> {},
                    t -> subscriber.onError(new RuntimeException("Failed to schedule task [" + task + "]", t)),
                    () -> {
                        try {
                            subscriber.onNext(task);
                            subscriber.onCompleted();
                        } catch (Throwable t) {
                            subscriber.onError(t);
                        }
                    }
            )
        );
    }

    /**
     * Reschedules the task for subsequent execution. The current implementation assumes
     * repeating triggers. We need to update this method to handling single-execution
     * triggers. The task is inserted into the new queue and a new lease is created. This
     * observabe should execute on the tasks scheduler.
     *
     * @param task The task to be rescheduled.
     * @return An observable that emits the rescheduled task which contains the new/next
     *         trigger. {@link rx.Observer#onNext(Object) onNext} is invoked after the
     *         database queries for updating the queue and creating the lease have
     *         completed.
     */
    private Observable<Task2Impl> rescheduleTask(Task2Impl task) {
        Trigger nextTrigger = task.getTrigger().nextTrigger();
        if (nextTrigger == null) {
            logger.debug("There are no more executions for {}", task);
            return Observable.just(task);
        }
        int shard = computeShard(task.getGroupKey());
        Task2Impl newTask = new Task2Impl(task.getId(), task.getGroupKey(), task.getOrder(), task.getName(),
                task.getParameters(), task.getTrigger().nextTrigger());
        UDTValue triggerUDT = getTriggerValue(session, newTask.getTrigger());
        Date timeSlice = new Date(newTask.getTrigger().getTriggerTime());

        logger.debug("Next execution time for Task2Impl{id=" + newTask.getId() + ", name=" + newTask.getName() +
                "} is " + new Date(newTask.getTrigger().getTriggerTime()));

        Observable<ResultSet> updateQueue = session.execute(queries.insertIntoQueue.bind(timeSlice, shard,
                newTask.getId(), task.getGroupKey(), task.getOrder(), newTask.getName(), newTask.getParameters(),
                triggerUDT), tasksScheduler);
        Observable<ResultSet> createLease = session.execute(queries.createLease.bind(timeSlice, shard), tasksScheduler);

        Observable<Task2Impl> observable = Observable.create(subscriber -> {
            Observable.merge(updateQueue, createLease).subscribe(
                    resultSet -> {
                    },
                    // TODO handle rescheduling failure
                    // If either write fails, we treat it as a scheduling failure. We cannot
                    // delete the current task/lease until these writes succeed. I think we
                    // should try again after some delay, and if we continue to fail, then
                    // we shut down the scheduler.
                    t -> subscriber.onError(new RuntimeException("Failed to reschedule " + newTask, t)),
                    () -> {
                        try {
                            logger.debug("Received result set for reschedule task, {}", newTask);
                            subscriber.onNext(newTask);
                            subscriber.onCompleted();
                        } catch (Exception e) {
                            subscriber.onError(e);
                        }
                    }
            );
        });
        return observable;//.observeOn(Schedulers.immediate());
    }

    int computeShard(String key) {
        HashCode hashCode = hashFunction.hashBytes(key.getBytes());
        return Hashing.consistentHash(hashCode, numShards);
    }

    private static KeyspaceMetadata getKeyspace(RxSession session) {
        return session.getCluster().getMetadata().getKeyspace(session.getLoggedKeyspace());
    }

    static Trigger getTrigger(UDTValue value) {
        int type = value.getInt("type");

        switch (type) {
            case 0:
                return new SingleExecutionTrigger(value.getLong("trigger_time"));
            case 1:
                return new RepeatingTrigger(
                        value.getLong("interval"),
                        value.getLong("delay"),
                        value.getLong("trigger_time"),
                        value.getInt("repeat_count"),
                        value.getInt("execution_count")
                );
            default:
                throw new IllegalArgumentException("Trigger type [" + type + "] is not supported");
        }
    }

    static UDTValue getTriggerValue(RxSession session, Trigger trigger) {
        if (trigger instanceof RepeatingTrigger) {
            return getRepeatingTriggerValue(session, (RepeatingTrigger) trigger);
        }
        if (trigger instanceof SingleExecutionTrigger) {
            return getSingleExecutionTriggerValue(session, (SingleExecutionTrigger) trigger);
        }
        throw new IllegalArgumentException(trigger.getClass() + " is not a supported trigger type");
    }

    static UDTValue getSingleExecutionTriggerValue(RxSession session, SingleExecutionTrigger trigger) {
        UserType triggerType = getKeyspace(session).getUserType("trigger_def");
        UDTValue triggerUDT = triggerType.newValue();
        triggerUDT.setInt("type", 0);
        triggerUDT.setLong("trigger_time", trigger.getTriggerTime());

        return triggerUDT;
    }

    static UDTValue getRepeatingTriggerValue(RxSession session, RepeatingTrigger trigger) {
        UserType triggerType = getKeyspace(session).getUserType("trigger_def");
        UDTValue triggerUDT = triggerType.newValue();
        triggerUDT.setInt("type", 1);
        triggerUDT.setLong("interval", trigger.getInterval());
        triggerUDT.setLong("trigger_time", trigger.getTriggerTime());
        if (trigger.getDelay() > 0) {
            triggerUDT.setLong("delay", trigger.getDelay());
        }
        if (trigger.getRepeatCount() != null) {
            triggerUDT.setInt("repeat_count", trigger.getRepeatCount());
            triggerUDT.setInt("execution_count", trigger.getExecutionCount());
        }

        return triggerUDT;
    }
}
