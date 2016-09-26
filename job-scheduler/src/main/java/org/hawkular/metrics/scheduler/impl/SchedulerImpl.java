/*
 * Copyright 2014-2016 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.scheduler.impl;

import static org.hawkular.metrics.datetime.DateTimeService.currentMinute;
import static org.hawkular.metrics.datetime.DateTimeService.getTimeSlice;
import static org.hawkular.metrics.datetime.DateTimeService.now;
import static org.joda.time.Minutes.minutes;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.hawkular.metrics.scheduler.api.JobDetails;
import org.hawkular.metrics.scheduler.api.RepeatingTrigger;
import org.hawkular.metrics.scheduler.api.RetryPolicy;
import org.hawkular.metrics.scheduler.api.Scheduler;
import org.hawkular.metrics.scheduler.api.SingleExecutionTrigger;
import org.hawkular.metrics.scheduler.api.Trigger;
import org.hawkular.rx.cassandra.driver.RxSession;
import org.jboss.logging.Logger;
import org.joda.time.DateTime;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import rx.Completable;
import rx.Observable;
import rx.Single;
import rx.functions.Action0;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;
import rx.subscriptions.BooleanSubscription;

/**
 * @author jsanda
 */
public class SchedulerImpl implements Scheduler {

    private static Logger logger = Logger.getLogger(SchedulerImpl.class);

    private Map<String, Func1<JobDetails, Completable>> jobFactories;

    private Map<String, Func2<JobDetails, Throwable, RetryPolicy>> retryFunctions;

    private ScheduledExecutorService tickExecutor;

    private rx.Scheduler tickScheduler;

    private ExecutorService queryExecutor;

    private rx.Scheduler queryScheduler;

    private RxSession session;

    private PreparedStatement insertJob;

    private PreparedStatement insertScheduledJob;

    private PreparedStatement findScheduledJobs;

    private PreparedStatement deleteScheduledJobs;

    private PreparedStatement findFinishedJobs;

    private PreparedStatement deleteFinishedJobs;

    private PreparedStatement updateJobToFinished;

    private PreparedStatement findJob;

    private PreparedStatement addActiveTimeSlice;

    private PreparedStatement findActiveTimeSlices;

    private PreparedStatement deleteActiveTimeSlice;

    private LockManager lockManager;

    private boolean running;

    private AtomicInteger ticks = new AtomicInteger();

    private static Func2<JobDetails, Throwable, RetryPolicy> NO_RETRY = (details, throwable) -> RetryPolicy.NONE;

    static final String QUEUE_LOCK_PREFIX = "org.hawkular.metrics.scheduler.queue.";

    static final String SCHEDULING_LOCK = "scheduling";

    static final String TIME_SLICE_EXECUTION_LOCK = "executing";

    static final String JOB_EXECUTION_LOCK = "locked";

    // TODO We probably want this to be configurable
    static final int SCHEDULING_LOCK_TIMEOUT_IN_SEC = 5;

    // TODO We probably want this to be configurable
    static final int JOB_EXECUTION_LOCK_TIMEOUT_IN_SEC = 3600;

    /**
     * Test hook. See {@link #setTimeSlicesSubject(PublishSubject)}.
     */
    private Optional<PublishSubject<Date>> finishedTimeSlices;

    private Optional<PublishSubject<JobDetails>> jobFinished;

    private final Object lock = new Object();

    public SchedulerImpl(RxSession session) {
        this.session = session;
        jobFactories = new HashMap<>();
        retryFunctions = new HashMap<>();
        tickExecutor = Executors.newScheduledThreadPool(1,
                new ThreadFactoryBuilder().setNameFormat("ticker-pool-%d").build());
        tickScheduler = Schedulers.from(tickExecutor);

        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("query-thread-pool-%d").build();
        queryExecutor = new ThreadPoolExecutor(getQueryThreadPoolSize(), getQueryThreadPoolSize(), 0,
                TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), threadFactory,
                new ThreadPoolExecutor.DiscardPolicy());
        queryScheduler = Schedulers.from(queryExecutor);

        lockManager = new LockManager(session);

        insertJob = session.getSession().prepare(
                "INSERT INTO jobs (id, type, name, params, trigger) VALUES (?, ?, ?, ?, ?)");
        insertScheduledJob = session.getSession().prepare(
                "INSERT INTO scheduled_jobs_idx (time_slice, job_id) VALUES (?, ?)");
        findScheduledJobs = session.getSession().prepare(
                "SELECT job_id FROM scheduled_jobs_idx WHERE time_slice = ?");
        deleteScheduledJobs = session.getSession().prepare(
                "DELETE FROM scheduled_jobs_idx WHERE time_slice = ?");
        findFinishedJobs = session.getSession().prepare(
                "SELECT job_id FROM finished_jobs_idx WHERE time_slice = ?");
        deleteFinishedJobs = session.getSession().prepare(
                "DELETE FROM finished_jobs_idx WHERE time_slice = ?");
        updateJobToFinished = session.getSession().prepare(
                "INSERT INTO finished_jobs_idx (time_slice, job_id) VALUES (?, ?)");
        findJob = session.getSession().prepare("SELECT type, name, params, trigger FROM jobs WHERE id = ?");
        addActiveTimeSlice = session.getSession().prepare("INSERT INTO active_time_slices (time_slice) VALUES (?)");
        findActiveTimeSlices = session.getSession().prepare("SELECT DISTINCT time_slice FROM active_time_slices");
        deleteActiveTimeSlice = session.getSession().prepare("DELETE FROM active_time_slices WHERE time_slice = ?");

        finishedTimeSlices = Optional.empty();
        jobFinished = Optional.empty();
    }

    // TODO make configurable
    private int getQueryThreadPoolSize() {
        return Math.min(Runtime.getRuntime().availableProcessors() / 2, 1);
    }

    /**
     * Test hook to allow control of when ticks are emitted.
     */
    public void setTickScheduler(rx.Scheduler scheduler) {
        tickScheduler = scheduler;
    }

    /**
     * Test hook to broadcast when the job scheduler has finished all work for a time slice. This includes not only
     * executing jobs that are scheduled for a particular time slice but also post-job execution clean up.
     */
    public void setTimeSlicesSubject(PublishSubject<Date> timeSlicesSubject) {
        finishedTimeSlices = Optional.of(timeSlicesSubject);
    }

    /**
     * Test hook to broadcast when jobs finish executing.
     */
    public void setJobFinishedSubject(PublishSubject<JobDetails> subject) {
        jobFinished = Optional.of(subject);
    }

    @Override
    public void register(String jobType, Func1<JobDetails, Completable> jobProducer) {
        jobFactories.put(jobType, jobProducer);
    }

    @Override
    public void register(String jobType, Func1<JobDetails, Completable> jobProducer,
            Func2<JobDetails, Throwable, RetryPolicy> retryFunction) {
        jobFactories.put(jobType, jobProducer);
        retryFunctions.put(jobType, retryFunction);
    }

    @Override
    public Single<JobDetails> scheduleJob(String type, String name, Map<String, String> parameter, Trigger trigger) {
        if (now.get().getMillis() >= trigger.getTriggerTime()) {
            return Single.error(new RuntimeException("Trigger time has already passed"));
        }
        String lockName = QUEUE_LOCK_PREFIX + trigger.getTriggerTime();
        return lockManager.acquireLock(lockName, SCHEDULING_LOCK, SCHEDULING_LOCK_TIMEOUT_IN_SEC)
                .map(acquired -> {
                    if (acquired) {
                        UUID jobId = UUID.randomUUID();
                        return new JobDetails(jobId, type, name,parameter, trigger);
                    }
                    throw new RuntimeException("Failed to acquire scheduling lock [" + lockName + "]");
                })
                .toSingle()
                .flatMap(details -> Completable.merge(insertIntoJobsTable(details), updateScheduledJobsIndex(details))
                            .andThen(Single.just(details)));
    }

    @Override
    public void start() {
        running = true;
        NavigableSet<Date> activeTimeSlices = new ConcurrentSkipListSet<>();
        Set<UUID> activeJobs = new ConcurrentSkipListSet<>();

        doOnTick(() -> {
            logger.debug("Activating scheduler for [" + currentMinute().toDate() + "]");

            Date timeSlice = currentMinute().toDate();
            updateActiveTimeSlices(timeSlice)
                    .flatMap(aVoid -> findTimeSlices())
                    .filter(d -> {
                        synchronized (lock) {
                            if (!activeTimeSlices.contains(d)) {
                                activeTimeSlices.add(d);
                                return true;
                            }
                            return false;
                        }
                    })
                    .doOnNext(d -> {
                        logger.debug("Running job scheduler for [" + d + "]");
                    })
                    .flatMap(this::acquireTimeSliceLock)
                    .flatMap(timeSliceLock -> findScheduledJobs(timeSliceLock.getTimeSlice())
                            .flatMap(scheduledJobs -> computeRemainingJobs(scheduledJobs, timeSliceLock
                                            .getTimeSlice(),
                                    activeJobs))
                            .flatMap(Observable::from)
                            .flatMap(this::acquireJobLock).filter(JobLock::isAcquired)
                            .flatMap(jobLock -> findJob(jobLock.getJobId()))
                            .flatMap(details -> doJobExecution(details, activeJobs).toObservable()
                                    .map(o -> timeSliceLock.getTimeSlice()))
                            .defaultIfEmpty(timeSliceLock.getTimeSlice()))
                    .flatMap(time -> {
                        Observable<? extends Set<UUID>> scheduled = findScheduledJobs(time);
                        Observable<? extends Set<UUID>> finished = findFinishedJobs(time);
                        return Observable.sequenceEqual(scheduled, finished).flatMap(allFinished -> {
                            if (allFinished) {
                                logger.debug("All jobs for time slice [" + time + "] have finished");
                                return Completable.merge(
                                        deleteActiveTimeSlice(time),
                                        deleteFinishedJobs(time),
                                        deleteScheduledJobs(time)
                                        // Without the reduce call here, the completable does not get executed.
                                        // Not sure why.
                                ).toObservable().reduce(null, (o1, o2) -> o2).map(o -> time);
                            }
                            return Observable.just(time);
                        });
                    })
                    .subscribe(
                            d -> {
                                logger.debug("Finished post job execution clean up for [" + d + "]");
                                // TODO should this be in a synchronized block?
                                activeTimeSlices.remove(d);
                                finishedTimeSlices.ifPresent(subject -> subject.onNext(d));
                            },
                            t -> logger.warn("Job execution failed", t),
                            () ->  logger.debug("Done!")
                    );
        });
    }

    private Observable<TimeSliceLock> acquireTimeSliceLock(Date timeSlice) {
        String lockName = QUEUE_LOCK_PREFIX + timeSlice.getTime();
        int delay = 5;
        Observable<TimeSliceLock> observable = Observable.create(subscriber -> {
            lockManager.acquireLock(lockName, TIME_SLICE_EXECUTION_LOCK, JOB_EXECUTION_LOCK_TIMEOUT_IN_SEC)
                    .map(acquired -> {
                        if (!acquired) {
                            logger.debug("Failed to acquire time slice lock for ["  + timeSlice + "]. Will attempt " +
                                    "to acquire it again in " + delay + " seconds.");
                            throw new RuntimeException();
                        }
                        return new TimeSliceLock(timeSlice, lockName, acquired);
                    })
                    .subscribe(subscriber::onNext, subscriber::onError, subscriber::onCompleted);
        });
        // We keep retrying until we acquire the lock. If the lock is held for scheduling, it will expire in 5 seconds
        // and we can obtain it for executing jobs. We should eventually acquire the lock because once the system
        // clock catches up to the time slice, we won't allow any more clients to obtain the lock for scheduling. If
        // system clocks are screwed up, we could have a problem.
        return observable.retryWhen(errors -> errors.flatMap(e -> Observable.timer(delay, TimeUnit.SECONDS,
                queryScheduler)));
    }

    private Observable<JobLock> acquireJobLock(UUID jobId) {
        String jobLock = "org.hawkular.metrics.scheduler.job." + jobId;
        return lockManager.acquireLock(jobLock, JOB_EXECUTION_LOCK, JOB_EXECUTION_LOCK_TIMEOUT_IN_SEC)
                .map(acquired -> new JobLock(jobId, acquired));
    }

    private Completable doJobExecution(JobDetails details, Set<UUID> activeJobs) {
        logger.debug("Starting execution for " + details);
        Func1<JobDetails, Completable> factory = jobFactories.get(details.getJobType());
        Completable job = factory.call(details);
        Date timeSlice = new Date(details.getTrigger().getTriggerTime());
        return Completable.concat(
                job.onErrorResumeNext(t -> {
                    RetryPolicy retryPolicy = retryFunctions.getOrDefault(details.getJobType(), NO_RETRY)
                            .call(details, t);
                    if (retryPolicy == RetryPolicy.NONE) {
                        return Completable.complete();
                    }
                    if (details.getTrigger().nextTrigger() != null) {
                        logger.warn("Retry Policies cannot be used with jobs that repeat. " + details +
                                " will execute again according to its next trigger.");
                    }
                    if (retryPolicy == RetryPolicy.NOW) {
                        return factory.call(details);
                    }
                    Trigger newTrigger = new Trigger() {
                        @Override
                        public long getTriggerTime() {
                            return details.getTrigger().getTriggerTime();
                        }

                        @Override
                        public Trigger nextTrigger() {
                            return new SingleExecutionTrigger.Builder()
                                    .withDelay(retryPolicy.getDelay(), TimeUnit.MILLISECONDS)
                                    .build();
                        }
                    };
                    JobDetails newDetails = new JobDetails(details.getJobId(), details.getJobType(),
                            details.getJobName(), details.getParameters(), newTrigger);
                    return rescheduleJob(newDetails, activeJobs);
                }),
                setJobFinished(timeSlice, details),
                rescheduleAndDeactivateJob(details, activeJobs),
                Completable.fromAction(() ->
                        jobFinished.ifPresent(subject -> subject.onNext(details)))
        ).subscribeOn(Schedulers.io());
    }

    private Completable rescheduleAndDeactivateJob(JobDetails details, Set<UUID> activeJobs) {
        return Completable.create(subscriber -> {
            Completable c;
            Trigger nextTrigger = details.getTrigger().nextTrigger();
            if (nextTrigger == null) {
                c = deactivateJob(activeJobs, details);
            } else if (nextTrigger.getTriggerTime() <= now.get().getMillis()) {
                c = rescheduleJob(details, activeJobs);
            } else {
                c = rescheduleJob(details, activeJobs).concatWith(deactivateJob(activeJobs, details));
            }
            c.subscribe(subscriber::onCompleted, subscriber::onError);
        });
    }

    private Trigger getNextTrigger(Date timeSlice, Trigger trigger) {
        Trigger next = trigger;
        while (timeSlice.getTime() > next.getTriggerTime()) {
            next = next.nextTrigger();
        }
        return next;
    }

    private Completable setJobFinished(Date timeSlice, JobDetails details) {
        return Completable.create(subscriber -> {
            session.execute(updateJobToFinished.bind(timeSlice, details.getJobId()), queryScheduler).subscribe(
                    resultSet -> {},
                    t -> {
                        logger.warn("Failed to set " + details + " finished for time slice [" + timeSlice + "]");
                        subscriber.onError(t);
                    },
                    () -> {
                        logger.debug("Updating " + details + " status to finished for time slice [" + timeSlice + "]");
                        subscriber.onCompleted();
                    }
            );
        });
    }

    private Completable rescheduleJob(JobDetails details, Set<UUID> activeJobs) {
        Trigger nextTrigger = details.getTrigger().nextTrigger();
        if (nextTrigger == null) {
            return Completable.fromAction(() -> logger.debug("No more scheduled executions for " + details));
        }

        JobDetails newDetails = new JobDetails(details.getJobId(), details.getJobType(), details.getJobName(),
                details.getParameters(), nextTrigger);

        if (nextTrigger.getTriggerTime() <= now.get().getMillis()) {
            logger.info(details + " missed its next execution at " + nextTrigger.getTriggerTime() +
                    ". It will scheduled for immediate execution.");
            AtomicLong nextTimeSlice = new AtomicLong(currentMinute().getMillis());
            Observable<Boolean> scheduled = Observable.defer(() ->
                    lockManager.acquireLock(QUEUE_LOCK_PREFIX + nextTimeSlice.addAndGet(60000L), SCHEDULING_LOCK,
                            SCHEDULING_LOCK_TIMEOUT_IN_SEC))
                    .map(acquired -> {
                        if (!acquired) {
                            throw new RuntimeException();
                        }
                        return acquired;
                    })
                    .retry();
            return Completable.concat(
                    scheduled.toCompletable(),
                    updateScheduledJobsIndex(nextTimeSlice.get(), details.getJobId()),
                    insertIntoJobsTable(newDetails),
                    scheduleImmediateJobExecution(newDetails, activeJobs));
        }

        return Completable.concat(
                Completable.fromAction(() -> logger.debug("Scheduling " + newDetails + " for next execution at " +
                        new Date(nextTrigger.getTriggerTime()))),
                updateScheduledJobsIndex(newDetails),
                insertIntoJobsTable(newDetails)
        );
    }

    private Completable scheduleImmediateJobExecution(JobDetails details, Set<UUID> activeJobs) {
        rx.Scheduler.Worker worker = Schedulers.io().createWorker();
        worker.schedule(() -> {
            String jobLock = "org.hawkular.metrics.scheduler.job." + details.getJobId();
            lockManager.renewLock(jobLock, JOB_EXECUTION_LOCK, JOB_EXECUTION_LOCK_TIMEOUT_IN_SEC).toCompletable()
                    .concatWith(doJobExecution(details, activeJobs)).subscribe(
                            () -> logger.debug("Finished executing " + details),
                            t -> logger.warn("There was an error executing " + details)
            );
        });
        return Completable.complete();
    }

    private Completable deactivateJob(Set<UUID> activeJobs, JobDetails details) {
        String jobLock = "org.hawkular.metrics.scheduler.job." + details.getJobId();
        Completable removeActiveJobId = Completable.fromAction(() -> {
            logger.debug("Removing " + details + " from active jobs");
            activeJobs.remove(details.getJobId());
        });

        Completable releaseLock = Completable.create(subscriber -> {
            BooleanSubscription cancel = new BooleanSubscription();
            subscriber.onSubscribe(cancel);

            lockManager.releaseLock(jobLock, JOB_EXECUTION_LOCK).subscribe(
                    released -> {
                        if (!released) {
                            logger.warn("Failed to release job lock for " + details);
                            subscriber.onError(new RuntimeException("Failed to release job lock for " + details));
                        }
                    },
                    t -> {
                        logger.warn("Failed to release job lock for " + details, t);
                        subscriber.onError(t);
                    },
                    () -> {
                        if (!cancel.isUnsubscribed()) {
                            cancel.unsubscribe();
                            subscriber.onCompleted();
                        }
                    }
            );
        });

        return removeActiveJobId.concatWith(releaseLock);
    }

    private Completable deleteScheduledJobs(Date timeSlice) {
        return session.execute(deleteScheduledJobs.bind(timeSlice), queryScheduler)
                .doOnCompleted(() -> logger.debug("Deleted scheduled jobs time slice [" + timeSlice + "]"))
                .toCompletable();
    }

    private Completable deleteFinishedJobs(Date timeSlice) {
        return session.execute(deleteFinishedJobs.bind(timeSlice), queryScheduler)
                .doOnCompleted((() -> logger.debug("Deleted finished jobs time slice [" + timeSlice + "]")))
                .toCompletable();
    }

    private Completable deleteActiveTimeSlice(Date timeSlice) {
        return session.execute(deleteActiveTimeSlice.bind(timeSlice), queryScheduler)
                .doOnCompleted(() -> logger.debug("Deleted active time slice [" + timeSlice + "]"))
                .toCompletable();
    }

    @Override
    public void shutdown() {
        try {
            running = false;
            tickExecutor.shutdown();
            tickExecutor.awaitTermination(5, TimeUnit.SECONDS);
            queryExecutor.shutdown();
            queryExecutor.awaitTermination(30, TimeUnit.SECONDS);

            logger.info("Shutdown complete");
        } catch (InterruptedException e) {
            logger.warn("Interrupted during shutdown", e);
        }
    }

    void reset(rx.Scheduler tickScheduler) {
        logger.debug("Starting reset");
        shutdown();
        jobFactories = new HashMap<>();
        this.tickScheduler = tickScheduler;
        queryExecutor = Executors.newFixedThreadPool(getQueryThreadPoolSize(),
                new ThreadFactoryBuilder().setNameFormat("query-thread-pool-%d").build());
        queryScheduler = Schedulers.from(queryExecutor);
    }

    private Observable<? extends Set<UUID>> findScheduledJobs(Date timeSlice) {
        logger.debug("Fetching scheduled jobs for [" + timeSlice + "]");
        return session.execute(findScheduledJobs.bind(timeSlice), queryScheduler)
                .flatMap(Observable::from)
                .map(row -> row.getUUID(0))
                .collect(HashSet<UUID>::new, HashSet::add);
    }

    private Observable<Set<UUID>> computeRemainingJobs(Set<UUID> scheduledJobs, Date timeSlice,
            Set<UUID> activeJobs) {
        Observable<? extends Set<UUID>> finished = findFinishedJobs(timeSlice);
        Observable<? extends Set<UUID>> scheduled = Observable.just(scheduledJobs);
        return finished.map(finishedJobs -> {
            Set<UUID> active = new HashSet<>(activeJobs);
            active.removeAll(finishedJobs);
            Set<UUID> jobs = Sets.difference(scheduledJobs, finishedJobs);
            return Sets.difference(jobs, active);
        });
    }

    private Observable<? extends Set<UUID>> findFinishedJobs(Date timeSlice) {
        return session.execute(findFinishedJobs.bind(timeSlice), queryScheduler)
                .flatMap(Observable::from)
                .map(row -> row.getUUID(0))
                .doOnNext(id -> logger.debug("Finished job [" + id + "]"))
                .collect(HashSet<UUID>::new, HashSet::add);
    }

    private Completable insertIntoJobsTable(JobDetails details) {
        return Completable.create(subscriber -> {
            session.execute(insertJob.bind(details.getJobId(), details.getJobType(), details.getJobName(),
                    details.getParameters(), getTriggerValue(session, details.getTrigger())), queryScheduler)
                    .subscribe(
                            resultSet -> {},
                            t -> {
                                logger.warn("Failed to insert " + details + " in jobs tables");
                                subscriber.onError(t);
                            },
                            subscriber::onCompleted
                    );
        });
    }

    private Completable updateScheduledJobsIndex(JobDetails details) {
        return updateScheduledJobsIndex(details.getTrigger().getTriggerTime(), details.getJobId());
    }

    private Completable updateScheduledJobsIndex(long timeSlice, UUID jobId) {
        return Completable.create(subscriber -> {
            session.execute(insertScheduledJob.bind(new Date(timeSlice), jobId), queryScheduler).subscribe(
                    resultSet -> {},
                    t -> {
                        logger.warn("Failed to update scheduled jobs index for job id " + jobId);
                        subscriber.onError(t);
                    },
                    subscriber::onCompleted
            );
        });
    }

    private void doOnTick(Action0 action) {
        Action0 wrapper = () -> {
            Date timeSlice = getTimeSlice(new DateTime(tickScheduler.now()), minutes(1).toStandardDuration()).toDate();
            logger.debug("[TICK][" + timeSlice + "] executing action");
            action.call();
            logger.debug("Finished tick for [" + timeSlice + "]");
        };
        AtomicReference<DateTime> previousTimeSliceRef = new AtomicReference<>();
        // TODO Do we really need a separate, dedicated scheduler for emitting ticks?
        // TODO Emit ticks at the start of every minute
        Observable.interval(0, 1, TimeUnit.MINUTES, tickScheduler)
                .doOnNext(tick -> logger.debug("CURRENT MINUTE = " + currentMinute().toDate()))
                .filter(tick -> {
                    DateTime time = currentMinute();
                    if (previousTimeSliceRef.get() == null) {
                        previousTimeSliceRef.set(time);
                        return true;
                    }
                    logger.debug("previous=[" + previousTimeSliceRef.get().toLocalDateTime() + "], current=[" +
                            time.toLocalDateTime() + "]");
                    if (previousTimeSliceRef.get().equals(time)) {
                        return false;
                    }
                    previousTimeSliceRef.set(time);
                    return true;
                })
                .takeUntil(d -> !running)
                .subscribe(tick -> wrapper.call(), t -> logger.warn(t));
    }

    private Observable<Void> updateActiveTimeSlices(Date timeSlice) {
        return session.execute(addActiveTimeSlice.bind(timeSlice)).map(resultSet -> null);
    }

    private Observable<Date> findTimeSlices() {
        return session.execute(findActiveTimeSlices.bind(), queryScheduler)
                .flatMap(Observable::from)
                .map(row -> row.getTimestamp(0))
                .toSortedList()
                .flatMap(Observable::from);
    }

    private Observable<JobDetails> findJob(UUID jobId) {
        return session.execute(findJob.bind(jobId), queryScheduler)
                .flatMap(Observable::from)
                .map(row -> new JobDetails(jobId, row.getString(0), row.getString(1),
                        row.getMap(2, String.class, String.class), getTrigger(row.getUDTValue(3))))
                .doOnError(t -> logger.warn("Failed to fetch job [" + jobId + "]", t));
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

    private static KeyspaceMetadata getKeyspace(RxSession session) {
        return session.getCluster().getMetadata().getKeyspace(session.getLoggedKeyspace());
    }

    private static class TimeSliceLock {
        private Date timeSlice;
        private String name;
        private boolean acquired;

        public TimeSliceLock(Date timeSlice, String name, boolean acquired) {
            this.timeSlice = timeSlice;
            this.name = name;
            this.acquired = acquired;
        }

        public Date getTimeSlice() {
            return timeSlice;
        }

        public String getName() {
            return name;
        }

        public boolean isAcquired() {
            return acquired;
        }
    }

    private static class JobLock {
        private UUID jobId;
        private boolean acquired;

        public JobLock(UUID jobId, boolean acquired) {
            this.jobId = jobId;
            this.acquired = acquired;
        }

        public UUID getJobId() {
            return jobId;
        }

        public String getName() {
            return "org.hawkular.metrics.scheduler.job." + jobId;
        }

        public boolean isAcquired() {
            return acquired;
        }
    }
}
