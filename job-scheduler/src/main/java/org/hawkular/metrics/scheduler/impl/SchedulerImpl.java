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

import static java.util.stream.Collectors.toSet;

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
import org.hawkular.metrics.scheduler.api.JobStatus;
import org.hawkular.metrics.scheduler.api.RetryPolicy;
import org.hawkular.metrics.scheduler.api.Scheduler;
import org.hawkular.metrics.scheduler.api.SingleExecutionTrigger;
import org.hawkular.metrics.scheduler.api.Trigger;
import org.hawkular.rx.cassandra.driver.RxSession;
import org.jboss.logging.Logger;
import org.joda.time.DateTime;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import rx.Completable;
import rx.Observable;
import rx.Single;
import rx.functions.Action0;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;

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

    private PreparedStatement deleteScheduleJob;

    private PreparedStatement findScheduledJobs;

    private PreparedStatement deleteScheduledJobs;

    private PreparedStatement deleteScheduledJob;

    private PreparedStatement findFinishedJobs;

    private PreparedStatement deleteFinishedJobs;

    private PreparedStatement updateJobToFinished;

    private PreparedStatement findJob;

    private PreparedStatement findAllJobs;

    private PreparedStatement addActiveTimeSlice;

    private PreparedStatement findActiveTimeSlices;

    private PreparedStatement deleteActiveTimeSlice;

    private LockManager lockManager;

    private JobsService jobsService;

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
        jobsService = new JobsService(session);

        insertJob = initQuery("INSERT INTO jobs (id, type, name, params, trigger) VALUES (?, ?, ?, ?, ?)");
        insertScheduledJob = initQuery("INSERT INTO scheduled_jobs_idx (time_slice, job_id) VALUES (?, ?)");
        findScheduledJobs = initQuery("SELECT job_id FROM scheduled_jobs_idx WHERE time_slice = ?");
        deleteScheduledJobs = initQuery("DELETE FROM scheduled_jobs_idx WHERE time_slice = ?");
        deleteScheduledJob = initQuery("DELETE FROM scheduled_jobs_idx WHERE time_slice = ? AND job_id = ?");
        findFinishedJobs = initQuery("SELECT job_id FROM finished_jobs_idx WHERE time_slice = ?");
        deleteFinishedJobs = initQuery("DELETE FROM finished_jobs_idx WHERE time_slice = ?");
        updateJobToFinished = initQuery("INSERT INTO finished_jobs_idx (time_slice, job_id) VALUES (?, ?)");
        findJob = initQuery("SELECT type, name, params, trigger FROM jobs WHERE id = ?");
        findAllJobs = initQuery("SELECT id, type, name, params, trigger FROM jobs");
        addActiveTimeSlice = initQuery("INSERT INTO active_time_slices (time_slice) VALUES (?)");
        findActiveTimeSlices = initQuery("SELECT DISTINCT time_slice FROM active_time_slices");
        deleteActiveTimeSlice = initQuery("DELETE FROM active_time_slices WHERE time_slice = ?");

        finishedTimeSlices = Optional.empty();
        jobFinished = Optional.empty();
    }

    private PreparedStatement initQuery(String cql) {
        return session.getSession().prepare(cql).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
    }

    // TODO make configurable
    private int getQueryThreadPoolSize() {
        return Math.max(Runtime.getRuntime().availableProcessors() / 2, 1);
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
        return lockManager.acquireSharedLock(lockName, SCHEDULING_LOCK, SCHEDULING_LOCK_TIMEOUT_IN_SEC)
                .map(acquired -> {
                    if (acquired) {
                        UUID jobId = UUID.randomUUID();
                        return new JobDetails(jobId, type, name,parameter, trigger);
                    }
                    throw new RuntimeException("Failed to acquire scheduling lock [" + lockName + "]");
                })
                .flatMap(details -> jobsService.insert(new Date(trigger.getTriggerTime()), details)
                        .map(resultSet -> details))
                .toSingle();
    }

    @Override
    public void start() {
        running = true;
        NavigableSet<Date> activeTimeSlices = new ConcurrentSkipListSet<>();
        Set<UUID> activeJobs = new ConcurrentSkipListSet<>();

        doOnTick(() -> {
            logger.debug("Activating scheduler for [" + currentMinute().toDate() + "]");
            Observable.just(currentMinute().toDate())
                    .flatMap(time -> jobsService.findActiveTimeSlices(time, queryScheduler))
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
                            .doOnError(t -> {
                                logger.warn("Failed to find scheduled jobs for time slice " + timeSliceLock.timeSlice);
                            })
                            .doOnNext(jobs -> logger.debug("[" + timeSliceLock.timeSlice + "] scheduled jobs: " + jobs))
                            .flatMap(scheduledJobs -> computeRemainingJobs(scheduledJobs, timeSliceLock.getTimeSlice(),
                                    activeJobs))
                            .doOnNext(jobs -> logger.debug("[" + timeSliceLock.timeSlice + "] remaining jobs: " + jobs))
                            .flatMap(Observable::from)
                            .filter(jobDetails -> !activeJobs.contains(jobDetails.getJobId()))
                            .flatMap(this::acquireJobLock)
                            .filter(jobLock -> jobLock.acquired)
                            .map(jobLock -> jobLock.jobDetails)
                            .doOnNext(details -> logger.debug("Acquired job lock for " + details + " in time slice " +
                                    timeSliceLock.timeSlice))
                            .flatMap(details -> executeJob(details, timeSliceLock.timeSlice, activeJobs).toObservable()
                                    .map(o -> timeSliceLock.getTimeSlice()))
                            .defaultIfEmpty(timeSliceLock.getTimeSlice()))
                    .flatMap(time -> {
                        Observable<? extends Set<UUID>> scheduled = jobsService.findScheduledJobsForTime(time,
                                queryScheduler)
                                .map(JobDetails::getJobId)
                                .collect(HashSet<UUID>::new, HashSet::add);
                        Observable<? extends Set<UUID>> finished = findFinishedJobs(time);
                        return Observable.sequenceEqual(scheduled, finished).flatMap(allFinished -> {
                            if (allFinished) {
                                logger.debug("All jobs for time slice [" + time + "] have finished");
                                return deleteFinishedJobs(time).mergeWith(deleteScheduledJobs(time))
                                        // Without the reduce call here, the completable does not get executed.
                                        // Not sure why.
                                        .toObservable()
                                        .reduce(null, (o1, o2) -> o2)
                                        .map(o -> time);
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
                            t -> {
                                logger.warn("Job execution failed", t);
                                synchronized (lock) {
                                    // When there is an error we need to make sure the corresponding time slice is
                                    // removed from activeTimeSlices. See HWKMETRICS-518 for more details.
                                    //
                                    // Since wee do not have the timestamp here we simply remove everything. This will
                                    // cause any duplicate job execution since each job has a lock associated with it.
                                    // Clearing this cache may result in extra attempts at acquiring job locks. I plan
                                    // change this. See HWKMETRICS-522.
                                    activeTimeSlices.clear();
                                }
                            },
                            () ->  logger.debug("Done!")
                    );
        });
    }

    private Observable<TimeSliceLock> acquireTimeSliceLock(Date timeSlice) {
        String lockName = QUEUE_LOCK_PREFIX + timeSlice.getTime();
        int delay = 5;
        Observable<TimeSliceLock> observable = Observable.create(subscriber -> {
            lockManager.acquireSharedLock(lockName, TIME_SLICE_EXECUTION_LOCK, JOB_EXECUTION_LOCK_TIMEOUT_IN_SEC)
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

    private Observable<JobLock> acquireJobLock(JobDetails jobDetails) {
        String jobLock = "org.hawkular.metrics.scheduler.job." + jobDetails.getJobId();
        return lockManager.acquireExclusiveLock(jobLock, JOB_EXECUTION_LOCK, JOB_EXECUTION_LOCK_TIMEOUT_IN_SEC)
                .map(acquired -> new JobLock(jobDetails, acquired));
    }

    private Completable executeJob(JobDetails details, Date timeSlice, Set<UUID> activeJobs) {
        logger.debug("Starting execution for " + details + " in time slice [" + timeSlice + "]");
        Stopwatch stopwatch = Stopwatch.createStarted();
        Func1<JobDetails, Completable> factory = jobFactories.get(details.getJobType());
        Completable job;

        if (details.getStatus() == JobStatus.FINISHED) {
            job = Completable.complete();
        } else {
            job = factory.call(details);
        }

        return job.onErrorResumeNext(t -> {
            logger.info("Execution of " + details + " in time slice [" + timeSlice + "] failed", t);

            RetryPolicy retryPolicy = retryFunctions.getOrDefault(details.getJobType(), NO_RETRY).call(details, t);
            if (retryPolicy == RetryPolicy.NONE) {
                return Completable.complete();
            }
            if (details.getTrigger().nextTrigger() != null) {
                logger.warn("Retry policies cannot be used with jobs that repeat. " + details + " will execute again " +
                        "according to its next trigger.");
                return Completable.complete();
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
            JobDetails newDetails = new JobDetails(details.getJobId(), details.getJobType(), details.getJobName(),
                    details.getParameters(), newTrigger);
            return reschedule(new JobExecutionState(newDetails, activeJobs)).toCompletable();
        })
                .doOnCompleted(() -> {
                    stopwatch.stop();
                    if (logger.isDebugEnabled()) {
                        logger.debug("Finished executing " + details + " in time slice [" + timeSlice + "] " +
                                stopwatch.elapsed(TimeUnit.MILLISECONDS) + " ms");
                    }
                })
                .toSingle(() -> new JobExecutionState(details, activeJobs))
                .flatMap(state -> jobsService.updateStatusToFinished(timeSlice, state.currentDetails.getJobId())
                            .toSingle()
                        .map(resultSet -> state))
                .flatMap(this::reschedule)
                .flatMap(state -> {
                    if (state.isBehindSchedule()) {
                        return setJobFinished(state).flatMap(this::scheduleImmediateExecutionIfNecessary);
                    }
                    return releaseJobExecutionLock(state)
                            .flatMap(this::deactivate)
                            .flatMap(this::setJobFinished);
                })
                .doOnError(t -> {
                    logger.debug("There was an error during post-job execution. Making sure " + details +
                        " is removed from active jobs cache");
                    activeJobs.remove(details.getJobId());
                    publishJobFinished(details);
                })
                .doOnSuccess(states -> publishJobFinished(states.currentDetails))
                .toCompletable()
                .subscribeOn(Schedulers.io());
    }

    private void publishJobFinished(JobDetails details) {
        jobFinished.ifPresent(subject -> subject.onNext(details));
    }

    private Single<JobExecutionState> setJobFinished(JobExecutionState state) {
        return session.execute(updateJobToFinished.bind(state.timeSlice,
                state.currentDetails.getJobId()), queryScheduler)
                .toSingle()
                .map(resultSet -> state)
                .doOnError(t -> logger.warn("There was an error while updating the finished jobs index for " +
                        state.currentDetails, t));
    }

    /**
     * If a job is single execution then this is a no-op; otherwise, the scheduled jobs index and jobs tables are
     * updated with the next execution time/details. If the job has missed its next execution, it will get scheduled
     * in the next available time slice. The job scheduler will actually execute the job immediately when the job falls
     * behind, but we still want to persist the scheduling update for durability.
     */
    private Single<JobExecutionState> reschedule(JobExecutionState executionState) {
        Trigger nextTrigger = executionState.currentDetails.getTrigger().nextTrigger();
        if (nextTrigger == null) {
            logger.debug("No more scheduled executions for " + executionState.currentDetails);
            return Single.just(executionState);
        }

        JobDetails details = executionState.currentDetails;
        JobDetails newDetails = new JobDetails(details.getJobId(), details.getJobType(), details.getJobName(),
                details.getParameters(), nextTrigger);

        if (nextTrigger.getTriggerTime() <= now.get().getMillis()) {
            logger.info(details + " missed its next execution at " + nextTrigger.getTriggerTime() +
                    ". It will be rescheduled for immediate execution");
            AtomicLong nextTimeSlice = new AtomicLong(currentMinute().getMillis());
            Observable<Boolean> scheduled = Observable.defer(() ->
                    lockManager.acquireSharedLock(QUEUE_LOCK_PREFIX + nextTimeSlice.addAndGet(60000L), SCHEDULING_LOCK,
                            SCHEDULING_LOCK_TIMEOUT_IN_SEC))
                    .map(acquired -> {
                        if (!acquired) {
                            throw new RuntimeException();
                        }
                        return acquired;
                    })
                    .retry();
            return scheduled
                    .map(acquired -> new JobExecutionState(executionState.currentDetails, executionState.timeSlice,
                            newDetails, new Date(nextTimeSlice.get()), executionState.activeJobs))
                    .flatMap(state -> jobsService.insert(state.nextTimeSlice, state.nextDetails)
                            .map(updated -> state))
                    .toSingle();
        }

        logger.debug("Scheduling " + newDetails + " for next execution at " + new Date(nextTrigger.getTriggerTime()));

        JobExecutionState newState = new JobExecutionState(details, executionState.timeSlice, newDetails,
                new Date(nextTrigger.getTriggerTime()), executionState.activeJobs);
        return jobsService.insert(newState.nextTimeSlice, newState.nextDetails)
                .map(updated -> newState)
                .toSingle();
    }

    private Single<JobExecutionState> scheduleImmediateExecutionIfNecessary(JobExecutionState state) {
        rx.Scheduler.Worker worker = Schedulers.io().createWorker();
        worker.schedule(() -> {
            logger.debug("Starting immediate execution of " + state.nextDetails);
            String jobLock = "org.hawkular.metrics.scheduler.job." + state.nextDetails.getJobId();
            lockManager.renewLock(jobLock, JOB_EXECUTION_LOCK, JOB_EXECUTION_LOCK_TIMEOUT_IN_SEC)
                    .map(renewed -> {
                        if (!renewed) {
                            throw new RuntimeException("Failed to renew job lock for " + state.nextDetails);
                        }
                        return renewed;
                    })
                    .toCompletable()
                    .concatWith(executeJob(state.nextDetails, state.nextTimeSlice, state.activeJobs))
                    .subscribe(
                            () -> logger.debug("Finished executing " + state.nextDetails),
                            t -> logger.warn("There was an error executing " + state.nextDetails)
                    );
        });
        return Single.just(state);
    }

    /**
     * Deactivating a job does two things. 1) The job is is removed from the activeJobs set, and 2) the job lock is
     * released. If the job is repeating and behind schedule, then this method is a no-op.
     */
    private Single<JobExecutionState> deactivate(JobExecutionState state) {
        logger.debug("Removing " + state.currentDetails + " from active jobs " + state.activeJobs);
        state.activeJobs.remove(state.currentDetails.getJobId());
        return Single.just(state);
    }

    private Single<JobExecutionState> releaseJobExecutionLock(JobExecutionState state) {
        String jobLock = "org.hawkular.metrics.scheduler.job." + state.currentDetails.getJobId();
        return lockManager.releaseLock(jobLock, JOB_EXECUTION_LOCK)
                .map(released -> {
                    if (!released) {
                        logger.warn("Failed to release job lock for " + state.currentDetails);
                        throw new RuntimeException("Failed to release job lock for " + state.currentDetails);
                    }
                    return state;
                })
                .toSingle()
                .doOnError(t -> logger.warn("There was an error trying to release job lock [" + jobLock +
                        "] for " + state.currentDetails, t));
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

    private Observable<? extends Set<JobDetails>> findScheduledJobs(Date timeSlice) {
        logger.debug("Fetching scheduled jobs for [" + timeSlice + "]");
        return jobsService.findScheduledJobsForTime(timeSlice, queryScheduler)
                .collect(HashSet<JobDetails>::new, HashSet::add);
    }

    private Observable<Set<JobDetails>> computeRemainingJobs(Set<JobDetails> scheduledJobs, Date timeSlice,
            Set<UUID> activeJobs) {
        Observable<? extends Set<UUID>> finished = findFinishedJobs(timeSlice);
        return finished.map(finishedJobs -> {
            Set<UUID> active = new HashSet<>(activeJobs);
            active.removeAll(finishedJobs);
            Set<JobDetails> jobs = scheduledJobs.stream().filter(details -> !finishedJobs.contains(details.getJobId()))
                    .collect(toSet());
            return jobs.stream().filter(details -> !active.contains(details.getJobId())).collect(toSet());
        });
    }

    private Observable<? extends Set<UUID>> findFinishedJobs(Date timeSlice) {
        return session.execute(findFinishedJobs.bind(timeSlice), queryScheduler)
                .flatMap(Observable::from)
                .map(row -> row.getUUID(0))
                .collect(HashSet<UUID>::new, HashSet::add);
    }

    private static class QueryExecution {
        private Statement query;
        private ResultSet resultSet;
        private Throwable error;

        public QueryExecution(Statement query, ResultSet resultSet) {
            this.resultSet = resultSet;
        }

        public QueryExecution(Statement query, Throwable t) {
            error = t;
        }
    }

    private void doOnTick(Action0 action) {
        Action0 wrapper = () -> {
            Date timeSlice = getTimeSlice(new DateTime(tickScheduler.now()), minutes(1).toStandardDuration()).toDate();
            logger.debug("[TICK][" + timeSlice + "] executing action");
            action.call();
            logger.debug("Finished tick for [" + timeSlice + "]");
        };
        AtomicReference<DateTime> previousTimeSliceRef = new AtomicReference<>();
        // TODO Emit ticks at the start of every minute
        Observable.interval(0, 1, TimeUnit.MINUTES, tickScheduler)
                .doOnNext(tick -> logger.debug("CURRENT MINUTE = " + currentMinute().toDate()))
                .filter(tick -> {
                    DateTime time = currentMinute();
                    if (previousTimeSliceRef.get() == null) {
                        previousTimeSliceRef.set(time);
                        return true;
                    }
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
        final JobDetails jobDetails;
        final boolean acquired;
        final String name;

        public JobLock(JobDetails jobDetails, boolean acquired) {
            this.jobDetails = jobDetails;
            this.acquired = acquired;
            this.name = "org.hawkular.metrics.scheduler.job." + jobDetails.getJobId();
        }
    }

    /**
     * Normally jobs are executed in the time slice specified by their triggers. If a job falls behind though, it
     * will be executed immediately
     */
    private static class JobExecutionState {
        final JobDetails currentDetails;
        final JobDetails nextDetails;
        final Set<UUID> activeJobs;
        final Date timeSlice;
        final Date nextTimeSlice;

        public JobExecutionState(JobDetails details, Set<UUID> activeJobs) {
            this.currentDetails = details;
            this.activeJobs = activeJobs;
            this.timeSlice = new Date(details.getTrigger().getTriggerTime());
            nextDetails = null;
            nextTimeSlice = null;
        }

        public JobExecutionState(JobDetails details, Date timeSlice, JobDetails nextDetails, Date nextTimeSlice,
                Set<UUID> activeJobs) {
            this.currentDetails = details;
            this.timeSlice = timeSlice;
            this.nextDetails = nextDetails;
            this.nextTimeSlice = nextTimeSlice;
            this.activeJobs = activeJobs;
        }

        public boolean isRepeating() {
            return nextDetails != null && nextTimeSlice != null;
        }

        public boolean isBehindSchedule() {
            return isRepeating() && nextTimeSlice.getTime() > nextDetails.getTrigger().getTriggerTime();
        }

    }

}
