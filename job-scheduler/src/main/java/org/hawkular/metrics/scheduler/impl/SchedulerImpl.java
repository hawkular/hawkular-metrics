/*
 * Copyright 2014-2017 Red Hat, Inc. and/or its affiliates
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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.hawkular.metrics.scheduler.api.JobDetails;
import org.hawkular.metrics.scheduler.api.JobStatus;
import org.hawkular.metrics.scheduler.api.RepeatingTrigger;
import org.hawkular.metrics.scheduler.api.RetryPolicy;
import org.hawkular.metrics.scheduler.api.Scheduler;
import org.hawkular.metrics.scheduler.api.SingleExecutionTrigger;
import org.hawkular.metrics.scheduler.api.Trigger;
import org.hawkular.rx.cassandra.driver.RxSession;
import org.jboss.logging.Logger;
import org.joda.time.DateTime;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
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

    private PreparedStatement deleteScheduleJob;

    private PreparedStatement deleteScheduledJobs;

    private PreparedStatement findFinishedJobs;

    private PreparedStatement deleteFinishedJobs;

    private PreparedStatement updateJobToFinished;

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
    static final int JOB_EXECUTION_LOCK_TIMEOUT_IN_SEC = 1800;

    /**
     * Test hook. See {@link #setTimeSlicesSubject(PublishSubject)}.
     */
    private Optional<PublishSubject<Date>> finishedTimeSlices;

    private Optional<PublishSubject<JobDetails>> jobFinished;

    private final Object lock = new Object();

    private String hostname;

    public SchedulerImpl(RxSession session, String hostname) {
        this(session, hostname, new JobsService(session));
    }

    public SchedulerImpl(RxSession session, String hostname, JobsService jobsService) {
        this.session = session;
        jobFactories = new HashMap<>();
        retryFunctions = new HashMap<>();

        tickExecutor = new ScheduledThreadPoolExecutor(1, new ThreadFactoryBuilder()
                .setNameFormat("ticker-pool-%d").build(), new ThreadPoolExecutor.DiscardPolicy());
        tickScheduler = Schedulers.from(tickExecutor);

        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("query-thread-pool-%d").build();
        queryExecutor = new ThreadPoolExecutor(getQueryThreadPoolSize(), getQueryThreadPoolSize(), 0,
                TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), threadFactory,
                new ThreadPoolExecutor.DiscardPolicy());
        queryScheduler = Schedulers.from(queryExecutor);

        lockManager = new LockManager(session);
        this.jobsService = jobsService;

        deleteScheduledJobs = initQuery("DELETE FROM scheduled_jobs_idx WHERE time_slice = ?");
        findFinishedJobs = initQuery("SELECT job_id FROM finished_jobs_idx WHERE time_slice = ?");
        deleteFinishedJobs = initQuery("DELETE FROM finished_jobs_idx WHERE time_slice = ?");
        updateJobToFinished = initQuery("INSERT INTO finished_jobs_idx (time_slice, job_id) VALUES (?, ?)");
        addActiveTimeSlice = initQuery("INSERT INTO active_time_slices (time_slice) VALUES (?)");
        findActiveTimeSlices = initQuery("SELECT DISTINCT time_slice FROM active_time_slices");
        deleteActiveTimeSlice = initQuery("DELETE FROM active_time_slices WHERE time_slice = ?");

        finishedTimeSlices = Optional.empty();
        jobFinished = Optional.empty();

        this.hostname = hostname;
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
    public Single<? extends JobDetails> scheduleJob(String type, String name, Map<String, String> parameters,
            Trigger trigger) {
        if (now.get().getMillis() >= trigger.getTriggerTime()) {
            return Single.error(new RuntimeException("Trigger time has already passed"));
        }
        String lockName = QUEUE_LOCK_PREFIX + trigger.getTriggerTime();
        // The lock we obtain here is basically a shared lock. We need a lock to avoid a possible race condition.
        // Without the lock the job could could be written to scheduled_jobs_idx and wind up having the partition to
        // which it was written deleted before the job is ever executed. The job would then never get executed.
        return lockManager.acquireLock(lockName, SCHEDULING_LOCK, SCHEDULING_LOCK_TIMEOUT_IN_SEC, false)
                .map(lock -> {
                    if (lock.isLocked()) {
                        UUID jobId = UUID.randomUUID();
                        return jobsService.createJobDetails(jobId, type, name, parameters, trigger,
                                new Date(trigger.getTriggerTime()));
                    }
                    throw new RuntimeException("Failed to acquire scheduling lock [" + lockName + "]");
                })
                .flatMap(details -> jobsService.insert(new Date(trigger.getTriggerTime()), details)
                        .map(resultSet -> details))
                .toSingle()
                .doOnError(t -> logger.warn("Failed to schedule job " + name, t));
    }

    @Override
    public Completable unscheduleJobById(String jobId) {
        return jobsService.deleteJob(UUID.fromString(jobId), queryScheduler);
    }

    @Override public Completable unscheduleJobByTypeAndName(String jobType, String jobName) {
        return Completable.merge(
                jobsService.findAllScheduledJobs(queryScheduler)
                .filter(details -> details.getJobType().equals(jobType) && details.getJobName().equals(jobName))
                .map(details -> jobsService.deleteJob(details.getJobId(), queryScheduler))
        );
    }

    @Override
    public void start() {
        running = true;
        NavigableSet<Date> activeTimeSlices = new ConcurrentSkipListSet<>();
        Set<UUID> activeJobs = new ConcurrentSkipListSet<>();

        doOnTick(() -> {
            logger.debugf("Activating scheduler for [%s]", currentMinute().toDate());
            Observable.just(currentMinute().toDate())
                    .concatMap(time -> jobsService.findActiveTimeSlices(time, queryScheduler))
                    .filter(d -> {
                        synchronized (lock) {
                            if (!activeTimeSlices.contains(d)) {
                                activeTimeSlices.add(d);
                                return true;
                            }
                            return false;
                        }
                    })
                    .concatMap(this::acquireTimeSliceLock)
                    .concatMap(timeSliceLock -> findScheduledJobs(timeSliceLock.getTimeSlice())
                            .doOnError(t -> {
                                logger.warnf("Failed to find schedule jobs for time slice %s", timeSliceLock
                                        .timeSlice);
                            })
                            .doOnNext(jobs -> logger.debugf("[%s] scheduled jobs: %s", timeSliceLock.timeSlice, jobs))
                            .flatMap(scheduledJobs -> computeRemainingJobs(scheduledJobs, timeSliceLock.getTimeSlice(),
                                    activeJobs))
                            .doOnNext(jobs -> logger.debugf("[%s] remaining jobs: %s", timeSliceLock.timeSlice, jobs))
                            .flatMap(Observable::from)
                            .filter(jobDetails -> !activeJobs.contains(jobDetails.getJobId()))
                            .flatMap(this::acquireJobLock)
                            .filter(jobLock -> jobLock.acquired)
                            .map(jobLock -> jobLock.jobDetails)
                            .doOnNext(details -> {
                                logger.debugf("Acquired job lock for %s in time slice %s", details,
                                        timeSliceLock.timeSlice);
                                activeJobs.add(details.getJobId());
                            })
                            // The following observeOn call is critical as it causes execution of the job Completable
                            // subscription to execute on the I/O scheduler. We do not want subscription to run on
                            // either the computation or the queryScheduler. See HWKMETRICS-579 for more details.
                            .observeOn(Schedulers.io())
                            .flatMap(details -> executeJob(details, timeSliceLock.timeSlice, activeJobs)
                                    .doOnTerminate(() -> activeJobs.remove(details.getJobId()))
                                    .toObservable()
                                    .map(o -> timeSliceLock.getTimeSlice()))
                            .defaultIfEmpty(timeSliceLock.getTimeSlice()))
                    // Resume execution back on the queryScheduler (and the computation scheduler) since everything
                    // else that happens from this point on is either non-block or very short lived computations.
                    .observeOn(queryScheduler)
                    .concatMap(time -> {
                        Observable<? extends Set<UUID>> scheduled = jobsService.findScheduledJobsForTime(time,
                                queryScheduler)
                                .map(JobDetails::getJobId)
                                .collect(HashSet<UUID>::new, HashSet::add);
                        Observable<? extends Set<UUID>> finished = findFinishedJobs(time);
                        return Observable.sequenceEqual(scheduled, finished).flatMap(allFinished -> {
                            if (allFinished) {
                                logger.debugf("All jobs for time slice [%s] have finished", time);
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
                                logger.debugf("Finished post job execution clean up for [%s]", d);
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
            // This is a shared lock and it is mutually exclusive with the one that is acquired for scheduling a job.
            lockManager.acquireLock(lockName, TIME_SLICE_EXECUTION_LOCK, JOB_EXECUTION_LOCK_TIMEOUT_IN_SEC, false)
                    .map(lock -> {
                        if (!lock.isLocked()) {
                            logger.debugf("Failed to acquire time slice lock for [%s]. Will attempt to acquire it " +
                                    "again in %d seconds", timeSlice, delay);
                            throw new RuntimeException();
                        }
                        return new TimeSliceLock(timeSlice, lockName, lock.isLocked());
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

    private Observable<JobLock> acquireJobLock(JobDetailsImpl jobDetails) {
        String jobLock = "org.hawkular.metrics.scheduler.job." + jobDetails.getJobId();
        int timeout = calculateTimeout(jobDetails.getTrigger());
        return lockManager.acquireLock(jobLock, hostname, timeout, true)
                .map(lock -> new JobLock(jobDetails, lock))
                .doOnNext(lock -> logger.debugf("Acquired lock for %s? %s", jobDetails.getJobName(),
                        lock.acquired));
    }

    private int calculateTimeout(Trigger trigger) {
        if (trigger instanceof RepeatingTrigger) {
            int interval = (int) (((RepeatingTrigger) trigger).getInterval() / 1000);
            return (int) (interval + (interval * 0.25));
        }
        return JOB_EXECUTION_LOCK_TIMEOUT_IN_SEC;
    }

    private Completable executeJob(JobDetailsImpl details, Date timeSlice, Set<UUID> activeJobs) {
        logger.debugf("Starting execution for %s in time slice [%s]", details, timeSlice);
        Stopwatch stopwatch = Stopwatch.createStarted();
        Func1<JobDetails, Completable> factory = jobFactories.get(details.getJobType());
        Completable job;

        if (factory == null) {
            job = Completable.error(new UnregisteredJobException(details, timeSlice));
        } else if (details.getStatus() == JobStatus.FINISHED) {
            Observable<Completable> observable = jobsService.findScheduledExecutions(details.getJobId(), queryScheduler)
                    .filter(scheduledExecution -> scheduledExecution.getJobDetails().getStatus() == JobStatus.NONE &&
                            scheduledExecution.getJobDetails().getTrigger().getTriggerTime() >
                                    details.getTrigger().getTriggerTime())
                    .isEmpty()
                    .map(isEmpty -> {
                        if (isEmpty) {
                            return doPostJobExecution(Completable.complete(), details, timeSlice, activeJobs);
                        }
                        return doPostJobExecutionWithoutRescheduling(Completable.complete(), details, timeSlice,
                                activeJobs);
                    });
            job = Completable.merge(observable);
        } else {
            jobsService.prepareJobDetailsForExecution(details, timeSlice);
            job = factory
                    .call(details)
                    .onErrorResumeNext(t -> {
                        logger.infof(t, "Execution of %s in time slice [%s] failed", details, timeSlice);

                        RetryPolicy retryPolicy =
                                retryFunctions.getOrDefault(details.getJobType(), NO_RETRY).call(details, t);
                        if (retryPolicy == RetryPolicy.NONE) {
                            return Completable.complete();
                        }
                        if (details.getTrigger().nextTrigger() != null) {
                            logger.warnf("Retry policies cannot be used with jobs that repeat. %s will execute " +
                                    "again according to its next trigger", details);
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
                        JobDetailsImpl newDetails = new JobDetailsImpl(details, newTrigger);
                        return reschedule(new JobExecutionState(newDetails, activeJobs)).toCompletable();
            })
                    .doOnTerminate(() -> jobsService.resetJobDetails(details))
                    .doOnCompleted(() -> {
                        stopwatch.stop();
                        if (logger.isDebugEnabled()) {
                            logger.debugf("Finished executing %s in time slice [%s] in %s ms", details, timeSlice,
                                    stopwatch.elapsed(TimeUnit.MILLISECONDS));
                        }
            });
            job = doPostJobExecution(job, details, timeSlice, activeJobs);
        }

        return job.subscribeOn(Schedulers.io());
    }

    private Completable doPostJobExecution(Completable job, JobDetailsImpl jobDetails, Date timeSlice,
            Set<UUID> activeJobs) {
        return job
                .toSingle(() -> new JobExecutionState(jobDetails, timeSlice, null, null, activeJobs))
                .flatMap(state -> jobsService.updateStatusToFinished(timeSlice, state.currentDetails.getJobId())
                        .toSingle()
                        .map(resultSet -> state))
                .flatMap(this::reschedule)
                .flatMap(state -> releaseJobExecutionLock(state).flatMap(this::setJobFinished))
                .doOnError(t -> {
                    logger.debug("There was an error during post-job execution", t);
                    publishJobFinished(jobDetails);
                })
                .doOnSuccess(states -> publishJobFinished(states.currentDetails))
                .toCompletable();
    }

    private Completable doPostJobExecutionWithoutRescheduling(Completable job, JobDetailsImpl jobDetails,
            Date timeSlice, Set<UUID> activeJobs) {
        return job
                .toSingle(() -> new JobExecutionState(jobDetails, activeJobs))
                .flatMap(this::releaseJobExecutionLock)
                .flatMap(this::setJobFinished)
                .doOnError(t -> {
                    logger.debug("There was an error during post-job execution, but the job has already been " +
                            "rescheduled.", t);
                    publishJobFinished(jobDetails);
                })
                .doOnSuccess(states -> publishJobFinished(states.currentDetails))
                .toCompletable();
    }

    private void publishJobFinished(JobDetails details) {
        jobFinished.ifPresent(subject -> subject.onNext(details));
    }

    private Single<JobExecutionState> setJobFinished(JobExecutionState state) {
        return session.execute(updateJobToFinished.bind(state.timeSlice,
                state.currentDetails.getJobId()), queryScheduler)
                .toSingle()
                .map(resultSet -> state)
                .doOnError(t -> logger.warnf(t, "There was an error while updated the finished jobs index for %s",
                        state.currentDetails));
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
            logger.debugf("No more scheduled executions for %s", executionState.currentDetails);
            return Single.just(executionState);
        }

        JobDetailsImpl details = executionState.currentDetails;
        JobDetailsImpl newDetails = new JobDetailsImpl(details, nextTrigger);

        if (nextTrigger.getTriggerTime() <= now.get().getMillis()) {
            logger.infof("%s missed its next execution at %d. It will be rescheduled for immediate execution.",
                    details, nextTrigger.getTriggerTime());
            AtomicLong nextTimeSlice = new AtomicLong(currentMinute().getMillis());
            Observable<Lock> scheduled = Observable.defer(() ->
                    lockManager.acquireLock(QUEUE_LOCK_PREFIX + nextTimeSlice.addAndGet(60000L), SCHEDULING_LOCK,
                            SCHEDULING_LOCK_TIMEOUT_IN_SEC, false))
                    .map(lock -> {
                        if (!lock.isLocked()) {
                            throw new RuntimeException();
                        }
                        return lock;
                    })
                    .retry();
            return scheduled
                    .map(lock -> new JobExecutionState(executionState.currentDetails, executionState.timeSlice,
                            newDetails, new Date(nextTimeSlice.get()), executionState.activeJobs))
                    .flatMap(state -> jobsService.insert(state.nextTimeSlice, state.nextDetails)
                            .map(updated -> state))
                    .doOnNext(state -> logger.debugf("Rescheduled %s to execute in time slice %s with trigger time " +
                            "of %s", state.nextDetails.getJobName(), state.nextTimeSlice, new Date(state.nextDetails
                            .getTrigger().getTriggerTime())))
                    .toSingle();
        }

        logger.debugf("Scheduling %s for next execution at %s", newDetails, new Date(nextTrigger.getTriggerTime()));

        JobExecutionState newState = new JobExecutionState(details, executionState.timeSlice, newDetails,
                new Date(nextTrigger.getTriggerTime()), executionState.activeJobs);
        return jobsService.insert(newState.nextTimeSlice, newState.nextDetails)
                .map(updated -> newState)
                .toSingle();
    }

    private Single<JobExecutionState> releaseJobExecutionLock(JobExecutionState state) {
        String jobLock = "org.hawkular.metrics.scheduler.job." + state.currentDetails.getJobId();
        return lockManager.releaseLock(jobLock, hostname)
                .map(released -> {
                    if (!released) {
                        logger.warnf("Failed to release job lock for %s", state.currentDetails);
                    }
                    return state;
                })
                .toSingle()
                .doOnError(t -> logger.warnf(t, "There was an error trying to release job lock [%s] for %s", jobLock,
                        state.currentDetails));
    }

    private Completable deleteScheduledJobs(Date timeSlice) {
        return session.execute(deleteScheduledJobs.bind(timeSlice), queryScheduler)
                .doOnCompleted(() -> logger.debugf("Deleted scheduled jobs time slice [%s]", timeSlice))
                .toCompletable();
    }

    private Completable deleteFinishedJobs(Date timeSlice) {
        return session.execute(deleteFinishedJobs.bind(timeSlice), queryScheduler)
                .doOnCompleted((() -> logger.debugf("Deleted finished jobs time slice [%s]", timeSlice)))
                .toCompletable();
    }

    private Completable deleteActiveTimeSlice(Date timeSlice) {
        return session.execute(deleteActiveTimeSlice.bind(timeSlice), queryScheduler)
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
            lockManager.shutdown();
            logger.info("Shutdown complete");
        } catch (InterruptedException e) {
            logger.warn("Interrupted during shutdown", e);
        }
    }

    private Observable<? extends Set<JobDetailsImpl>> findScheduledJobs(Date timeSlice) {
        logger.debugf("Fetching scheduled jobs for [%s]", timeSlice);
        return jobsService.findScheduledJobsForTime(timeSlice, queryScheduler)
                .collect(HashSet<JobDetailsImpl>::new, HashSet::add);
    }

    private Observable<Set<JobDetailsImpl>> computeRemainingJobs(Set<JobDetailsImpl> scheduledJobs, Date timeSlice,
            Set<UUID> activeJobs) {
        Observable<? extends Set<UUID>> finished = findFinishedJobs(timeSlice);
        return finished.map(finishedJobs -> {
            Set<UUID> active = new HashSet<>(activeJobs);
            active.removeAll(finishedJobs);
            Set<JobDetailsImpl> jobs = scheduledJobs.stream()
                    .filter(details -> !finishedJobs.contains(details.getJobId()))
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

    private void doOnTick(Action0 action) {
        Action0 wrapper = () -> {
            Date timeSlice = getTimeSlice(new DateTime(tickScheduler.now()), minutes(1).toStandardDuration()).toDate();
            action.call();
        };
        AtomicReference<DateTime> previousTimeSliceRef = new AtomicReference<>();
        // TODO Emit ticks at the start of every minute
        Observable.interval(0, 1, TimeUnit.MINUTES, tickScheduler)
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
        final JobDetailsImpl jobDetails;
        final boolean acquired;
        final String name;

        public JobLock(JobDetailsImpl jobDetails, Lock lock) {
            this.jobDetails = jobDetails;
            this.acquired = lock.isLocked();
            this.name = "org.hawkular.metrics.scheduler.job." + jobDetails.getJobId();
        }
    }

    /**
     * Normally jobs are executed in the time slice specified by their triggers. If a job falls behind though, it
     * will be executed immediately
     */
    private static class JobExecutionState {
        final JobDetailsImpl currentDetails;
        final JobDetailsImpl nextDetails;
        final Set<UUID> activeJobs;
        final Date timeSlice;
        final Date nextTimeSlice;

        public JobExecutionState(JobDetailsImpl details, Set<UUID> activeJobs) {
            this.currentDetails = details;
            this.activeJobs = activeJobs;
            this.timeSlice = new Date(details.getTrigger().getTriggerTime());
            nextDetails = null;
            nextTimeSlice = null;
        }

        public JobExecutionState(JobDetailsImpl details, Date timeSlice, JobDetailsImpl nextDetails, Date nextTimeSlice,
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
            return isRepeating() && nextTimeSlice != null && nextTimeSlice.getTime() > nextDetails.getTrigger()
                    .getTriggerTime();
        }

    }

}
