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

import static java.util.Collections.emptySet;

import static org.hawkular.metrics.datetime.DateTimeService.getTimeSlice;
import static org.joda.time.Minutes.minutes;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.hawkular.metrics.scheduler.api.JobDetails;
import org.hawkular.metrics.scheduler.api.RepeatingTrigger;
import org.hawkular.metrics.scheduler.api.Scheduler;
import org.hawkular.metrics.scheduler.api.SingleExecutionTrigger;
import org.hawkular.metrics.scheduler.api.Trigger;
import org.hawkular.metrics.sysconfig.ConfigurationService;
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
import rx.functions.Action0;
import rx.functions.Func1;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;

/**
 * @author jsanda
 */
public class SchedulerImpl implements Scheduler {

    private static Logger logger = Logger.getLogger(SchedulerImpl.class);

    private Map<String, Func1<JobDetails, Completable>> jobFactories;

    private ScheduledExecutorService tickExecutor;

    private rx.Scheduler tickScheduler;

    private ExecutorService queueExecutor;

    private rx.Scheduler queueScheduler;

    private ExecutorService queryExecutor;

    private rx.Scheduler queryScheduler;

    private RxSession session;

    private PreparedStatement insertJob;

    private PreparedStatement insertJobInQueue;

    private PreparedStatement findScheduledJobs;

    private PreparedStatement deleteScheduledJobs;

    private PreparedStatement findFinishedJobs;

    private PreparedStatement deleteFinishedJobs;

    private PreparedStatement updateJobToFinished;

    private PreparedStatement findJob;

    private LockManager lockManager;

    private ConfigurationService configurationService;

    private String name;

    private boolean running;

    /**
     * Test hook. See {@link #setTimeSlicesSubject(PublishSubject)}.
     */
    private Optional<PublishSubject<Date>> finishedTimeSlices;

    private Optional<PublishSubject<JobDetails>> jobFinished;

    public SchedulerImpl(RxSession session) {
        this.session = session;
        jobFactories = new HashMap<>();
        tickExecutor = Executors.newScheduledThreadPool(1,
                new ThreadFactoryBuilder().setNameFormat("ticker-pool-%d").build());
        queueExecutor = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat("job-queue-pool-%d").build());
        tickScheduler = Schedulers.from(tickExecutor);
        queueScheduler = Schedulers.from(queueExecutor);

        queryExecutor = Executors.newFixedThreadPool(4,
                new ThreadFactoryBuilder().setNameFormat("query-thread-pool-%d").build());
        queryScheduler = Schedulers.from(queryExecutor);

        lockManager = new LockManager(session);

        insertJob = session.getSession().prepare(
                "INSERT INTO jobs (id, type, name, params, trigger) VALUES (?, ?, ?, ?, ?)");
        insertJobInQueue = session.getSession().prepare(
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

        finishedTimeSlices = Optional.empty();
        jobFinished = Optional.empty();

        try {
            name = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Test hook to allow control of when ticks are emitted.
     */
    void setTickScheduler(rx.Scheduler scheduler) {
        tickScheduler = scheduler;
    }

    /**
     * Test hook to broadcast when the job scheduler has finished all work for a time slice. This includes not only
     * executing jobs that are scheduled for a particular time slice but also post-job execution clean up.
     */
    void setTimeSlicesSubject(PublishSubject<Date> timeSlicesSubject) {
        finishedTimeSlices = Optional.of(timeSlicesSubject);
    }

    /**
     * Test hook to broadcast when jobs finish executing.
     */
    void setJobFinishedSubject(PublishSubject<JobDetails> subject) {
        jobFinished = Optional.of(subject);
    }

    public void setConfigurationService(ConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    @Override
    public void registerJobFactory(String jobType, Func1<JobDetails, Completable> factory) {
        jobFactories.put(jobType, factory);
    }

    public Observable<JobDetails> scheduleJob(String type, String name, Map<String, String> parameter,
            Trigger trigger) {
        String lockName = "org.hawkular.metrics.scheduler.queue." + trigger.getTriggerTime();
        String permit = getLockOwner();
        return lockManager.acquirePermit(lockName, permit, 30)
                .map(acquired -> {
                    if (acquired) {
                        UUID jobId = UUID.randomUUID();
                        return new JobDetails(jobId, type, name, parameter, trigger);
                    } else {
                        throw new RuntimeException("Failed to schedule job");
                    }
                })
                .flatMap(jobDetails -> findActiveTimeSlice().map(timeSlice -> {
                    if (jobDetails.getTrigger().getTriggerTime() >= timeSlice.getTime()) {
                        return jobDetails;
                    } else {
                        throw new RuntimeException("Failed to schedule job");
                    }
                }))
                .flatMap(this::insertIntoJobsTable)
                .flatMap(this::addJobToQueue)
                .flatMap(details -> lockManager.releasePermit(lockName, permit)
                        .map(released -> {
                            if (released) {
                                return details;
                            } else {
                                throw new RuntimeException("Failed to schedule job.");
                            }
                        }));
    }

    public void start() {
        running = true;
        NavigableSet<Date> activeTimeSlices = new ConcurrentSkipListSet<>();
        Set<UUID> activeJobs = new ConcurrentSkipListSet<>();
        Map<String, String> jobLocks = new ConcurrentHashMap<>();

        doOnTick(() -> {
            Date timeSlice;
            if (activeTimeSlices.isEmpty()) {
                // TODO handle null
                timeSlice = findActiveTimeSlice().toBlocking().lastOrDefault(null);
            } else {
                Date latestActiveTimeSlice = activeTimeSlices.last();
                timeSlice = getTimeSlice(new DateTime(latestActiveTimeSlice.getTime()).plusMinutes(1),
                        minutes(1).toStandardDuration()).toDate();
            }
            activeTimeSlices.add(timeSlice);

            logger.debug("Running job scheduler for [" + timeSlice + "]");
            logger.debug("Currently active time slices are [" + activeTimeSlices + "]");

            Set<UUID> scheduledJobs = findScheduledJobsBlocking(timeSlice);
            logger.debug("Scheduled jobs = " + scheduledJobs);
            getJobsToExecute(timeSlice, scheduledJobs, activeJobs, jobLocks)
                    .observeOn(Schedulers.computation())
                    .map(details -> {
                        logger.debug("Starting execution for " + details);
                        Func1<JobDetails, Completable> jobFactory = jobFactories.get(details.getJobType());
                        Completable job = jobFactory.call(details);

                        logger.debug("Preparing to execute " + details);
                        return Completable.merge(
                                job,
                                setJobFinished(timeSlice, details),
                                rescheduleJob(details),
                                deactivateJob(activeJobs, jobLocks, details),
                                Completable.fromAction(() -> jobFinished.ifPresent(subject -> subject.onNext(details)))
                        ).subscribeOn(Schedulers.io());

//                        return Completable.concat(
//                                Completable.fromAction(() -> {
//                                    job.call(details);
//                                }),
//                                setJobFinished(timeSlice, details),
//                                rescheduleJob(details),
//                                deactivateJob(activeJobs, jobLocks, details),
//                                Completable.fromAction(() -> jobFinished.ifPresent(subject -> subject.onNext(details)))
//                        ).subscribeOn(Schedulers.io());
                    })
                    .doOnNext(c -> logger.debug("Started job execution"))
                    // We need to use the version of reduce that take an initial value versus the version that only
                    // takes an accumulator function. There may be time slices for which there are no jobs to execute.
                    // When there are no jobs, the version of reduce that only takes an accumulator will result in an
                    // NoSuchElementException: Sequence contains no elements.
                    .reduce(Completable.complete(), Completable::merge)
                    .flatMap(completable -> Completable.fromAction(completable::await).toObservable())
                    .observeOn(Schedulers.computation())
                    .subscribe(
                            completable -> {},
                            t -> logger.warn("Job execution for time slice [" + timeSlice + "] failed", t),
                            () -> {
                                logger.debug("No more jobs to execute for [" + timeSlice + "]");
                                Observable<Set<UUID>> scheduled = Observable.just(scheduledJobs);
                                Observable<? extends Set<UUID>> finished = findFinishedJobs(timeSlice);
                                Observable.sequenceEqual(scheduled, finished).flatMap(allJobsFinished -> {
                                    if (allJobsFinished) {
                                        logger.debug("All jobs for time slice [" + timeSlice + "] have finished");
                                        Date nextTimeSlice = new Date(timeSlice.getTime() + 60000);
                                        logger.debug("Setting active-queue to [" + nextTimeSlice + "]");

                                        return setActiveTimeSlice(nextTimeSlice)
                                                .concatWith(deleteScheduledJobs(timeSlice))
                                                .concatWith(deleteFinishedJobs(timeSlice))
                                                .toObservable();
                                    }
                                    logger.debug("Scheduled = " + scheduled.toBlocking().first());
                                    logger.debug("Finished = " + finished.toBlocking().first());
                                    return Observable.empty();
                                }).observeOn(queryScheduler).subscribe(
                                        object -> {},
                                        t -> {
                                            logger.warn("Post job execution clean up failed", t);
                                            finishedTimeSlices.ifPresent(subject -> subject.onError(t));
                                        },
                                        () -> {
                                            logger.debug("Post job execution clean up done for [" + timeSlice + "]");
                                            activeTimeSlices.remove(timeSlice);
                                            finishedTimeSlices.ifPresent(subject -> subject.onNext(timeSlice));
                                            if (!running) {
                                                logger.debug("Done!");
                                            }
                                        }
                                );
                            }
            );
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
        return session.execute(updateJobToFinished.bind(timeSlice, details.getJobId()), queryScheduler)
                .doOnCompleted(() -> logger.debug("Updating " + details + " status to finished for time slice [" +
                        timeSlice + "]"))
                .toCompletable();
    }

    private Completable rescheduleJob(JobDetails details) {
        Trigger nextTrigger = details.getTrigger().nextTrigger();
        if (nextTrigger == null) {
            return Completable.fromAction(() -> logger.debug("No more scheduled executions for " + details));
        }

        JobDetails newDetails = new JobDetails(details.getJobId(), details.getJobType(), details.getJobName(),
                details.getParameters(), nextTrigger);
        return Completable.concat(
                Completable.fromAction(() -> logger.debug("Scheduling " + newDetails + " for next execution at " +
                        new Date(nextTrigger.getTriggerTime()))),
                addToJobQueueX(newDetails),
                insertIntoJobsTableX(newDetails)
        );
    }

    private Completable deactivateJob(Set<UUID> activeJobs, Map<String, String> jobLocks, JobDetails details) {
        String jobLock = "org.hawkular.metrics.scheduler.job." + details.getJobId();
        String lockValue = jobLocks.remove(jobLock);

        Completable removeActiveJobId = Completable.fromAction(() -> {
            logger.debug("Removing " + details + " from active jobs");
            activeJobs.remove(details.getJobId());
        });

        Completable releaseLock = lockManager.releaseLock(jobLock, lockValue)
                .doOnNext(released -> logger.debug("Released job lock [" + jobLock + "]? " + released))
                .toCompletable();

        return removeActiveJobId.concatWith(releaseLock);
    }

    private Completable setActiveTimeSlice(Date timeSlice) {
        return configurationService.save("org.hawkular.metrics.scheduler", "active-queue",
                Long.toString(timeSlice.getTime()), queryScheduler).toCompletable();
    }

    private Completable deleteScheduledJobs(Date timeSlice) {
        return session.execute(deleteScheduledJobs.bind(timeSlice), queryScheduler).toCompletable();
    }

    private Completable deleteFinishedJobs(Date timeSlice) {
        return session.execute(deleteFinishedJobs.bind(timeSlice), queryScheduler).toCompletable();
    }

    public void shutdown() {
        try {
            running = false;
            tickExecutor.shutdown();
            tickExecutor.awaitTermination(5, TimeUnit.SECONDS);
            queueExecutor.shutdown();
            queueExecutor.awaitTermination(30, TimeUnit.SECONDS);
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
        queueExecutor = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat("job-queue-pool-%d").build());
        this.tickScheduler = tickScheduler;
        queueScheduler = Schedulers.from(queueExecutor);
        queryExecutor = Executors.newFixedThreadPool(2,
                new ThreadFactoryBuilder().setNameFormat("query-thread-pool-%d").build());
        queryScheduler = Schedulers.from(queryExecutor);
    }

    private Observable<JobDetails> getJobsToExecute(Date timeSlice, Set<UUID> scheduledJobs,
            Set<UUID> activeJobs, Map<String, String> jobLocks) {
        return Observable.create(subscriber -> {
            String lockName = "org.hawkular.metrics.scheduler.queue." + timeSlice.getTime();

            logger.debug("Acquiring queue lock [" + lockName + "] for " + timeSlice);
            boolean acquiredLock = lockManager.acquireLock(lockName, "locked", 300, queryScheduler).toBlocking()
                    .lastOrDefault(false);
            TimeSliceLock lock = new TimeSliceLock(timeSlice, getLockOwner(), acquiredLock);
            logger.debug("Compute remaining jobs....");

            try {
                Set<UUID> remainingJobs = computeRemainingJobs(scheduledJobs, lock.getTimeSlice(), emptySet());

                logger.debug("The jobs scheduled for time slice [" + lock.getTimeSlice() + "] are " + scheduledJobs);
                logger.debug("Remaining jobs = " + remainingJobs + ", active jobs = " + activeJobs);

                while (!remainingJobs.isEmpty()) {
                    logger.debug("Scanning remaining jobs");
                    remainingJobs.stream().limit(2).filter(jobId -> !activeJobs.contains(jobId)).forEach(jobId -> {
                        String jobLock = "org.hawkular.metrics.scheduler.job." + jobId;
                        activeJobs.add(jobId);
                        boolean acquired = lockManager.acquireLock(jobLock, getLockOwner(), 3600, queryScheduler)
                                .toBlocking().first();
                        logger.debug("Acquired job lock for job [" + jobId + "]? " + acquired);
                        if (acquired) {
                            // TODO handle null
                            JobDetails jobDetails = findJob(jobId).toBlocking().firstOrDefault(null);
                            logger.debug("Acquired job execution lock for " + jobDetails);
                            jobLocks.put(jobLock, getLockOwner());
                            subscriber.onNext(jobDetails);
                            logger.debug("Emitted job " + jobDetails);
                        } else {
                            logger.debug("Failed to acquire job [" + jobId + "]");
                        }
                    });
                    remainingJobs = computeRemainingJobs(scheduledJobs, lock.getTimeSlice(), activeJobs);
                    logger.debug("Remaining jobs = " + remainingJobs);
                }
                logger.debug("No more jobs! [" + lock.getTimeSlice() + "]");
                subscriber.onCompleted();
            } catch (Exception e) {
                logger.warn("There was an error getting jobs", e);
                subscriber.onError(e);
            }
        }).doOnCompleted(() -> logger.debug("Finished scanning for jobs")).cast(JobDetails.class);
    }

    private Set<UUID> findScheduledJobsBlocking(Date timeSlice) {
        logger.debug("Fetching scheduled jobs for [" + timeSlice + "]");
        return session.execute(findScheduledJobs.bind(timeSlice))
                .flatMap(Observable::from)
                .map(row -> row.getUUID(0))
                .doOnNext(uuid -> logger.debug("Scheduled job [" + uuid + "]"))
                .collect(HashSet<UUID>::new, HashSet::add)
                .toBlocking()
                .firstOrDefault(new HashSet<>());
    }

    private Set<UUID> computeRemainingJobs(Set<UUID> scheduledJobs, Date timeSlice, Set<UUID> activeJobs) {
        Set<UUID> finishedJobs = findFinishedJobsBlocking(timeSlice);
        activeJobs.removeAll(finishedJobs);
        Set<UUID> jobs = Sets.difference(scheduledJobs, finishedJobs);
        return Sets.difference(jobs, activeJobs);
    }

    private Set<UUID> findFinishedJobsBlocking(Date timeSlice) {
        return session.execute(findFinishedJobs.bind(timeSlice), queryScheduler)
                .flatMap(Observable::from)
                .map(row -> row.getUUID(0))
                .collect(HashSet<UUID>::new, HashSet::add)
                .toBlocking()
                .firstOrDefault(new HashSet<>());
    }

    private Observable<? extends Set<UUID>> findFinishedJobs(Date timeSlice) {
        return session.execute(findFinishedJobs.bind(timeSlice), queryScheduler)
                .flatMap(Observable::from)
                .map(row -> row.getUUID(0))
                .collect(HashSet<UUID>::new, HashSet::add);
    }

    private Observable<JobDetails> insertIntoJobsTable(JobDetails details) {
        return session.execute(insertJob.bind(details.getJobId(), details.getJobType(), details.getJobName(),
                details.getParameters(), getTriggerValue(session, details.getTrigger())))
                .map(resultSet -> details);
    }

    private Observable<JobDetails> addJobToQueue(JobDetails details) {
        return session.execute(insertJobInQueue.bind(new Date(details.getTrigger().getTriggerTime()),
                details.getJobId()))
                .map(resultSet -> details);
    }

    private Completable insertIntoJobsTableX(JobDetails details) {
        return session.execute(insertJob.bind(details.getJobId(), details.getJobType(), details.getJobName(),
                details.getParameters(), getTriggerValue(session, details.getTrigger())), queryScheduler)
                .toCompletable();
    }

    private Completable addToJobQueueX(JobDetails details) {
        return session.execute(insertJobInQueue.bind(new Date(details.getTrigger().getTriggerTime()),
                details.getJobId()), queryScheduler).toCompletable();
    }

    private void doOnTick(Action0 action) {
        Action0 wrapper = () -> {
            Date timeSlice = getTimeSlice(new DateTime(tickScheduler.now()), minutes(1).toStandardDuration()).toDate();
            logger.debug("[TICK][" + timeSlice + "] executing action");
            action.call();
            logger.debug("Finished tick for [" + timeSlice + "]");
        };
        AtomicReference<DateTime> previousTimeSliceRef = new AtomicReference<>();
        Observable.interval(0, 1, TimeUnit.MINUTES, tickScheduler)
                .filter(tick -> {
                    DateTime currentTimeSlice = new DateTime(getTimeSlice(tickScheduler.now(),
                            minutes(1).toStandardDuration()));
                    if (previousTimeSliceRef.get() == null) {
                        previousTimeSliceRef.set(currentTimeSlice);
                        return false;
                    }
                    logger.debug("previous=[" + previousTimeSliceRef.get().toLocalDateTime() + "], current=[" +
                            currentTimeSlice.toLocalDateTime() + "]");
                    if (previousTimeSliceRef.get().isBefore(currentTimeSlice)) {
                        previousTimeSliceRef.set(currentTimeSlice);
                        return true;
                    }
                    return false;
                })
                .takeUntil(d -> !running)
                .observeOn(queueScheduler)
                .subscribe(tick -> wrapper.call(), t -> logger.warn(t));
    }

    private Observable<Date> findActiveTimeSlice() {
        // TODO We shouldn't ever get an empty result set but need to handle it to be safe
        return configurationService.load("org.hawkular.metrics.scheduler")
                .map(config -> new Date(Long.parseLong(config.get("active-queue"))));
    }

    private Observable<JobDetails> findJob(UUID jobId) {
        return session.execute(findJob.bind(jobId), queryScheduler)
                .flatMap(Observable::from)
                .map(row -> new JobDetails(jobId, row.getString(0), row.getString(1),
                        row.getMap(2, String.class, String.class), getTrigger(row.getUDTValue(3))))
                .doOnError(t -> logger.warn("Failed to fetch job [" + jobId + "]", t));
    }

    private String getLockOwner() {
        return name + ":" + Thread.currentThread().getName();
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
}
