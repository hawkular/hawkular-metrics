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
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
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

    private Map<String, Func1<JobDetails, Observable<Void>>> jobCreators;

    private ScheduledExecutorService tickExecutor;

    private rx.Scheduler tickScheduler;

    private ThreadPoolExecutor queueExecutor;

    private rx.Scheduler queueScheduler;

    private RxSession session;

    private PreparedStatement insertJob;

    private PreparedStatement insertJobInQueue;

    private PreparedStatement findScheduledJobs;

    private PreparedStatement deleteScheduledJobs;

    private PreparedStatement findFinishedJobs;

    private PreparedStatement deleteFinishedJobs;

    private PreparedStatement updateJobToFinished;

    private PreparedStatement findJob;

    private PreparedStatement removeJobFromQueue;

    private LockManager lockManager;

    private ConfigurationService configurationService;

    private String name;

    private boolean running;

    private Optional<PublishSubject<Date>> finishedTimeSlices;

    public SchedulerImpl(RxSession session) {
        this.session = session;
        jobCreators = new HashMap<>();
        tickExecutor = Executors.newScheduledThreadPool(1,
                new ThreadFactoryBuilder().setNameFormat("ticker-pool-%d").build());
//        queueExecutor = Executors.newSingleThreadExecutor(
//                new ThreadFactoryBuilder().setNameFormat("job-queue-pool-%d").build());
        queueExecutor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(),
                new ThreadFactoryBuilder().setNameFormat("job-queue-pool-%d").build());
        tickScheduler = Schedulers.from(tickExecutor);
        queueScheduler = Schedulers.from(queueExecutor);

        lockManager = new LockManager(session);

        insertJob = session.getSession().prepare(
                "INSERT INTO jobs (id, type, name, params, trigger) VALUES (?, ?, ?, ?, ?)");
        insertJobInQueue = session.getSession().prepare(
                "INSERT INTO jobs_status (time_slice, job_id) VALUES (?, ?)");
        findScheduledJobs = session.getSession().prepare("SELECT job_id, status FROM jobs_status WHERE time_slice = ?");
        deleteScheduledJobs = session.getSession().prepare("DELETE FROM jobs_status WHERE time_slice = ?");
        findFinishedJobs = session.getSession().prepare(
                "SELECT job_id FROM finished_jobs_time_idx WHERE time_slice = ?");
        deleteFinishedJobs = session.getSession().prepare(
                "DELETE FROM finished_jobs_time_idx WHERE time_slice = ?");
        updateJobToFinished = session.getSession().prepare(
                "INSERT INTO finished_jobs_time_idx (time_slice, job_id) VALUES (?, ?)");
        findJob = session.getSession().prepare("SELECT type, name, params, trigger FROM jobs WHERE id = ?");
        removeJobFromQueue = session.getSession().prepare(
                "DELETE FROM jobs_status WHERE time_slice = ? AND job_id = ?");

        finishedTimeSlices = Optional.empty();

        try {
            name = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Test hook
     */
    void setTickScheduler(rx.Scheduler scheduler) {
        tickScheduler = scheduler;
    }

    void setTimeSlicesSubject(PublishSubject<Date> timeSlicesSubject) {
        finishedTimeSlices = Optional.of(timeSlicesSubject);
    }

    public void setConfigurationService(ConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    @Override
    public void registerJobCreator(String jobType, Func1<JobDetails, Observable<Void>> jobCreator) {
        jobCreators.put(jobType, jobCreator);
    }

    public Observable<JobDetails> scheduleJob(String type, String name, Map<String, String> parameter,
            Trigger trigger) {
        String lock = "org.hawkular.metrics.scheduler.queue." + trigger.getTriggerTime();
        return lockManager.acquireSharedLock(getLockOwner(), lock, 30)
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
                .flatMap(details -> lockManager.releaseSharedLock(lock, name)
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
        doOnTick(() -> {
            Date timeSlice;
            if (activeTimeSlices.isEmpty()) {
                timeSlice = findActiveTimeSlice().toBlocking().lastOrDefault(null);
            } else {
                Date latestActiveTimeSlice = activeTimeSlices.last();
                timeSlice = getTimeSlice(new DateTime(latestActiveTimeSlice.getTime()).plusMinutes(1),
                        minutes(1).toStandardDuration()).toDate();
            }
            activeTimeSlices.add(timeSlice);

            logger.debug("Running job scheduler for [" + timeSlice + "]");
            logger.debug("Currently active time slices are [" + activeTimeSlices + "]");

            Set<UUID> scheduledJobs = findScheduledJobsSync(timeSlice);
            getJobsToExecuteBlocking(timeSlice, scheduledJobs, activeJobs)
                    .flatMap(jobDetails -> Observable.just(jobDetails).subscribeOn(Schedulers.computation()))
                    .subscribe(
                            jobDetails -> {
                                Func1<JobDetails, Observable<Void>> factory = jobCreators.get(jobDetails.getJobType());
                                Observable<Void> job = factory.call(jobDetails);
                                job.subscribe(
                                        aVoid -> {},
                                        t -> logger.warn("There was an error executing " + jobDetails),
                                        () -> {
                                            logger.debug("Finished executing " + jobDetails);
                                            logger.debug("Updating " + jobDetails + " to finished");
                                            session.execute(updateJobToFinished.bind(timeSlice, jobDetails.getJobId()))
                                                    .subscribe(
                                                            resultSet -> logger.debug(jobDetails + " is finished"),
                                                            t -> logger.warn("Failed to set " + jobDetails +
                                                                    " to finished")
                                                    );
                                        }
                                );
                            },
                            t -> logger.warn("Job execution for [" + timeSlice + "] failed", t),
                            () -> {
                                Observable<Set<UUID>> scheduled = Observable.just(scheduledJobs);
                                Observable<? extends Set<UUID>> finished = findFinishedJobsAsync(timeSlice);
                                Observable.sequenceEqual(scheduled, finished).flatMap(allJobsFinished -> {
                                    logger.debug("All jobs finished? " + allJobsFinished);
                                    if (allJobsFinished) {
                                        logger.debug("All jobs for time slice [" + timeSlice + "] have finished");
                                        Date nextTimeSlice = new Date(timeSlice.getTime() + 60000);
                                        logger.debug("Setting active-queue to [" + nextTimeSlice + "]");
                                        Observable<Void> activeTimeSliceUpdated = configurationService.save(
                                                "org.hawkular.metrics.scheduler", "active-queue",
                                                Long.toString(nextTimeSlice.getTime()));
                                        Observable<Void> deleteScheduled = session.execute(deleteScheduledJobs.bind(
                                                timeSlice)).map(resultSet -> (Void) null);
                                        Observable<Void> deleteFinished = session.execute(deleteFinishedJobs.bind(
                                                timeSlice)).map(resultSet -> (Void) null);
                                        return Observable.merge(activeTimeSliceUpdated, deleteScheduled,
                                                deleteFinished);
                                    }
                                    return Observable.empty();
                                }).subscribe(
                                        aVoid -> {},
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
                            });
        });
    }

    public void shutdown() {
        running = false;
    }

    private Observable<JobDetails> getJobsToExecuteBlocking(Date timeSlice, Set<UUID> scheduledJobs,
            Set<UUID> activeJobs) {
        return Observable.create(subscriber -> {
            String lockName = "org.hawkular.metrics.scheduler.queue." + timeSlice.getTime();
            logger.debug("Acquiring queue lock [" + lockName + "] for " + timeSlice);
            TimeSliceLock lock = lockManager.acquireExclusiveShared(getLockOwner(), lockName).map(acquired -> {
                logger.debug("Acquired lock for [" + timeSlice + "]? " + acquired);
                return new TimeSliceLock(timeSlice, getLockOwner(), acquired);
            }).toBlocking().lastOrDefault(null);

            try {
                Set<UUID> remainingJobs = computeRemainingJobs(scheduledJobs, lock.getTimeSlice(), emptySet());

                logger.debug("The jobs scheduled for time slice [" + lock.getTimeSlice() + "] are " + scheduledJobs);
                logger.debug("Remaining jobs = " + remainingJobs + ", active jobs = " + activeJobs);

                while (!remainingJobs.isEmpty()) {
                    remainingJobs.stream().limit(2).filter(jobId -> !activeJobs.contains(jobId)).forEach(jobId -> {
                        String jobLock = "org.hawkular.metrics.scheduler.job." + jobId;
                        activeJobs.add(jobId);
                        boolean acquired = lockManager.acquireExclusiveShared(getLockOwner(), jobLock).toBlocking()
                                .first();
                        logger.debug("Acquired job lock for job [" + jobId + "]? " + acquired);
                        if (acquired) {
//                            JobDetails jobDetails = findJob(jobId).toBlocking().first();
                            JobDetails jobDetails = findJob(jobId).toBlocking().firstOrDefault(null);
                            logger.debug("Acquired job execution lock for " + jobDetails);
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
        });
    }

    private Set<UUID> findScheduledJobsSync(Date timeSlice) {
        return session.executeAndFetch(findScheduledJobs.bind(timeSlice))
                .map(row -> row.getUUID(0))
                .collect(HashSet<UUID>::new, HashSet::add)
                .toBlocking()
                .firstOrDefault(new HashSet<>());
    }

    private Set<UUID> computeRemainingJobs(Set<UUID> scheduledJobs, Date timeSlice, Set<UUID> activeJobs) {
        Set<UUID> finishedJobs = findFinishedJobs(timeSlice);
        activeJobs.removeAll(finishedJobs);
        Set<UUID> jobs = Sets.difference(scheduledJobs, finishedJobs);
        return Sets.difference(jobs, activeJobs);
    }

    private Set<UUID> findFinishedJobs(Date timeSlice) {
        return session.execute(findFinishedJobs.bind(timeSlice))
                .flatMap(Observable::from)
                .map(row -> row.getUUID(0))
                .collect(HashSet<UUID>::new, HashSet::add)
                .toBlocking()
                .firstOrDefault(new HashSet<>());
    }

    private Observable<? extends Set<UUID>> findFinishedJobsAsync(Date timeSlice) {
        return session.execute(findFinishedJobs.bind(timeSlice))
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

    private void doOnTick(Action0 action) {
        Action0 wrapper = () -> {
            logger.debug("[TICK][" + getTimeSlice(new DateTime(tickScheduler.now()), minutes(1).toStandardDuration())
                    .toLocalTime() + "] executing action");
            action.call();
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

    private Observable<Date> findActiveTimeSlice(rx.Scheduler scheduler) {
        // TODO We shouldn't ever get an empty result set but need to handle it to be safe
        return configurationService.load("org.hawkular.metrics.scheduler", scheduler)
                .map(config -> new Date(Long.parseLong(config.get("active-queue"))))
                .doOnNext(timeSlice -> logger.debug("Next time slice [" + timeSlice + "]"));
    }

    private Observable<JobDetails> findJob(UUID jobId) {
        return session.executeAndFetch(findJob.bind(jobId))
                .map(row -> new JobDetails(jobId, row.getString(0), row.getString(1),
                        row.getMap(2, String.class, String.class), getTrigger(row.getUDTValue(3))))
                .doOnError(t -> logger.warn("Failed to fetch job [" + jobId + "]", t));

//        return session.execute(findJob.bind(jobId))
//                .flatMap(Observable::from)
//                .map(row -> new JobDetails(jobId, row.getString(0), row.getString(1),
//                        row.getMap(2, String.class, String.class), getTrigger(row.getUDTValue(3))));
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

    private static class JobLock {

        private UUID jobId;
        private String name;
        private boolean acquired;

        public JobLock(UUID jobId, String name, boolean acquired) {
            this.jobId = jobId;
            this.name = name;
            this.acquired = acquired;
        }

        public UUID getJobId() {
            return jobId;
        }

        public String getName() {
            return name;
        }

        public boolean isAcquired() {
            return acquired;
        }
    }
}
