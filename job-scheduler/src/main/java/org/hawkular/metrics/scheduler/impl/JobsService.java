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

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Function;

import org.hawkular.metrics.scheduler.api.JobDetails;
import org.hawkular.metrics.scheduler.api.JobStatus;
import org.hawkular.metrics.scheduler.api.RepeatingTrigger;
import org.hawkular.metrics.scheduler.api.SingleExecutionTrigger;
import org.hawkular.metrics.scheduler.api.Trigger;
import org.hawkular.rx.cassandra.driver.RxSession;
import org.jboss.logging.Logger;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;

import rx.Completable;
import rx.Observable;

/**
 * @author jsanda
 */
public class JobsService {

    private static Logger logger = Logger.getLogger(JobsService.class);

    private RxSession session;

    private PreparedStatement findTimeSlices;

    private PreparedStatement findScheduledForTime;

    private PreparedStatement findAllScheduled;

    private PreparedStatement insertScheduled;

    private PreparedStatement update;

    private PreparedStatement updateStatus;

    private PreparedStatement deleteScheduled;

    private PreparedStatement updateJobParameters;

    private static final Function<Map<String, String>, Completable> SAVE_PARAMS_NO_OP =
            params -> Completable.complete();

    public JobsService(RxSession session) {
        this.session = session;
        findTimeSlices = session.getSession().prepare("SELECT DISTINCT time_slice FROM scheduled_jobs_idx");
        findScheduledForTime = session.getSession().prepare(
                "SELECT job_id, job_type, job_name, job_params, trigger, status FROM scheduled_jobs_idx " +
                "WHERE time_slice = ?");

        // In general this is not a good way to execute queries in Cassandra; however, the number partitions with which
        // we are dealing is going to very small. The Cassandra clusters are also generally only two or three nodes.
        findAllScheduled = session.getSession().prepare(
                "SELECT time_slice, job_id, job_type, job_name, job_params, trigger, status FROM scheduled_jobs_idx");

        update = session.getSession().prepare(
                "INSERT INTO jobs (id, type, name, params, trigger) VALUES (?, ?, ?, ?, ?)");
        insertScheduled =  session.getSession().prepare(
                "INSERT INTO scheduled_jobs_idx (time_slice, job_id, job_type, job_name, job_params, trigger, " +
                "status) VALUES (?, ?, ?, ?, ?, ?, ?)");
        updateStatus = session.getSession().prepare(
                "UPDATE scheduled_jobs_idx SET status = ? WHERE time_slice = ? AND job_id = ?");
        deleteScheduled = session.getSession().prepare(
                "DELETE FROM scheduled_jobs_idx WHERE time_slice = ? AND job_id = ?");
        updateJobParameters = session.getSession().prepare(
                "UPDATE scheduled_jobs_idx SET job_params = ? WHERE time_slice = ? AND job_id = ?");
    }

    public Observable<Date> findActiveTimeSlices(Date currentTime, rx.Scheduler scheduler) {
        return session.executeAndFetch(findTimeSlices.bind(), scheduler)
                .map(row -> row.getTimestamp(0))
                .filter(timestamp -> timestamp.compareTo(currentTime) < 0)
                .toSortedList()
                .doOnNext(timeSlices -> logger.debugf("Active time slices %s", timeSlices))
                .flatMap(Observable::from)
                .concatWith(Observable.just(currentTime));
    }

    public Observable<JobDetailsImpl> findAllScheduledJobs(rx.Scheduler scheduler) {
        return session.executeAndFetch(findAllScheduled.bind(), scheduler)
                .map(row -> createJobDetails(
                        row.getUUID(1),
                        row.getString(2),
                        row.getString(3),
                        row.getMap(4, String.class, String.class),
                        getTrigger(row.getUDTValue(5)),
                        JobStatus.fromCode(row.getByte(6)),
                        row.getTimestamp(0)));
    }

    public Observable<JobDetailsImpl> findJobs(Date timeSlice, rx.Scheduler scheduler) {
        return findActiveTimeSlices(timeSlice, scheduler)
                .flatMap(time -> findScheduledJobsForTime(time, scheduler))
                .reduce(new HashMap<>(), (HashMap<UUID, JobDetailsImpl> map, JobDetailsImpl details) -> {
                    JobDetailsImpl other = map.get(details.getJobId());
                    if (other == null) {
                        map.put(details.getJobId(), details);
                    } else {
                        if (details.getTrigger().getTriggerTime() < other.getTrigger().getTriggerTime()) {
                            map.put(details.getJobId(), details);
                        }
                    }
                    return map;
                })
                .flatMap(map -> Observable.from(map.values()));
    }

    public Completable deleteJob(UUID jobId, rx.Scheduler scheduler) {
        return session.executeAndFetch(findTimeSlices.bind(), scheduler)
                .map(row -> row.getTimestamp(0))
                .flatMap(timeSlice -> session.execute(deleteScheduled.bind(timeSlice, jobId)))
                .toCompletable();
    }

    /**
     * This method is currently unused.
     */
    public Observable<JobDetails> findScheduledJobs(Date timeSlice, rx.Scheduler scheduler) {
        return session.executeAndFetch(findAllScheduled.bind(), scheduler)
                .filter(row -> row.getTimestamp(6).compareTo(timeSlice) <= 0)
                .map(row -> createJobDetails(
                        row.getUUID(0),
                        row.getString(1),
                        row.getString(2),
                        row.getMap(3, String.class, String.class),
                        getTrigger(row.getUDTValue(4)),
                        JobStatus.fromCode(row.getByte(5)),
                        timeSlice))
                .collect(HashMap::new, (Map<UUID, SortedSet<JobDetails>> map, JobDetails details) -> {
                    SortedSet<JobDetails> set = map.get(details.getJobId());
                    if (set == null) {
                        set = new TreeSet<>((JobDetails d1, JobDetails d2) ->
                                Long.compare(d1.getTrigger().getTriggerTime(), d2.getTrigger().getTriggerTime()));
                    }
                    set.add(details);
                    map.put(details.getJobId(), set);
                })
                .flatMap(map -> Observable.from(map.entrySet()))
                .map(entry -> entry.getValue().first());
    }

    public Observable<JobDetailsImpl> findScheduledJobsForTime(Date timeSlice, rx.Scheduler scheduler) {
        return session.executeAndFetch(findScheduledForTime.bind(timeSlice), scheduler)
                .map(row -> createJobDetails(
                        row.getUUID(0),
                        row.getString(1),
                        row.getString(2),
                        row.getMap(3, String.class, String.class),
                        getTrigger(row.getUDTValue(4)),
                        JobStatus.fromCode(row.getByte(5)),
                        timeSlice))
                .doOnSubscribe(() -> logger.debugf("Fetching scheduled jobs tor time slice [%s]", timeSlice))
                .doOnNext(details -> logger.debugf("Found job details %s", details));
    }

    public Observable<ScheduledExecution> findScheduledExecutions(UUID jobId, rx.Scheduler scheduler) {
        return session.executeAndFetch(findAllScheduled.bind(), scheduler)
                .filter(row -> row.getUUID(1).equals(jobId))
                .map(row -> new ScheduledExecution(row.getTimestamp(0), createJobDetails(
                        row.getUUID(1),
                        row.getString(2),
                        row.getString(3),
                        row.getMap(4, String.class, String.class),
                        getTrigger(row.getUDTValue(5)),
                        JobStatus.fromCode(row.getByte(6)),
                        row.getTimestamp(0))));
    }

    public Observable<ResultSet> insert(Date timeSlice, JobDetails job) {
        return session.execute(insertScheduled.bind(timeSlice, job.getJobId(), job.getJobType(), job.getJobName(),
                job.getParameters().getMap(), getTriggerValue(session, job.getTrigger())));
    }

    public Observable<ResultSet> updateStatusToFinished(Date timeSlice, UUID jobId) {
        return session.execute(updateStatus.bind((byte) 1, timeSlice, jobId))
                .doOnError(t -> logger.warnf("There was an error updating the status to finished for %s in time " +
                        "slice [%s]", jobId, timeSlice.getTime()));
    }

    public JobDetailsImpl createJobDetails(UUID jobId, String jobType, String jobName, Map<String, String> parameters,
            Trigger trigger, Date timeSlice) {
        return createJobDetails(jobId, jobType, jobName, parameters, trigger, JobStatus.NONE, timeSlice);
    }

    public JobDetailsImpl createJobDetails(UUID jobId, String jobType, String jobName, Map<String, String> parameters,
            Trigger trigger, JobStatus status, Date timeSlice) {
        Function<Map<String, String>, Completable> saveParameters = params ->
                session.execute(updateJobParameters.bind(params, timeSlice, jobId)).toCompletable();
        return new JobDetailsImpl(jobId,  jobType, jobName, new JobParametersImpl(parameters, SAVE_PARAMS_NO_OP),
                trigger, status);
    }

    public void prepareJobDetailsForExecution(JobDetailsImpl jobDetails, Date timeSlice) {
        Function<Map<String, String>, Completable> saveParameters = params ->
                session.execute(updateJobParameters.bind(jobDetails.getParameters().getMap(), timeSlice,
                        jobDetails.getJobId())).toCompletable();
        jobDetails.setSaveParameters(saveParameters);
    }

    public void resetJobDetails(JobDetailsImpl jobDetails) {
        jobDetails.setSaveParameters(SAVE_PARAMS_NO_OP);
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

}
