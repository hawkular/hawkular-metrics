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

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;

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

    public JobsService(RxSession session) {
        this.session = session;
        findTimeSlices = session.getSession().prepare("SELECT DISTINCT time_slice FROM scheduled_jobs_idx");
        findScheduledForTime = session.getSession().prepare(
                "SELECT job_id, job_type, job_name, job_params, trigger, status FROM scheduled_jobs_idx " +
                "WHERE time_slice = ?");
        findAllScheduled = session.getSession().prepare(
                "SELECT job_id, job_type, job_name, job_params, trigger, status, time_slice FROM scheduled_jobs_idx");
        update = session.getSession().prepare(
                "INSERT INTO jobs (id, type, name, params, trigger) VALUES (?, ?, ?, ?, ?)");
        insertScheduled =  session.getSession().prepare(
                "INSERT INTO scheduled_jobs_idx (time_slice, job_id, job_type, job_name, job_params, trigger, " +
                "status) VALUES (?, ?, ?, ?, ?, ?, ?)");
        updateStatus = session.getSession().prepare(
                "UPDATE scheduled_jobs_idx SET status = ? WHERE time_slice = ? AND job_id = ?");
    }

    public Observable<Date> findActiveTimeSlices(Date currentTime, rx.Scheduler scheduler) {
        return session.executeAndFetch(findTimeSlices.bind(), scheduler)
                .map(row -> row.getTimestamp(0))
                .filter(timestamp -> timestamp.compareTo(currentTime) < 0)
                .toSortedList()
                .doOnNext(timeSlices -> logger.debug("ACTIVE TIME SLICES " + timeSlices))
                .flatMap(Observable::from)
                .concatWith(Observable.just(currentTime));
    }

    /**
     * This method is currently unused.
     */
    public Observable<JobDetails> findScheduledJobs(Date timeSlice, rx.Scheduler scheduler) {
        return session.executeAndFetch(findAllScheduled.bind(), scheduler)
                .filter(row -> row.getTimestamp(6).compareTo(timeSlice) <= 0)
                .map(row -> new JobDetails(
                        row.getUUID(0),
                        row.getString(1),
                        row.getString(2),
                        row.getMap(3, String.class, String.class),
                        getTrigger(row.getUDTValue(4)),
                        JobStatus.fromCode(row.getByte(5))))
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

    public Observable<JobDetails> findScheduledJobsForTime(Date timeSlice, rx.Scheduler scheduler) {
        return session.executeAndFetch(findScheduledForTime.bind(timeSlice), scheduler)
                .map(row -> new JobDetails(
                        row.getUUID(0),
                        row.getString(1),
                        row.getString(2),
                        row.getMap(3, String.class, String.class),
                        getTrigger(row.getUDTValue(4)),
                        JobStatus.fromCode(row.getByte(5))));
    }

    public Observable<ResultSet> insert(Date timeSlice, JobDetails job) {
        return session.execute(insertScheduled.bind(timeSlice, job.getJobId(), job.getJobType(), job.getJobName(),
                job.getParameters(), getTriggerValue(session, job.getTrigger())));
    }

    public Observable<ResultSet> updateStatusToFinished(Date timeSlice, UUID jobId) {
        return session.execute(updateStatus.bind((byte) 1, timeSlice, jobId))
                .doOnError(t -> logger.warn("There was an error updating the status to finished for [" + jobId +
                        "] in time slice [" + timeSlice.getTime() + "]"));
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
