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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.hawkular.metrics.scheduler.api.JobDetails;
import org.hawkular.metrics.scheduler.api.RepeatingTrigger;
import org.hawkular.metrics.scheduler.api.Scheduler;
import org.hawkular.metrics.scheduler.api.SingleExecutionTrigger;
import org.hawkular.metrics.scheduler.api.Trigger;
import org.hawkular.metrics.sysconfig.ConfigurationService;
import org.hawkular.rx.cassandra.driver.RxSession;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import rx.Observable;
import rx.functions.Func1;
import rx.schedulers.Schedulers;

/**
 * @author jsanda
 */
public class SchedulerImpl implements Scheduler {

    private Map<String, Func1<JobDetails, Observable<Void>>> jobCreators;

    private ScheduledExecutorService tickExecutor;

    private rx.Scheduler tickScheduler;

    private ExecutorService queueExecutor;

    private rx.Scheduler queueScheduler;

    private RxSession session;

    private PreparedStatement insertJob;

    private PreparedStatement insertJobInQueue;

    private LockManager lockManager;

    private ConfigurationService configurationService;

    private String name;

    public SchedulerImpl(RxSession session) {
        this.session = session;
        jobCreators = new HashMap<>();
        tickExecutor = Executors.newScheduledThreadPool(1,
                new ThreadFactoryBuilder().setNameFormat("ticker-pool-%d").build());
        queueExecutor = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat("status-pool-%d").build());
        tickScheduler = Schedulers.from(tickExecutor);
        queueScheduler = Schedulers.from(queueExecutor);

        lockManager = new LockManager(session);

        insertJob = session.getSession().prepare(
                "INSERT INTO jobs (id, type, name, params, trigger) VALUES (?, ?, ?, ?, ?)");

        insertJobInQueue = session.getSession().prepare(
                "INSERT INTO jobs_status (time_slice, job_id) VALUES (?, ?)");

        try {
            name = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    public void setConfigurationService(ConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    @Override
    public void registerJobCreator(String jobType, Func1<JobDetails, Observable<Void>> jobCreator) {

    }

    public Observable<JobDetails> scheduleJob(String type, String name, Map<String, String> parameter,
            Trigger trigger) {
        String lock = "org.hawkular.metrics.scheduler.queue." + trigger.getTriggerTime();
        return lockManager.acquireSharedLock(lock, name, 30)
                .map(acquired -> {
                    if (acquired) {
                        UUID jobId = UUID.randomUUID();
                        return new JobDetails(jobId, type, name, parameter, trigger);
                    } else {
                        throw new RuntimeException("Failed to schedule job");
                    }
                })
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
        createTicks().observeOn(queueScheduler).doOnNext(tick -> {
//            findActiveTimeSlice()


        });
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

    private Observable<Date> createTicks() {
        return Observable.empty();
    }

    private Observable<Date> findActiveTimeSlice() {
        // TODO We shouldn't ever get an empty result set but need to handle it to be safe
        return configurationService.load("org.hawkular.metrics.scheduler")
                .map(config -> new Date(Long.parseLong(config.get("active-queue"))));
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
