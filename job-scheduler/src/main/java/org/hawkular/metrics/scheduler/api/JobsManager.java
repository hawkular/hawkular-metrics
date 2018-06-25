/*
 * Copyright 2014-2018 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.scheduler.api;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.hawkular.metrics.datetime.DateTimeService;
import org.hawkular.metrics.scheduler.impl.SchedulerImpl;
import org.hawkular.metrics.sysconfig.Configuration;
import org.hawkular.metrics.sysconfig.ConfigurationService;
import org.hawkular.rx.cassandra.driver.RxSession;
import org.hawkular.rx.cassandra.driver.RxSessionImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;

import rx.Completable;

/**
 * @author jsanda
 */
public class JobsManager {

    private static Logger logger = LoggerFactory.getLogger(JobsManager.class);
    public static final String COMPRESS_DATA_JOB = "COMPRESS_DATA";

    private ConfigurationService configurationService;
    private Scheduler scheduler;

    public JobsManager(Session session) {
        RxSession rxSession = new RxSessionImpl(session);
        configurationService = new ConfigurationService();
        configurationService.init(rxSession);
        // The hostname is not important here as this class is only used during initialization to persist
        // reoccurring jobs
        scheduler = new SchedulerImpl(rxSession, "localhost");
    }

    public List<JobDetails> installJobs() {
        logger.info("Installing scheduled jobs");
        unscheduleDeleteExpiredMetrics();
        List<JobDetails> backgroundJobs = new ArrayList<>();
        maybeScheduleCompressData(backgroundJobs);
        return backgroundJobs;
    }

    private void unscheduleDeleteExpiredMetrics() {
        String jobName = "DELETE_EXPIRED_METRICS";
        String configId = "org.hawkular.metrics.jobs.DELETE_EXPIRED_METRICS";
        // We load the configuration first so that delete is done only if it exists in order to avoid generating
        // tombstones.
        Completable deleteConfig = configurationService.load(configId)
                .map(config -> configurationService.delete(configId))
                .toCompletable();
        // unscheduleJobByTypeAndName will not generate unnecessary tombstones as it does reads before writes
        Completable unscheduleJob = scheduler.unscheduleJobByTypeAndName(jobName, jobName);
        Completable.merge(deleteConfig, unscheduleJob).await();
    }

    private void maybeScheduleCompressData(List<JobDetails> backgroundJobs) {
        String configId = "org.hawkular.metrics.jobs." + COMPRESS_DATA_JOB;
        Configuration config = configurationService.load(configId).toBlocking()
                .firstOrDefault(new Configuration(configId, new HashMap<>()));
        if (config.get("jobId") == null) {
            logger.info("Preparing to create and schedule " + COMPRESS_DATA_JOB + " job");

            // Get next start of odd hour
            long nextStart = LocalDateTime.now(ZoneOffset.UTC)
                    .with(DateTimeService.startOfNextOddHour())
                    .toInstant(ZoneOffset.UTC).toEpochMilli();
            JobDetails jobDetails = scheduler.scheduleJob(COMPRESS_DATA_JOB, COMPRESS_DATA_JOB,
                    ImmutableMap.of(), new RepeatingTrigger.Builder().withTriggerTime(nextStart)
                            .withInterval(2, TimeUnit.HOURS).build()).toBlocking().value();
            backgroundJobs.add(jobDetails);
            configurationService.save(configId, "jobId", jobDetails.getJobId().toString()).toBlocking();

            logger.info("Created and scheduled " + jobDetails);
        }
    }
}
