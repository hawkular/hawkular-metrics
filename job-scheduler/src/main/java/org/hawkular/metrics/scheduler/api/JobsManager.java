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
import java.time.temporal.ChronoUnit;
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

    public static final String COMPRESS_DATA_CONFIG_ID = "org.hawkular.metrics.jobs." + COMPRESS_DATA_JOB;

    public static final String TEMP_TABLE_CREATOR_JOB = "TEMP_TABLE_CREATOR";

    public static final String TEMP_TABLE_CREATE_CONFIG_ID = "org.hawkular.metrics.jobs." + TEMP_TABLE_CREATOR_JOB;

    public static final String TEMP_DATA_COMPRESSOR_JOB = "TEMP_DATA_COMPRESSOR";

    public static final String TEMP_DATA_COMPRESSOR_CONFIG_ID = "org.hawkular.metrics.jobs." + TEMP_DATA_COMPRESSOR_JOB;

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

        List<JobDetails> backgroundJobs = new ArrayList<>();

        unscheduleCompressData();
        unscheduleDeleteExpiredMetrics();
        maybeScheduleTableCreator(backgroundJobs);
        maybeScheduleTempDataCompressor(backgroundJobs);

        return backgroundJobs;
    }

    private void unscheduleCompressData() {
        Configuration config = configurationService.load(COMPRESS_DATA_CONFIG_ID).toBlocking()
                .firstOrDefault(new Configuration(COMPRESS_DATA_CONFIG_ID, new HashMap<>()));
        String jobId = config.get("jobId");

        if (config.getProperties().isEmpty()) {
            // This means we have a new installation and not an upgrade. The CompressData job has not been previously
            // installed so there is no db clean up necessary.
        } else {
            Completable unscheduled;
            if (jobId == null) {
                logger.info("Expected to find a jobId property in database for {}. Attempting to unschedule job by " +
                        "name.", COMPRESS_DATA_JOB);
                unscheduled = scheduler.unscheduleJobByTypeAndName(COMPRESS_DATA_JOB, COMPRESS_DATA_JOB);
            } else {
                unscheduled = scheduler.unscheduleJobById(jobId);
            }
            unscheduled.await();
            if (!config.getProperties().isEmpty()) {
                configurationService.delete(COMPRESS_DATA_CONFIG_ID).await();
            }
        }
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

    private void maybeScheduleTableCreator(List<JobDetails> backgroundJobs) {
        Configuration config = configurationService.load(TEMP_TABLE_CREATE_CONFIG_ID).toBlocking()
                .firstOrDefault(new Configuration(TEMP_TABLE_CREATE_CONFIG_ID, new HashMap<>()));
        if (config.get("jobId") == null) {
            long nextTrigger = LocalDateTime.now(ZoneOffset.UTC)
                    .truncatedTo(ChronoUnit.MINUTES).plusMinutes(2)
                    .toInstant(ZoneOffset.UTC).toEpochMilli();

            JobDetails jobDetails = scheduler.scheduleJob(TEMP_TABLE_CREATOR_JOB, TEMP_TABLE_CREATOR_JOB,
                    ImmutableMap.of(), new RepeatingTrigger.Builder().withTriggerTime(nextTrigger)
                            .withInterval(2, TimeUnit.HOURS).build()).toBlocking().value();
            backgroundJobs.add(jobDetails);
            configurationService.save(TEMP_TABLE_CREATE_CONFIG_ID, "jobId", jobDetails.getJobId().toString())
                    .toBlocking();
            logger.info("Scheduled temporary table creator " + jobDetails);
        }
    }

    private void maybeScheduleTempDataCompressor(List<JobDetails> backgroundJobs) {
        Configuration config = configurationService.load(TEMP_DATA_COMPRESSOR_CONFIG_ID).toBlocking()
                .firstOrDefault(new Configuration(TEMP_DATA_COMPRESSOR_CONFIG_ID, new HashMap<>()));
        if (config.get("jobId") == null) {
            logger.info("Preparing to create and schedule " + TEMP_DATA_COMPRESSOR_JOB + " job");

            // Get next start of odd hour
            long nextStart = LocalDateTime.now(ZoneOffset.UTC)
                    .with(DateTimeService.startOfNextOddHour())
                    .toInstant(ZoneOffset.UTC).toEpochMilli();

            // Temp table processing
            JobDetails jobDetails = scheduler.scheduleJob(TEMP_DATA_COMPRESSOR_JOB, TEMP_DATA_COMPRESSOR_JOB,
                    ImmutableMap.of(), new RepeatingTrigger.Builder().withTriggerTime(nextStart)
                            .withInterval(2, TimeUnit.HOURS).build()).toBlocking().value();
            backgroundJobs.add(jobDetails);
            configurationService.save(TEMP_DATA_COMPRESSOR_CONFIG_ID, "jobId", jobDetails.getJobId().toString())
                    .toBlocking();

            logger.info("Created and scheduled " + jobDetails);
        }
    }

}
