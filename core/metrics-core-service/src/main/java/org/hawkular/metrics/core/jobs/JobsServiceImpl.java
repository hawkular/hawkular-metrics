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
package org.hawkular.metrics.core.jobs;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.hawkular.metrics.core.service.MetricsService;
import org.hawkular.metrics.datetime.DateTimeService;
import org.hawkular.metrics.scheduler.api.JobDetails;
import org.hawkular.metrics.scheduler.api.RepeatingTrigger;
import org.hawkular.metrics.scheduler.api.RetryPolicy;
import org.hawkular.metrics.scheduler.api.Scheduler;
import org.hawkular.metrics.scheduler.api.SingleExecutionTrigger;
import org.hawkular.metrics.sysconfig.Configuration;
import org.hawkular.metrics.sysconfig.ConfigurationService;
import org.hawkular.rx.cassandra.driver.RxSession;
import org.jboss.logging.Logger;
import org.joda.time.Minutes;

import com.google.common.collect.ImmutableMap;

import rx.Single;
import rx.functions.Func2;

/**
 * @author jsanda
 */
public class JobsServiceImpl implements JobsService, JobsServiceImplMBean {

    private static Logger logger = Logger.getLogger(JobsServiceImpl.class);

    private Scheduler scheduler;

    private RxSession session;

    private MetricsService metricsService;

    private DeleteTenant deleteTenant;

    private DeleteExpiredMetrics deleteExpiredMetrics;
    private int metricExpirationJobFrequencyInDays;
    private int metricExpirationDelay;
    private boolean metricExpirationJobEnabled;

    private ConfigurationService configurationService;

    public JobsServiceImpl() {
        this(1, 7, true);
    }

    public JobsServiceImpl(int metricExpirationDelay, int metricExpirationJobFrequencyInDays,
            boolean metricExpirationJobEnabled) {
        this.metricExpirationJobFrequencyInDays = metricExpirationJobFrequencyInDays;
        this.metricExpirationDelay = metricExpirationDelay;
        this.metricExpirationJobEnabled = metricExpirationJobEnabled;
    }

    public void setMetricsService(MetricsService metricsService) {
        this.metricsService = metricsService;
    }

    public void setSession(RxSession session) {
        this.session = session;
    }

    public void setConfigurationService(ConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    /**
     * Ideally I think the scheduler should be an implementation detail of this service. This method is here though as
     * a test hook.
     */
    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @Override
    public List<JobDetails> start() {
        List<JobDetails> backgroundJobs = new ArrayList<>();

        deleteTenant = new DeleteTenant(session, metricsService);

        // Use a simple retry policy to make sure tenant deletion does complete in the event of failure. For now
        // we simply retry after 5 minutes. We can implement a more sophisticated strategy later on if need be.
        Func2<JobDetails, Throwable, RetryPolicy> deleteTenantRetryPolicy = (details, throwable) ->
                () -> {
                    logger.warn("Execution of " + details + " failed", throwable);
                    logger.info(details + " will be retried in 5 minutes");
                    return Minutes.minutes(5).toStandardDuration().getMillis();
                };
        scheduler.register(DeleteTenant.JOB_NAME, deleteTenant, deleteTenantRetryPolicy);

        CompressData compressDataJob = new CompressData(metricsService, configurationService);
        scheduler.register(CompressData.JOB_NAME, compressDataJob);
        maybeScheduleCompressData(backgroundJobs);

        deleteExpiredMetrics = new DeleteExpiredMetrics(metricsService, session, configurationService,
                this.metricExpirationDelay);
        scheduler.register(DeleteExpiredMetrics.JOB_NAME, deleteExpiredMetrics);
        maybeScheduleMetricExpirationJob(backgroundJobs);

        scheduler.start();

        return backgroundJobs;
    }

    @Override
    public void shutdown() {
        scheduler.shutdown();
    }

    @Override
    public Single<? extends JobDetails> submitDeleteTenantJob(String tenantId, String jobName) {
        return scheduler.scheduleJob(DeleteTenant.JOB_NAME, jobName, ImmutableMap.of("tenantId", tenantId),
                new SingleExecutionTrigger.Builder().withDelay(1, TimeUnit.MINUTES).build());
    }

    @Override
    public Single<? extends JobDetails> submitDeleteExpiredMetricsJob(long expiration, String jobName) {
        return scheduler.scheduleJob(DeleteExpiredMetrics.JOB_NAME, jobName,
                ImmutableMap.of("expirationTimestamp", expiration + ""),
                new SingleExecutionTrigger.Builder().withDelay(1, TimeUnit.MINUTES).build());
    }

    private void maybeScheduleCompressData(List<JobDetails> backgroundJobs) {
        String configId = "org.hawkular.metrics.jobs." + CompressData.JOB_NAME;
        Configuration config = configurationService.load(configId).toBlocking()
                .firstOrDefault(new Configuration(configId, new HashMap<>()));
        if (config.get("jobId") == null) {
            logger.info("Preparing to create and schedule " + CompressData.JOB_NAME + " job");

            // Get next start of odd hour
            long nextStart = LocalDateTime.now(ZoneOffset.UTC)
                    .with(DateTimeService.startOfNextOddHour())
                    .toInstant(ZoneOffset.UTC).toEpochMilli();
            JobDetails JobDetails = scheduler.scheduleJob(CompressData.JOB_NAME, CompressData.JOB_NAME,
                    ImmutableMap.of(), new RepeatingTrigger.Builder().withTriggerTime(nextStart)
                            .withInterval(2, TimeUnit.HOURS).build()).toBlocking().value();
            backgroundJobs.add(JobDetails);
            configurationService.save(configId, "jobId", JobDetails.getJobId().toString()).toBlocking();

            logger.info("Created and scheduled " + JobDetails);
        }
    }

    private void maybeScheduleMetricExpirationJob(List<JobDetails> backgroundJobs) {
        String jobIdConfigKey = "jobId";
        String jobFrequencyKey = "jobFrequency";

        String configId = "org.hawkular.metrics.jobs." + DeleteExpiredMetrics.JOB_NAME;
        Configuration config = configurationService.load(configId).toBlocking()
                .firstOrDefault(new Configuration(configId, new HashMap<>()));

        if (config.get(jobIdConfigKey) != null) {
            Integer configuredJobFrequency = null;
            try {
                configuredJobFrequency = Integer.parseInt(config.get(jobFrequencyKey));
            } catch (Exception e) {
                //do nothing, the parsing failed which makes the value unknown
            }

            if (configuredJobFrequency == null || configuredJobFrequency != this.metricExpirationJobFrequencyInDays
                    || this.metricExpirationJobFrequencyInDays <= 0 || configuredJobFrequency <= 0 ||
                    !this.metricExpirationJobEnabled) {
                scheduler.unscheduleJob(config.get(jobIdConfigKey)).await();
                configurationService.delete(configId, jobIdConfigKey).toBlocking();
                config.delete(jobIdConfigKey);
                configurationService.delete(configId, jobFrequencyKey).toBlocking();
                config.delete(jobFrequencyKey);
            }
        }

        if (config.get(jobIdConfigKey) == null && this.metricExpirationJobFrequencyInDays > 0
                && this.metricExpirationJobEnabled) {
            logger.info("Preparing to create and schedule " + DeleteExpiredMetrics.JOB_NAME + " job");

            //Get start of next day
            long nextStart = DateTimeService.current24HourTimeSlice().plusDays(1).getMillis();
            JobDetails JobDetails = scheduler.scheduleJob(DeleteExpiredMetrics.JOB_NAME, DeleteExpiredMetrics.JOB_NAME,
                    ImmutableMap.of(), new RepeatingTrigger.Builder().withTriggerTime(nextStart)
                            .withInterval(this.metricExpirationJobFrequencyInDays, TimeUnit.DAYS).build())
                    .toBlocking().value();
            backgroundJobs.add(JobDetails);
            configurationService.save(configId, jobIdConfigKey, JobDetails.getJobId().toString()).toBlocking();
            configurationService.save(configId, jobFrequencyKey, this.metricExpirationJobFrequencyInDays + "")
                    .toBlocking();

            logger.info("Created and scheduled " + JobDetails);
        }
    }

    // JMX management
    private void submitCompressJob(Map<String, String> parameters) {
        String jobName = String.format("%s_single_%s", CompressData.JOB_NAME, parameters.get(CompressData.TARGET_TIME));
        // Blocking to ensure it is actually scheduled..
        scheduler.scheduleJob(CompressData.JOB_NAME, jobName, parameters,
                new SingleExecutionTrigger.Builder().withDelay(1, TimeUnit.MINUTES).build())
                .toBlocking().value();
    }

    @Override
    public void submitCompressJob(long timestamp) {
        logger.debugf("Scheduling manual submitCompressJob with default blocksize, timestamp->%d", timestamp);
        submitCompressJob(ImmutableMap.of(CompressData.TARGET_TIME, Long.valueOf(timestamp).toString()));
    }

    @Override
    public void submitCompressJob(long timestamp, String blockSize) {
        logger.debugf("Scheduling manual submitCompressJob with defined blocksize, timestamp->%d, blockSize->%s",
                timestamp, blockSize);
        submitCompressJob(ImmutableMap.of(
                CompressData.TARGET_TIME, Long.valueOf(timestamp).toString(),
                CompressData.BLOCK_SIZE, blockSize)
        );
    }

    @Override
    public void submitDeleteExpiredMetrics() {
        long time = System.currentTimeMillis();
        logger.debugf("Scheduling manual deleteExpiredMetrics job with timestamp->%d", time);
        submitDeleteExpiredMetricsJob(time, "delete_expired_" + time);
    }
}
