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
package org.hawkular.metrics.core.jobs;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
import rx.Subscriber;
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

    private ConfigurationService configurationService;

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
        scheduler.start();

        List<JobDetails> backgroundJobs = new ArrayList<>();

        deleteTenant = new DeleteTenant(session, metricsService);
        Map<JobDetails, Integer> deleteTenantAttempts = new ConcurrentHashMap<>();
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

        return backgroundJobs;
    }

    @Override
    public void shutdown() {
        scheduler.shutdown();
    }

    @Override
    public Single<JobDetails> submitDeleteTenantJob(String tenantId, String jobName) {
        return scheduler.scheduleJob(DeleteTenant.JOB_NAME, jobName, ImmutableMap.of("tenantId", tenantId),
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
            JobDetails jobDetails = scheduler.scheduleJob(CompressData.JOB_NAME, CompressData.JOB_NAME,
                    ImmutableMap.of(), new RepeatingTrigger.Builder().withTriggerTime(nextStart)
                            .withInterval(2, TimeUnit.HOURS).build()).toBlocking().value();
            backgroundJobs.add(jobDetails);
            configurationService.save(configId, "jobId", jobDetails.getJobId().toString()).toBlocking();

            logger.info("Created and scheduled " + jobDetails);
        }
    }

    // JMX management
    private void submitCompressJob(Map<String, String> parameters) {
        String jobName = String.format("%s_single_%s", CompressData.JOB_NAME, parameters.get(CompressData.TARGET_TIME));
        Single<JobDetails> jobDetailsSingle = scheduler.scheduleJob(CompressData.JOB_NAME, jobName, parameters,
                new SingleExecutionTrigger.Builder().withDelay(1, TimeUnit.MINUTES).build());
        jobDetailsSingle.subscribe(new Subscriber<JobDetails>() {
            @Override public void onCompleted() {
                logger.info("submitCompressJob->onCompleted");
            }

            @Override public void onError(Throwable throwable) {
                logger.error("submitCompressJob->onError", throwable);
            }

            @Override public void onNext(JobDetails jobDetails) {
                logger.info("submitCompressJob->onNext");
            }
        });
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
}
