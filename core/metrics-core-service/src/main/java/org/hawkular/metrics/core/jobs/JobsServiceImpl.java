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
import java.util.HashMap;
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

import rx.Observable;
import rx.Single;
import rx.functions.Func2;

/**
 * @author jsanda
 */
public class JobsServiceImpl implements JobsService {

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
    public void start() {
        scheduler.start();

        Configuration configuration = configurationService.load("org.hawkular.metrics.jobs")
                .toBlocking().lastOrDefault(null);

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

        Boolean compressionEnabled = Boolean.valueOf(configuration.get(CompressData.ENABLED_CONFIG, "true"));
        if(compressionEnabled) {
            maybeScheduleCompressData();
        }
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

    private void maybeScheduleCompressData() {
        String configId = "org.hawkular.metrics.jobs." + CompressData.JOB_NAME;
        Configuration config = configurationService.load(configId).toBlocking()
                .firstOrDefault(new Configuration(configId, new HashMap<>()));
        if (config.get(CompressData.JOB_NAME) == null) {
            logger.info("Preparing to create and schedule " + CompressData.JOB_NAME + " job");

            // Get next start of odd hour
            long nextStart = LocalDateTime.now(ZoneOffset.UTC)
                    .with(DateTimeService.startOfNextOddHour())
                    .toInstant(ZoneOffset.UTC).toEpochMilli();
            JobDetails jobDetails = scheduler.scheduleJob(CompressData.JOB_NAME, CompressData.JOB_NAME,
                    ImmutableMap.of(), new RepeatingTrigger.Builder().withTriggerTime(nextStart)
                            .withInterval(2, TimeUnit.HOURS).build()).toBlocking().value();
            config.set("jobId", jobDetails.getJobId().toString());
            configurationService.save(config).toBlocking();

            logger.info("Created and scheduled " + jobDetails);
        }
    }

    @Override
    public Observable<JobDetails> getJobDetails() {
        return scheduler.getAllJobs();
    }

}
