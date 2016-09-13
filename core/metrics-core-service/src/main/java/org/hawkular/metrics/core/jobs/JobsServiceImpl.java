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

import static java.util.Collections.emptyMap;

import static org.hawkular.metrics.datetime.DateTimeService.currentMinute;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.hawkular.metrics.core.service.MetricsService;
import org.hawkular.metrics.core.service.cache.CacheService;
import org.hawkular.metrics.scheduler.api.JobDetails;
import org.hawkular.metrics.scheduler.api.RepeatingTrigger;
import org.hawkular.metrics.scheduler.api.RetryPolicy;
import org.hawkular.metrics.scheduler.api.Scheduler;
import org.hawkular.metrics.scheduler.api.SingleExecutionTrigger;
import org.hawkular.metrics.scheduler.api.Trigger;
import org.hawkular.metrics.sysconfig.Configuration;
import org.hawkular.metrics.sysconfig.ConfigurationService;
import org.hawkular.rx.cassandra.driver.RxSession;
import org.jboss.logging.Logger;
import org.joda.time.DateTime;
import org.joda.time.Minutes;

import com.google.common.collect.ImmutableMap;

import rx.Single;
import rx.functions.Func2;

/**
 * @author jsanda
 */
public class JobsServiceImpl implements JobsService {

    private static Logger logger = Logger.getLogger(DeleteTenant.class);

    private Scheduler scheduler;

    private RxSession session;

    private MetricsService metricsService;

    private ConfigurationService configService;

    private CacheService cacheService;

    private DeleteTenant deleteTenant;

    public void setMetricsService(MetricsService metricsService) {
        this.metricsService = metricsService;
    }

    public void setConfigurationService(ConfigurationService configurationService) {
        configService = configurationService;
    }

    public void setCacheService(CacheService cacheService) {
        this.cacheService = cacheService;
    }

    public void setSession(RxSession session) {
        this.session = session;
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

        ComputeRollups computeRollups = new ComputeRollups(cacheService);
        scheduler.register(ComputeRollups.JOB_NAME, computeRollups);
        maybeScheduleComputeRollups();
    }

    private void maybeScheduleComputeRollups() {
        Configuration configuration = configService.load("org.hawkular.metrics.rollups").toBlocking()
                .firstOrDefault(null);
        if (configuration == null || configuration.get("jobId") == null) {
            DateTime nextTimeSlice = currentMinute().plusMinutes(1);
            Trigger trigger = new RepeatingTrigger.Builder()
                    .withTriggerTime(nextTimeSlice.getMillis())
                    .withInterval(1, TimeUnit.MINUTES)
                    .build();
            logger.debug("Scheduling " + ComputeRollups.JOB_NAME + " with start time [" +
                    new Date(trigger.getTriggerTime()) + "]");
            JobDetails details = scheduler.scheduleJob(ComputeRollups.JOB_NAME, ComputeRollups.JOB_NAME, emptyMap(),
                    trigger).toBlocking().value();
            configuration.set("jobId", details.getJobId().toString());
            configService.save(configuration).toCompletable().await();
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

}
