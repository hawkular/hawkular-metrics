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
package org.hawkular.metrics.core.jobs;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.hawkular.metrics.core.service.MetricsService;
import org.hawkular.metrics.scheduler.api.JobDetails;
import org.hawkular.metrics.scheduler.api.Scheduler;
import org.hawkular.metrics.scheduler.api.SingleExecutionTrigger;
import org.hawkular.metrics.sysconfig.ConfigurationService;
import org.hawkular.rx.cassandra.driver.RxSession;
import org.jboss.logging.Logger;

import com.google.common.collect.ImmutableMap;

import rx.Single;

/**
 * @author jsanda
 */
public class JobsServiceImpl implements JobsService, JobsServiceImplMBean {

    public static final String CONFIG_PREFIX = "org.hawkular.metrics.jobs.";

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
        deleteTenant = new DeleteTenant(session, metricsService);

        scheduler.register(DeleteTenant.JOB_NAME, deleteTenant);

        TempTableCreator tempCreator = new TempTableCreator(metricsService, configurationService);
        scheduler.register(TempTableCreator.JOB_NAME, tempCreator);

        TempDataCompressor tempJob = new TempDataCompressor(metricsService, configurationService);
        scheduler.register(TempDataCompressor.JOB_NAME, tempJob);

        scheduler.start();
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

}
