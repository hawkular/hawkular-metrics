/*
 * Copyright 2016 Red Hat, Inc. and/or its affiliates
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

import org.hawkular.metrics.core.service.MetricsService;
import org.hawkular.metrics.scheduler.api.JobDetails;
import org.hawkular.metrics.scheduler.api.Scheduler;
import org.hawkular.rx.cassandra.driver.RxSession;

import rx.Single;

/**
 * @author jsanda
 */
public class JobsServiceImpl implements JobsService {

    private Scheduler scheduler;

    private RxSession session;

    private MetricsService metricsService;

    public void setMetricsService(MetricsService metricsService) {
        this.metricsService = metricsService;
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
        // Register jobs here and start scheduler

        scheduler.start();
    }

    @Override
    public void shutdown() {
        scheduler.shutdown();
    }

    @Override
    public Single<JobDetails> submitDeleteTenantJob(String tenantId) {
//        return scheduler.scheduleJob("DELETE_TENANT", "DELETE_TENANT", ImmutableMap.of("tenantId", tenantId),
//                new SingleExecutionTrigger.Builder().withDelay(1, TimeUnit.MINUTES).build());
        // This work is being done in HWKMETRICS-446
        return Single.error(new UnsupportedOperationException());
    }

}
