/*
 * Copyright 2014-2015 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.core.impl;

import java.util.concurrent.CountDownLatch;

import org.hawkular.metrics.core.api.MetricsService;
import org.hawkular.metrics.core.api.Tenant;
import org.hawkular.metrics.tasks.api.Task2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.functions.Action1;

/**
 * @author jsanda
 */
public class CreateTenants implements Action1<Task2> {

    private static final Logger logger = LoggerFactory.getLogger(CreateTenants.class);

    private MetricsService metricsService;

    private DataAccess dataAccess;

    public CreateTenants(MetricsService metricsService, DataAccess dataAccess) {
        this.metricsService = metricsService;
        this.dataAccess = dataAccess;
    }

    @Override public void call(Task2 task) {
        long bucket = task.getTrigger().getTriggerTime();
        CountDownLatch latch = new CountDownLatch(1);

        Observable<Tenant> tenants = dataAccess.findTenantIds(bucket)
                .flatMap(Observable::from)
                .map(row -> row.getString(0))
                .flatMap(tenantId -> getTenant(tenantId).map(tenant -> tenant == null ? tenantId : tenant.getId()))
                .filter(tenantId -> tenantId != null)
                .map(Tenant::new);

        metricsService.createTenants(tenants).subscribe(
                aVoid -> {},
                t -> {
                    logger.warn("Tenant creation failed", t);
                    latch.countDown();
                },
                () -> {
                    dataAccess.deleteTenantsBucket(bucket).subscribe(
                            resultSet -> {},
                            t -> {
                                logger.warn("Failed to delete tenants bucket [" + bucket + "]", t);
                                latch.countDown();
                            },
                            latch::countDown
                    );
                }
        );
        try {
            latch.await();
        } catch (InterruptedException e) {
        }
    }

    private Observable<Tenant> getTenant(String tenantId) {
        return dataAccess.findTenant(tenantId)
                .flatMap(Observable::from)
                .map(Functions::getTenant);
    }
}
