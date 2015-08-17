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

import org.hawkular.metrics.core.api.Tenant;
import org.hawkular.metrics.tasks.api.Task2;
import org.hawkular.metrics.tasks.api.Trigger;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;

import rx.Observable;
import rx.functions.Action1;

/**
 * @author jsanda
 */
public class CreateTenants implements Action1<Task2> {

    private static final Logger logger = LoggerFactory.getLogger(CreateTenants.class);

    public static final String TASK_NAME = "create-tenants";

    private TenantsService tenantsService;

    private DataAccess dataAccess;

    public CreateTenants(TenantsService tenantsService, DataAccess dataAccess) {
        this.tenantsService = tenantsService;
        this.dataAccess = dataAccess;
    }

    @Override
    public void call(Task2 task) {
        long bucket = getBucket(task.getTrigger());
        CountDownLatch latch = new CountDownLatch(1);

        Observable<String> tenantIds = dataAccess.findTenantIds(bucket)
                .flatMap(Observable::from)
                .map(row -> row.getString(0))
                .flatMap(tenantId -> tenantDoesNotExist(tenantId).map(doesNotExist -> doesNotExist ? tenantId : ""))
                .filter(tenantId -> !tenantId.isEmpty());

        tenantsService.createTenants(bucket, tenantIds).subscribe(
                aVoid -> {},
                t -> {
                    logger.warn("Tenant creation failed", t);
                    latch.countDown();
                },
                () -> dataAccess.deleteTenantsBucket(bucket).subscribe(
                        resultSet -> {},
                        t -> {
                            logger.warn("Failed to delete tenants bucket [" + bucket + "]", t);
                            latch.countDown();
                        },
                        latch::countDown
                )
        );
        try {
            latch.await();
        } catch (InterruptedException e) {
        }
    }

    private long getBucket(Trigger trigger) {
        DateTime end = new DateTime(trigger.getTriggerTime());
        return end.minusMinutes(30).getMillis();
    }

    private Observable<Boolean> tenantDoesNotExist(String tenantId) {
        return dataAccess.findTenant(tenantId).map(ResultSet::isExhausted);
    }

    private Observable<Tenant> getTenant(String tenantId) {
        return dataAccess.findTenant(tenantId)
                .flatMap(Observable::from)
                .map(Functions::getTenant);
    }
}
