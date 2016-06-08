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

import org.hawkular.metrics.model.MetricType;
import org.hawkular.metrics.scheduler.api.JobDetails;
import org.hawkular.rx.cassandra.driver.RxSession;

import com.datastax.driver.core.PreparedStatement;

import rx.Completable;
import rx.functions.Func1;

/**
 * @author jsanda
 */
public class DeleteMetric implements Func1<JobDetails, Completable> {

    private RxSession session;

    private PreparedStatement deleteData;

    private PreparedStatement deleteFromMetricsIndex;

    public DeleteMetric(RxSession session) {
        this.session = session;
        deleteData = session.getSession().prepare(
                "DELETE FROM data WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = 0");
        deleteFromMetricsIndex = session.getSession().prepare(
                "DELETE FROM metrics_idx WHERE tenant_id = ? AND type = ? AND metric = ?");
    }

    private Completable deleteData(String tenantId, MetricType<?> type, String metricName) {
        return session.execute(deleteData.bind(tenantId, type.getCode(), metricName)).toCompletable();
    }

    private Completable deleteFromMetricsIndex(String tenantId, MetricType<?> type, String metricName) {
        return session.execute(deleteFromMetricsIndex.bind(tenantId, type.getCode(), metricName)).toCompletable();
    }

    @Override
    public Completable call(JobDetails jobDetails) {
        String tenantId = jobDetails.getParameters().get("tenantId");
        MetricType<?> type = MetricType.fromTextCode(jobDetails.getParameters().get("metricType"));
        String metricName = jobDetails.getParameters().get("metricName");

        return deleteData(tenantId, type, metricName).mergeWith(deleteFromMetricsIndex(tenantId, type, metricName));
    }
}
