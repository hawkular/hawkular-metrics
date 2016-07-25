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

import org.hawkular.metrics.core.service.MetricsService;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricType;
import org.hawkular.metrics.scheduler.api.JobDetails;
import org.hawkular.rx.cassandra.driver.RxSession;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;

import rx.Completable;
import rx.Observable;
import rx.functions.Func1;

/**
 * @author jsanda
 */
public class DeleteTenant implements Func1<JobDetails, Completable> {

    public static final String JOB_NAME = "DELETE_TENANT";

    private RxSession session;

    private PreparedStatement deleteTenant;

    private PreparedStatement deleteData;

    private PreparedStatement deleteFromMetricsIndex;

    private PreparedStatement deleteTag;

    private PreparedStatement deleteRetentions;

    private MetricsService metricsService;

    public DeleteTenant(RxSession session, MetricsService metricsService) {
        this.session = session;
        this.metricsService = metricsService;
        deleteTenant = session.getSession().prepare("DELETE FROM tenants WHERE id = ?");
        deleteData = session.getSession().prepare(
                "DELETE FROM data WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = 0");
        deleteFromMetricsIndex = session.getSession().prepare(
                "DELETE FROM metrics_idx WHERE tenant_id = ? AND type = ? AND metric = ?");
        deleteTag = session.getSession().prepare("DELETE FROM metrics_tags_idx WHERE tenant_id = ? AND tname = ? " +
                "AND tvalue = ? AND type = ? AND metric = ?");
        deleteRetentions = session.getSession().prepare("DELETE FROM retentions_idx WHERE tenant_id = ? AND type = ?");
    }

    @Override public Completable call(JobDetails details) {
        String tenantId = details.getParameters().get("tenantId");

        return Completable.merge(
                deleteMetrics(tenantId).toCompletable(),
                deleteRetentions(tenantId).toCompletable(),
                deleteTenant(tenantId).toCompletable()
        );
    }

    private Observable<ResultSet> deleteMetrics(String tenantId) {
        return Observable.from(MetricType.all())
                .flatMap(type -> metricsService.findMetrics(tenantId, type).flatMap(metric ->
                        Observable.merge(deleteMetricData(metric), deleteTags(metric))
                                .concatWith(updateMetricsIndex(metric))));
    }

    private <T> Observable<ResultSet> deleteMetricData(Metric<T> metric) {
        return session.execute(deleteData.bind(metric.getMetricId().getTenantId(),
                metric.getMetricId().getType().getCode(), metric.getMetricId().getName()));
    }

    private <T> Observable<ResultSet> updateMetricsIndex(Metric<T> metric) {
        return session.execute(deleteFromMetricsIndex.bind(metric.getMetricId().getTenantId(),
                metric.getMetricId().getType().getCode(), metric.getMetricId().getName()));
    }

    private <T> Observable<ResultSet> deleteTags(Metric<T> metric) {
        return Observable.from(metric.getTags().entrySet())
                .flatMap(entry -> session.execute(deleteTag.bind(metric.getMetricId().getTenantId(), entry.getKey(),
                        entry.getValue(), metric.getMetricId().getType().getCode(), metric.getMetricId().getName())));
    }

    private <T> Observable<ResultSet> deleteRetentions(String tenantId) {
        return Observable.from(MetricType.all())
                .flatMap(type -> session.execute(deleteRetentions.bind(tenantId, type.getCode())));
    }

    private Observable<ResultSet> deleteTenant(String tenantId) {
        return session.execute(deleteTenant.bind(tenantId));
    }
}
