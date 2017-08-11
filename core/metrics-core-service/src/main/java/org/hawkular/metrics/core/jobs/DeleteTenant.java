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

import org.hawkular.metrics.core.service.MetricsService;
import org.hawkular.metrics.model.MetricType;
import org.hawkular.metrics.scheduler.api.JobDetails;
import org.hawkular.rx.cassandra.driver.RxSession;
import org.jboss.logging.Logger;

import com.datastax.driver.core.PreparedStatement;

import rx.Completable;
import rx.Observable;
import rx.functions.Func1;

/**
 * @author jsanda
 */
public class DeleteTenant implements Func1<JobDetails, Completable> {

    private static Logger logger = Logger.getLogger(DeleteTenant.class);

    public static final String JOB_NAME = "DELETE_TENANT";

    private RxSession session;

    private PreparedStatement deleteTenant;
    private PreparedStatement deleteFromMetricsIndex;
    private PreparedStatement findTags;
    private PreparedStatement deleteTag;
    private PreparedStatement deleteRetentions;

    private MetricsService metricsService;

    public DeleteTenant(RxSession session, MetricsService metricsService) {
        this.session = session;
        this.metricsService = metricsService;
        deleteTenant = session.getSession().prepare("DELETE FROM tenants WHERE id = ?");
        deleteFromMetricsIndex = session.getSession().prepare(
                "DELETE FROM metrics_idx WHERE tenant_id = ? AND type = ?");
        findTags = session.getSession().prepare("SELECT DISTINCT tenant_id, tname FROM metrics_tags_idx");
        deleteTag = session.getSession().prepare("DELETE FROM metrics_tags_idx WHERE tenant_id = ? AND tname = ?");
        deleteRetentions = session.getSession().prepare("DELETE FROM retentions_idx WHERE tenant_id = ? AND type = ?");
    }

    @Override
    public Completable call(JobDetails details) {
        String tenantId = details.getParameters().get("tenantId");

        // The concat operator is used instead of merge to ensure things execute in order. The deleteMetricData
        // method queries the metrics index, so we want to update the index only after we have finished deleting
        // data.
        return deleteMetricData(tenantId)
                .concatWith(deleteTenant(tenantId))
                .concatWith(deleteRetentions(tenantId))
                .concatWith(deleteMetricsIndex(tenantId))
                .concatWith(deleteTags(tenantId))
                .toCompletable()
                .doOnCompleted(() -> logger.infof("Finished deleting " + tenantId));
    }

    private Observable<Void> deleteMetricData(String tenantId) {
        return Observable.from(MetricType.all())
                .flatMap(type -> metricsService.findMetrics(tenantId, type))
                .flatMap(metric -> metricsService.deleteMetric(metric.getMetricId()));
    }

    private Observable<Void> deleteMetricsIndex(String tenantId) {
        return Observable.from(MetricType.all())
                .flatMap(type -> session.execute(deleteFromMetricsIndex.bind(tenantId, type.getCode())))
                .map(r -> null);
    }

    private Observable<Void> deleteTags(String tenantId) {
        return session.execute(findTags.bind())
                .flatMap(Observable::from)
                .filter(row -> row.getString(0).equals(tenantId))
                .flatMap(row -> session.execute(deleteTag.bind(row.getString(0), row.getString(1))))
                .map(r -> null);
    }

    private Observable<Void> deleteRetentions(String tenantId) {
        return Observable.from(MetricType.all())
                .flatMap(type -> session.execute(deleteRetentions.bind(tenantId, type.getCode())))
                .map(r -> null);
    }

    private Observable<Void> deleteTenant(String tenantId) {
        return session.execute(deleteTenant.bind(tenantId)).map(r -> null);
    }
}
