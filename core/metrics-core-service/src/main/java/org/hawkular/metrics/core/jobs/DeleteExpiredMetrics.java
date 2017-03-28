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
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.MetricType;
import org.hawkular.metrics.scheduler.api.JobDetails;
import org.hawkular.rx.cassandra.driver.RxSession;

import com.datastax.driver.core.PreparedStatement;

import rx.Completable;
import rx.Observable;
import rx.functions.Func1;

/**
 * @author Stefan Negrea
 */
public class DeleteExpiredMetrics implements Func1<JobDetails, Completable> {

    public static final String JOB_NAME = "DELETE_EXPIRED_METRICS";

    private MetricsService metricsService;
    private RxSession session;
    private PreparedStatement findEligibleTenants;
    private PreparedStatement findEligibleMetrics;

    public DeleteExpiredMetrics(MetricsService metricsService, RxSession session) {
        this.metricsService = metricsService;
        this.session = session;

        findEligibleTenants = session.getSession()
                .prepare("SELECT DISTINCT tenant_id, type FROM metrics_expiration_idx");
        findEligibleMetrics = session.getSession()
                .prepare(
                        "SELECT tenant_id, type, metric, time FROM metrics_expiration_idx WHERE tenant_id = ? AND type = ?");
    }

    @Override
    public Completable call(JobDetails jobDetails) {
        String unparsedConfigExpirationTime = jobDetails.getParameters().get("expirationTimestamp");
        Long configuredExpirationTime = null;
        if (unparsedConfigExpirationTime != null && !unparsedConfigExpirationTime.isEmpty()) {
            try {
                configuredExpirationTime = Long.parseLong(unparsedConfigExpirationTime);
            } catch (Exception exp) {
                //do nothing just use the default configuration
            }
        }

        long expirationTime = (configuredExpirationTime != null ? configuredExpirationTime
                : System.currentTimeMillis());

        return session.execute(findEligibleTenants.bind())
                .flatMap(Observable::from)
                .flatMap(row -> session.execute(findEligibleMetrics.bind(row.getString(0), row.getByte(1))))
                .flatMap(Observable::from)
                .filter(row -> row.getTimestamp(3).getTime() < expirationTime)
                .map(row -> new MetricId(row.getString(0), MetricType.fromCode(row.getByte(1)), row.getString(2)))
                .flatMap(metricId -> metricsService.deleteMetric(metricId)).toCompletable();
    }
}
