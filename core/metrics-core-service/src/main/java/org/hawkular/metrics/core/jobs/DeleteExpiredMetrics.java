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

import java.util.concurrent.TimeUnit;

import org.hawkular.metrics.core.service.MetricsService;
import org.hawkular.metrics.datetime.DateTimeService;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.MetricType;
import org.hawkular.metrics.scheduler.api.JobDetails;
import org.hawkular.metrics.sysconfig.ConfigurationService;
import org.hawkular.rx.cassandra.driver.RxSession;
import org.jboss.logging.Logger;

import com.datastax.driver.core.PreparedStatement;
import com.google.common.base.Stopwatch;

import rx.Completable;
import rx.Observable;
import rx.functions.Func1;

/**
 * @author Stefan Negrea
 */
public class DeleteExpiredMetrics implements Func1<JobDetails, Completable> {

    private static Logger logger = Logger.getLogger(DeleteExpiredMetrics.class);

    public static final String JOB_NAME = "DELETE_EXPIRED_METRICS";

    private MetricsService metricsService;
    private RxSession session;
    private ConfigurationService configurationService;
    private PreparedStatement findEligibleTenants;
    private PreparedStatement findEligibleMetrics;
    private PreparedStatement findUnexpiredDataPoints;
    private long metricExpirationDelay;

    public DeleteExpiredMetrics(MetricsService metricsService, RxSession session,
            ConfigurationService configurationService, int metricExpirationDelayInDays) {
        this.metricsService = metricsService;
        this.session = session;
        this.configurationService = configurationService;

        findEligibleTenants = session.getSession()
                .prepare("SELECT DISTINCT tenant_id, type FROM metrics_expiration_idx");
        findEligibleMetrics = session.getSession()
                .prepare(
                        "SELECT tenant_id, type, metric, time FROM metrics_expiration_idx WHERE tenant_id = ? AND type = ?");
        findUnexpiredDataPoints = session.getSession()
                .prepare(
                        "SELECT * FROM data WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = 0 LIMIT 1;");

        this.metricExpirationDelay = metricExpirationDelayInDays * 24 * 3600 * 1000L;
    }

    @Override
    public Completable call(JobDetails jobDetails) {
        logger.info("Starting delete expired metrics job");
        Stopwatch stopwatch = Stopwatch.createStarted();

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
                : DateTimeService.now.get().getMillis()) - metricExpirationDelay;

        Observable<MetricId<?>> expirationIndexResults = session.execute(findEligibleTenants.bind())
                .flatMap(Observable::from)
                .flatMap(row -> session.execute(findEligibleMetrics.bind(row.getString(0), row.getByte(1))))
                .flatMap(Observable::from)
                .filter(row -> row.getTimestamp(3).getTime() < expirationTime)
                .map(row -> new MetricId<>(row.getString(0), MetricType.fromCode(row.getByte(1)), row.getString(2)));

        //If the compression job is disabled then check the data point table for data
        String compressJobEnabledConfig = configurationService.load(CompressData.CONFIG_ID, "enabled").toBlocking()
                .firstOrDefault(null);

        boolean compressJobEnabled = false;
        if (compressJobEnabledConfig != null && !compressJobEnabledConfig.isEmpty()) {
            try {
                compressJobEnabled = Boolean.parseBoolean(compressJobEnabledConfig);
            } catch (Exception e) {
                //do nothing, assume the compression job is disabled
            }
        }
        if (!compressJobEnabled) {
            expirationIndexResults = expirationIndexResults
                    .flatMap(r -> session
                            .execute(findUnexpiredDataPoints.bind(r.getTenantId(), r.getType().getCode(), r.getName()))
                            .flatMap(Observable::from)
                            .isEmpty()
                            .filter(empty -> empty)
                            .map(empty -> r));
        }

        return expirationIndexResults
                .concatMap(metricId -> metricsService.deleteMetric(metricId))
                .onErrorResumeNext(e -> {
                    logger.error("Failed to delete metric data", e);
                    return Observable.empty();
                })
                .doOnError(t -> {
                    stopwatch.stop();
                    logger.error("The job for deleting expired metrics failed. Total run time "
                            + stopwatch.elapsed(TimeUnit.MILLISECONDS) + " ms", t);
                })
                .doOnCompleted(() -> {
                    stopwatch.stop();
                    logger.info("The job for deleting expired metrics finished. Total run time "
                            + stopwatch.elapsed(TimeUnit.MILLISECONDS) + " ms");
                })
                .toCompletable();
    }
}
