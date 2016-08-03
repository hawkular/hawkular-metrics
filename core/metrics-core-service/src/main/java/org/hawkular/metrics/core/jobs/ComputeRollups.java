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

import static java.util.Arrays.asList;

import static org.hawkular.metrics.core.service.Order.ASC;
import static org.hawkular.metrics.model.MetricType.GAUGE;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hawkular.metrics.core.service.MetricsService;
import org.hawkular.metrics.core.service.transformers.NumericDataPointCollector;
import org.hawkular.metrics.model.Buckets;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.NumericBucketPoint;
import org.hawkular.metrics.model.Percentile;
import org.hawkular.metrics.scheduler.api.JobDetails;
import org.hawkular.rx.cassandra.driver.RxSession;
import org.jboss.logging.Logger;
import org.joda.time.DateTime;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;

import rx.Completable;
import rx.Observable;
import rx.functions.Func0;
import rx.functions.Func1;

/**
 * @author jsanda
 */
public class ComputeRollups implements Func1<JobDetails, Completable> {

    private static Logger logger = Logger.getLogger(ComputeRollups.class);

    public static final String JOB_NAME = "COMPUTE_ROLLUPS";

    private RxSession session;

    private MetricsService metricsService;

    private PreparedStatement insertDataPoint;

    public ComputeRollups(RxSession session, MetricsService metricsService) {
        this.session = session;
        this.metricsService = metricsService;

        insertDataPoint = session.getSession().prepare(
                "INSERT INTO rollup5min (tenant_id, metric, shard, time, min, max, avg, median, sum, samples, " +
                    "percentiles) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
    }

    @Override
    public Completable call(JobDetails details) {
        DateTime time = new DateTime(details.getTrigger().getTriggerTime());

        long end = details.getTrigger().getTriggerTime();
        long start = new DateTime(end).minusMinutes(5).getMillis();

        logger.debug("Preparing to compute roll ups for time range {start=" + start + ", end=" + end + "}");

        Buckets buckets = Buckets.fromCount(start, end, 1);
        Func0<NumericDataPointCollector> getCollector = () -> new NumericDataPointCollector(buckets, 0,
                asList(new Percentile("90", 90.0), new Percentile("95", 95.0), new Percentile("99", 99.0)));

        return metricsService.getTenants()
                .flatMap(tenant -> {
                    logger.debug("Fetching metrics for " + tenant);
                    return metricsService.findMetrics(tenant.getId(), GAUGE);
                })
                .doOnNext(metric -> logger.debug("Computing rollup for " + metric))
                .flatMap(metric -> metricsService.findDataPoints(metric.getMetricId(), start, end, 0, ASC)
                        .collect(getCollector, NumericDataPointCollector::increment)
                        .map(NumericDataPointCollector::toBucketPoint)
                        .flatMap(dataPoint -> insertDataPoint(metric.getMetricId(), dataPoint)))
                .toCompletable()
                .doOnError(t -> logger.warn("Computing roll ups failed for time slice [" + start + "]", t));
    }

    private Observable<ResultSet> insertDataPoint(MetricId<Double> metricId, NumericBucketPoint dataPoint) {
        return session.execute(insertDataPoint.bind(metricId.getTenantId(), metricId.getName(), 0L,
                new Date(dataPoint.getStart()), dataPoint.getMin(), dataPoint.getMax(), dataPoint.getAvg(),
                dataPoint.getMedian(), dataPoint.getSum(), dataPoint.getSamples(), toMap(dataPoint.getPercentiles())));
    }

    private Map<Float, Double> toMap(List<Percentile> percentiles) {
        Map<Float, Double> map = new HashMap<>();
        percentiles.forEach(p -> map.put((float) p.getQuantile(), p.getValue()));
        return map;
    }

}
