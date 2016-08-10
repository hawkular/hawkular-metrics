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
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

import static org.hawkular.metrics.model.MetricType.GAUGE;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import org.hawkular.metrics.core.service.MetricsService;
import org.hawkular.metrics.core.service.cache.CacheService;
import org.hawkular.metrics.core.service.cache.DataPointKey;
import org.hawkular.metrics.core.service.transformers.NumericDataPointCollector;
import org.hawkular.metrics.model.Buckets;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.NumericBucketPoint;
import org.hawkular.metrics.model.Percentile;
import org.hawkular.metrics.scheduler.api.JobDetails;
import org.hawkular.rx.cassandra.driver.RxSession;
import org.infinispan.AdvancedCache;
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

    private CacheService cacheService;

    private PreparedStatement insertDataPoint;

    public ComputeRollups(RxSession session, MetricsService metricsService, CacheService cacheService) {
        this.session = session;
        this.metricsService = metricsService;
        this.cacheService = cacheService;

        insertDataPoint = session.getSession().prepare(
                "INSERT INTO rollup5min (tenant_id, metric, shard, time, min, max, avg, median, sum, samples, " +
                    "percentiles) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
    }

    @Override
    public Completable call(JobDetails details) {
        DateTime time = new DateTime(details.getTrigger().getTriggerTime());

        long end = details.getTrigger().getTriggerTime();
        long start = new DateTime(end).minusMinutes(1).getMillis();

        logger.debug("Preparing to compute roll ups for time range {start=" + start + ", end=" + end + "}");

        Buckets buckets = Buckets.fromCount(start, end, 1);
        Func0<NumericDataPointCollector> getCollector = () -> new NumericDataPointCollector(buckets, 0,
                asList(new Percentile("90", 90.0), new Percentile("95", 95.0), new Percentile("99", 99.0)));

        Supplier<NumericDataPointCollector> supplier = () ->  new NumericDataPointCollector(buckets, 0,
                asList(new Percentile("90", 90.0), new Percentile("95", 95.0), new Percentile("99", 99.0)));

        AdvancedCache<DataPointKey, DataPoint<? extends Number>> cache = cacheService.getRawDataCache()
                .getAdvancedCache();
        Map<DataPointKey, DataPoint<? extends Number>> group = cache.getGroup(Long.toString(start));

        BiConsumer<NumericDataPointCollector, DataPoint<? extends Number>> consumer =
                NumericDataPointCollector::increment;

        BiConsumer<NumericDataPointCollector, NumericDataPointCollector> combiner = (c1, c2) -> {};

        List<Observable<ResultSet>> inserts = group.entrySet().stream()
                .collect(groupingBy(entry -> new MetricId<>(entry.getKey().getTenantId(), GAUGE,
                        entry.getKey().getMetric())))
                .entrySet()
                .stream()
                .map(entry -> {
                    NumericBucketPoint bucketPoint = entry.getValue().stream().map(Map.Entry::getValue)
                            .collect(supplier, NumericDataPointCollector::increment, combiner).toBucketPoint();
                    return insertDataPoint(entry.getKey(), bucketPoint);
                })
                .collect(toList());

        return Observable.merge(inserts).toCompletable().andThen(Completable.fromAction(() ->
                cache.removeGroup(Long.toString(start))));
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
