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

import static org.hawkular.metrics.datetime.DateTimeService.getTimeSlice;
import static org.hawkular.metrics.model.MetricType.GAUGE;
import static org.joda.time.Duration.standardMinutes;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import org.hawkular.metrics.core.service.MetricsService;
import org.hawkular.metrics.core.service.cache.CacheService;
import org.hawkular.metrics.core.service.cache.CompositeCollector;
import org.hawkular.metrics.core.service.cache.DataPointKey;
import org.hawkular.metrics.core.service.cache.RollupKey;
import org.hawkular.metrics.core.service.transformers.NumericDataPointCollector;
import org.hawkular.metrics.model.Buckets;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.NumericBucketPoint;
import org.hawkular.metrics.model.Percentile;
import org.hawkular.metrics.scheduler.api.JobDetails;
import org.hawkular.rx.cassandra.driver.RxSession;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.jboss.logging.Logger;
import org.joda.time.DateTime;

import com.datastax.driver.core.PreparedStatement;
import com.google.common.collect.ImmutableMap;

import rx.Completable;
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

    private Map<Integer, PreparedStatement> inserts;

    public ComputeRollups(RxSession session, MetricsService metricsService, CacheService cacheService) {
        this.session = session;
        this.metricsService = metricsService;
        this.cacheService = cacheService;

        inserts = ImmutableMap.of(
                60, session.getSession().prepare(getInsertCQL(60)),
                300, session.getSession().prepare(getInsertCQL(300))
        );
    }

    private String getInsertCQL(int rollup) {
        return "INSERT INTO rollup" + rollup + " (tenant_id, metric, shard, time, min, max, avg, median, sum, " +
                "samples, percentiles) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
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

        BiConsumer<CompositeCollector, CompositeCollector> combiner = (c1, c2) -> {};

        List<Completable> updates = group.entrySet().stream()
                .collect(groupingBy(entry -> new MetricId<>(entry.getKey().getTenantId(), GAUGE,
                        entry.getKey().getMetric())))
                .entrySet()
                .stream()
                .map(entry -> entry.getValue().stream().map(Map.Entry::getValue)
                        .collect(
                                () -> getCollector(entry.getKey(), start, end),
                                CompositeCollector::increment,
                                (c1, c2) -> {}))
                .flatMap(collector -> collector.getCollectors().entrySet().stream())
                .map(entry -> updateRollup(entry.getKey(), entry.getValue(), end))
                .collect(toList());

        return Completable.merge(updates).andThen(Completable.fromAction(() ->
                cache.removeGroup(Long.toString(start))));
    }

    private CompositeCollector getCollector(MetricId<Double> metricId, long start, long timeSlice) {
        RollupKey rollup60Key = new RollupKey(new DataPointKey(metricId.getTenantId(), metricId.getName(), start,
                start), 60);

        long rollup300TimeSlice = getTimeSlice(new DateTime(timeSlice), standardMinutes(5)).minusMinutes(5).getMillis();
        RollupKey rollup300Key = new RollupKey(new DataPointKey(metricId.getTenantId(), metricId.getName(),
                rollup300TimeSlice, rollup300TimeSlice), 300);

        return new CompositeCollector(ImmutableMap.of(
                rollup60Key, get1MinuteCollector(rollup60Key.getKey()),
                rollup300Key, getRollupCollector(rollup300Key)
        ));
    }

    private NumericDataPointCollector get1MinuteCollector(DataPointKey key) {
        long end = new DateTime(key.getTimestamp()).plusMinutes(1).getMillis();
        Buckets buckets = Buckets.fromCount(key.getTimestamp(), end, 1);
        return new NumericDataPointCollector(buckets, 0, asList(new Percentile("90", 90.0), new Percentile("95", 95.0),
                new Percentile("99", 99.0)));
    }

    private NumericDataPointCollector getRollupCollector(RollupKey key) {
        Cache<DataPointKey, NumericDataPointCollector> cache = cacheService.getRollupCache(key.getRollup());
        NumericDataPointCollector collector = cache.remove(key.getKey());
        if (collector == null) {
            long end = new DateTime(key.getKey().getTimestamp()).plusSeconds(key.getRollup()).getMillis();
            Buckets buckets = Buckets.fromCount(key.getKey().getTimestamp(), end, 1);
            return new NumericDataPointCollector(buckets, 0, asList(new Percentile("90", 90.0),
                    new Percentile("95", 95.0), new Percentile("99", 99.0)));
        }
        return collector;
    }

    private Completable updateRollup(RollupKey key, NumericDataPointCollector collector, long timeSlice) {
        if (key.getRollup() == 60) {
            return insertDataPoint(key.getKey(), collector.toBucketPoint(), inserts.get(60));
        }

        if (new DateTime(key.getKey().getTimestamp()).plusSeconds(key.getRollup()).getMillis() == timeSlice) {
            return insertDataPoint(key.getKey(), collector.toBucketPoint(), inserts.get(key.getRollup())).concatWith(
                    cacheService.remove(key.getKey(), key.getRollup()));
        }
        return cacheService.put(key.getKey(), collector, key.getRollup());
    }

    private Completable insertDataPoint(DataPointKey key, NumericBucketPoint dataPoint,
            PreparedStatement insert) {
        logger.debug("Preparing to execute " + insert.getQueryString());
        return session.execute(insert.bind(key.getTenantId(), key.getMetric(), 0L, new Date(dataPoint.getStart()),
                dataPoint.getMin(), dataPoint.getMax(), dataPoint.getAvg(), dataPoint.getMedian(), dataPoint.getSum(),
                dataPoint.getSamples(), toMap(dataPoint.getPercentiles()))).toCompletable();
    }

    private Map<Float, Double> toMap(List<Percentile> percentiles) {
        Map<Float, Double> map = new HashMap<>();
        percentiles.forEach(p -> map.put((float) p.getQuantile(), p.getValue()));
        return map;
    }

}
