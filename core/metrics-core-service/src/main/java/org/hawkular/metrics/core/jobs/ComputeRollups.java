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
import static org.joda.time.Duration.standardSeconds;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import org.hawkular.metrics.core.service.cache.CacheService;
import org.hawkular.metrics.core.service.cache.DataPointKey;
import org.hawkular.metrics.core.service.cache.RollupKey;
import org.hawkular.metrics.core.service.rollup.RollupService;
import org.hawkular.metrics.core.service.transformers.NumericDataPointCollector;
import org.hawkular.metrics.model.Buckets;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.Percentile;
import org.hawkular.metrics.scheduler.api.JobDetails;
import org.hawkular.rx.cassandra.driver.RxSession;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.jboss.logging.Logger;
import org.joda.time.DateTime;

import com.google.common.base.Stopwatch;

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

    private RollupService rollupService;

    private CacheService cacheService;

    public ComputeRollups(RxSession session, RollupService rollupService, CacheService cacheService) {
        this.session = session;
        this.rollupService = rollupService;
        this.cacheService = cacheService;
    }

    @Override
    public Completable call(JobDetails details) {
//        if (true) return Completable.complete();

        Stopwatch stopwatch = Stopwatch.createStarted();

        // TODO Handle scenario in which there is no raw data but rollups need to be updated
        // While this scenario may seem somewhat contrived, I ran across it while writing a test and it is more likely
        // to happen with less frequent sampling rates, e.g., every 2 or 3 minutes vs. every 20 or 30 seconds. When
        // the 5 minute or 1 hour time slice is finished, we need to persist those data point regardless of whether or
        // not the past minute has any raw data collected. The current implementation only updates the rollups if there
        // is raw data for the past minute.

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
                                () -> getCollector(entry.getKey(), start),
                                CompositeCollector::increment,
                                // TODO add support for parallel streams. The combiner arg is used with parallel streams
                                // which is not currently used. That's why we use a no-op here for now.
                                (c1, c2) -> {}))
                .flatMap(collector -> collector.getCollectors().entrySet().stream())
                .map(entry -> updateRollup(entry.getKey(), entry.getValue(), end))
                .collect(toList());

        return Completable.merge(updates)
                .andThen(Completable.fromAction(() -> cache.removeGroup(Long.toString(start))))
                .doOnCompleted(() -> {
                    stopwatch.stop();
                    logger.debug("Finished in " + stopwatch.elapsed(TimeUnit.MILLISECONDS) + " ms");
                });
    }

    private CompositeCollector getCollector(MetricId<Double> metricId, long start) {
        Map<RollupKey, NumericDataPointCollector> collectors = new HashMap<>();
        for (RollupService.RollupBucket bucket : RollupService.RollupBucket.values()) {
            long timeSlice = getTimeSlice(start, standardSeconds(bucket.getDuration()));
            RollupKey rollupKey = new RollupKey(new DataPointKey(metricId.getTenantId(), metricId.getName(),
                    timeSlice, timeSlice), bucket.getDuration());
            collectors.put(rollupKey, getRollupCollector(rollupKey));
        }
        return new CompositeCollector(collectors);
    }

    private NumericDataPointCollector getRollupCollector(RollupKey key) {
        long end = new DateTime(key.getKey().getTimestamp()).plusSeconds(key.getRollup()).getMillis();
        Buckets buckets = Buckets.fromCount(key.getKey().getTimestamp(), end, 1);

        if (key.getRollup() == 60) {
            // The 1 minute roll ups are a bit of a special case because we do not cache them. Raw data points are
            // cached, aggregated, and persisted every minute so there is no need to cache the collectors like we do
            // for other roll ups.
            return new NumericDataPointCollector(buckets, 0, asList(new Percentile("90", 90.0),
                    new Percentile("95", 95.0), new Percentile("99", 99.0)));
        }

        Cache<DataPointKey, NumericDataPointCollector> cache = cacheService.getRollupCache(key.getRollup());
        NumericDataPointCollector collector = cache.remove(key.getKey());
        if (collector == null) {
            return new NumericDataPointCollector(buckets, 0, asList(new Percentile("90", 90.0),
                    new Percentile("95", 95.0), new Percentile("99", 99.0)));
        }
        return collector;
    }

    private Completable updateRollup(RollupKey key, NumericDataPointCollector collector, long timeSlice) {
        if (key.getRollup() == 60) {
            return rollupService.insert(getGaugeId(key.getKey()), collector.toBucketPoint(), 60);
        }

        if (new DateTime(key.getKey().getTimestamp()).plusSeconds(key.getRollup()).getMillis() == timeSlice) {
//            return rollupService.insert(getGaugeId(key.getKey()), collector.toBucketPoint(), key.getRollup())
//                    .concatWith(cacheService.remove(key.getKey(), key.getRollup()));
            return cacheService.remove(key.getKey(), key.getRollup());
        }
        return cacheService.put(key.getKey(), collector, key.getRollup());
    }

    private MetricId<Double> getGaugeId(DataPointKey key) {
        return new MetricId<>(key.getTenantId(), GAUGE, key.getMetric());
    }

}
