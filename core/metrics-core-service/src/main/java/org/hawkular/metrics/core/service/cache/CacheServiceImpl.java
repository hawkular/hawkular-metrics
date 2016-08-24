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
package org.hawkular.metrics.core.service.cache;

import static org.joda.time.Duration.standardMinutes;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hawkular.metrics.core.service.transformers.NumericDataPointCollector;
import org.hawkular.metrics.datetime.DateTimeService;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.util.concurrent.NotifyingFuture;
import org.infinispan.context.Flag;
import org.infinispan.manager.EmbeddedCacheManager;

import rx.Completable;
import rx.Single;
import rx.schedulers.Schedulers;

/**
 * @author jsanda
 */
public class CacheServiceImpl implements CacheService {

    protected EmbeddedCacheManager cacheManager;

    AdvancedCache<DataPointKey, DataPoint<? extends Number>> rawDataCache;

    public void init(){
//        try {
//            cacheManager = new DefaultCacheManager(CacheServiceImpl.class.getResourceAsStream(
//                    "/metrics-infinispan.xml"));
//            cacheManager.startCaches(cacheManager.getCacheNames().toArray(new String[0]));
//            Cache<DataPointKey, DataPoint<? extends Number>> cache = cacheManager.getCache("rawData");
////            rawDataCache = cache.getAdvancedCache();
//            rawDataCache = cache.getAdvancedCache().withFlags(Flag.IGNORE_RETURN_VALUES, Flag.SKIP_LOCKING);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
    }

    public void shutdown() {
        cacheManager.stop();
    }

    @Override
    public Cache<DataPointKey, DataPoint<? extends Number>> getRawDataCache() {
        return rawDataCache;
    }

    @Override
    public Cache<DataPointKey, NumericDataPointCollector> getRollupCache(int rollup) {
        return cacheManager.getCache("rollup" + rollup);
    }

    @Override
    public Single<DataPoint<? extends Number>> put(MetricId<? extends Number> metricId,
            DataPoint<? extends Number> dataPoint) {
        Cache<DataPointKey, DataPoint<? extends Number>> cache = getRawDataCache();
        long timeSlice = DateTimeService.getTimeSlice(dataPoint.getTimestamp(), standardMinutes(1));
        DataPointKey key = new DataPointKey(metricId.getTenantId(), metricId.getName(), dataPoint.getTimestamp(),
                timeSlice);
        NotifyingFuture<DataPoint<? extends Number>> future = cache.putAsync(key, dataPoint);
        return from(future);
    }

    @Override
    public <T> Completable putAll(List<Metric<T>> metrics) {
        Map<DataPointKey, DataPoint<? extends Number>> map = new HashMap<>();
        metrics.forEach(metric -> {
            metric.getDataPoints().forEach(dataPoint -> {
                long timeSlice = DateTimeService.getTimeSlice(dataPoint.getTimestamp(), standardMinutes(1));
                DataPointKey key = new DataPointKey(metric.getMetricId().getTenantId(),
                        metric.getMetricId().getName(), dataPoint.getTimestamp(), timeSlice);
                map.put(key, (DataPoint<? extends Number>) dataPoint);
            });
        });
        NotifyingFuture<Void> future = rawDataCache.putAllAsync(map);
        return from(future).toCompletable();
    }

    @Override
    public <T> Completable putAllAsync(List<Metric<T>> metrics) {
        return Completable.create(subscriber -> {
            try {
                Map<DataPointKey, DataPoint<? extends Number>> map = new HashMap<>();
                metrics.forEach(metric -> {
                    metric.getDataPoints().forEach(dataPoint -> {
                        long timeSlice = DateTimeService.getTimeSlice(dataPoint.getTimestamp(), standardMinutes(1));
                        DataPointKey key = new DataPointKey(metric.getMetricId().getTenantId(),
                                metric.getMetricId().getName(), dataPoint.getTimestamp(), timeSlice);
                        map.put(key, (DataPoint<? extends Number>) dataPoint);
                    });
                });
                NotifyingFuture<Void> future = rawDataCache.putAllAsync(map);
                future.attachListener(f -> subscriber.onCompleted());
            } catch (Throwable t) {
                subscriber.onError(t);
            }
        }).subscribeOn(Schedulers.io());
    }

    @Override
    public Completable put(DataPointKey key, NumericDataPointCollector collector, int rollup) {
        AdvancedCache<DataPointKey, NumericDataPointCollector> cache = getRollupCache(rollup).getAdvancedCache()
                .withFlags(Flag.IGNORE_RETURN_VALUES, Flag.SKIP_LOCKING);
        NotifyingFuture<NumericDataPointCollector> future = cache.putAsync(key, collector);
        return from(future).toCompletable();
    }

    @Override
    public Completable remove(DataPointKey key, int rollup) {
        Cache<DataPointKey, NumericDataPointCollector> cache = getRollupCache(rollup);
        NotifyingFuture<NumericDataPointCollector> future = cache.removeAsync(key);
        return from(future).toCompletable();
    }

    private <T> Single<T> from(NotifyingFuture<T> notifyingFuture) {
        return Single.create(subscriber -> {
            notifyingFuture.attachListener(future -> {
                try {
                    subscriber.onSuccess(future.get());
                } catch (Exception e) {
                    subscriber.onError(e);
                }
            });
        });
    }

}
