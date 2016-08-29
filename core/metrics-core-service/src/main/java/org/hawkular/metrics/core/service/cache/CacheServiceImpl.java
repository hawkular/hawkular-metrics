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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hawkular.metrics.datetime.DateTimeService;
import org.hawkular.metrics.model.Metric;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.util.concurrent.NotifyingFuture;
import org.infinispan.context.Flag;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.jboss.logging.Logger;
import org.msgpack.MessagePack;

import com.google.common.base.Stopwatch;

import rx.Completable;
import rx.Single;

/**
 * @author jsanda
 */
public class CacheServiceImpl implements CacheService {

    private static final Logger logger = Logger.getLogger(CacheServiceImpl.class);

    protected EmbeddedCacheManager cacheManager;

    private MessagePack messagePack = new MessagePack();

    AdvancedCache<DataPointKey, Double> rawDataCache;

    public void init(){
        try {
            logger.info("Initializing caches");
            cacheManager = new DefaultCacheManager(CacheServiceImpl.class.getResourceAsStream(
                    "/metrics-infinispan.xml"));
            cacheManager.startCaches(cacheManager.getCacheNames().toArray(new String[0]));
            Cache<DataPointKey, Double> cache = cacheManager.getCache("rawData");
            rawDataCache = cache.getAdvancedCache().withFlags(Flag.IGNORE_RETURN_VALUES, Flag.SKIP_LOCKING);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void shutdown() {
        logger.info("Shutting down");
        cacheManager.stop();
    }

    @Override
    public AdvancedCache<DataPointKey, Double> getRawDataCache() {
        return rawDataCache;
    }

    @Override
    public Completable removeFromRawDataCache(long timeSlice) {
        return Completable.fromAction(() -> {
            String group = Long.toString(timeSlice / 1000);
            rawDataCache.removeGroup(group);
        });
    }

//    @Override
//    public Single<DataPoint<? extends Number>> put(MetricId<? extends Number> metricId,
//            DataPoint<? extends Number> dataPoint) {
//        long timeSlice = DateTimeService.getTimeSlice(dataPoint.getTimestamp(), standardMinutes(1));
//        DataPointKey key = new DataPointKey(metricId.getTenantId(), metricId.getName(), dataPoint.getTimestamp(),
//                timeSlice);
//        NotifyingFuture<Double> future = rawDataCache.putAsync(key, dataPoint.getValue());
//        return from(future);
//    }

    @Override
    public <T> Completable putAll(List<Metric<T>> metrics) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        Map<DataPointKey, Double> map = new HashMap<>();
        metrics.forEach(metric -> {
            metric.getDataPoints().forEach(dataPoint -> {
                long minute = DateTimeService.getTimeSlice(dataPoint.getTimestamp(), standardMinutes(1));
                String timeSlice = Long.toString(minute / 1000);
                DataPointKey key = new DataPointKey(encode(metric.getMetricId().getTenantId(),
                        metric.getMetricId().getName(), timeSlice), timeSlice);
                map.put(key, (Double) dataPoint.getValue());
            });
        });
        NotifyingFuture<Void> future = rawDataCache.putAllAsync(map);
        return from(future).toCompletable();
    }

    private byte[] encode(String tenantId, String metric, String timeSlice) {
        try {
            List<String> src = new ArrayList<>(3);
            src.add(tenantId);
            src.add(metric);
            src.add(timeSlice);

            MessagePack messagePack = new MessagePack();
            return messagePack.write(src);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
