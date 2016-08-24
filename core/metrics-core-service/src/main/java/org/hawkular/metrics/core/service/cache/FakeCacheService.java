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

import java.util.List;

import org.hawkular.metrics.core.service.transformers.NumericDataPointCollector;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.infinispan.Cache;

import rx.Completable;
import rx.Single;

/**
 * @author jsanda
 */
public class FakeCacheService implements CacheService {

    @Override
    public Cache<DataPointKey, DataPoint<? extends Number>> getRawDataCache() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Cache<DataPointKey, NumericDataPointCollector> getRollupCache(int rollup) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Single<DataPoint<? extends Number>> put(MetricId<? extends Number> metricId,
            DataPoint<? extends Number> dataPoint) {
        return Single.just(dataPoint);
    }

    @Override
    public Completable put(DataPointKey key, NumericDataPointCollector collector, int rollup) {
        return Completable.complete();
    }

    @Override
    public <T> Completable putAll(List<Metric<T>> metrics) {
        return Completable.complete();
    }

    @Override
    public <T> Completable putAllAsync(List<Metric<T>> metrics) {
        return Completable.complete();
    }

    @Override
    public Completable remove(DataPointKey key, int rollup) {
        return Completable.complete();
    }
}
