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

/**
 * @author jsanda
 */
// TODO Should CacheService be combined with RollupService?
// Right now, the cache API is solely in support of rollups. That may change in the future,
// it might be more clear for now to make this API part of RollupService.
public interface CacheService {

//    Cache<DataPointKey, DataPoint<? extends Number>> getRawDataCache();
//
//    Cache<DataPointKey, NumericDataPointCollector> getRollupCache(int rollup);
//
//    Single<DataPoint<? extends Number>> put(MetricId<? extends Number> metricId, DataPoint<? extends Number> dataPoint);
//
//    <T> Completable putAll(List<Metric<T>> metrics);
//
//    <T> Completable putAllAsync(List<Metric<T>> metrics);
//
//    Completable put(DataPointKey key, NumericDataPointCollector collector, int rollup);
//
//    Completable remove(DataPointKey key, int rollup);
}
