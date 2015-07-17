/*
 * Copyright 2014-2015 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.core.bus;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import org.hawkular.metrics.core.api.AvailabilityBucketDataPoint;
import org.hawkular.metrics.core.api.AvailabilityType;
import org.hawkular.metrics.core.api.BucketedOutput;
import org.hawkular.metrics.core.api.Buckets;
import org.hawkular.metrics.core.api.DataPoint;
import org.hawkular.metrics.core.api.GaugeBucketDataPoint;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.MetricType;
import org.hawkular.metrics.core.api.MetricsService;
import org.hawkular.metrics.core.api.Tenant;

import rx.Observable;
import rx.functions.Func1;

/**
 * @author snegrea
 *
 */
public class MetricsServiceHooks implements MetricsService {

    private List<MetricsService> preProcessingServices;
    private MetricsService principalService;
    private List<MetricsService> postProcessingServices;

    private Func1<Func1<MetricsService, Observable<?>>, Observable<?>> sinkFunc = (processingFunction) -> {
        if(preProcessingServices != null && !preProcessingServices.isEmpty()) {
            Observable.from(preProcessingServices).doOnNext(service -> {
                processingFunction.call(service);
            });
        }

        Observable<?> principalServiceObservable = processingFunction.call(principalService);
        principalServiceObservable.doOnCompleted(() -> {
            if (postProcessingServices != null && !postProcessingServices.isEmpty()) {
                Observable.from(postProcessingServices).doOnNext(service -> {
                    processingFunction.call(service);
                });
            }
        });

        return principalServiceObservable;
    };

    @SuppressWarnings("unchecked")
    @Override
    public Observable<Void> createTenant(Tenant tenant) {
        return (Observable<Void>) sinkFunc.call(service -> {
            return service.createTenant(tenant);
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public Observable<Tenant> getTenants() {
        return (Observable<Tenant>) sinkFunc.call(service -> {
            return service.getTenants();
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public Observable<Void> createMetric(Metric<?> metric) {
        return (Observable<Void>) sinkFunc.call(service -> {
            return service.createMetric(metric);
        });
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public Observable<Metric> findMetric(String tenantId, MetricType type, MetricId id) {
        return (Observable<Metric>) sinkFunc.call(service -> {
            return service.findMetric(tenantId, type, id);
        });
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public Observable<Metric> findMetrics(String tenantId, MetricType type) {
        return (Observable<Metric>) sinkFunc.call(service -> {
            return service.findMetrics(tenantId, type);
        });
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public Observable<Metric> findMetricsWithTags(String tenantId, Map<String, String> tags, MetricType type) {
        return (Observable<Metric>) sinkFunc.call(service -> {
            return service.findMetricsWithTags(tenantId, tags, type);
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public Observable<Optional<Map<String, String>>> getMetricTags(String tenantId, MetricType type, MetricId id) {
        return (Observable<Optional<Map<String, String>>>) sinkFunc.call(service -> {
            return service.getMetricTags(tenantId, type, id);
        });
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public Observable<Void> addTags(Metric metric, Map<String, String> tags) {
        return (Observable<Void>) sinkFunc.call(service -> {
            return service.addTags(metric, tags);
        });
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public Observable<Void> deleteTags(Metric metric, Map<String, String> tags) {
        return (Observable<Void>) sinkFunc.call(service -> {
            return service.deleteTags(metric, tags);
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public Observable<Void> addGaugeData(Observable<Metric<Double>> gaugeObservable) {
        return (Observable<Void>) sinkFunc.call(service -> {
            return service.addGaugeData(gaugeObservable);
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public Observable<DataPoint<Double>> findGaugeData(String tenantId, MetricId id, Long start, Long end) {
        return (Observable<DataPoint<Double>>) sinkFunc.call(service -> {
            return service.findGaugeData(tenantId, id, start, end);
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Observable<T> findGaugeData(String tenantId, MetricId id, Long start, Long end,
            Func1<Observable<DataPoint<Double>>, Observable<T>>... funcs) {
        return (Observable<T>) sinkFunc.call(service -> {
            return service.findGaugeData(tenantId, id, start, end, funcs);
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public Observable<BucketedOutput<GaugeBucketDataPoint>> findGaugeStats(Metric<Double> metric, long start, long end,
            Buckets buckets) {
        return (Observable<BucketedOutput<GaugeBucketDataPoint>>) sinkFunc.call(service -> {
            return service.findGaugeStats(metric, start, end, buckets);
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public Observable<Void> addAvailabilityData(Observable<Metric<AvailabilityType>> availabilities) {
        return (Observable<Void>) sinkFunc.call(service -> {
            return service.addAvailabilityData(availabilities);
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public Observable<DataPoint<AvailabilityType>> findAvailabilityData(String tenantId, MetricId id, long start,
            long end) {
        return (Observable<DataPoint<AvailabilityType>>) sinkFunc.call(service -> {
            return service.findAvailabilityData(tenantId, id, start, end);
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public Observable<DataPoint<AvailabilityType>> findAvailabilityData(String tenantId, MetricId id, long start,
            long end, boolean distinct) {
        return (Observable<DataPoint<AvailabilityType>>) sinkFunc.call(service -> {
            return service.findAvailabilityData(tenantId, id, start, end, distinct);
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public Observable<BucketedOutput<AvailabilityBucketDataPoint>> findAvailabilityStats(
            Metric<AvailabilityType> metric, long start, long end, Buckets buckets) {
        return (Observable<BucketedOutput<AvailabilityBucketDataPoint>>) sinkFunc.call(service -> {
            return service.findAvailabilityStats(metric, start, end, buckets);
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public Observable<Boolean> idExists(String id) {
        return (Observable<Boolean>) sinkFunc.call(service -> {
            return service.idExists(id);
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public Observable<Void> tagGaugeData(Metric<Double> metric, Map<String, String> tags, long start, long end) {
        return (Observable<Void>) sinkFunc.call(service -> {
            return service.tagGaugeData(metric, tags, start, end);
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public Observable<Void> tagAvailabilityData(Metric<AvailabilityType> metric, Map<String, String> tags, long start,
            long end) {
        return (Observable<Void>) sinkFunc.call(service -> {
            return service.tagAvailabilityData(metric, tags, start, end);
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public Observable<Void> tagGaugeData(Metric<Double> metric, Map<String, String> tags, long timestamp) {
        return (Observable<Void>) sinkFunc.call(service -> {
            return service.tagGaugeData(metric, tags, timestamp);
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public Observable<Void> tagAvailabilityData(Metric<AvailabilityType> metric, Map<String, String> tags,
            long timestamp) {
        return (Observable<Void>) sinkFunc.call(service -> {
            return service.tagAvailabilityData(metric, tags, timestamp);
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public Observable<Map<MetricId, Set<DataPoint<Double>>>> findGaugeDataByTags(String tenantId,
            Map<String, String> tags) {
        return (Observable<Map<MetricId, Set<DataPoint<Double>>>>) sinkFunc.call(service -> {
            return service.findGaugeDataByTags(tenantId, tags);
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public Observable<Map<MetricId, Set<DataPoint<AvailabilityType>>>> findAvailabilityByTags(String tenantId,
            Map<String, String> tags) {
        return (Observable<Map<MetricId, Set<DataPoint<AvailabilityType>>>>) sinkFunc.call(service -> {
            return service.findAvailabilityByTags(tenantId, tags);
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public Observable<Void> addCounterData(Observable<Metric<Long>> counters) {
        return (Observable<Void>) sinkFunc.call(service -> {
            return service.addCounterData(counters);
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public Observable<DataPoint<Long>> findCounterData(String tenantId, MetricId id, long start, long end) {
        return (Observable<DataPoint<Long>>) sinkFunc.call(service -> {
            return service.findCounterData(tenantId, id, start, end);
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public Observable<DataPoint<Double>> findRateData(String tenantId, MetricId id, long start, long end) {
        return (Observable<DataPoint<Double>>) sinkFunc.call(service -> {
            return service.findRateData(tenantId, id, start, end);
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public Observable<List<long[]>> getPeriods(String tenantId, MetricId id, Predicate<Double> predicate, long start,
            long end) {
        return (Observable<List<long[]>>) sinkFunc.call(service -> {
            return service.findRateData(tenantId, id, start, end);
        });
    }

}
