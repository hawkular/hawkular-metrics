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

import javax.jms.JMSException;
import javax.naming.NamingException;

import org.hawkular.bus.common.ConnectionContextFactory;
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
 * @author Stefan Negrea
 *
 */
public class MetricsServiceBusDelivery implements MetricsService {

    private final BusMessageSender busMessageSender;

    public MetricsServiceBusDelivery() throws NamingException, JMSException {
        busMessageSender = new BusMessageSender();
    }

    public MetricsServiceBusDelivery(ConnectionContextFactory ccf) {
        busMessageSender = new BusMessageSender(ccf);
    }

    @Override
    public Observable<Void> createTenant(Tenant tenant) {
        return null;
    }

    @Override
    public Observable<Tenant> getTenants() {
        return null;
    }

    @Override
    public Observable<Void> createMetric(Metric<?> metric) {
        return Observable.create(subscriber -> {
            switch (metric.getType()) {
            case AVAILABILITY:
                busMessageSender.sendMessage(metric, BusMessageSender.AVAILABILITY_METRICS_TOPIC);
                break;
            case COUNTER:
                busMessageSender.sendMessage(metric, BusMessageSender.COUNTER_METRICS_TOPIC);
                break;
            case GAUGE:
                busMessageSender.sendMessage(metric, BusMessageSender.GAUGE_METRICS_TOPIC);
                break;
            case COUNTER_RATE:
            default:
                break;
            }

            subscriber.onCompleted();
        });
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Observable<Metric> findMetric(String tenantId, MetricType type, MetricId id) {
        return null;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Observable<Metric> findMetrics(String tenantId, MetricType type) {
        return null;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Observable<Metric> findMetricsWithTags(String tenantId, Map<String, String> tags, MetricType type) {
        return null;
    }

    @Override
    public Observable<Optional<Map<String, String>>> getMetricTags(String tenantId, MetricType type, MetricId id) {
        return null;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Observable<Void> addTags(Metric metric, Map<String, String> tags) {
        return null;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Observable<Void> deleteTags(Metric metric, Map<String, String> tags) {
        return null;
    }

    @Override
    public Observable<Void> addGaugeData(Observable<Metric<Double>> gaugeObservable) {
        return Observable.create(subscriber -> {
            gaugeObservable.doOnNext(gauge -> {
                busMessageSender.sendMessage(gauge, BusMessageSender.GAUGE_METRICS_DATA_TOPIC);
            });

            subscriber.onCompleted();
        });
    }

    @Override
    public Observable<DataPoint<Double>> findGaugeData(String tenantId, MetricId id, Long start, Long end) {
        return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Observable<T> findGaugeData(String tenantId, MetricId id, Long start, Long end,
            Func1<Observable<DataPoint<Double>>, Observable<T>>... funcs) {
        return null;
    }

    @Override
    public Observable<BucketedOutput<GaugeBucketDataPoint>> findGaugeStats(Metric<Double> metric, long start, long end,
            Buckets buckets) {
        return null;
    }

    @Override
    public Observable<Void> addAvailabilityData(Observable<Metric<AvailabilityType>> availabilities) {
        return Observable.create(subscriber -> {
            availabilities.doOnNext(availability -> {
                busMessageSender.sendMessage(availability, BusMessageSender.AVAILABILITY_METRICS_DATA_TOPIC);
            });

            subscriber.onCompleted();
        });
    }

    @Override
    public Observable<DataPoint<AvailabilityType>> findAvailabilityData(String tenantId, MetricId id, long start,
            long end) {
        return null;
    }

    @Override
    public Observable<DataPoint<AvailabilityType>> findAvailabilityData(String tenantId, MetricId id, long start,
            long end, boolean distinct) {
        return null;
    }

    @Override
    public Observable<BucketedOutput<AvailabilityBucketDataPoint>> findAvailabilityStats(
            Metric<AvailabilityType> metric, long start, long end, Buckets buckets) {
        return null;
    }

    @Override
    public Observable<Boolean> idExists(String id) {
        return null;
    }

    @Override
    public Observable<Void> tagGaugeData(Metric<Double> metric, Map<String, String> tags, long start, long end) {
        return null;
    }

    @Override
    public Observable<Void> tagAvailabilityData(Metric<AvailabilityType> metric, Map<String, String> tags, long start,
            long end) {
        return null;
    }

    @Override
    public Observable<Void> tagGaugeData(Metric<Double> metric, Map<String, String> tags, long timestamp) {
        return null;
    }

    @Override
    public Observable<Void> tagAvailabilityData(Metric<AvailabilityType> metric, Map<String, String> tags,
            long timestamp) {
        return null;
    }

    @Override
    public Observable<Map<MetricId, Set<DataPoint<Double>>>> findGaugeDataByTags(String tenantId,
            Map<String, String> tags) {
        return null;
    }

    @Override
    public Observable<Map<MetricId, Set<DataPoint<AvailabilityType>>>> findAvailabilityByTags(String tenantId,
            Map<String, String> tags) {
        return null;
    }

    @Override
    public Observable<Void> addCounterData(Observable<Metric<Long>> counters) {
        return Observable.create(subscriber -> {
            counters.doOnNext(counter -> {
                busMessageSender.sendMessage(counter, BusMessageSender.COUNTER_METRICS_DATA_TOPIC);
            });

            subscriber.onCompleted();
        });
    }

    @Override
    public Observable<DataPoint<Long>> findCounterData(String tenantId, MetricId id, long start, long end) {
        return null;
    }

    @Override
    public Observable<DataPoint<Double>> findRateData(String tenantId, MetricId id, long start, long end) {
        return null;
    }

    @Override
    public Observable<List<long[]>> getPeriods(String tenantId, MetricId id, Predicate<Double> predicate, long start,
            long end) {
        return null;
    }

}
