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
package org.hawkular.metrics.core.api;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.ListenableFuture;

import rx.Observable;

/**
 * Interface that defines the functionality of the Metrics Service.
 * @author Heiko W. Rupp
 */
public interface MetricsService {

    public enum State {
        STARTING, STARTED, STOPPING, STOPPED, FAILED
    }


    // For now we will use a default or fake tenant id until we get APIs in place for
    // creating tenants.
    String DEFAULT_TENANT_ID = "test";

    /**
     * Startup with a given cassandra session
     * @param session
     */
    void startUp(Session session);

    State getState();

    void setState(State state);

    void shutdown();

    /**
     * <p>
     * This method should be call before ever inserting any data to ensure that the tenant id is unique and to establish
     * any global configuration for data retention and for pre-computed aggregates. An exception is thrown if a tenant
     * with the same id already exists.
     * </p>
     * <p>
     * All data is associated with a {@link org.hawkular.metrics.core.api.Tenant tenant} via the tenant id; however,
     * the foreign key like relationship is not enforced. Data can be inserted with a non-existent tenant id. More
     * importantly, data could be inserted with a tenant id that already exists.
     * </p>
     *
     * @param tenant The {@link Tenant tenant} to create
     * @return
     * @throws org.hawkular.metrics.core.api.TenantAlreadyExistsException
     */
    Observable<Void> createTenant(Tenant tenant);

    Observable<Tenant> getTenants();

    Observable<Void> createMetric(Metric<?> metric);

    Observable<? extends Metric<? extends MetricData>> findMetric(String tenantId, MetricType type,
                                                                  MetricId id);

    Observable<Metric<?>> findMetrics(String tenantId, MetricType type);

    Observable<Optional<Map<String, String>>> getMetricTags(String tenantId, MetricType type, MetricId id);

    Observable<Void> addTags(Metric metric, Map<String, String> tags);

    Observable<Void> deleteTags(Metric metric, Map<String, String> tags);

    Observable<Void> addGaugeData(Observable<Gauge> gaugeObservable);

    Observable<GaugeData> findGaugeData(String tenantId, MetricId id, Long start, Long end);

    Observable<BucketedOutput<GaugeBucketDataPoint>> findGaugeStats(
            Gauge metric, long start, long end, Buckets buckets
    );

    Observable<Void> addAvailabilityData(List<Availability> metrics);

    Observable<AvailabilityData> findAvailabilityData(String tenantId, MetricId id, long start, long end);

    Observable<AvailabilityData> findAvailabilityData(String tenantId, MetricId id, long start, long end,
                                                      boolean distinct);

    Observable<BucketedOutput<AvailabilityBucketDataPoint>> findAvailabilityStats(
            Availability metric, long start, long end, Buckets buckets
    );

    ListenableFuture<Void> updateCounter(Counter counter);

    ListenableFuture<Void> updateCounters(Collection<Counter> counters);

    ListenableFuture<List<Counter>> findCounters(String group);

    ListenableFuture<List<Counter>> findCounters(String group, List<String> counterNames);

    /** Check if a metric with the passed {id} has been stored in the system */
    Observable<Boolean> idExists(String id);

    Observable<Void> tagGaugeData(Gauge metric, Map<String, String> tags,
                                  long start, long end);

    Observable<Void> tagAvailabilityData(Availability metric, Map<String, String> tags,
                                         long start, long end);

    Observable<Void> tagGaugeData(Gauge metric, Map<String, String> tags,
                                  long timestamp);

    Observable<Void> tagAvailabilityData(Availability metric, Map<String, String> tags,
                                         long timestamp);

    Observable<Map<MetricId, Set<GaugeData>>> findGaugeDataByTags(String tenantId, Map<String, String> tags);

    Observable<Map<MetricId, Set<AvailabilityData>>> findAvailabilityByTags(String tenantId,
                                                                            Map<String, String> tags);

    /**
     * <p>
     * For a specified date range, return a list of periods in which the predicate evaluates to true for each
     * consecutive data point. The periods are returned in ascending order. Consider the following data points,
     * </p>
     * <p>
     * {time: 1, value: 5}, {time: 2, value: 11}, {time: 3, value: 12}, {time: 4, value: 8}, {time: 5, value: 14},
     * {time: 6, value: 7}, {time: 7, value: 16}
     * </p>
     *<p>
     * And a predicate that tests for values greater than 10. The results would be,
     *</p>
     * <p>
     * {start: 2, end: 3}, {start: 5, end: 5}, {start: 7, end: 7}
     * </p>
     *
     * @param tenantId
     * @param id
     * @param predicate A function applied to the value of each data point
     * @param start The start time inclusive
     * @param end The end time exclusive
     * @return Each element in the list is a two element array. The first element is the start time inclusive for which
     * the predicate matches, and the second element is the end time inclusive for which the predicate matches.
     */
    Observable<List<long[]>> getPeriods(String tenantId, MetricId id, Predicate<Double> predicate, long start,
                                        long end);
}
