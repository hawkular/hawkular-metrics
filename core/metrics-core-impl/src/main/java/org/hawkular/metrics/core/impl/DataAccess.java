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
package org.hawkular.metrics.core.impl;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import org.hawkular.metrics.core.api.AvailabilityDataPoint;
import org.hawkular.metrics.core.api.Counter;
import org.hawkular.metrics.core.api.DataPoint;
import org.hawkular.metrics.core.api.GaugeDataPoint;
import org.hawkular.metrics.core.api.Interval;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.MetricType;
import org.hawkular.metrics.core.api.Retention;
import org.hawkular.metrics.core.api.Tenant;
import rx.Observable;

/**
 * @author John Sanda
 */
public interface DataAccess {
    Observable<ResultSet> insertTenant(Tenant tenant);

    Observable<ResultSet> findAllTenantIds();

    Observable<ResultSet> findTenant(String id);

    ResultSetFuture insertMetricInMetricsIndex(Metric metric);

    Observable<ResultSet> findMetric(String tenantId, MetricType type, MetricId id, long dpart);

    Observable<ResultSet> addTagsAndDataRetention(Metric metric);

    Observable<ResultSet> getMetricTags(String tenantId, MetricType type, MetricId id, long dpart);

    Observable<ResultSet> addTags(Metric metric, Map<String, String> tags);

    Observable<ResultSet> deleteTags(Metric metric, Set<String> tags);

    Observable<ResultSet> updateTagsInMetricsIndex(Metric metric, Map<String, String> additions, Set<String> deletions);

    <T extends DataPoint> ResultSetFuture updateMetricsIndex(List<? extends Metric<T>> metrics);

    Observable<ResultSet> updateMetricsIndexRx(Observable<? extends Metric> metrics);

    Observable<ResultSet> findMetricsInMetricsIndex(String tenantId, MetricType type);

    Observable<ResultSet> insertData(Observable<GaugeAndTTL> gaugeObservable);

    Observable<ResultSet> findData(String tenantId, MetricId id, long startTime, long endTime);

    Observable<ResultSet> findData(Metric<GaugeDataPoint> metric, long startTime, long endTime, Order order);

    Observable<ResultSet> findData(String tenantId, MetricId id, long startTime, long endTime,
            boolean includeWriteTime);

    Observable<ResultSet> findData(Metric<GaugeDataPoint> metric, long timestamp, boolean includeWriteTime);

    Observable<ResultSet> findAvailabilityData(Metric<AvailabilityDataPoint> metric, long startTime, long endTime);

    Observable<ResultSet> findAvailabilityData(Metric<AvailabilityDataPoint> metric, long startTime, long endTime,
            boolean includeWriteTime);

    Observable<ResultSet> findAvailabilityData(Metric<AvailabilityDataPoint> metric, long timestamp);

    Observable<ResultSet> deleteGaugeMetric(String tenantId, String metric, Interval interval, long dpart);

    Observable<ResultSet> findAllGaugeMetrics();

    Observable<ResultSet> insertGaugeTag(String tag, String tagValue, Metric<GaugeDataPoint> metric,
            Observable<TTLDataPoint<GaugeDataPoint>> data);

    Observable<ResultSet> insertAvailabilityTag(String tag, String tagValue, Metric<AvailabilityDataPoint> metric,
            Observable<TTLDataPoint<AvailabilityDataPoint>> data);

    Observable<ResultSet> updateDataWithTag(Metric metric, DataPoint dataPoint, Map<String, String> tags);

    Observable<ResultSet> findGaugeDataByTag(String tenantId, String tag, String tagValue);

    Observable<ResultSet> findAvailabilityByTag(String tenantId, String tag, String tagValue);

    Observable<ResultSet> insertAvailabilityData(Metric<AvailabilityDataPoint> metric, int ttl);

    Observable<ResultSet> findAvailabilityData(String tenantId, MetricId id, long startTime, long endTime);

    ResultSetFuture updateCounter(Counter counter);

    ResultSetFuture updateCounters(Collection<Counter> counters);

    ResultSetFuture findDataRetentions(String tenantId, MetricType type);

    ResultSetFuture updateRetentionsIndex(String tenantId, MetricType type, Set<Retention> retentions);

    ResultSetFuture updateRetentionsIndex(Metric metric);

    Observable<ResultSet> insertIntoMetricsTagsIndex(Metric metric, Map<String, String> tags);

    Observable<ResultSet> deleteFromMetricsTagsIndex(Metric metric, Map<String, String> tags);

    Observable<ResultSet> findMetricsByTag(String tenantId, String tag);
}
