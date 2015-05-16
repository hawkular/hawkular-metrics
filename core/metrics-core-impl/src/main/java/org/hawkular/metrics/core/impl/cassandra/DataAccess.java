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
package org.hawkular.metrics.core.impl.cassandra;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import org.hawkular.metrics.core.api.Availability;
import org.hawkular.metrics.core.api.AvailabilityData;
import org.hawkular.metrics.core.api.Counter;
import org.hawkular.metrics.core.api.Gauge;
import org.hawkular.metrics.core.api.GaugeData;
import org.hawkular.metrics.core.api.Interval;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricData;
import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.MetricType;
import org.hawkular.metrics.core.api.Retention;
import org.hawkular.metrics.core.api.Tenant;
import rx.Observable;

/**
 * @author John Sanda
 */
public interface DataAccess {
    ResultSetFuture insertTenant(Tenant tenant);

    ResultSetFuture findAllTenantIds();

    ResultSetFuture findTenant(String id);

    ResultSetFuture insertMetricInMetricsIndex(Metric<?> metric);

    Observable<ResultSet> findMetric(String tenantId, MetricType type, MetricId id, long dpart);

    ResultSetFuture addTagsAndDataRetention(Metric<?> metric);

    ResultSetFuture getMetricTags(String tenantId, MetricType type, MetricId id, long dpart);

    ResultSetFuture addTags(Metric<?> metric, Map<String, String> tags);

    ResultSetFuture deleteTags(Metric<?> metric, Set<String> tags);

    ResultSetFuture updateTagsInMetricsIndex(Metric<?> metric, Map<String, String> additions,
        Set<String> deletions);

    <T extends Metric<?>> ResultSetFuture updateMetricsIndex(List<T> metrics);

    Observable<ResultSet> findMetricsInMetricsIndex(String tenantId, MetricType type);

    ResultSetFuture insertData(Gauge metric, int ttl);

    Observable<ResultSet> findData(String tenantId, MetricId id, long startTime, long endTime);

    ResultSetFuture findData(Gauge metric, long startTime, long endTime, Order order);

    Observable<ResultSet> findData(String tenantId, MetricId id, long startTime, long endTime,
            boolean includeWriteTime);

    ResultSetFuture findData(Gauge metric, long timestamp, boolean includeWriteTime);

    ResultSetFuture findData(Availability metric, long startTime, long endTime);

    ResultSetFuture findData(Availability metric, long startTime, long endTime, boolean includeWriteTime);

    ResultSetFuture findData(Availability metric, long timestamp);

    ResultSetFuture deleteGuageMetric(String tenantId, String metric, Interval interval, long dpart);

    ResultSetFuture findAllGuageMetrics();

    ResultSetFuture insertGuageTag(String tag, String tagValue, Gauge metric, List<GaugeData> data);

    ResultSetFuture insertAvailabilityTag(String tag, String tagValue, Availability metric,
            List<AvailabilityData> data);

    ResultSetFuture updateDataWithTag(Metric<?> metric, MetricData data, Map<String, String> tags);

    ResultSetFuture findGuageDataByTag(String tenantId, String tag, String tagValue);

    ResultSetFuture findAvailabilityByTag(String tenantId, String tag, String tagValue);

    ResultSetFuture insertData(Availability metric, int ttl);

    ResultSetFuture findAvailabilityData(String tenantId, MetricId id, long startTime, long endTime);

    ResultSetFuture updateCounter(Counter counter);

    ResultSetFuture updateCounters(Collection<Counter> counters);

    ResultSetFuture findDataRetentions(String tenantId, MetricType type);

    ResultSetFuture updateRetentionsIndex(String tenantId, MetricType type, Set<Retention> retentions);

    ResultSetFuture updateRetentionsIndex(Metric<?> metric);

    ResultSetFuture insertIntoMetricsTagsIndex(Metric<?> metric, Map<String, String> tags);

    ResultSetFuture deleteFromMetricsTagsIndex(Metric<?> metric, Map<String, String> tags);

    ResultSetFuture findMetricsByTag(String tenantId, String tag);
}
