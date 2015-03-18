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

import com.datastax.driver.core.ResultSetFuture;

import org.hawkular.metrics.core.api.Availability;
import org.hawkular.metrics.core.api.AvailabilityMetric;
import org.hawkular.metrics.core.api.Counter;
import org.hawkular.metrics.core.api.Interval;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricData;
import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.MetricType;
import org.hawkular.metrics.core.api.NumericData;
import org.hawkular.metrics.core.api.NumericMetric;
import org.hawkular.metrics.core.api.Retention;
import org.hawkular.metrics.core.api.Tenant;

/**
 * @author John Sanda
 */
public interface DataAccess {
    ResultSetFuture insertTenant(Tenant tenant);

    ResultSetFuture findAllTenantIds();

    ResultSetFuture findTenant(String id);

    ResultSetFuture insertMetricInMetricsIndex(Metric<?> metric);

    ResultSetFuture findMetric(String tenantId, MetricType type, MetricId id, long dpart);

    ResultSetFuture addTagsAndDataRetention(Metric<?> metric);

    ResultSetFuture addTags(Metric<?> metric, Map<String, String> tags);

    ResultSetFuture deleteTags(Metric<?> metric, Set<String> tags);

    ResultSetFuture updateTagsInMetricsIndex(Metric<?> metric, Map<String, String> additions,
        Set<String> deletions);

    <T extends Metric<?>> ResultSetFuture updateMetricsIndex(List<T> metrics);

    ResultSetFuture findMetricsInMetricsIndex(String tenantId, MetricType type);

    ResultSetFuture insertData(NumericMetric metric, int ttl);

    ResultSetFuture findData(NumericMetric metric, long startTime, long endTime);

    ResultSetFuture findData(NumericMetric metric, long startTime, long endTime, Order order);

    ResultSetFuture findData(NumericMetric metric, long startTime, long endTime, boolean includeWriteTime);

    ResultSetFuture findData(NumericMetric metric, long timestamp, boolean includeWriteTime);

    ResultSetFuture findData(AvailabilityMetric metric, long startTime, long endTime);

    ResultSetFuture findData(AvailabilityMetric metric, long startTime, long endTime, boolean includeWriteTime);

    ResultSetFuture findData(AvailabilityMetric metric, long timestamp);

    ResultSetFuture deleteNumericMetric(String tenantId, String metric, Interval interval, long dpart);

    ResultSetFuture findAllNumericMetrics();

    ResultSetFuture insertNumericTag(String tag, String tagValue, NumericMetric metric, List<NumericData> data);

    ResultSetFuture insertAvailabilityTag(String tag, String tagValue, AvailabilityMetric metric,
            List<Availability> data);

    ResultSetFuture updateDataWithTag(Metric<?> metric, MetricData data, Map<String, String> tags);

    ResultSetFuture findNumericDataByTag(String tenantId, String tag, String tagValue);

    ResultSetFuture findAvailabilityByTag(String tenantId, String tag, String tagValue);

    ResultSetFuture insertData(AvailabilityMetric metric, int ttl);

    ResultSetFuture findAvailabilityData(AvailabilityMetric metric, long startTime, long endTime);

    ResultSetFuture updateCounter(Counter counter);

    ResultSetFuture updateCounters(Collection<Counter> counters);

    ResultSetFuture findDataRetentions(String tenantId, MetricType type);

    ResultSetFuture updateRetentionsIndex(String tenantId, MetricType type, Set<Retention> retentions);

    ResultSetFuture updateRetentionsIndex(Metric<?> metric);

    ResultSetFuture insertIntoMetricsTagsIndex(Metric<?> metric, Map<String, String> tags);

    ResultSetFuture deleteFromMetricsTagsIndex(Metric<?> metric, Map<String, String> tags);

    ResultSetFuture findMetricsByTag(String tenantId, String tag);
}
