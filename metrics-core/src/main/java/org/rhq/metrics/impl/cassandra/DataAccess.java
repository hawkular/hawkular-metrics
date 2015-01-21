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
package org.rhq.metrics.impl.cassandra;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.datastax.driver.core.ResultSetFuture;

import org.rhq.metrics.core.Availability;
import org.rhq.metrics.core.AvailabilityMetric;
import org.rhq.metrics.core.Counter;
import org.rhq.metrics.core.Interval;
import org.rhq.metrics.core.Metric;
import org.rhq.metrics.core.MetricData;
import org.rhq.metrics.core.MetricId;
import org.rhq.metrics.core.MetricType;
import org.rhq.metrics.core.NumericData;
import org.rhq.metrics.core.NumericMetric;
import org.rhq.metrics.core.Retention;
import org.rhq.metrics.core.Tenant;

/**
 * @author John Sanda
 */
public interface DataAccess {
        ResultSetFuture insertTenant(Tenant tenant);

    ResultSetFuture findAllTenantIds();

    ResultSetFuture findTenant(String id);

    ResultSetFuture insertMetricInMetricsIndex(Metric metric);

    ResultSetFuture findMetric(String tenantId, MetricType type, MetricId id, long dpart);

    ResultSetFuture addMetadata(Metric metric);

    ResultSetFuture updateMetadata(Metric metric, Map<String, String> additions, Set<String> removals);

    ResultSetFuture updateMetadataInMetricsIndex(Metric metric, Map<String, String> additions,
        Set<String> deletions);

    <T extends Metric> ResultSetFuture updateMetricsIndex(List<T> metrics);

    ResultSetFuture findMetricsInMetricsIndex(String tenantId, MetricType type);

    ResultSetFuture insertData(NumericMetric metric, int ttl);

    ResultSetFuture findData(NumericMetric metric, long startTime, long endTime);

    ResultSetFuture findData(NumericMetric metric, long startTime, long endTime, boolean includeWriteTime);

    ResultSetFuture findData(NumericMetric metric, long timestamp, boolean includeWriteTime);

    ResultSetFuture findData(AvailabilityMetric metric, long startTime, long endTime);

    ResultSetFuture findData(AvailabilityMetric metric, long startTime, long endTime, boolean includeWriteTime);

    ResultSetFuture findData(AvailabilityMetric metric, long timestamp);

    ResultSetFuture deleteNumericMetric(String tenantId, String metric, Interval interval, long dpart);

    ResultSetFuture findAllNumericMetrics();

    ResultSetFuture insertNumericTag(String tag, List<NumericData> data);

    ResultSetFuture insertAvailabilityTag(String tag, List<Availability> data);

    ResultSetFuture updateDataWithTag(MetricData data, Set<String> tags);

    ResultSetFuture findNumericDataByTag(String tenantId, String tag);

    ResultSetFuture findAvailabilityByTag(String tenantId, String tag);

    ResultSetFuture insertData(AvailabilityMetric metric, int ttl);

    ResultSetFuture findAvailabilityData(AvailabilityMetric metric, long startTime, long endTime);

    ResultSetFuture updateCounter(Counter counter);

    ResultSetFuture updateCounters(Collection<Counter> counters);

    ResultSetFuture findDataRetentions(String tenantId, MetricType type);

    ResultSetFuture updateRetentionsIndex(String tenantId, MetricType type, Set<Retention> retentions);

    ResultSetFuture updateRetentionsIndex(Metric metric);
}
