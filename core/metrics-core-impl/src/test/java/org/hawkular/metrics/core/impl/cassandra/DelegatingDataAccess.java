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
public class DelegatingDataAccess implements DataAccess {

    private final DataAccess delegate;

    public DelegatingDataAccess(DataAccess delegate) {
        this.delegate = delegate;
    }

    @Override
    public ResultSetFuture insertTenant(Tenant tenant) {
        return delegate.insertTenant(tenant);
    }

    @Override
    public ResultSetFuture findAllTenantIds() {
        return delegate.findAllTenantIds();
    }

    @Override
    public ResultSetFuture findTenant(String id) {
        return delegate.findTenant(id);
    }

    @Override
    public ResultSetFuture insertMetricInMetricsIndex(Metric metric) {
        return delegate.insertMetricInMetricsIndex(metric);
    }

    @Override
    public ResultSetFuture findMetric(String tenantId, MetricType type, MetricId id, long dpart) {
        return delegate.findMetric(tenantId, type, id, dpart);
    }

    @Override
    public ResultSetFuture addTagsAndDataRetention(Metric metric) {
        return delegate.addTagsAndDataRetention(metric);
    }

    @Override
    public ResultSetFuture addTags(Metric<?> metric, Map<String, String> tags) {
        return delegate.addTags(metric, tags);
    }

    @Override
    public ResultSetFuture deleteTags(Metric<?> metric, Set<String> tags) {
        return delegate.deleteTags(metric, tags);
    }

    @Override
    public ResultSetFuture updateTagsInMetricsIndex(Metric<?> metric, Map<String, String> additions,
        Set<String> deletions) {
        return delegate.updateTagsInMetricsIndex(metric, additions, deletions);
    }

    @Override
    public <T extends Metric<?>> ResultSetFuture updateMetricsIndex(List<T> metrics) {
        return delegate.updateMetricsIndex(metrics);
    }

    @Override
    public ResultSetFuture findMetricsInMetricsIndex(String tenantId, MetricType type) {
        return delegate.findMetricsInMetricsIndex(tenantId, type);
    }

    @Override
    public ResultSetFuture insertData(NumericMetric metric, int ttl) {
        return delegate.insertData(metric, ttl);
    }

    @Override
    public ResultSetFuture findData(NumericMetric metric, long startTime, long endTime) {
        return delegate.findData(metric, startTime, endTime);
    }

    @Override
    public ResultSetFuture findData(NumericMetric metric, long startTime, long endTime, Order order) {
        return delegate.findData(metric, startTime, endTime, order);
    }

    @Override
    public ResultSetFuture findData(NumericMetric metric, long startTime, long endTime, boolean includeWriteTime) {
        return delegate.findData(metric, startTime, endTime, includeWriteTime);
    }

    @Override
    public ResultSetFuture findData(NumericMetric metric, long timestamp, boolean includeWriteTime) {
        return delegate.findData(metric, timestamp, includeWriteTime);
    }

    @Override
    public ResultSetFuture findData(AvailabilityMetric metric, long startTime, long endTime) {
        return delegate.findData(metric, startTime, endTime);
    }

    @Override
    public ResultSetFuture findData(AvailabilityMetric metric, long startTime, long endTime, boolean includeWriteTime) {
        return delegate.findData(metric, startTime, endTime, includeWriteTime);
    }

    @Override
    public ResultSetFuture findData(AvailabilityMetric metric, long timestamp) {
        return delegate.findData(metric, timestamp);
    }

    @Override
    public ResultSetFuture deleteNumericMetric(String tenantId, String metric, Interval interval, long dpart) {
        return delegate.deleteNumericMetric(tenantId, metric, interval, dpart);
    }

    @Override
    public ResultSetFuture findAllNumericMetrics() {
        return delegate.findAllNumericMetrics();
    }

    @Override
    public ResultSetFuture insertNumericTag(String tag, String tagValue, NumericMetric metric,
            List<NumericData> data) {
        return delegate.insertNumericTag(tag, tagValue, metric, data);
    }

    @Override
    public ResultSetFuture insertAvailabilityTag(String tag, String tagValue, AvailabilityMetric metric,
            List<Availability> data) {
        return delegate.insertAvailabilityTag(tag, tagValue, metric, data);
    }

    @Override
    public ResultSetFuture updateDataWithTag(Metric<?> metric, MetricData data, Map<String, String> tags) {
        return delegate.updateDataWithTag(metric, data, tags);
    }

    @Override
    public ResultSetFuture findNumericDataByTag(String tenantId, String tag, String tagValue) {
        return delegate.findNumericDataByTag(tenantId, tag, tagValue);
    }

    @Override
    public ResultSetFuture findAvailabilityByTag(String tenantId, String tag, String tagValue) {
        return delegate.findAvailabilityByTag(tenantId, tag, tagValue);
    }

    @Override
    public ResultSetFuture insertData(AvailabilityMetric metric, int ttl) {
        return delegate.insertData(metric, ttl);
    }

    @Override
    public ResultSetFuture findAvailabilityData(AvailabilityMetric metric, long startTime, long endTime) {
        return delegate.findAvailabilityData(metric, startTime, endTime);
    }

    @Override
    public ResultSetFuture updateCounter(Counter counter) {
        return delegate.updateCounter(counter);
    }

    @Override
    public ResultSetFuture updateCounters(Collection<Counter> counters) {
        return delegate.updateCounters(counters);
    }

    @Override
    public ResultSetFuture findDataRetentions(String tenantId, MetricType type) {
        return delegate.findDataRetentions(tenantId, type);
    }

    @Override
    public ResultSetFuture updateRetentionsIndex(String tenantId, MetricType type, Set<Retention> retentions) {
        return delegate.updateRetentionsIndex(tenantId, type, retentions);
    }

    @Override
    public ResultSetFuture updateRetentionsIndex(Metric metric) {
        return delegate.updateRetentionsIndex(metric);
    }

    @Override
    public ResultSetFuture insertIntoMetricsTagsIndex(Metric<?> metric, Map<String, String> tags) {
        return delegate.insertIntoMetricsTagsIndex(metric, tags);
    }

    @Override
    public ResultSetFuture deleteFromMetricsTagsIndex(Metric<?> metric, Map<String, String> tags) {
        return delegate.deleteFromMetricsTagsIndex(metric, tags);
    }

    @Override
    public ResultSetFuture findMetricsByTag(String tenantId, String tag) {
        return delegate.findMetricsByTag(tenantId, tag);
    }
}
