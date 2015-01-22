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
public class DelegatingDataAccess implements DataAccess {

    private DataAccess delegate;

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
    public ResultSetFuture addMetadata(Metric metric) {
        return delegate.addMetadata(metric);
    }

    @Override
    public ResultSetFuture updateMetadata(Metric metric, Map<String, String> additions, Set<String> removals) {
        return delegate.updateMetadata(metric, additions, removals);
    }

    @Override
    public ResultSetFuture updateMetadataInMetricsIndex(Metric metric, Map<String, String> additions,
        Set<String> deletions) {
        return delegate.updateMetadataInMetricsIndex(metric, additions, deletions);
    }

    @Override
    public <T extends Metric> ResultSetFuture updateMetricsIndex(List<T> metrics) {
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
    public ResultSetFuture insertNumericTag(String tag, List<NumericData> data) {
        return delegate.insertNumericTag(tag, data);
    }

    @Override
    public ResultSetFuture insertAvailabilityTag(String tag, List<Availability> data) {
        return delegate.insertAvailabilityTag(tag, data);
    }

    @Override
    public ResultSetFuture updateDataWithTag(MetricData data, Set<String> tags) {
        return delegate.updateDataWithTag(data, tags);
    }

    @Override
    public ResultSetFuture findNumericDataByTag(String tenantId, String tag) {
        return delegate.findNumericDataByTag(tenantId, tag);
    }

    @Override
    public ResultSetFuture findAvailabilityByTag(String tenantId, String tag) {
        return delegate.findAvailabilityByTag(tenantId, tag);
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
}
