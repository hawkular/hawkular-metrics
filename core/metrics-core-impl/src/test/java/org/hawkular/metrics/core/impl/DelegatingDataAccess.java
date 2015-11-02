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

import java.util.Map;
import java.util.Set;

import org.hawkular.metrics.core.api.AvailabilityType;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.MetricType;
import org.hawkular.metrics.core.api.Tenant;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;

import rx.Observable;

/**
 * @author John Sanda
 */
public class DelegatingDataAccess implements DataAccess {

    private final DataAccess delegate;

    public DelegatingDataAccess(DataAccess delegate) {
        this.delegate = delegate;
    }

    @Override
    public Observable<ResultSet> insertTenant(String tenantId) {
        return delegate.insertTenant(tenantId);
    }

    @Override
    public Observable<ResultSet> insertTenant(Tenant tenant) {
        return delegate.insertTenant(tenant);
    }

    @Override public Observable<ResultSet> insertTenantId(long time, String id) {
        return delegate.insertTenantId(time, id);
    }

    @Override
    public Observable<ResultSet> findAllTenantIds() {
        return delegate.findAllTenantIds();
    }

    @Override
    public Observable<ResultSet> findTenant(String id) {
        return delegate.findTenant(id);
    }

    @Override public Observable<ResultSet> findTenantIds(long time) {
        return delegate.findTenantIds(time);
    }

    @Override public Observable<ResultSet> deleteTenantsBucket(long time) {
        return delegate.deleteTenantsBucket(time);
    }

    @Override
    public <T> Observable<ResultSet> insertMetricInMetricsIndex(Metric<T> metric) {
        return delegate.insertMetricInMetricsIndex(metric);
    }

    @Override
    public <T> Observable<ResultSet> findMetric(MetricId<T> id) {
        return delegate.findMetric(id);
    }

    @Override
    public <T> Observable<ResultSet> getMetricTags(MetricId<T> id) {
        return delegate.getMetricTags(id);
    }

    @Override
    public <T> Observable<ResultSet> addTags(Metric<T> metric, Map<String, String> tags) {
        return delegate.addTags(metric, tags);
    }

    @Override
    public <T> Observable<ResultSet> deleteTags(Metric<T> metric, Set<String> tags) {
        return delegate.deleteTags(metric, tags);
    }

    @Override
    public <T> Observable<Integer> updateMetricsIndex(Observable<Metric<T>> metrics) {
        return delegate.updateMetricsIndex(metrics);
    }

    @Override
    public <T> Observable<ResultSet> findMetricsInMetricsIndex(String tenantId, MetricType<T> type) {
        return delegate.findMetricsInMetricsIndex(tenantId, type);
    }

    @Override
    public Observable<Integer> insertGaugeData(Metric<Double> gauge) {
        return delegate.insertGaugeData(gauge);
    }

    @Override
    public Observable<Integer> insertCounterData(Metric<Long> counter) {
        return delegate.insertCounterData(counter);
    }

    @Override
    public Observable<ResultSet> findCounterData(MetricId<Long> id, long startTime, long endTime) {
        return delegate.findCounterData(id, startTime, endTime);
    }

    @Override
    public Observable<ResultSet> findGaugeData(MetricId<Double> id, long startTime, long endTime) {
        return delegate.findGaugeData(id, startTime, endTime);
    }

    @Override
    public Observable<ResultSet> findGaugeData(Metric<Double> metric, long startTime, long endTime, Order
            order) {
        return delegate.findGaugeData(metric, startTime, endTime, order);
    }

    @Override
    public Observable<ResultSet> findGaugeData(MetricId<Double> id, long startTime, long endTime,
            boolean includeWriteTime) {
        return delegate.findGaugeData(id, startTime, endTime, includeWriteTime);
    }

    @Override
    public Observable<ResultSet> findGaugeData(Metric<Double> metric, long timestamp, boolean includeWriteTime) {
        return delegate.findGaugeData(metric, timestamp, includeWriteTime);
    }

    @Override
    public Observable<ResultSet> findAvailabilityData(Metric<AvailabilityType> metric, long startTime, long
            endTime) {
        return delegate.findAvailabilityData(metric, startTime, endTime);
    }

    @Override
    public Observable<ResultSet> findAvailabilityData(Metric<AvailabilityType> metric, long startTime, long
            endTime, boolean
            includeWriteTime) {
        return delegate.findAvailabilityData(metric, startTime, endTime, includeWriteTime);
    }

    @Override
    public Observable<ResultSet> findAvailabilityData(Metric<AvailabilityType> metric, long timestamp) {
        return delegate.findAvailabilityData(metric, timestamp);
    }

//    @Override
//    public Observable<ResultSet> deleteGaugeMetric(String tenantId, String metric, Interval interval, long dpart) {
//        return delegate.deleteGaugeMetric(tenantId, metric, interval, dpart);
//    }

    @Override
    public Observable<ResultSet> findAllGaugeMetrics() {
        return delegate.findAllGaugeMetrics();
    }

    @Override
    public Observable<Integer> insertAvailabilityData(Metric<AvailabilityType> metric) {
        return delegate.insertAvailabilityData(metric);
    }

    @Override
    public Observable<ResultSet> findAvailabilityData(MetricId<AvailabilityType> id, long startTime, long endTime) {
        return delegate.findAvailabilityData(id, startTime, endTime);
    }

    @Override
    public <T> ResultSetFuture findDataRetentions(String tenantId, MetricType<T> type) {
        return delegate.findDataRetentions(tenantId, type);
    }

    @Override
    public <T> Observable<ResultSet> updateRetentionsIndex(String tenantId, MetricType<T> type,
                                                       Map<String, Integer> retentions) {
        return delegate.updateRetentionsIndex(tenantId, type, retentions);
    }

    @Override
    public <T> ResultSetFuture updateRetentionsIndex(Metric<T> metric) {
        return delegate.updateRetentionsIndex(metric);
    }

    @Override
    public <T> Observable<ResultSet> insertIntoMetricsTagsIndex(Metric<T> metric, Map<String, String> tags) {
        return delegate.insertIntoMetricsTagsIndex(metric, tags);
    }

    @Override
    public <T> Observable<ResultSet> deleteFromMetricsTagsIndex(Metric<T> metric, Map<String, String> tags) {
        return delegate.deleteFromMetricsTagsIndex(metric, tags);
    }

    @Override
    public Observable<ResultSet> findMetricsByTagName(String tenantId, String tag) {
        return delegate.findMetricsByTagName(tenantId, tag);
    }

    @Override
    public Observable<ResultSet> findMetricsByTagNameValue(String tenantId, String tag, String tvalue) {
        return delegate.findMetricsByTagNameValue(tenantId, tag, tvalue);
    }
}
