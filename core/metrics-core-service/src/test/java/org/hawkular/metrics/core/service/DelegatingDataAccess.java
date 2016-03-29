/*
 * Copyright 2014-2016 Red Hat, Inc. and/or its affiliates
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

package org.hawkular.metrics.core.service;

import java.util.Map;
import java.util.Set;

import org.hawkular.metrics.model.AvailabilityType;
import org.hawkular.metrics.model.Interval;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.MetricType;
import org.hawkular.metrics.model.Tenant;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;

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

    @Override
    public Observable<Row> findAllTenantIds() {
        return delegate.findAllTenantIds();
    }

    @Override
    public Observable<Row> findTenant(String id) {
        return delegate.findTenant(id);
    }

    @Override
    public <T> ResultSetFuture insertMetricInMetricsIndex(Metric<T> metric) {
        return delegate.insertMetricInMetricsIndex(metric);
    }

    @Override
    public <T> Observable<Row> findMetric(MetricId<T> id) {
        return delegate.findMetric(id);
    }

    @Override
    public <T> Observable<Row> getMetricTags(MetricId<T> id) {
        return delegate.getMetricTags(id);
    }

    @Override
    public <T> Observable<ResultSet> addDataRetention(Metric<T> metric) {
        return delegate.addDataRetention(metric);
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
    public <T> Observable<Row> findMetricsInMetricsIndex(String tenantId, MetricType<T> type) {
        return delegate.findMetricsInMetricsIndex(tenantId, type);
    }

    @Override
    public Observable<Integer> insertGaugeData(Metric<Double> gauge, int ttl) {
        return delegate.insertGaugeData(gauge, ttl);
    }

    @Override
    public Observable<Integer> insertCounterData(Metric<Long> counter, int ttl) {
        return delegate.insertCounterData(counter, ttl);
    }

    @Override
    public Observable<Row> findCounterData(MetricId<Long> id, long startTime, long endTime, int limit,
            Order order) {
        return delegate.findCounterData(id, startTime, endTime, limit, order);
    }

    @Override
    public Observable<Row> findGaugeData(MetricId<Double> id, long startTime, long endTime, int limit, Order order) {
        return delegate.findGaugeData(id, startTime, endTime, 0, order);
    }

    @Override
    public Observable<Row> findAvailabilityData(MetricId<AvailabilityType> id, long startTime, long endTime,
            int limit, Order order) {
        return delegate.findAvailabilityData(id, startTime, endTime, limit, order);
    }

    @Override
    public Observable<ResultSet> deleteGaugeMetric(String tenantId, String metric, Interval interval, long dpart) {
        return delegate.deleteGaugeMetric(tenantId, metric, interval, dpart);
    }

    @Override
    public Observable<Integer> insertAvailabilityData(Metric<AvailabilityType> metric, int ttl) {
        return delegate.insertAvailabilityData(metric, ttl);
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
    public Observable<Row> findMetricsByTagName(String tenantId, String tag) {
        return delegate.findMetricsByTagName(tenantId, tag);
    }

    @Override
    public Observable<Row> findMetricsByTagNameValue(String tenantId, String tag, String tvalue) {
        return delegate.findMetricsByTagNameValue(tenantId, tag, tvalue);
    }

    @Override
    public Observable<Row> findAvailabilityData(MetricId<AvailabilityType> id, long timestamp) {
        return delegate.findAvailabilityData(id, timestamp);
    }
}
