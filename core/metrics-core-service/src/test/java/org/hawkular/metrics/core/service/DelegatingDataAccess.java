/*
 * Copyright 2014-2017 Red Hat, Inc. and/or its affiliates
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
import java.util.function.Function;

import org.hawkular.metrics.core.service.compress.CompressedPointContainer;
import org.hawkular.metrics.model.AvailabilityType;
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
    public Observable<ResultSet> insertTenant(Tenant tenant, boolean overwrite) {
        return delegate.insertTenant(tenant, overwrite);
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
    public <T> ResultSetFuture insertMetricInMetricsIndex(Metric<T> metric, boolean overwrite) {
        return delegate.insertMetricInMetricsIndex(metric, overwrite);
    }

    @Override
    public <T> Observable<Row> findMetricInData(MetricId<T> id) {
        return delegate.findMetricInData(id);
    }

    @Override
    public <T> Observable<Row> findMetricInMetricsIndex(MetricId<T> id) {
        return delegate.findMetricInMetricsIndex(id);
    }

    @Override
    public <T> Observable<Row> getMetricTags(MetricId<T> id) {
        return delegate.getMetricTags(id);
    }

    @Override public Observable<Row> getTagNames() {
        return delegate.getTagNames();
    }

    @Override
    public Observable<Row> getTagNamesWithType() {
        return delegate.getTagNamesWithType();
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
    public Observable<Row> findAllMetricsInData() {
        return delegate.findAllMetricsInData();
    }

    @Override
    public Observable<Integer> insertGaugeDatas(Observable<Metric<Double>> gauges,
            Function<MetricId<Double>, Integer> ttlFunction) {
        return delegate.insertGaugeDatas(gauges, ttlFunction);
    }

    @Override
    public Observable<Integer> insertGaugeData(Metric<Double> metric) {
        return delegate.insertGaugeData(metric);
    }

    @Override
    public Observable<Integer> insertGaugeData(Metric<Double> gauge, int ttl) {
        return delegate.insertGaugeData(gauge, ttl);
    }

    @Override
    public Observable<Integer> insertStringDatas(Observable<Metric<String>> strings,
            Function<MetricId<String>, Integer> ttlFetcher, int maxSize) {
        return delegate.insertStringDatas(strings, ttlFetcher, maxSize);
    }

    @Override
    public Observable<Integer> insertCounterData(Metric<Long> counter, int ttl) {
        return delegate.insertCounterData(counter, ttl);
    }

    @Override
    public Observable<Row> findCounterData(MetricId<Long> id, long startTime, long endTime, int limit,
            Order order, int pageSize) {
        return delegate.findCounterData(id, startTime, endTime, limit, order, pageSize);
    }

    @Override
    public Observable<Row> findCompressedData(MetricId<?> id, long startTime, long endTime, int limit, Order order) {
        return delegate.findCompressedData(id, startTime, endTime, limit, order);
    }

    @Override
    public Observable<Row> findGaugeData(MetricId<Double> id, long startTime, long endTime, int limit, Order order,
            int pageSize) {
        return delegate.findGaugeData(id, startTime, endTime, limit, order, pageSize);
    }

    @Override
    public Observable<Integer> insertStringData(Metric<String> metric, int maxSize) {
        return delegate.insertStringData(metric, maxSize);
    }

    @Override
    public Observable<Integer> insertCounterData(Metric<Long> counter) {
        return null;
    }

    @Override
    public Observable<Integer> insertStringData(Metric<String> metric, int ttl, int maxSize) {
        return delegate.insertStringData(metric, ttl, maxSize);
    }

    @Override
    public Observable<Integer> insertCounterDatas(Observable<Metric<Long>> counters,
            Function<MetricId<Long>, Integer> ttlFetcher) {
        return delegate.insertCounterDatas(counters, ttlFetcher);
    }

    @Override
    public Observable<Row> findStringData(MetricId<String> id, long startTime, long endTime, int limit, Order order,
            int pageSize) {
        return delegate.findStringData(id, startTime, endTime, limit, order, pageSize);
    }

    @Override
    public Observable<Row> findAvailabilityData(MetricId<AvailabilityType> id, long startTime, long endTime,
            int limit, Order order, int pageSize) {
        return delegate.findAvailabilityData(id, startTime, endTime, limit, order, pageSize);
    }

    @Override
    public <T> Observable<ResultSet> deleteMetricData(MetricId<T> id) {
        return delegate.deleteMetricData(id);
    }

    @Override
    public <T> Observable<ResultSet> deleteMetricFromRetentionIndex(MetricId<T> id) {
        return delegate.deleteMetricFromRetentionIndex(id);
    }

    @Override
    public <T> Observable<ResultSet> deleteMetricFromMetricsIndex(MetricId<T> id) {
        return delegate.deleteMetricFromMetricsIndex(id);
    }

    @Override
    public Observable<Integer> insertAvailabilityDatas(Observable<Metric<AvailabilityType>> avail,
            Function<MetricId<AvailabilityType>, Integer> ttlFetcher) {
        return delegate.insertAvailabilityDatas(avail, ttlFetcher);
    }

    @Override
    public Observable<Integer> insertAvailabilityData(Metric<AvailabilityType> metric) {
        return delegate.insertAvailabilityData(metric);
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
    public <T> Observable<ResultSet> deleteFromMetricsTagsIndex(MetricId<T> id, Map<String, String> tags) {
        return delegate.deleteFromMetricsTagsIndex(id, tags);
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

    @Override
    public <T> Observable<ResultSet> deleteAndInsertCompressedGauge(MetricId<T> id, long timeslice,
                                                                    CompressedPointContainer cpc,
                                                              long sliceStart, long sliceEnd, int ttl) {
        return delegate.deleteAndInsertCompressedGauge(id, timeslice, cpc, sliceStart, sliceEnd, ttl);
    }

    @Override
    public Observable<Row> findAllMetricsFromTagsIndex() {
        return delegate.findAllMetricsFromTagsIndex();
    }

    @Override
    public <T> Observable<ResultSet> updateMetricExpirationIndex(MetricId<T> id, long expirationTime) {
        return delegate.updateMetricExpirationIndex(id, expirationTime);
    }

    @Override
    public <T> Observable<ResultSet> deleteFromMetricExpirationIndex(MetricId<T> id) {
        return delegate.deleteFromMetricExpirationIndex(id);
    }

    @Override
    public <T> Observable<Row> findMetricExpiration(MetricId<T> id) {
        return delegate.findMetricExpiration(id);
    }
}
