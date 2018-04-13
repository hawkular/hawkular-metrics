/*
 * Copyright 2014-2018 Red Hat, Inc. and/or its affiliates
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
    public Observable<ResultSet> createTempTablesIfNotExists(Set<Long> timestamps) {
        return delegate.createTempTablesIfNotExists(timestamps);
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

    @Override public Observable<Observable<Row>> findAllDataFromBucket(long timestamp, int pageSize, int maxConcurrency) {
        return delegate.findAllDataFromBucket(timestamp, pageSize, maxConcurrency);
    }

    @Override public Observable<ResultSet> dropTempTable(long timestamp) {
        return delegate.dropTempTable(timestamp);
    }

//    @Override public Completable resetTempTable(long timestamp) {
//        return delegate.resetTempTable(timestamp);
//    }

    @Override
    public Observable<Row> findAllMetricIdentifiersInData() {
        return delegate.findAllMetricIdentifiersInData();
    }

    @Override
    public <T> Observable<Integer> insertData(Observable<Metric<T>> metrics) {
        return delegate.insertData(metrics);
    }

    @Override
    public Observable<Integer> insertStringDatas(Observable<Metric<String>> strings,
            Function<MetricId<String>, Integer> ttlFetcher, int maxSize) {
        return delegate.insertStringDatas(strings, ttlFetcher, maxSize);
    }

    @Override
    public Observable<Row> findCompressedData(MetricId<?> id, long startTime, long endTime, int limit, Order order) {
        return delegate.findCompressedData(id, startTime, endTime, limit, order);
    }

    @Override
    public <T> Observable<Row> findTempData(MetricId<T> id, long startTime, long endTime, int limit, Order order,
                                            int pageSize) {
        return delegate.findTempData(id, startTime, endTime, limit, order, pageSize);
    }

//    @Override
//    public <T> Observable<Row> findOldData(MetricId<T> id, long startTime, long endTime, int limit, Order order,
//                                           int pageSize) {
//        return delegate.findOldData(id, startTime, endTime, limit, order, pageSize);
//    }

    @Override
    public Observable<Integer> insertStringData(Metric<String> metric, int maxSize) {
        return delegate.insertStringData(metric, maxSize);
    }
    @Override
    public Observable<Integer> insertStringData(Metric<String> metric, int ttl, int maxSize) {
        return delegate.insertStringData(metric, ttl, maxSize);
    }

    @Override
    public Observable<Row> findStringData(MetricId<String> id, long startTime, long endTime, int limit, Order order,
            int pageSize) {
        return delegate.findStringData(id, startTime, endTime, limit, order, pageSize);
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
    public Observable<Row> findMetricsByTagNameValue(String tenantId, String tag, String ... tvalues) {
        return delegate.findMetricsByTagNameValue(tenantId, tag, tvalues);
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
    public <T> Observable<ResultSet> insertCompressedData(MetricId<T> id, long timeslice, CompressedPointContainer cpc,
                                                          int ttl) {
        return delegate.insertCompressedData(id, timeslice, cpc, ttl);
    }

    @Override public void shutdown() {
        delegate.shutdown();
    }
}
