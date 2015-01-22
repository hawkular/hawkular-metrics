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
package org.rhq.metrics.impl.memory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;

import com.datastax.driver.core.Session;
import com.google.common.base.Function;
import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import org.rhq.metrics.core.Availability;
import org.rhq.metrics.core.AvailabilityMetric;
import org.rhq.metrics.core.Counter;
import org.rhq.metrics.core.Metric;
import org.rhq.metrics.core.MetricId;
import org.rhq.metrics.core.MetricType;
import org.rhq.metrics.core.MetricsService;
import org.rhq.metrics.core.MetricsThreadFactory;
import org.rhq.metrics.core.NumericData;
import org.rhq.metrics.core.NumericMetric;
import org.rhq.metrics.core.Tenant;

import gnu.trove.map.TLongDoubleMap;
import gnu.trove.map.hash.TLongDoubleHashMap;

/**
 * A memory based storage backend for rapid prototyping.
 * <br><br>
 * Note that this implementation currently supports only numeric raw data. There is no multi-tenancy support yet, nor
 * is there any concurrency control yet.
 *
 * @author Heiko W. Rupp
 */
public class MemoryMetricsService implements MetricsService {

    private static final ListenableFuture<Void> VOID_FUTURE = Futures.immediateFuture(null);

    private Map<String,TLongDoubleMap> storage = new HashMap<>();

    Table<String, String, Long> counters = TreeBasedTable.create();

    private ListeningExecutorService metricsTasks = MoreExecutors
        .listeningDecorator(Executors.newFixedThreadPool(4, new MetricsThreadFactory()));

    @Override
    public void startUp(Session session) {
        throw new IllegalArgumentException("Not supported");
    }

    @Override
    public void startUp(Map<String, String> params) {

    }

    @Override
    public void shutdown() {
    }

    @Override
    public ListenableFuture<Void> createTenant(Tenant tenant) {
        return null;
    }

    @Override
    public ListenableFuture<List<Tenant>> getTenants() {
        return null;
    }

    @Override
    public ListenableFuture<Void> createMetric(Metric metric) {
        return null;
    }

    @Override
    public ListenableFuture<List<Metric>> findMetrics(String tenantId, MetricType type) {
        return null;
    }

    @Override
    public ListenableFuture<Void> updateMetadata(Metric metric, Map<String, String> metadata, Set<String> deletions) {
        return null;
    }

    @Override
    public ListenableFuture<Metric> findMetric(String tenantId, MetricType type, MetricId id) {
        return null;
    }

    @Override
    public ListenableFuture<Void> addNumericData(List<NumericMetric> metrics) {
        for (NumericMetric metric : metrics) {
            TLongDoubleMap map;
            if (storage.containsKey(metric.getId().getName())) {
                map = storage.get(metric.getId().getName());
            } else {
                map = new TLongDoubleHashMap();
            }
            for (NumericData d : metric.getData()) {
                map.put(d.getTimestamp(), d.getValue());
            }
            storage.put(metric.getId().getName(), map);
        }
        return Futures.immediateFuture(null);
    }

    @Override
    public ListenableFuture<Void> addAvailabilityData(List<AvailabilityMetric> metrics) {
        return null;
    }

    @Override
    public ListenableFuture<AvailabilityMetric> findAvailabilityData(AvailabilityMetric metric, long start, long end) {
        return null;
    }

    @Override
    public ListenableFuture<Void> updateCounter(Counter counter) {
        Long value = counters.get(counter.getGroup(), counter.getName());
        if (value == null)  {
            counters.put(counter.getGroup(), counter.getName(), counter.getValue());
        } else {
            counters.put(counter.getGroup(), counter.getName(), value + counter.getValue());
        }
        return VOID_FUTURE;
    }

    @Override
    public ListenableFuture<Void> updateCounters(Collection<Counter> counters) {
        for (Counter counter : counters) {
            updateCounter(counter);
        }
        return VOID_FUTURE;
    }

    @Override
    public ListenableFuture<List<Counter>> findCounters(String group) {
        Map<String, Long> row = counters.row(group);
        List<Counter> counters = new ArrayList<>(row.size());
        for (Map.Entry<String, Long> entry : row.entrySet()) {
            counters.add(new Counter(DEFAULT_TENANT_ID, group, entry.getKey(), entry.getValue()));
        }
        return Futures.immediateFuture(counters);
    }

    @Override
    public ListenableFuture<List<Counter>> findCounters(String group, List<String> counterNames) {
        Map<String, Long> row = counters.row(group);
        List<Counter> counters = new ArrayList<>(counterNames.size());
        for (String name : counterNames) {
            Long value = row.get(name);
            if (value != null) {
                counters.add(new Counter(DEFAULT_TENANT_ID, group, name, value));
            }
        }
        ListenableFuture<List<Counter>> listListenableFuture = Futures.immediateFuture(counters);
        return Futures.transform(listListenableFuture,new NoOpMapper<List<Counter>>(),metricsTasks);
    }

    @Override
    public ListenableFuture<List<NumericData>> findData(NumericMetric metric, long start, long end) {
        List<NumericData> data = new ArrayList<>();

        if (storage.containsKey(metric.getId().getName())) {
            TLongDoubleMap map = storage.get(metric.getId().getName());
            for (long ts : map.keys()) {
                if (ts>=start && ts<=end) {
                    data.add(new NumericData(metric, ts, map.get(ts)));
                }

            }
        }
        return Futures.immediateFuture(data);
    }

    @Override
    public ListenableFuture<NumericMetric> findNumericData(NumericMetric metric, long start, long end) {
        return null;
    }

    @Override
    public ListenableFuture<Boolean> idExists(String id) {
        Boolean containsKey = storage.containsKey(id);
        return Futures.immediateFuture(containsKey);
    }

    @Override
    public ListenableFuture<List<NumericData>> tagNumericData(NumericMetric metric, Set<String> tags, long start,
        long end) {
        return null;
    }

    @Override
    public ListenableFuture<List<Availability>> tagAvailabilityData(AvailabilityMetric metric, Set<String> tags,
        long start, long end) {
        return null;
    }

    @Override
    public ListenableFuture<List<NumericData>> tagNumericData(NumericMetric metric, Set<String> tags, long timestamp) {
        return null;
    }

    @Override
    public ListenableFuture<List<Availability>> tagAvailabilityData(AvailabilityMetric metric, Set<String> tags,
        long timestamp) {
        return null;
    }

    @Override
    public ListenableFuture<Map<MetricId, Set<NumericData>>> findNumericDataByTags(String tenantId, Set<String> tags) {
        return null;
    }

    @Override
    public ListenableFuture<Map<MetricId, Set<Availability>>> findAvailabilityByTags(String tenantId,
        Set<String> tags) {
        return null;
    }

    private static class NoOpMapper<T> implements Function<T,T> {

        public T apply(T input) {
            return input;
        }

    }
}
