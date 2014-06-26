package org.rhq.metrics.impl.memory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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

import org.rhq.metrics.core.Counter;
import org.rhq.metrics.core.MetricsService;
import org.rhq.metrics.core.MetricsThreadFactory;
import org.rhq.metrics.core.RawNumericMetric;

import gnu.trove.map.TLongDoubleMap;
import gnu.trove.map.hash.TLongDoubleHashMap;

/**
 * A memory based storage backend for rapid prototyping
 * @author Heiko W. Rupp
 */
public class MemoryMetricsService implements MetricsService {

    private static final ListenableFuture<Void> VOID_FUTURE = Futures.immediateFuture(null);

    private Map<String,TLongDoubleMap> storage = new HashMap<>();

    Table<String, String, Long> counters = TreeBasedTable.create();
//    private ListMultimap<String, Counter> counters = ArrayListMultimap.create();

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
    public ListenableFuture<Void> addData(RawNumericMetric data) {
        addMetric(data);
        return VOID_FUTURE;
    }

    @Override
    public ListenableFuture<Map<RawNumericMetric, Throwable>> addData(Set<RawNumericMetric> data) {

        for (RawNumericMetric metric : data) {
            addMetric(metric);

            // TODO expire an old entry
        }
        Map<RawNumericMetric, Throwable> errors = Collections.emptyMap();
        return Futures.immediateFuture(errors);
    }

    private void addMetric(RawNumericMetric metric) {
        TLongDoubleMap map;
        String metricId = metric.getId();
        if (storage.containsKey(metricId)) {
            map = storage.get(metricId);
        } else {
            map = new TLongDoubleHashMap();
            storage.put(metricId,map);
        }
        map.put(metric.getTimestamp(), metric.getAvg()); // TODO getAvg() may be wrong in future
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
            counters.add(new Counter(group, entry.getKey(), entry.getValue()));
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
                counters.add(new Counter(group, name, value));
            }
        }
        ListenableFuture<List<Counter>> listListenableFuture = Futures.immediateFuture(counters);
        return Futures.transform(listListenableFuture,new NoOpCounterMapper(),metricsTasks);
    }

    @Override
    public ListenableFuture<List<RawNumericMetric>> findData(String bucket, String id, long start, long end) {
        return findData(id, start, end);
    }

    @Override
    public ListenableFuture<List<RawNumericMetric>> findData(String id, long start, long end) {
        List<RawNumericMetric> metrics = new ArrayList<>();

        if (storage.containsKey(id)) {
            TLongDoubleMap map = storage.get(id);
            for (long ts : map.keys()) {
                if (ts>=start && ts<=end) {
                    RawNumericMetric metric = new RawNumericMetric(id,map.get(ts),ts);
                    metrics.add(metric);
                }

            }
        }
        ListenableFuture<List<RawNumericMetric>> listListenableFuture = Futures.immediateFuture(metrics);

        return Futures.transform(listListenableFuture,new NoOpDataMapper(),metricsTasks);
    }

    @Override
    public ListenableFuture<Boolean> idExists(String id) {
        Boolean containsKey = storage.containsKey(id);
        return Futures.immediateFuture(containsKey);
    }

    @Override
    public ListenableFuture<List<String>> listMetrics() {
        List<String> metrics = new ArrayList<>(storage.keySet().size());
        metrics.addAll(storage.keySet());

        ListenableFuture<List<String>> future = Futures.immediateFuture(metrics);
        return Futures.transform(future, new NoOpStringListMapper(), metricsTasks);
    }

    @Override
    public ListenableFuture<Boolean> deleteMetric(String id) {
        storage.remove(id);
        return Futures.immediateFuture(true);
    }

    private class NoOpDataMapper implements Function<List<RawNumericMetric>, List<RawNumericMetric>> {
        @Override
        public List<RawNumericMetric> apply(List<RawNumericMetric> input) {
            return input;
        }
    }

    private class NoOpCounterMapper implements Function<List<Counter>, List<Counter>> {
        @Override
        public List<Counter> apply(List<Counter> input) {
            return input;
        }
    }

    private class NoOpStringListMapper implements Function<List<String >, List<String>> {
        @Override
        public List<String> apply(List<String> input) {
            return input;
        }
    }
}
