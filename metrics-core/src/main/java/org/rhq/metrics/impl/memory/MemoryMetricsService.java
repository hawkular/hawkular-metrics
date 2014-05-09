package org.rhq.metrics.impl.memory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import gnu.trove.map.TLongDoubleMap;
import gnu.trove.map.hash.TLongDoubleHashMap;

import com.datastax.driver.core.ResultSetFuture;

import org.rhq.metrics.core.MetricsService;
import org.rhq.metrics.core.NumericMetric;
import org.rhq.metrics.core.RawNumericMetric;

/**
 * A memory based storage backend for rapid prototyping
 * @author Heiko W. Rupp
 */
public class MemoryMetricsService implements MetricsService {

    private Map<String,TLongDoubleMap> storage = new HashMap<>();

    @Override
    public void addData(Set<RawNumericMetric> data) {

        TLongDoubleMap map ;
        for (RawNumericMetric metric : data) {
            String metricId = metric.getId();
            if (storage.containsKey(metricId)) {
                map = storage.get(metricId);
            } else {
                map = new TLongDoubleHashMap();
                storage.put(metricId,map);
            }
            map.put(metric.getTimestamp(), metric.getAvg()); // TODO getAvg() may be wrong in future

            // TODO expire an old entry
        }

    }

    @Override
    public ResultSetFuture findData(String bucket, String id, long start, long end) {
        // Bucket is always raw for this.
        // We don't implement this here.
        return null;
    }

    @Override
    public List<NumericMetric> findData(String id, long start, long end) {
        List<NumericMetric> metrics = new ArrayList<>();

        if (storage.containsKey(id)) {
            TLongDoubleMap map = storage.get(id);
            for (long ts : map.keys()) {
                if (ts>=start && ts<=end) {
                    NumericMetric metric = new RawNumericMetric(id,map.get(ts),ts);
                    metrics.add(metric);
                }

            }
        }
        return metrics;
    }

    @Override
    public boolean idExists(String id) {
        return storage.containsKey(id);
    }

    @Override
    public List<String> listMetrics() {
        List<String> metrics = new ArrayList<>(storage.keySet().size());
        metrics.addAll(storage.keySet());

        return metrics;
    }
}
