package org.rhq.metrics.core;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Interface that defines the functionality of the Metrics Service.
 * @author Heiko W. Rupp
 */
public interface MetricsService {

    /** called to start the service up if needed
     * @param params from e.g. servlet context */
    void startUp(Map<String, String> params);

    /**
     * Startup with a given cassandra session
     * @param session
     */
    void startUp(Session session);

    void shutdown();

    ListenableFuture<Void> addData(RawNumericMetric data);

    ListenableFuture<Map<RawNumericMetric, Throwable>> addData(Set<RawNumericMetric> data);

    ListenableFuture<List<RawNumericMetric>> findData(String bucket, String id, long start, long end);

    ListenableFuture<Void> updateCounter(Counter counter);

    ListenableFuture<Void> updateCounters(Collection<Counter> counters);

    ListenableFuture<List<Counter>> findCounters(String group);

    ListenableFuture<List<Counter>> findCounters(String group, List<String> counterNames);

    /** Find and return raw metrics for {id} that have a timestamp between {start} and {end} */
    ListenableFuture<List<RawNumericMetric>> findData(String id, long start, long end);

    /** Check if a metric with the passed {id} has been stored in the system */
    ListenableFuture<Boolean> idExists(String id);

    /** Return a list of all metric names */
    ListenableFuture<List<String>> listMetrics();

    /** Delete the metric with the passed id */
    ListenableFuture<Boolean> deleteMetric(String id);

}
