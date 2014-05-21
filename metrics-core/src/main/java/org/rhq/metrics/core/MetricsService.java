package org.rhq.metrics.core;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * Interface that defines the functionality of the Metrics Service.
 * @author Heiko W. Rupp
 */
public interface MetricsService {

    /** called to start the service up if needed
     * @param params from e.g. servlet context */
    void startUp(Map<String, String> params);

    void shutdown();

    ListenableFuture<Map<RawNumericMetric, Throwable>> addData(Set<RawNumericMetric> data);

    ListenableFuture<List<RawNumericMetric>> findData(String bucket, String id, long start, long end);

    /** Find and return raw metrics for {id} that have a timestamp between {start} and {end} */
    ListenableFuture<List<RawNumericMetric>> findData(String id, long start, long end);

    /** Check if a metric with the passed {id} has been stored in the system */
    // TODO Do we really need this method?
    // The C* based impl for this may very well require additional schema so I want us to
    // be careful about about introducing new query operations.
    //
    // jsanda
    public boolean idExists(String id);

    /** Return a list of all metric names */
    // TODO Do we really need this method?
    // The C* based impl for this may very well require additional schema so I want us to
    // be careful about about introducin new query operations.
    //
    // jsanda
    List<String> listMetrics();
}
