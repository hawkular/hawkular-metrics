package org.rhq.metrics.core;

import java.util.List;
import java.util.Set;

import com.datastax.driver.core.ResultSetFuture;

/**
 * Interface that defines the functionality of the Metrics Service.
 * @author Heiko W. Rupp
 */
public interface MetricsService {
    void addData(Set<RawNumericMetric> data);

    ResultSetFuture findData(String bucket, String id, long start, long end);

    /** Find and return raw metrics for {id} that have a timestamp between {start} and {end} */
    List<NumericMetric> findData(String id, long start, long end);

    /** Check if a metric with the passed {id} has been stored in the system */
    public boolean idExists(String id);

    /** Return a list of all metric names */
    List<String> listMetrics();
}
