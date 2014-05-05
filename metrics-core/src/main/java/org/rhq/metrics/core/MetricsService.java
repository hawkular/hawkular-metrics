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

    List<NumericMetric> findData(String id, long start, long end);

    public boolean idExists(String id);
}
