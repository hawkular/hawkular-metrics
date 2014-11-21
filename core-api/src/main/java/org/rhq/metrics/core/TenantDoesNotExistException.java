package org.rhq.metrics.core;

import java.util.List;

/**
 * @author John Sanda
 */
public class TenantDoesNotExistException extends RuntimeException {

    private List<? extends Metric> invalidMetrics;

    public TenantDoesNotExistException(List<? extends Metric> invalidMetrics) {
        this.invalidMetrics = invalidMetrics;
    }

    public TenantDoesNotExistException(String msg, List<? extends Metric> invalidMetrics) {
        super(msg);
        this.invalidMetrics = invalidMetrics;
    }

    public List<? extends Metric> getInvalidMetrics() {
        return invalidMetrics;
    }
}
