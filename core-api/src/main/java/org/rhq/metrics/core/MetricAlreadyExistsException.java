package org.rhq.metrics.core;

/**
 * @author John Sanda
 */
public class MetricAlreadyExistsException extends RuntimeException {

    private Metric metric;

    public MetricAlreadyExistsException(Metric metric) {
        this.metric = metric;
    }

    public Metric getMetric() {
        return metric;
    }
}
