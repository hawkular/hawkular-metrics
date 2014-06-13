package org.rhq.metrics.clients.ptrans;

/**
 * Type of the metric
 * @author Heiko W. Rupp
 */
public enum MetricType {
    SIMPLE,
    TIMING,
    COUNTER,
    GAUGE;

    public static MetricType from(String type) {
        switch (type) {
        case "c":
            return COUNTER;
        case "g":
            return GAUGE;
        case "ms":
            return TIMING;
        default:
            return SIMPLE;
        }
    }
}
