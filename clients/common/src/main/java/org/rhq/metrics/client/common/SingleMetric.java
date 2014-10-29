package org.rhq.metrics.client.common;

/**
 * One single metric
 *
 * @author Heiko W. Rupp
 */
public class SingleMetric {

    private final String source;
    private final long timestamp;
    private final Double value;
    private final MetricType metricType;

    public SingleMetric(String source, long timestamp, Double value) {
        this.timestamp = timestamp;
        this.source = source;
        this.value = value;
        metricType = null;
    }

    public SingleMetric(String source, long timestamp, Double value, MetricType metricType) {
        this.source = source;
        this.timestamp = timestamp;
        this.value = value;
        this.metricType = metricType;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getSource() {
        return source;
    }

    public Double getValue() {
        return value;
    }

    public MetricType getMetricType() {
        return metricType;
    }

    @Override
    public String toString() {
        return "SingleMetric{" +
            "type=" + metricType +
            ", time=" + timestamp +
            ", src='" + source + '\'' +
            ", val=" + value +
            '}';
    }

    public String toJson() {
        return "{\"id\":\"" + source + "\"," + "\"timestamp\":" + String.valueOf(timestamp) + "," + "\"value\":"
            + String.valueOf(value) + "}";
    }
}
