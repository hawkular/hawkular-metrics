package org.rhq.metrics.client.common;

/**
 * One single metric
 *
 * @author Heiko W. Rupp
 */
public class SingleMetric {

    private static final String JSON_TEMPLATE = "{\"id\":\"%1\"," +
            "\"timestamp\":%2,"+
            "\"value\":%3}";

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
        String payload = JSON_TEMPLATE.replaceAll("%1",source)
            .replaceAll("%2", String.valueOf(timestamp))
            .replaceAll("%3", String.valueOf(value));
        return payload;
    }
}
