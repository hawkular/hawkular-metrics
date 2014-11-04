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
        if (source==null) {
            throw new IllegalArgumentException("Source must not be null");
        }
        this.timestamp = timestamp;
        this.source = source;
        this.value = value;
        metricType = null;
    }

    public SingleMetric(String source, long timestamp, Double value, MetricType metricType) {
        if (source==null) {
            throw new IllegalArgumentException("Source must not be null");
        }
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SingleMetric metric = (SingleMetric) o;

        if (timestamp != metric.timestamp) return false;
        if (metricType != metric.metricType) return false;
        if (!source.equals(metric.source)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = source.hashCode();
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        result = 31 * result + (metricType != null ? metricType.hashCode() : 0);
        return result;
    }
}
