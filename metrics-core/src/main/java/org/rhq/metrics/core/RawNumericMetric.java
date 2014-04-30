package org.rhq.metrics.core;

import com.google.common.base.Objects;

/**
 * @author John Sanda
 */
public class RawNumericMetric implements NumericMetric {

    private String bucket = "raw";

    private String id;

    private Double value;

    private long timestamp;

    public RawNumericMetric(String id, Double value, long timestamp) {
        this.id = id;
        this.value = value;
        this.timestamp = timestamp;
    }

    public String getBucket() {
        return bucket;
    }

    public String getId() {
        return id;
    }

    public Double getMin() {
        return value;
    }

    public Double getMax() {
        return value;
    }

    public Double getAvg() {
        return value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RawNumericMetric that = (RawNumericMetric) o;

        if (timestamp != that.timestamp) return false;
        if (!id.equals(that.id)) return false;
        if (!value.equals(that.value)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 47 * result + value.hashCode();
        result = 47 * result + (int) (timestamp ^ (timestamp >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(getClass().getSimpleName())
            .add("bucket", bucket)
            .add("id", id)
            .add("value", value)
            .add("timestamp", timestamp)
            .toString();
    }
}
