package org.rhq.metrics.core;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.datastax.driver.core.utils.UUIDs;
import com.google.common.base.Objects;

/**
 * @author John Sanda
 */
public class NumericData {

    private String tenantId;

    private String metric;

    private Interval interval;

    private long dpart;

    private UUID timeUUID;

    private Map<String, String> attributes = new HashMap<>();

    private double value;

    public String getTenantId() {
        return tenantId;
    }

    public NumericData setTenantId(String tenantId) {
        this.tenantId = tenantId;
        return this;
    }

    public String getMetric() {
        return metric;
    }

    public NumericData setMetric(String metric) {
        this.metric = metric;
        return this;
    }

    public Interval getInterval() {
        return interval;
    }

    public NumericData setInterval(Interval interval) {
        this.interval = interval;
        return this;
    }

    public long getDpart() {
        return dpart;
    }

    public NumericData setDpart(long dpart) {
        this.dpart = dpart;
        return this;
    }

    public UUID getTimeUUID() {
        return timeUUID;
    }

    public NumericData setTimeUUID(UUID timeUUID) {
        this.timeUUID = timeUUID;
        return this;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public NumericData putAttribute(String name, String value) {
        attributes.put(name, value);
        return this;
    }

    public NumericData putAttributes(Map<String, String> attributes) {
        this.attributes.putAll(attributes);
        return this;
    }

    public double getValue() {
        return value;
    }

    public NumericData setValue(double value) {
        this.value = value;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NumericData that = (NumericData) o;

        if (dpart != that.dpart) return false;
        if (Double.compare(that.value, value) != 0) return false;
        if (!attributes.equals(that.attributes)) return false;
        if (interval != null ? !interval.equals(that.interval) : that.interval != null) return false;
        if (!metric.equals(that.metric)) return false;
        if (!tenantId.equals(that.tenantId)) return false;
        if (!timeUUID.equals(that.timeUUID)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = tenantId.hashCode();
        result = 31 * result + metric.hashCode();
        result = 31 * result + (interval != null ? interval.hashCode() : 0);
        result = 31 * result + (int) (dpart ^ (dpart >>> 32));
        result = 31 * result + timeUUID.hashCode();
        result = 31 * result + attributes.hashCode();
        temp = Double.doubleToLongBits(value);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
            .add("tenantId", tenantId)
            .add("metric", metric)
            .add("interval", interval)
            .add("dpart", dpart)
            .add("attributes", attributes)
            .add("timeUUID", timeUUID)
            .add("timestamp", UUIDs.unixTimestamp(timeUUID))
            .add("value", value)
            .toString();
    }
}
