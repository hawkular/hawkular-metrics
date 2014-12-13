package org.rhq.metrics.core;

import com.google.common.base.Objects;

/**
 * @author John Sanda
 */
public class Retention {

    private MetricId id;

    private int value;

    public Retention(MetricId id, int value) {
        this.id = id;
        this.value = value;
    }

    public MetricId getId() {
        return id;
    }

    public int getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Retention retention = (Retention) o;

        if (value != retention.value) return false;
        if (!id.equals(retention.id)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + value;
        return result;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("id", id).add("value", value).toString();
    }
}
