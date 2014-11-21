package org.rhq.metrics.core;

import com.google.common.base.Objects;

/**
 * @author John Sanda
 */
public class MetricId {

    private String name;

    private Interval interval;

    public MetricId(String name) {
        this(name, Interval.NONE);
    }

    public MetricId(String name, Interval interval) {
        this.name = name;
        this.interval = interval;
    }

    public String getName() {
        return name;
    }

    public Interval getInterval() {
        return interval;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MetricId metricId = (MetricId) o;

        if (!interval.equals(metricId.interval)) return false;
        if (!name.equals(metricId.name)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + interval.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("name", name).add("interval", interval).toString();
    }
}
