package org.rhq.metrics.core;

import java.util.Set;

import com.google.common.base.Objects;

/**
 * @author John Sanda
 */
public class AggregationTemplate {

    private MetricType type;

    // TODO make the interval strongly typed
    // An interval consists of two parts - A numeric part that represents a duration and
    // units, e.g., minute, hour, day.
    private String interval;

    // TODO make functions strongly typed
    private Set<String> functions;

    public MetricType getType() {
        return type;
    }

    public AggregationTemplate setType(MetricType type) {
        this.type = type;
        return this;
    }

    public String getInterval() {
        return interval;
    }

    public AggregationTemplate setInterval(String interval) {
        this.interval = interval;
        return this;
    }

    public Set<String> getFunctions() {
        return functions;
    }

    public AggregationTemplate setFunctions(Set<String> functions) {
        this.functions = functions;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AggregationTemplate that = (AggregationTemplate) o;

        if (functions != null ? !functions.equals(that.functions) : that.functions != null) return false;
        if (interval != null ? !interval.equals(that.interval) : that.interval != null) return false;
        if (type != null ? !type.equals(that.type) : that.type != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = type != null ? type.hashCode() : 0;
        result = 31 * result + (interval != null ? interval.hashCode() : 0);
        result = 31 * result + (functions != null ? functions.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
            .add("type", type)
            .add("interval", interval)
            .add("functions", functions)
            .toString();
    }
}
