package org.rhq.metrics.core;

import java.util.Set;

import com.google.common.base.Objects;

/**
 * <p>
 * A template for creating pre-computed aggregate metrics. Templates are used for tenant-level configuration. A template
 * applies to all metrics (of the specified type) for a tenant. For example, suppose we create a template for numeric
 * metrics to compute the max and min at an interval of 5 minutes. Then for every numeric metric aggregate metrics that
 * consist of the max and min will be computed every 5 minutes.
 * </p>
 * <p>
 * There are a couple caveats with using templates. First, the input must be a raw metric, be it numeric, availability,
 * or a log event. Secondly, the input source can only be a single metric.
 * </p>
 *
 * @author John Sanda
 */
public class AggregationTemplate {

    private MetricType type;

    private Interval interval;

    // TODO make functions strongly typed
    private Set<String> functions;

    /**
     * The {@link org.rhq.metrics.core.MetricType type} of metric to which the template applies.
     */
    public MetricType getType() {
        return type;
    }

    public AggregationTemplate setType(MetricType type) {
        this.type = type;
        return this;
    }

    /**
     * How frequently the aggregate metrics created from this template should be updated.
     */
    public Interval getInterval() {
        return interval;
    }

    public AggregationTemplate setInterval(Interval interval) {
        this.interval = interval;
        return this;
    }

    /**
     * The functions to apply on the source data.
     * <br/><br/>
     * <strong>Note:</strong> Once we have some of the functions support in place, this will most likely change to be
     * a strongly typed collection.
     */
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
