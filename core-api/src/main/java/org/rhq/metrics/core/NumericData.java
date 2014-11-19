package org.rhq.metrics.core;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.Objects;

import org.rhq.metrics.util.TimeUUIDUtils;

/**
 * A numeric metric data point. This class currently represents both raw and aggregated data; however, at some point
 * we may subclasses for each of them.
 *
 * @author John Sanda
 */
public class NumericData extends MetricData {

    private double value;

    private Set<AggregatedValue> aggregatedValues = new HashSet<>();

    public NumericData(NumericMetric2 metric, long timestamp, double value) {
        this(metric, TimeUUIDUtils.getTimeUUID(timestamp), value);
    }

    public NumericData(NumericMetric2 metric, UUID timeUUID, double value) {
        super(metric, timeUUID);
        this.metric = metric;
        this.value = value;
    }

    /**
     * Currently not used. It wil be used for breaking up a metric time series into multiple partitions by date. For
     * example, if we choose to partition by month, then all data collected for a given metric during October would go
     * into one partition, and data collected during November would go into another partition. This will not impact
     * writes, but it will make reads more complicated and potentially more expensive if we make the date partition too
     * small.
     */
//    public long getDpart() {
//        return dpart;
//    }
//
//    public NumericData setDpart(long dpart) {
//        this.dpart = dpart;
//        return this;
//    }

    public Metric getMetric() {
        return metric;
    }

    //    /**
//     * A set of key/value pairs that are shared by all data points for the metric. A good example is units like KB / sec.
//     */
//    public Map<String, String> getAttributes() {
//        return attributes;
//    }
//
//    /**
//     * Stores an attribute which will be shared by all data points for the metric when it is persisted. If an attribute
//     * with the same name already exists, it will be overwritten.
//     *
//     * @param name The attribute name.
//     * @param value The attribute value
//     */
//    public NumericData putAttribute(String name, String value) {
//        attributes.put(name, value);
//        return this;
//    }
//
//    /**
//     * Stores attributes which will be shared by all data points for the metric. If an attribute with the same name
//     * already exists, it will be overwritten.
//     *
//     * @param attributes The key/value pairs to store.
//     * @return
//     */
//    public NumericData putAttributes(Map<String, String> attributes) {
//        this.attributes.putAll(attributes);
//        return this;
//    }

    /**
     * The value of the raw data point. This should only be set for raw data. It should be null for aggregated data.
     */
    public double getValue() {
        return value;
    }

    public NumericData setValue(double value) {
        this.value = value;
        return this;
    }

    /**
     * A set of the aggregated values that make up this aggregated data point. This should return an empty set for raw
     * data.
     */
    public Set<AggregatedValue> getAggregatedValues() {
        return aggregatedValues;
    }

    public NumericData addAggregatedValue(AggregatedValue aggregatedValue) {
        aggregatedValues.add(aggregatedValue);
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NumericData)) return false;
        if (!super.equals(o)) return false;

        NumericData that = (NumericData) o;

        if (Double.compare(that.value, value) != 0) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        long temp;
        temp = Double.doubleToLongBits(value);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
            .add("timeUUID", timeUUID)
            .add("timestamp", getTimestamp())
            .add("value", value)
            .add("metric", metric)
            .toString();
    }
}
