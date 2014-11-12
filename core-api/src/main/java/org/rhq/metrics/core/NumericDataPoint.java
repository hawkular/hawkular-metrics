package org.rhq.metrics.core;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.datastax.driver.core.utils.UUIDs;
import com.google.common.base.Objects;

import org.rhq.metrics.util.TimeUUIDUtils;

/**
 * @author John Sanda
 */
public class NumericDataPoint {

    private UUID timeUUID;

    // value and aggregatedValues are mutually exclusive. One or the other should be set
    // but not both. It may make sense to introduce subclasses for raw and aggregated data.

    private Double value;

    private Set<AggregatedValue> aggregatedValues = new HashSet<>();

    public UUID getTimeUUID() {
        return timeUUID;
    }

    public void setTimeUUID(UUID timeUUID) {
        this.timeUUID = timeUUID;
    }

    public long getTimestamp() {
        return UUIDs.unixTimestamp(timeUUID);
    }

    public void setTimestamp(long timestamp) {
        timeUUID = TimeUUIDUtils.getTimeUUID(timestamp);
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }

    public Set<AggregatedValue> getAggregatedValues() {
        return aggregatedValues;
    }

    public void setAggregatedValues(Set<AggregatedValue> aggregatedValues) {
        this.aggregatedValues = aggregatedValues;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NumericDataPoint that = (NumericDataPoint) o;

        if (aggregatedValues != null ? !aggregatedValues.equals(that.aggregatedValues) : that.aggregatedValues != null)
            return false;
        if (!timeUUID.equals(that.timeUUID)) return false;
        if (value != null ? !value.equals(that.value) : that.value != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = timeUUID.hashCode();
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + (aggregatedValues != null ? aggregatedValues.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
            .add("timeUUID", timeUUID)
            .add("timestamp", UUIDs.unixTimestamp(timeUUID))
            .add("value", value)
            .toString();
    }
}
