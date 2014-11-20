package org.rhq.metrics.core;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.datastax.driver.core.utils.UUIDs;

import org.rhq.metrics.util.TimeUUIDUtils;

/**
 * @author John Sanda
 */
public abstract class MetricData {


    public static final Comparator<NumericData> TIME_UUID_COMPARATOR = new Comparator<NumericData>() {
        @Override
        public int compare(NumericData d1, NumericData d2) {
            return TimeUUIDUtils.compare(d1.timeUUID, d2.timeUUID);
        }
    };

    protected UUID timeUUID;

    protected Metric metric;

    protected Set<Tag> tags = new HashSet<>();

    public MetricData(Metric metric, UUID timeUUID) {
        this.metric = metric;
        this.timeUUID = timeUUID;
    }

    public MetricData(Metric metric, UUID timeUUID, Set<Tag> tags) {
        this.metric = metric;
        this.timeUUID = timeUUID;
        this.tags = tags;
    }

    public MetricData(Metric metric, long timestamp) {
        this.metric = metric;
        this.timeUUID = TimeUUIDUtils.getTimeUUID(timestamp);
    }

    public MetricData(UUID timeUUID) {
        this.timeUUID = timeUUID;
    }

    public MetricData(long timestamp) {
        timeUUID = TimeUUIDUtils.getTimeUUID(timestamp);
    }

    public Metric getMetric() {
        return metric;
    }

    public void setMetric(Metric metric) {
        this.metric = metric;
    }

    /**
     * The time based UUID for this data point
     */
    public UUID getTimeUUID() {
        return timeUUID;
    }

    /**
     * The UNIX timestamp of the {@link #getTimeUUID() timeUUID}
     */
    public long getTimestamp() {
        return UUIDs.unixTimestamp(timeUUID);
    }

    public Set<Tag> getTags() {
        return tags;
    }

    public void setTags(Set<Tag> tags) {
        this.tags = tags;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MetricData)) return false;

        MetricData that = (MetricData) o;

        if (!timeUUID.equals(that.timeUUID)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return timeUUID.hashCode();
    }

}
