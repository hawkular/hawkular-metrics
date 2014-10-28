package org.rhq.metrics.core;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Objects;

/**
 * @author John Sanda
 */
public class RetentionSettings {

    public static class RetentionKey {
        public MetricType metricType;
        public Interval interval;

        public RetentionKey(MetricType metricType) {
            this.metricType = metricType;
        }

        public RetentionKey(MetricType metricType, Interval interval) {
            this.metricType = metricType;
            this.interval = interval;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            RetentionKey that = (RetentionKey) o;

            if (interval != null ? !interval.equals(that.interval) : that.interval != null) return false;
            if (metricType != that.metricType) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = metricType.hashCode();
            result = 31 * result + (interval != null ? interval.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                .add("metricType", metricType)
                .add("interval", interval)
                .toString();
        }
    }

    private Map<RetentionKey, Integer> retentions = new HashMap<>();

    public Integer put(MetricType rawType, int hours) {
        return retentions.put(new RetentionKey(rawType), hours);
    }

    public Integer put(MetricType metricType, Interval interval, int hours) {
        return retentions.put(new RetentionKey(metricType, interval), hours);
    }

    public Integer get(MetricType type) {
        return retentions.get(new RetentionKey(type));
    }

    public Integer get(MetricType type, Interval interval) {
        return retentions.get(new RetentionKey(type, interval));
    }

    public Integer get(RetentionKey key) {
        return retentions.get(key);
    }

    public Set<RetentionKey> keySet() {
        return retentions.keySet();
    }

    public Map<RetentionKey, Integer> getMap() {
        return retentions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RetentionSettings that = (RetentionSettings) o;

        if (!retentions.equals(that.retentions)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return retentions.hashCode();
    }

    @Override
    public String toString() {
        return retentions.toString();
    }
}
