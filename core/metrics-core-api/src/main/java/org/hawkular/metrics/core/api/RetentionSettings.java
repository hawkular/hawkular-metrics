/*
 * Copyright 2014-2015 Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hawkular.metrics.core.api;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Objects;

/**
 * The data retention settings configured and stored at the tenant level. This includes retention both raw and
 * aggregated data.  Note that data retention can also be configured per metric, but that is stored and tracked
 * separately.
 *
 * @author John Sanda
 */
public class RetentionSettings {

    /**
     * The key class used in {@link RetentionSettings} internal map. Note that {@link #interval} can be null, in which
     * case this means the raw data type.
     */
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
