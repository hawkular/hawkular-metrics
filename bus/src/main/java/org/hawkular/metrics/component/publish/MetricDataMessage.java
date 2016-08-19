/*
 * Copyright 2014-2016 Red Hat, Inc. and/or its affiliates
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

package org.hawkular.metrics.component.publish;


import java.util.List;
import java.util.Objects;

import org.hawkular.bus.common.AbstractMessage;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * A bus message for messages on HawkularMetricData Topic.
 *
 * @author Jay Shaughnessy
 * @author Lucas Ponce
 */

public class MetricDataMessage extends AbstractMessage {

    // the basic message body - it will be exposed to the JSON output
    @JsonInclude
    private MetricData metricData;

    protected MetricDataMessage() {
    }

    public MetricDataMessage(MetricData metricData) {
        this.metricData = metricData;
    }

    public MetricData getMetricData() {
        return metricData;
    }

    public void setMetricData(MetricData metricData) {
        this.metricData = metricData;
    }

    @Override
    public String toString() {
        return "MetricDataMessage{" +
                "metricData=" + metricData +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MetricDataMessage that = (MetricDataMessage) o;
        return Objects.equals(metricData, that.metricData);
    }

    @Override
    public int hashCode() {
        return Objects.hash(metricData);
    }

    public static class MetricData {
        @JsonInclude
        String tenantId;
        @JsonInclude
        List<SingleMetric> data;

        public MetricData() {
        }

        public String getTenantId() {
            return tenantId;
        }

        public void setTenantId(String tenantId) {
            this.tenantId = tenantId;
        }

        public List<SingleMetric> getData() {
            return data;
        }

        public void setData(List<SingleMetric> data) {
            this.data = data;
        }

        @Override
        public String toString() {
            return "MetricData [tenantId=" + tenantId + ", data=" + data + "]";
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MetricData that = (MetricData) o;
            return Objects.equals(tenantId, that.tenantId) &&
                    Objects.equals(data, that.data);
        }

        @Override public int hashCode() {
            return Objects.hash(tenantId, data);
        }
    }

    /**
     * This is meant to parse out an instance of <code>org.rhq.metrics.client.common.SingleMetric</code>
     */
    public static class SingleMetric {
        @JsonInclude
        private String type;
        @JsonInclude
        private String source;
        @JsonInclude
        private long timestamp;
        @JsonInclude
        private double value;

        public SingleMetric() {
        }

        public SingleMetric(String type, String source, long timestamp, double value) {
            this.type = type;
            this.source = source;
            this.timestamp = timestamp;
            this.value = value;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getSource() {
            return source;
        }

        public void setSource(String source) {
            this.source = source;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public double getValue() {
            return value;
        }

        public void setValue(double value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            SingleMetric that = (SingleMetric) o;

            if (timestamp != that.timestamp) return false;
            if (Double.compare(that.value, value) != 0) return false;
            if (type != null ? !type.equals(that.type) : that.type != null) return false;
            return source != null ? source.equals(that.source) : that.source == null;

        }

        @Override
        public int hashCode() {
            int result;
            long temp;
            result = type != null ? type.hashCode() : 0;
            result = 31 * result + (source != null ? source.hashCode() : 0);
            result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
            temp = Double.doubleToLongBits(value);
            result = 31 * result + (int) (temp ^ (temp >>> 32));
            return result;
        }

        @Override
        public String toString() {
            return "SingleMetric{" +
                    "type='" + type + '\'' +
                    ", source='" + source + '\'' +
                    ", timestamp=" + timestamp +
                    ", value=" + value +
                    '}';
        }
    }
}
