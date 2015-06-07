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
package org.hawkular.metrics.api.jaxrs.request;

import static java.util.Collections.unmodifiableList;

import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.wordnik.swagger.annotations.ApiModel;
import org.hawkular.metrics.core.api.AvailabilityDataPoint;

/**
 * A container for availability data points to be persisted. Note that the tenant id is not included because it is
 * obtained from the tenant header in the HTTP request.
 *
 * @author jsanda
 */
@ApiModel(description = "Data points of an Availability metric that are to be persisted")
public class Availability {

    public static class DataPoint {
        private final long timestamp;

        private final String value;

        @JsonCreator
        public DataPoint(@JsonProperty("timestamp") long timestamp, @JsonProperty("value") String value) {
            this.timestamp = timestamp;
            this.value = value.toLowerCase();
        }

        public DataPoint(AvailabilityDataPoint dataPoint) {
            this.timestamp = dataPoint.getTimestamp();
            this.value = dataPoint.getValue().getText().toLowerCase();
        }

        public long getTimestamp() {
            return timestamp;
        }

        public String getValue() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DataPoint dataPoint = (DataPoint) o;
            return Objects.equals(timestamp, dataPoint.timestamp) &&
                    Objects.equals(value, dataPoint.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(timestamp, value);
        }

        @Override
        public String toString() {
            return "DataPoint{" +
                    "timestamp=" + timestamp +
                    ", value='" + value + '\'' +
                    '}';
        }
    }

    @JsonProperty
    private String id;

    @JsonProperty
    private List<DataPoint> data;

    public String getId() {
        return id;
    }

    public List<DataPoint> getData() {
        return unmodifiableList(data);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Availability that = (Availability) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "Availability{" +
                "id='" + id + '\'' +
                '}';
    }
}
