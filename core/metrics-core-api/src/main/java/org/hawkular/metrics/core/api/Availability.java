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

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

/**
 * @author John Sanda
 */
public class Availability extends MetricData {

    @JsonProperty("value")
    private AvailabilityType type;

    public Availability(long timestamp, String availability) {
        this(timestamp, AvailabilityType.fromString(availability));
    }

    @JsonCreator
    public Availability(@JsonProperty("timestamp") long timestamp, @JsonProperty("value") AvailabilityType type) {
        super(timestamp);
        this.type = type;
    }

    public Availability(UUID timeUUID, String availability) {
        super(timeUUID);
        this.type = AvailabilityType.fromString(availability);
    }

    public Availability(UUID timeUUID, AvailabilityType type) {
        super(timeUUID);
        this.type = type;
    }

    public Availability(UUID timeUUID, ByteBuffer bytes) {
        super(timeUUID);
        type = AvailabilityType.fromBytes(bytes);
    }

    public Availability(UUID timeUUID, ByteBuffer bytes, Map<String, String> tags) {
        super(timeUUID, tags);
        type = AvailabilityType.fromBytes(bytes);
    }

    public Availability(UUID timeUUID, ByteBuffer bytes, Map<String, String> tags, Long writeTime) {
        super(timeUUID, tags, writeTime);
        type = AvailabilityType.fromBytes(bytes);
    }

    public AvailabilityType getType() {
        return type;
    }

    @JsonIgnore
    public ByteBuffer getBytes() {
        return ByteBuffer.wrap(new byte[] {type.getCode()});
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Availability)) return false;
        if (!super.equals(o)) return false;

        Availability that = (Availability) o;

        if (type != that.type) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + type.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
            .add("timeUUID", timeUUID)
            .add("timestamp", getTimestamp())
            .add("type", type)
            .toString();
    }
}
