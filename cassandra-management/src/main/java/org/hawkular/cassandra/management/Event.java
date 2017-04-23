/*
 * Copyright 2017 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.cassandra.management;

import java.util.Map;
import java.util.Objects;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;

/**
 * An event that occurs within the Cassandra cluster. Currently the only supported event is
 * {@link EventType#NODE_ADDED}. More event types will be added.
 *
 * @author jsanda
 */
public class Event {

    private long timestamp;

    private EventType type;

    private Map<String, String> details;

    public Event(EventType type, Map<String, String> details) {
        this(System.currentTimeMillis(), type, details);
    }

    public Event(long timestamp, EventType type, Map<String, String> details) {
        this.timestamp = timestamp;
        this.type = type;
        this.details = ImmutableMap.copyOf(details);
    }

    public long getTimestamp() {
        return timestamp;
    }

    public EventType getType() {
        return type;
    }

    public Map<String, String> getDetails() {
        return details;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Event event = (Event) o;
        return Objects.equals(timestamp, event.timestamp) &&
                type == event.type &&
                Objects.equals(details, event.details);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, type, details);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("timestamp", timestamp)
                .add("type", type)
                .add("details", details)
                .toString();
    }
}
