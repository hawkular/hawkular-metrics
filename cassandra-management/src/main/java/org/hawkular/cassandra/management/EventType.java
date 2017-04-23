/*
 * Copyright 2014-2017 Red Hat, Inc. and/or its affiliates
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

/**
 * The types of events that occur within the Cassandra cluster that Hawkular cares about for maintenance purposes.
 *
 * @author jsanda
 */
public enum EventType {

    /**
     * A new Cassandra node has joined the cluster
     */
    NODE_ADDED((short) 1, "Node Added");

    private final short code;

    private final String text;

    EventType(short code, String text) {
        this.code = code;
        this.text = text;
    }

    public static EventType fromCode(short code) {
        if (code == 1) {
            return NODE_ADDED;
        }
        throw new IllegalArgumentException(code + " is not a recognized event type code");
    }

    public short getCode() {
        return code;
    }

    public String getText() {
        return text;
    }

    @Override public String toString() {
        return text;
    }
}
