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
package org.hawkular.metrics.clients.ptrans.fullstack;

/**
 * @author Thomas Segismont
 */
class Point {
    final String name;
    final long timestamp;
    final double value;

    Point(String name, long timestamp, double value) {
        this.name = name;
        this.timestamp = timestamp;
        this.value = value;
    }

    String getName() {
        return name;
    }

    long getTimestamp() {
        return timestamp;
    }

    double getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "Point[" +
               "name='" + name + '\'' +
               ", timestamp=" + timestamp +
               ", value=" + value +
               ']';
    }
}
