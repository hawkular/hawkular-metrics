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
package org.hawkular.metrics.clients.ptrans.data;

import static java.util.stream.Collectors.joining;

import java.util.Comparator;
import java.util.List;

/**
 * @author Thomas Segismont
 */
public class Point {
    public static final Comparator<Point> POINT_COMPARATOR;

    static {
        POINT_COMPARATOR = Comparator.comparing(Point::getName).thenComparing(Point::getTimestamp);
    }

    private final String name;
    private final long timestamp;
    private final double value;

    public Point(String name, long timestamp, double value) {
        this.name = name;
        this.timestamp = timestamp;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public double getValue() {
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

    public static String listToString(List<Point> data) {
        return data.stream().map(Point::toString).collect(joining(System.getProperty("line.separator")));
    }
}
