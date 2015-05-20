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
package org.hawkular.metrics.api.jaxrs.influx;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Transfer object which is returned by Influx queries
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class InfluxObject {
    private final String name;
    private final List<String> columns;
    private final List<List<?>> points;

    @JsonCreator
    private InfluxObject(@JsonProperty("name") String name, @JsonProperty("columns") List<String> columns,
        @JsonProperty("points") List<List<?>> points) {
        this.name = name;
        this.columns = columns;
        this.points = points == null ? Collections.emptyList() : points;
    }

    public String getName() {
        return name;
    }

    public List<String> getColumns() {
        return columns;
    }

    public List<List<?>> getPoints() {
        return points;
    }

    public static class Builder {
        private final String name;
        private final List<String> columns;
        private ArrayList<List<?>> points;

        public Builder(String name, List<String> columns) {
            this.name = name;
            this.columns = columns;
        }

        public Builder withForeseenPoints(int count) {
            if (points == null) {
                points = new ArrayList<>();
            }
            points.ensureCapacity(count);
            return this;
        }

        public Builder addPoint(List<?> point) {
            if (points == null) {
                points = new ArrayList<>();
            }
            points.add(point);
            return this;
        }

        public InfluxObject createInfluxObject() {
            return new InfluxObject(name, columns, points);
        }
    }
}
