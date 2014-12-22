package org.rhq.metrics.restServlet.influx;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Transfer object which is returned by Influx queries
 */
public class InfluxObject {
    private final String name;
    private final List<String> columns;
    private final List<List<?>> points;

    @JsonCreator
    private InfluxObject(@JsonProperty("name") String name, @JsonProperty("columns") List<String> columns,
        @JsonProperty("points") List<List<?>> points) {
        this.name = name;
        this.columns = columns;
        this.points = points;
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
