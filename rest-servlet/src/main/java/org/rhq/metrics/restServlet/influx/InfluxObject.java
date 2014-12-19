package org.rhq.metrics.restServlet.influx;

import java.util.List;

/**
 * Transfer object which is returned by Influx queries
 */
public class InfluxObject {
    private String name;
    private List<String> columns;
    private List<List<?>> points;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public List<List<?>> getPoints() {
        return points;
    }

    public void setPoints(List<List<?>> points) {
        this.points = points;
    }
}
