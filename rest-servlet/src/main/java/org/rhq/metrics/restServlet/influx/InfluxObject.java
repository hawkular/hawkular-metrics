package org.rhq.metrics.restServlet.influx;

import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * Transfer object which is returned by Influx queries
 */
@SuppressWarnings("unused")
@XmlRootElement
public class InfluxObject {

    public InfluxObject() {
    }

    public InfluxObject(String name) {
        this.name = name;
    }

    public String name;
    public List<String> columns;
    public List<List<?>> points;

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
