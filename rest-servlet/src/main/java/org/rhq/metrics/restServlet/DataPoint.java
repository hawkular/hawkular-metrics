package org.rhq.metrics.restServlet;

/**
 * One single data point
 * @author Heiko W. Rupp
 */
public class DataPoint {

    private long timestamp;
    private double value;

    public DataPoint() {
    }

    public DataPoint(long timestamp, double value) {
        this.timestamp = timestamp;
        this.value = value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }
}
