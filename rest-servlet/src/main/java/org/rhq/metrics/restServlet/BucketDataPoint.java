package org.rhq.metrics.restServlet;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * A point in time with some data for min/avg/max to express
 * that at this point in time multiple values were recorded.
 * @author Heiko W. Rupp
 */
@XmlRootElement
public class BucketDataPoint extends IdDataPoint {

    private double min;
    private double max;
    private double avg;

    public BucketDataPoint() {
    }

    public BucketDataPoint(String id, long timestamp, double min, double avg, double max) {
        super();
        this.setId(id);
        this.setTimestamp(timestamp);
        this.min = min;
        this.max = max;
        this.avg = avg;
    }

    public double getMin() {
        return min;
    }

    public void setMin(double min) {
        this.min = min;
    }

    public double getMax() {
        return max;
    }

    public void setMax(double max) {
        this.max = max;
    }

    public double getAvg() {
        return avg;
    }

    public void setAvg(double avg) {
        this.avg = avg;
    }

    public boolean isEmpty() {
        return Double.isNaN(avg) || Double.isNaN(max) || Double.isNaN(min);
    }

    @Override
    public String toString() {
        return "BucketDataPoint{" +
            "min=" + min +
            ", max=" + max +
            ", avg=" + avg +
            '}';
    }
}
