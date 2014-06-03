package org.rhq.metrics.restServlet;

import javax.xml.bind.annotation.XmlRootElement;

import com.wordnik.swagger.annotations.ApiClass;
import com.wordnik.swagger.annotations.ApiProperty;

/**
 * One single data point
 * @author Heiko W. Rupp
 */
@ApiClass("A data point for collections where each data point has the same id.")
@XmlRootElement
public class DataPoint {

    private long timestamp;
    private double value;

    public DataPoint() {
    }

    public DataPoint(long timestamp, double value) {
        this.timestamp = timestamp;
        this.value = value;
    }

    @ApiProperty("Time when the value was obtained in milliseconds since epoch")
    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @ApiProperty("The value of this data point")
    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }
}
