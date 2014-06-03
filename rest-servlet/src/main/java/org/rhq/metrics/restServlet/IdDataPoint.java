package org.rhq.metrics.restServlet;

import javax.xml.bind.annotation.XmlRootElement;

import com.wordnik.swagger.annotations.ApiClass;
import com.wordnik.swagger.annotations.ApiProperty;

/**
 * A data point with an Id
 * @author Heiko W. Rupp
 */
@ApiClass("One data point for a metric with id, timestamp and value. Inherits from DataPoint.")
@XmlRootElement
public class IdDataPoint extends DataPoint {

    private String id;

    public IdDataPoint() {
    }

    public IdDataPoint(long timestamp, double value, String id) {
        super(timestamp, value);
        this.id = id;
    }

    @ApiProperty("Id of the metric")
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
