package org.rhq.metrics.restServlet;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * A data point with an Id
 * @author Heiko W. Rupp
 */
@XmlRootElement
public class IdDataPoint extends DataPoint {

    private String id;

    public IdDataPoint() {
    }

    public IdDataPoint(long timestamp, double value, String id) {
        super(timestamp, value);
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
