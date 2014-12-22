package org.rhq.metrics.restServlet.influx.write.validation;

import org.rhq.metrics.restServlet.influx.InfluxObject;

/**
 * @author Thomas Segismont
 */
public interface InfluxObjectValidationRule {
    /**
     * @param influxObject
     * @throws InvalidObjectException
     */
    void checkInfluxObject(InfluxObject influxObject) throws InvalidObjectException;
}
