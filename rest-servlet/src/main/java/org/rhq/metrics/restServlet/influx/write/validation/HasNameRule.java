package org.rhq.metrics.restServlet.influx.write.validation;

import org.rhq.metrics.restServlet.influx.InfluxObject;

/**
 * @author Thomas Segismont
 */
public class HasNameRule implements InfluxObjectValidationRule {
    @Override
    public void checkInfluxObject(InfluxObject influxObject) throws InvalidObjectException {
        if (influxObject.getName() == null || influxObject.getName().isEmpty()) {
            throw new InvalidObjectException("Object has no name attribute");
        }
    }
}
