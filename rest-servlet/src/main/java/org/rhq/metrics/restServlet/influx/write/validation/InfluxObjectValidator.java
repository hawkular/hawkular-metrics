package org.rhq.metrics.restServlet.influx.write.validation;

import java.util.List;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.rhq.metrics.restServlet.influx.InfluxObject;

/**
 * @author Thomas Segismont
 */
@ApplicationScoped
public class InfluxObjectValidator {

    @Inject
    @InfluxObjectRules
    List<InfluxObjectValidationRule> validationRules;

    public void validateInfluxObjects(List<InfluxObject> influxObjects) throws InvalidObjectException {
        for (InfluxObject influxObject : influxObjects) {
            if (influxObject == null) {
                throw new InvalidObjectException("Null object");
            }
            for (InfluxObjectValidationRule validationRule : validationRules) {
                validationRule.checkInfluxObject(influxObject);
            }
        }
    }
}
