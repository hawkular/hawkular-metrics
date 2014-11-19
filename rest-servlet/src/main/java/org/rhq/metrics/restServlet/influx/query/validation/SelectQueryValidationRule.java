package org.rhq.metrics.restServlet.influx.query.validation;

import org.rhq.metrics.restServlet.influx.query.parse.definition.SelectQueryDefinitions;

/**
 * @author Thomas Segismont
 */
public interface SelectQueryValidationRule {
    /**
     * @param queryDefinitions
     * @throws org.rhq.metrics.restServlet.influx.query.validation.IllegalQueryException
     */
    void checkQuery(SelectQueryDefinitions queryDefinitions) throws IllegalQueryException;
}
