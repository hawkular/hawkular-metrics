package org.rhq.metrics.restServlet.influx.query.validation;

import java.util.List;

import org.rhq.metrics.restServlet.influx.query.parse.definition.FunctionArgument;

/**
 * @author Thomas Segismont
 */
public interface AggregationFunctionValidationRule {
    /**
     * @param aggregationFunctionArguments
     * @throws org.rhq.metrics.restServlet.influx.query.validation.IllegalQueryException
     */
    void checkArguments(List<FunctionArgument> aggregationFunctionArguments) throws IllegalQueryException;
}
