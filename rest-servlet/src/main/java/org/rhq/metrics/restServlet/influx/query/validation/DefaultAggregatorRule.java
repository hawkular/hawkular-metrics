package org.rhq.metrics.restServlet.influx.query.validation;

import java.util.List;

import org.rhq.metrics.restServlet.influx.query.parse.definition.FunctionArgument;
import org.rhq.metrics.restServlet.influx.query.parse.definition.NameFunctionArgument;

/**
 * @author Thomas Segismont
 */
public class DefaultAggregatorRule implements AggregationFunctionValidationRule {
    public static final AggregationFunctionValidationRule DEFAULT_INSTANCE = new DefaultAggregatorRule();

    @Override
    public void checkArguments(List<FunctionArgument> aggregationFunctionArguments) throws IllegalQueryException {
        if (aggregationFunctionArguments == null || aggregationFunctionArguments.size() == 0) {
            throw new IllegalQueryException("Empty argument list");
        }
        if (aggregationFunctionArguments.size() != 1) {
            throw new IllegalQueryException("Expected exactly one argument");
        }
        if (!(aggregationFunctionArguments.get(0) instanceof NameFunctionArgument)) {
            throw new IllegalQueryException("Expected a name argument");
        }
    }
}
