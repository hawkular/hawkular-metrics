package org.rhq.metrics.restServlet.influx.query.validation;

import java.util.Iterator;
import java.util.List;

import org.rhq.metrics.restServlet.influx.query.parse.definition.FunctionArgument;
import org.rhq.metrics.restServlet.influx.query.parse.definition.NameFunctionArgument;
import org.rhq.metrics.restServlet.influx.query.parse.definition.NumberFunctionArgument;

/**
 * @author Thomas Segismont
 */
public class PercentileAggregatorRule implements AggregationFunctionValidationRule {
    @Override
    public void checkArguments(List<FunctionArgument> aggregationFunctionArguments) throws IllegalQueryException {
        if (aggregationFunctionArguments == null || aggregationFunctionArguments.size() == 0) {
            throw new IllegalQueryException("Empty argument list");
        }
        if (aggregationFunctionArguments.size() != 2) {
            throw new IllegalQueryException("Expected exactly two arguments");
        }
        Iterator<FunctionArgument> argumentIterator = aggregationFunctionArguments.iterator();
        if (!(argumentIterator.next() instanceof NameFunctionArgument)) {
            throw new IllegalQueryException("Expected a name argument first");
        }
        FunctionArgument secondArgument = argumentIterator.next();
        if (!(secondArgument instanceof NumberFunctionArgument)) {
            throw new IllegalQueryException("Expected a number argument");
        }
    }
}
