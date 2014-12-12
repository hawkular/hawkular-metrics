package org.rhq.metrics.restServlet.influx.query.parse.definition;

import java.util.List;

/**
 * @author Thomas Segismont
 */
public class AggregatedColumnDefinition extends ColumnDefinition {
    private final String aggregationFunction;
    private final List<FunctionArgument> aggregationFunctionArguments;

    public AggregatedColumnDefinition(String aggregationFunction, List<FunctionArgument> aggregationFunctionArguments,
        String alias) {
        super(alias);
        this.aggregationFunction = aggregationFunction;
        this.aggregationFunctionArguments = aggregationFunctionArguments;
    }

    public String getAggregationFunction() {
        return aggregationFunction;
    }

    public List<FunctionArgument> getAggregationFunctionArguments() {
        return aggregationFunctionArguments;
    }

    public String getDisplayName() {
        return isAliased() ? getAlias() : aggregationFunction;
    }
}
