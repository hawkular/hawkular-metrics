package org.rhq.metrics.restServlet.influx.query.parse.definition;

import java.util.List;

/**
 * @author Thomas Segismont
 */
public class AggregatedColumnDefinitionBuilder extends ColumnDefinitionBuilder {
    private String aggregationFunction;
    private List<FunctionArgument> aggregationFunctionArguments;

    public AggregatedColumnDefinitionBuilder setAggregationFunction(String aggregationFunction) {
        this.aggregationFunction = aggregationFunction;
        return this;
    }

    public AggregatedColumnDefinitionBuilder setAggregationFunctionArguments(
        List<FunctionArgument> aggregationFunctionArguments) {
        this.aggregationFunctionArguments = aggregationFunctionArguments;
        return this;
    }

    @Override
    public AggregatedColumnDefinition createColumnDefinition() {
        return new AggregatedColumnDefinition(aggregationFunction, aggregationFunctionArguments, alias);
    }
}