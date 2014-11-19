package org.rhq.metrics.restServlet.influx.query.validation;

import org.rhq.metrics.restServlet.influx.query.parse.definition.AggregatedColumnDefinition;
import org.rhq.metrics.restServlet.influx.query.parse.definition.ColumnDefinition;
import org.rhq.metrics.restServlet.influx.query.parse.definition.SelectQueryDefinitions;

/**
 * @author Thomas Segismont
 */
public class AggregatorsRule implements SelectQueryValidationRule {
    @Override
    public void checkQuery(SelectQueryDefinitions queryDefinitions) throws IllegalQueryException {
        if (queryDefinitions.isStarColumn()) {
            return;
        }
        for (ColumnDefinition columnDefinition : queryDefinitions.getColumnDefinitions()) {
            if (columnDefinition instanceof AggregatedColumnDefinition) {
                AggregatedColumnDefinition definition = (AggregatedColumnDefinition) columnDefinition;
                checkAggregatedColumnDefinition(definition);
            }
        }
    }

    private void checkAggregatedColumnDefinition(AggregatedColumnDefinition definition) throws IllegalQueryException {
        AggregationFunction function = AggregationFunction.findByName(definition.getAggregationFunction());
        if (function == null) {
            throw new QueryNotSupportedException("Aggregation function: " + definition.getAggregationFunction());
        }
        function.getValidationRule().checkArguments(definition.getAggregationFunctionArguments());
    }
}
