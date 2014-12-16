package org.rhq.metrics.restServlet.influx.query.validation;

import java.util.List;

import org.rhq.metrics.restServlet.influx.query.parse.definition.AggregatedColumnDefinition;
import org.rhq.metrics.restServlet.influx.query.parse.definition.ColumnDefinition;
import org.rhq.metrics.restServlet.influx.query.parse.definition.NameFunctionArgument;
import org.rhq.metrics.restServlet.influx.query.parse.definition.RawColumnDefinition;
import org.rhq.metrics.restServlet.influx.query.parse.definition.SelectQueryDefinitions;

/**
 * @author Thomas Segismont
 */
public class OnlyOneColumnRule implements SelectQueryValidationRule {
    @Override
    public void checkQuery(SelectQueryDefinitions queryDefinitions) throws IllegalQueryException {
        if (queryDefinitions.isStarColumn()) {
            return;
        }
        List<ColumnDefinition> columnDefinitions = queryDefinitions.getColumnDefinitions();
        if (columnDefinitions.size() != 1) {
            throw new QueryNotSupportedException("More than one column");
        }
        ColumnDefinition columnDefinition = columnDefinitions.get(0);
        if (columnDefinition instanceof RawColumnDefinition) {
            RawColumnDefinition definition = (RawColumnDefinition) columnDefinition;
            if (!definition.getName().equals("value")) {
                throw new QueryNotSupportedException("Column name is not 'value'");
            }
        } else if (columnDefinition instanceof AggregatedColumnDefinition) {
            AggregatedColumnDefinition definition = (AggregatedColumnDefinition) columnDefinition;
            NameFunctionArgument arg = (NameFunctionArgument) definition.getAggregationFunctionArguments().get(0);
            if (!arg.getName().equals("value")) {
                throw new QueryNotSupportedException("Column name is not 'value'");
            }
        }
    }
}
