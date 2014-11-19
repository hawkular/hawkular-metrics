package org.rhq.metrics.restServlet.influx.query.validation;

import java.util.List;

import org.rhq.metrics.restServlet.influx.query.parse.definition.AggregatedColumnDefinition;
import org.rhq.metrics.restServlet.influx.query.parse.definition.AndBooleanExpression;
import org.rhq.metrics.restServlet.influx.query.parse.definition.BooleanExpression;
import org.rhq.metrics.restServlet.influx.query.parse.definition.ColumnDefinition;
import org.rhq.metrics.restServlet.influx.query.parse.definition.EqBooleanExpression;
import org.rhq.metrics.restServlet.influx.query.parse.definition.FromClause;
import org.rhq.metrics.restServlet.influx.query.parse.definition.FunctionArgument;
import org.rhq.metrics.restServlet.influx.query.parse.definition.GtBooleanExpression;
import org.rhq.metrics.restServlet.influx.query.parse.definition.LtBooleanExpression;
import org.rhq.metrics.restServlet.influx.query.parse.definition.NameFunctionArgument;
import org.rhq.metrics.restServlet.influx.query.parse.definition.NameOperand;
import org.rhq.metrics.restServlet.influx.query.parse.definition.NeqBooleanExpression;
import org.rhq.metrics.restServlet.influx.query.parse.definition.Operand;
import org.rhq.metrics.restServlet.influx.query.parse.definition.OrBooleanExpression;
import org.rhq.metrics.restServlet.influx.query.parse.definition.RawColumnDefinition;
import org.rhq.metrics.restServlet.influx.query.parse.definition.SelectQueryDefinitions;

/**
 * @author Thomas Segismont
 */
public class MetricNameRule implements SelectQueryValidationRule {
    @Override
    public void checkQuery(SelectQueryDefinitions queryDefinitions) throws IllegalQueryException {
        FromClause fromClause = queryDefinitions.getFromClause();
        String metricEffectiveName = fromClause.isAliased() ? fromClause.getAlias() : fromClause.getName();
        if (!queryDefinitions.isStarColumn()) {
            checkColumnDefinitions(queryDefinitions.getColumnDefinitions(), metricEffectiveName);
        }
        BooleanExpression whereClause = queryDefinitions.getWhereClause();
        if (whereClause != null) {
            checkBooleanExpression(whereClause, metricEffectiveName);
        }
    }

    private void checkColumnDefinitions(List<ColumnDefinition> columnDefinitions, String metricEffectiveName)
        throws IllegalQueryException {

        for (ColumnDefinition columnDefinition : columnDefinitions) {
            if (columnDefinition instanceof RawColumnDefinition) {
                RawColumnDefinition definition = (RawColumnDefinition) columnDefinition;
                if (definition.isPrefixed() && !definition.getPrefix().equals(metricEffectiveName)) {
                    throw new IllegalQueryException("Unexpected prefix: " + definition.getPrefix());
                }
            } else if (columnDefinition instanceof AggregatedColumnDefinition) {
                AggregatedColumnDefinition definition = (AggregatedColumnDefinition) columnDefinition;
                for (FunctionArgument argument : definition.getAggregationFunctionArguments()) {
                    if (argument instanceof NameFunctionArgument) {
                        NameFunctionArgument nameArgument = (NameFunctionArgument) argument;
                        if (nameArgument.isPrefixed() && !nameArgument.getPrefix().equals(metricEffectiveName)) {
                            throw new IllegalQueryException("Unexpected prefix: " + nameArgument.getPrefix());
                        }
                    }
                }
            } else {
                throw new IllegalQueryException("Unexpected definition type: " + columnDefinition.getClass());
            }
        }
    }

    private void checkBooleanExpression(BooleanExpression booleanExpression, String metricEffectiveName)
        throws IllegalQueryException {

        if (booleanExpression instanceof AndBooleanExpression) {
            AndBooleanExpression and = (AndBooleanExpression) booleanExpression;
            checkBooleanExpression(and.getLeftExpression(), metricEffectiveName);
            checkBooleanExpression(and.getRightExpression(), metricEffectiveName);
        } else if (booleanExpression instanceof OrBooleanExpression) {
            OrBooleanExpression or = (OrBooleanExpression) booleanExpression;
            checkBooleanExpression(or.getLeftExpression(), metricEffectiveName);
            checkBooleanExpression(or.getRightExpression(), metricEffectiveName);
        } else if (booleanExpression instanceof NeqBooleanExpression) {
            NeqBooleanExpression neq = (NeqBooleanExpression) booleanExpression;
            checkOperand(neq.getLeftOperand(), metricEffectiveName);
            checkOperand(neq.getRightOperand(), metricEffectiveName);
        } else if (booleanExpression instanceof GtBooleanExpression) {
            GtBooleanExpression gt = (GtBooleanExpression) booleanExpression;
            checkOperand(gt.getLeftOperand(), metricEffectiveName);
            checkOperand(gt.getRightOperand(), metricEffectiveName);
        } else if (booleanExpression instanceof LtBooleanExpression) {
            LtBooleanExpression lt = (LtBooleanExpression) booleanExpression;
            checkOperand(lt.getLeftOperand(), metricEffectiveName);
            checkOperand(lt.getRightOperand(), metricEffectiveName);
        } else if (booleanExpression instanceof EqBooleanExpression) {
            EqBooleanExpression eq = (EqBooleanExpression) booleanExpression;
            checkOperand(eq.getLeftOperand(), metricEffectiveName);
            checkOperand(eq.getRightOperand(), metricEffectiveName);
        } else {
            throw new IllegalQueryException("Unexpected expression type: " + booleanExpression.getClass());
        }
    }

    private void checkOperand(Operand operand, String metricEffectiveName) throws IllegalQueryException {
        if (operand instanceof NameOperand) {
            NameOperand nameOperand = (NameOperand) operand;
            if (nameOperand.isPrefixed() && !nameOperand.getPrefix().equals(metricEffectiveName)) {
                throw new IllegalQueryException("Unexpected prefix: " + nameOperand.getPrefix());
            }

        }
    }
}
