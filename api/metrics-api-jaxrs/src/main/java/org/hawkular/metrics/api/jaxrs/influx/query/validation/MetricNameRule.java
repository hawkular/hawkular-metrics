/*
 * Copyright 2014-2015 Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hawkular.metrics.api.jaxrs.influx.query.validation;

import java.util.List;

import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.AggregatedColumnDefinition;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.AndBooleanExpression;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.BooleanExpression;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.ColumnDefinition;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.EqBooleanExpression;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.FromClause;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.FunctionArgument;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.GtBooleanExpression;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.LtBooleanExpression;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.NameFunctionArgument;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.NameOperand;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.NeqBooleanExpression;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.Operand;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.OrBooleanExpression;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.RawColumnDefinition;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.SelectQueryDefinitions;

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
