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
import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.ColumnDefinition;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.NameFunctionArgument;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.RawColumnDefinition;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.SelectQueryDefinitions;

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
