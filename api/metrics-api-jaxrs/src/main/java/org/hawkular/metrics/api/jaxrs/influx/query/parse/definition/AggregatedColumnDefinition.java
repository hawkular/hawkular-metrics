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
package org.hawkular.metrics.api.jaxrs.influx.query.parse.definition;

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
