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

import java.util.HashMap;
import java.util.Map;

/**
 * @author Thomas Segismont
 */
public enum AggregationFunction {
    /** */
    MEAN("mean"),
    /** */
    MAX("max"),
    /** */
    MIN("min"),
    /** */
    SUM("sum"),
    /** */
    COUNT("count"),
    /** */
    FIRST("first"),
    /** */
    LAST("last"),
    /** */
    DIFFERENCE("difference"),
    /** */
    DERIVATIVE("derivative"),
    /** */
    MEDIAN("median"),
    /** */
    PERCENTILE("percentile", new NameAndNumberAggregatorRule()),
    /** */
    TOP("top", new NameAndNumberAggregatorRule()),
    /** */
    BOTTOM("bottom", new NameAndNumberAggregatorRule()),
    /** */
    HISTOGRAM("histogram"),
    /** */
    MODE("mode"),
    /** */
    STDDEV("stddev");

    private String name;
    private AggregationFunctionValidationRule validationRule;

    AggregationFunction(String name) {
        this(name, DefaultAggregatorRule.DEFAULT_INSTANCE);
    }

    AggregationFunction(String name, AggregationFunctionValidationRule validationRule) {
        this.name = name;
        this.validationRule = validationRule;
    }

    public String getName() {
        return name;
    }

    public AggregationFunctionValidationRule getValidationRule() {
        return validationRule;
    }

    private static final Map<String, AggregationFunction> FUNCTION_BY_NAME = new HashMap<>();

    static {
        for (AggregationFunction function : values()) {
            FUNCTION_BY_NAME.put(function.name.toLowerCase(), function);
        }
    }

    /**
     * @param name time unit name
     * @return the {@link AggregationFunction} which name is <code>name</code> (case-insensitive), null otherwise
     */
    public static AggregationFunction findByName(String name) {
        return FUNCTION_BY_NAME.get(name.toLowerCase());
    }
}
