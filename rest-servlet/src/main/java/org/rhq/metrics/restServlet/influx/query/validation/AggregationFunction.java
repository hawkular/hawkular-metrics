package org.rhq.metrics.restServlet.influx.query.validation;

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
    PERCENTILE("percentile", new PercentileAggregatorRule());

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
