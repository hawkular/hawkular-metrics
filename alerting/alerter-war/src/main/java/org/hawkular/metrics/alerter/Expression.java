/*
 * Copyright 2014-2016 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.alerter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Expression parser.
 *
 * TODO: tag/group based expressions need to be fully fleshed out. None yet available.
 *
 * @author jay shaughnessy
 * @author elias ross
 * @author lucas ponce
 */
public class Expression {
    // private final Logger log = Logger.getLogger(Expression.class);

    // (?i) = case insensitive
    // interval and period is in minutes with optional 'm' unit
    private static final Pattern SYNTAX = Pattern.compile(
            //         target:interval:func(metricId           op      threshold )[, period]
            "(?i)(metric|tag):(\\d+)m?+:(\\w+)\\((\\S+)?\\s*([<>]=?+)\\s*(\\S+)\\)(?:,\\s*(\\d+)m?+)?+");

    enum Target {
        Metric, Tag;
    }

    /**
     * Aggregate functions, also includes day and week averages.
     */
    enum Func {
        // Single or Group metric
        /** Metric average for the period */
        avg,
        /** Percent change from yesterday:  (((avg - avgSamePeriodYesterday) / avgSamePeriodYesterday) * 100) */
        avgd(1),
        /** Percent change from last week:  (((avg - avgSamePeriodLastWeek) / avgSamePeriodLastWeek) * 100) */
        avgw(7),
        /** Maximum-Minimum for the period (a measurement of volatility) */
        range,
        /** (Maximum-Minimum)/avg for the period (a measurement of volatility) */
        rangep,
        /** Minimum reported value for the period. */
        min,
        /** Maximum reported value for the period. */
        max,
        /** Count of UP avail for the period. */
        heartbeat;

        private final int days; // days back

        private Func() {
            this(0);
        }

        private Func(int days) {
            this.days = days;
        }

        public int getDays() {
            return days;
        }
    }

    /**
     * Supported comparison operations.
     */
    enum Op {
        GT(">"), LT("<"), GTE(">="), LTE("<=");

        private final String s;

        private Op(String s) {
            this.s = s;
        }

        @Override
        public String toString() {
            return s;
        }
    }

    private Target target;
    private Func func;
    private String metric;
    private Op op;
    private Double threshold;
    private Integer interval;
    private Integer period;

    public Expression(String expression) {
        Matcher matcher = SYNTAX.matcher(expression);
        if (!matcher.matches())
            throw new IllegalArgumentException("Invalid expression syntax. Must match: " + SYNTAX.pattern());
        target = valueOfIgnoreCase(Target.class, matcher.group(1));
        interval = Integer.parseInt(matcher.group(2));
        func = valueOfIgnoreCase(Func.class, matcher.group(3));
        metric = matcher.group(4);
        String sOp = matcher.group(5);
        if (">".equals(sOp))
            op = Op.GT;
        else if ("<".equals(sOp))
            op = Op.LT;
        else if (">=".equals(sOp))
            op = Op.GTE;
        else if ("<=".equals(sOp))
            op = Op.LTE;
        else
            throw new IllegalArgumentException("unknown op " + sOp);
        threshold = Double.parseDouble(matcher.group(6));
        String sPeriod = matcher.group(7);
        if (sPeriod != null) {
            period = Integer.parseInt(sPeriod);
        }

        // Currently, all of the supported functions require a period
        if (null == period) {
            throw new IllegalArgumentException("Invalid expression syntax. Function " + func.name()
            + " requires a time period");
        }

        // Currently, all of the supported functions support only a 'metric' target
        if (Target.Tag == target) {
            throw new IllegalArgumentException("Invalid expression syntax. Function " + func.name()
            + " does not yet support Tag targets");
        }
    }

    public static <T extends Enum<?>> T valueOfIgnoreCase(Class<T> clazz, String s) {
        for (T t : clazz.getEnumConstants()) {
            if (t.name().equalsIgnoreCase(s)) {
                return t;
            }
        }
        throw new IllegalArgumentException(s);
    }

    public Target getTarget() {
        return target;
    }

    public Integer getInterval() {
        return interval;
    }

    public Func getFunc() {
        return func;
    }

    public String getMetric() {
        return metric;
    }

    public Op getOp() {
        return op;
    }

    public double getThreshold() {
        return threshold;
    }

    public Integer getPeriod() {
        return period;
    }

    public boolean isTrue(Double value) {
        if (null == value || value.isNaN()) {
            return false;
        }
        switch (op) {
            case GT:
                return value > threshold;
            case GTE:
                return value >= threshold;
            case LT:
                return value < threshold;
            case LTE:
                return value <= threshold;
            default:
                return false;
        }
    }

    /**
     * Alert definition name. Do not change this rendering without
     * consideration.
     */
    @Override
    public String toString() {
        String s = target + ":" + interval + "m:" + func + "(" + metric + "" + op + "" + threshold + ")";
        if (period > 0)
            s += "," + period + "m";
        return s;
    }
}
