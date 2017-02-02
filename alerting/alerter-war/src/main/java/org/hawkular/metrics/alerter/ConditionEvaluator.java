/*
 * Copyright 2014-2017 Red Hat, Inc. and/or its affiliates
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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.hawkular.metrics.alerter.ConditionExpression.Function;
import org.hawkular.metrics.model.AvailabilityBucketPoint;
import org.hawkular.metrics.model.BucketPoint;
import org.hawkular.metrics.model.NumericBucketPoint;
import org.hawkular.metrics.model.Percentile;
import org.jboss.logging.Logger;

import com.udojava.evalex.Expression;

/**
 * This class is responsible for resolving the {@link ConditionExpression} eval expression string. See
 * <a href="http://github.com/uklimaschewski/EvalEx">here</a> for the supported expression format. The
 * expression syntax is extended with supported query variables of the form <code>q(queryName,functionName)</code>
 * where the <i>queryName</i> is a variable referring to a query defined for the {@link ConditionExpression} and
 * the <i>functionName</i> is a supported aggregate function for the query's MetricType.  The query variables are
 * replaced with actual data at query time, before the eval expression is resolved.
 *
 * @author Jay Shaughnessy
 * @author Lucas Ponce
 */
public class ConditionEvaluator {
    private static final Logger log = Logger.getLogger(ConditionEvaluator.class);

    private static final Pattern PATTERN_QUERY_FUNC = Pattern.compile("q\\((.*?),\\s*(.*?)\\)");
    private static final Pattern PATTERN_QUERY_VAR = Pattern.compile("q\\(.*?\\)");

    // Map variableName:query
    private Map<String, QueryFunc> queryVars;

    private String eval;
    private Expression expression;

    public ConditionEvaluator(String eval) {
        super();

        this.eval = eval;
        this.queryVars = new HashMap<>();

        this.expression = new Expression(replaceQueriesWithVariables(eval));

        log.debugf("eval [%s] produced [%s] with variables %s", eval, expression.getOriginalExpression(), queryVars);

        // Do a test evaluation to validate the expression
        try {
            for (String var : queryVars.keySet()) {
                this.expression.setVariable(var, "1");
            }
            this.expression.eval();
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid eval expression [" + eval + "]: " + e.getMessage());
        }
    }

    public String getEval() {
        return eval;
    }

    /**
     * Prepare to evaluate by supplying the required query data.
     * @param data Map queryName => queryResults
     * @return Map of the query variable replacement values used in the evaluation
     */
    public Map<String, String> prepare(Map<String, BucketPoint> queryMap) throws IllegalArgumentException {
        Map<String, String> result = new HashMap<>();

        for (Map.Entry<String, QueryFunc> entry : queryVars.entrySet()) {
            String var = entry.getKey();
            QueryFunc queryFunc = entry.getValue();
            BucketPoint bucketPoint = queryMap.get(queryFunc.getQueryName());
            if (null == bucketPoint) {
                throw new IllegalArgumentException("No data found for query name [" + queryFunc.getQueryName() + "]");
            }
            BigDecimal value = null;
            try {
                value = BigDecimal.valueOf(getValue(queryFunc.getFunction(), bucketPoint));
            } catch (Exception e) {
                throw new IllegalArgumentException("Could not prepare evaluation query ["
                        + queryFunc.getQueryName() + "]: " + e.getMessage());
            }
            this.expression.setVariable(var, value);
            result.put(queryFunc.getCanonical(), value.toString());
        }
        return result;
    }

    /**
     * Evaluate the prepared condition {@see #prepare(Map)).
     * @return true if expression resolves to true, otherwise false
     */
    public boolean evaluate() {
        return (this.expression.eval() != BigDecimal.ZERO);
    }

    /**
     * Just a convenience method to prepare and evaluate in one call.
     * @return true if expression resolves to true, otherwise false
     */
    public boolean prepareAndEvaluate(Map<String, BucketPoint> queryMap) {
        prepare(queryMap);
        return (this.expression.eval() != BigDecimal.ZERO);
    }

    private double getValue(String func, BucketPoint bucketPoint) throws Exception {
        if (bucketPoint instanceof AvailabilityBucketPoint) {
            return getAvailabilityValue(func, (AvailabilityBucketPoint) bucketPoint);
        }

        return getNumericValue(func, (NumericBucketPoint) bucketPoint);
    }

    private double getNumericValue(String func, NumericBucketPoint data) {
        if (null == data || null == data.getSamples() || 0 == data.getSamples()) {
            throw new IllegalArgumentException("NumericBucketPoint has no samples");
        }

        if (func.startsWith("%")) {
            return funcToPercentile(func, data.getPercentiles()).getValue();
        }

        try {
            switch (Function.valueOf(func)) {
                case avg:
                    return data.getAvg().doubleValue();
                case max:
                    return data.getMax().doubleValue();
                case median:
                    return data.getMedian().doubleValue();
                case min:
                    return data.getMin().doubleValue();
                case samples:
                    return data.getSamples() * 1.0;
                case sum:
                    return data.getSum().doubleValue();
                default:
                    throw new IllegalArgumentException("Unexpected func [" + func + "]");
            }
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid value for func [" + func + "] in " + data.toString());
        }
    }

    private double getAvailabilityValue(String func, AvailabilityBucketPoint data) {
        if (null == data || null == data.getSamples() || 0 == data.getSamples()) {
            throw new IllegalArgumentException("AvailabilityBucketPoint has no samples");
        }

        try {
            switch (Function.valueOf(func)) {
                case notUpCount:
                    return data.getNotUpCount().doubleValue();
                case notUpDuration:
                    return data.getNotUpDuration().doubleValue();
                case upCount:
                    return data.getUpCount().doubleValue();
                case uptimeRatio:
                    return data.getUptimeRatio().doubleValue();
                case samples:
                    return data.getSamples() * 1.0;
                default:
                    throw new IllegalArgumentException("Unexpected func [" + func + "]");
            }
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid value for func [" + func + "] in " + data.toString());
        }
    }

    Percentile funcToPercentile(String func, List<Percentile> percentiles) {
        String quantile = func.substring(1);
        for (Percentile p : percentiles) {
            if (p.getOriginalQuantile().equals(quantile)) {
                return p;
            }
        }
        throw new IllegalArgumentException("Failed to find Percentile for [" + func + "] in " + percentiles);
    }

    public Map<String, QueryFunc> getQueryVars() {
        return queryVars;
    }

    public Expression getExpression() {
        return expression;
    }

    private String replaceQueriesWithVariables(String eval) {
        if (null == queryVars) {
            queryVars = new HashMap<>();
        }
        queryVars.clear();

        // First, collect the queries
        List<String> queries = new ArrayList<String>();
        Matcher m = PATTERN_QUERY_VAR.matcher(eval);
        while (m.find()) {
            queries.add(m.group());
        }

        // Next, replace them with variables
        int varNum = 0;
        for (String q : queries) {
            String var = null;
            QueryFunc qf = new QueryFunc(q);
            if (queryVars.containsValue(qf)) {
                for (Map.Entry<String, QueryFunc> e : queryVars.entrySet()) {
                    if (e.getValue().equals(qf)) {
                        var = e.getKey();
                        break;
                    }
                }
            } else {
                var = "q" + varNum++;
                queryVars.put(var, qf);

            }
            eval = eval.replaceFirst("q\\(.*?\\)", var);
        }

        return eval;
    }

    static class QueryFunc {
        private String queryName;
        private String function;
        private String canonical;

        public QueryFunc(String queryName, String function) {
            super();
            this.queryName = queryName.trim();
            this.function = function.trim();
        }

        public QueryFunc(String query) {
            super();
            Matcher m = PATTERN_QUERY_FUNC.matcher(query);
            if (!m.matches() || m.groupCount() != 2) {
                throw new IllegalArgumentException(
                        "Query segment [" + query + "] failed to parse. Groups=[" + m.groupCount() + "]");
            }
            this.queryName = m.group(1).trim();
            this.function = m.group(2).trim();
        }

        public String getQueryName() {
            return queryName;
        }

        public String getFunction() {
            return function;
        }

        public String getCanonical() {
            if (null == this.canonical) {
                this.canonical = "q(" + queryName + "," + function + ")";
            }
            return canonical;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((function == null) ? 0 : function.hashCode());
            result = prime * result + ((queryName == null) ? 0 : queryName.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            QueryFunc other = (QueryFunc) obj;
            if (function == null) {
                if (other.function != null)
                    return false;
            } else if (!function.equals(other.function))
                return false;
            if (queryName == null) {
                if (other.queryName != null)
                    return false;
            } else if (!queryName.equals(other.queryName))
                return false;
            return true;
        }

        @Override
        public String toString() {
            return "QueryFunc [queryName=" + queryName + ", function=" + function + "]";
        }

    }
}
