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

import static org.hawkular.metrics.model.MetricType.AVAILABILITY;
import static org.hawkular.metrics.model.MetricType.COUNTER;
import static org.hawkular.metrics.model.MetricType.COUNTER_RATE;
import static org.hawkular.metrics.model.MetricType.GAUGE;
import static org.hawkular.metrics.model.MetricType.GAUGE_RATE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.hawkular.metrics.alerter.ConditionEvaluator.QueryFunc;
import org.hawkular.metrics.model.MetricType;
import org.hawkular.metrics.model.Percentile;
import org.hawkular.metrics.model.param.Duration;
import org.jboss.logging.Logger;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * This class is the Object representation of the JSON string required as the <code>ExternalCondition.expression</code>
 * value, for the Metrics External Alerter. Set <code>ExternalCondition.alerterId="MetricsCondition"</code>  and
 * the related  Trigger must be tagged with <code>{name="HawkularMetrics",value="MetricsCondition"}</code>.
 *
 * @author Jay Shaughnessy
 * @author Lucas Ponce
 */
public class ConditionExpression {

    /**
     * Determines whether the eval expression is performed on ALL or EACH of the metrics. The {@link Query#metrics}
     * and {@link Query#tags} fields define the set of metrics will be involved in the query. The query
     * will result in a set of data points for each metric. This field defines how to perform the
     * {@link ConditionExpression#eval} for the resulting data points.
     * <p>
     * <b>ALL</b> means that the data points for all of the metrics will be combined, then aggregated, and
     * {@link ConditionExpression#eval} is resolved one time (per run of the ConditionExpression). At most one Event
     * will be sent to Alerting if {@link ConditionExpression#eval} resolves to true.</p>
     * <p>
     * <b>EACH</b> means that the data points for each metric will be aggregated separately.
     * {@link ConditionExpression#eval} is resolved N times, once for each metric. Up to N Events
     * could be sent to Alerting depending on how often {@link ConditionExpression#eval} resolves to true.</p>
     * <p>
     * For example, assume we are dealing with two metrics, M1 and M2, the eval is "q(qNow,avg) > 100", and qNow is
     * define with an interval of 1h. On each run we get the M1 and M2 data points for the most recent hour.
     * Using <b>ALL</b> we combine all of the datapoints, generate the average, and resolve the expression. If true
     * we send an Event to alerting.  Using <b>EACH</b> we keep the data points for each metric separate, generate
     * the average for each, and resolve the expression for each. For each true resolution we send an Event to
     * Alerting.  Note that to distinguish the events we provide the metric name in the Event context. For example,
     * like context={"MetricName":"M1"}.</p>
     * <p>
     * A Note tabout using <b>EACH/b>.  If multiple queries are involved in the eval expression then
     * that only metrics common to each query will be evaluated.  For example, if the eval abbove were
     * "q(qNow,avg) >  q(qYesterday,avg)" and only M1 were common to both queries then the eval would only be
     * resolved using M1 data points.  In general each query will likely use the same metrics so this issue is
     * a corner case.
     * </p>
     * The default is ALL.
     */
    public enum EvalType {
        ALL, EACH
    }

    private static final Logger log = Logger.getLogger(ConditionExpression.class);

    /** 10mn */
    public static final String DEFAULT_FREQUENCY = "10mn";

    @JsonInclude
    private List<Query> queries;

    /** String in Metrics Duration format.  Minimum is 1mn */
    @JsonInclude
    private String frequency;

    @JsonIgnore
    private Duration frequencyDuration;

    @JsonInclude
    private EvalType evalType;

    @JsonInclude
    private String eval;

    @JsonInclude
    private int quietCount;

    @JsonIgnore
    private ConditionEvaluator evaluator;

    public ConditionExpression() {
        this((List<Query>) null, null, null, null, 0);
    }

    public ConditionExpression(Query query, String frequency, String eval) {
        this(Collections.singletonList(query), frequency, null, eval, 0);
    }

    public ConditionExpression(Query query, String frequency, EvalType evalType, String eval) {
        this(Collections.singletonList(query), frequency, evalType, eval, 0);
    }

    public ConditionExpression(List<Query> queries, String frequency, String eval) {
        this(queries, frequency, null, eval, 0);
    }

    public ConditionExpression(List<Query> queries, String frequency, EvalType evalType, String eval) {
        this(queries, frequency, evalType, eval, 0);
    }

    public ConditionExpression(Query query, String frequency, EvalType evalType, String eval, int quietCount) {
        this(Collections.singletonList(query), frequency, evalType, eval, quietCount);
    }

    public ConditionExpression(List<Query> queries, String frequency, EvalType evalType, String eval, int quietCount) {
        super();

        setQueries(queries);
        setFrequency(frequency);
        setEvalType(evalType);
        setEval(eval);
        setQuietCount(quietCount);
    }

    public List<Query> getQueries() {
        if (null == queries) {
            queries = new ArrayList<>();
        }
        return queries;
    }

    public void setQueries(List<Query> queries) {
        this.queries = (null == queries) ? new ArrayList<>() : queries;
        validateQueries();
    }

    private void validateQueries() {
        Set<String> queryNames = queries.stream().map(Query::getName).collect(Collectors.toSet());

        // ensure unique query names
        if (queries.size() >= 2) {
            if (queryNames.size() < queries.size()) {
                throw new IllegalArgumentException("Each query requires a unique name, duplicate found: " + queries);
            }
        }

        // validate queries referenced in eval
        if (null == evaluator) {
            return;
        }

        for (QueryFunc qf : evaluator.getQueryVars().values()) {
            String name = qf.getQueryName();
            String func = qf.getFunction();

            if (!queryNames.contains(name)) {
                throw new IllegalArgumentException("Eval [" + eval + "] uses undefined query [" + name + "]");
            }

            for (Query q : queries) {
                if (func.startsWith("%")) {
                    try {
                        evaluator.funcToPercentile(func, q.getMetricsPercentiles());
                    } catch (Exception e) {
                        throw new IllegalArgumentException(
                                "Eval [" + eval + "] uses undefined func [" + func + "] in query " + q);
                    }

                } else {
                    if (!Function.valueOf(func).isSupported(q.getMetricsType())) {
                        throw new IllegalArgumentException(
                                "Eval [" + eval + "] uses unsupported func [" + func + "] for query metric type: "
                                        + q);
                    }
                }
            }
        }
    }

    public String getFrequency() {
        return frequency;
    }

    /**
     * @param frequency in Metrics Duration format. Minimum is 1mn. Default is {@link #DEFAULT_FREQUENCY}.
     */
    public void setFrequency(String frequency) {
        this.frequency = (null == frequency) ? DEFAULT_FREQUENCY : frequency;
        this.frequencyDuration = new Duration(this.frequency);
        if (this.frequencyDuration.toMillis() < 60000) {
            this.frequency = "1mn";
            this.frequencyDuration = new Duration(frequency);
        }
    }

    public Duration getFrequencyDuration() {
        return frequencyDuration;
    }

    public EvalType getEvalType() {
        return evalType;
    }

    public void setEvalType(EvalType evalType) {
        this.evalType = (null == evalType) ? EvalType.ALL : evalType;
    }

    public String getEval() {
        return eval;
    }

    public void setEval(String eval) {
        this.eval = eval;
        this.evaluator = null == eval ? null : new ConditionEvaluator(eval);
    }

    public int getQuietCount() {
        return quietCount;
    }

    public void setQuietCount(int quietCount) {
        if (quietCount < 0) {
            quietCount = 0;
        }
        this.quietCount = quietCount;
    }

    public ConditionEvaluator getEvaluator() {
        return evaluator;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((eval == null) ? 0 : eval.hashCode());
        result = prime * result + ((evalType == null) ? 0 : evalType.hashCode());
        result = prime * result + ((frequency == null) ? 0 : frequency.hashCode());
        result = prime * result + ((queries == null) ? 0 : queries.hashCode());
        result = prime * result + quietCount;
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
        ConditionExpression other = (ConditionExpression) obj;
        if (eval == null) {
            if (other.eval != null)
                return false;
        } else if (!eval.equals(other.eval))
            return false;
        if (evalType != other.evalType)
            return false;
        if (frequency == null) {
            if (other.frequency != null)
                return false;
        } else if (!frequency.equals(other.frequency))
            return false;
        if (queries == null) {
            if (other.queries != null)
                return false;
        } else if (!queries.equals(other.queries))
            return false;
        if (quietCount != other.quietCount)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "ConditionExpression [queries=" + queries + ", frequency=" + frequency + ", evalType=" + evalType
                + ", eval=" + eval + ", quietCount=" + quietCount + "]";
    }

    public static ConditionExpression toObject(String json) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readValue(json, ConditionExpression.class);
        } catch (Exception e) {
            log.error("Failed to convert JSON to MetricsCondition", e);
            return null;
        }
    }

    public String toJson() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(this);
        } catch (Exception e) {
            log.error("Failed to convert MetricsCondition to JSON", e);
            return null;
        }
    }

    private static final MetricType<?>[] ALL_TYPES = { AVAILABILITY, COUNTER, COUNTER_RATE, GAUGE, GAUGE_RATE };
    private static final MetricType<?>[] NUMERIC_TYPES = { COUNTER, COUNTER_RATE, GAUGE, GAUGE_RATE };

    public enum Function {
        avg(NUMERIC_TYPES), //
        max(NUMERIC_TYPES), //
        median(NUMERIC_TYPES), //
        min(NUMERIC_TYPES), //
        notUpCount(AVAILABILITY), //
        notUpDuration(AVAILABILITY), //
        samples(ALL_TYPES), //
        sum(NUMERIC_TYPES), //
        uptimeRatio(AVAILABILITY), //
        upCount(AVAILABILITY);

        private Set<MetricType<?>> supportedMetricTypes;

        Function(MetricType<?>... supportedMetricTypes) {
            this.supportedMetricTypes = new HashSet<>(Arrays.asList(supportedMetricTypes));
        }

        public boolean isSupported(MetricType<?> metricType) {
            return this.supportedMetricTypes.contains(metricType);
        }
    }

    public static class Query {
        public static final Set<MetricType<?>> UNSUPPORTED_TYPES = new HashSet<>(Arrays.asList(MetricType.STRING));

        //private final Logger log = Logger.getLogger(ConditionExpression.Query.class);

        /** Query name to be referenced in {@link ConditionExpression#eval} */
        @JsonInclude
        private String name;

        /** Default: "gauge". Supported: @link {@link #UNSUPPORTED_TYPES} */
        @JsonInclude
        private String type;

        @JsonIgnore
        private MetricType<?> metricsType;

        /** Set of MetricIds under query. Required if tags not specified, otherwise not permitted */
        @JsonInclude
        private Set<String> metrics;

        /**
         * Standard Metrics tag query expression describing the metrics under query. Required if metrics not
         * specified, otherwise not permitted.
         */
        @JsonInclude
        private String tags;

        /** The percentiles to calculate on the resulting metric data, Strings like "0.85" for 85th percentile */
        @JsonInclude
        private Set<String> percentiles;

        @JsonIgnore
        private List<Percentile> metricsPercentiles;

        @JsonInclude
        private String duration;

        @JsonIgnore
        private Duration metricsDuration;

        @JsonInclude
        private String offset;

        @JsonIgnore
        private Duration metricsOffset;

        // for JSON deserialization
        Query() {
            this(null, null, null, null, null, null, null);
        }

        /**
         * It is common to execute basically the same query at different time offsets. For example, consider query
         * 'Qnow' with duration 5mn and no offset. It executes against the most recent 5 minutes of data.  To
         * compare values against the same time period yesterday, supply this constructor with name='Qyesterday',
         * offset='1d' and the Qnow Query.
         * @param name Each query for the condition expression needs a unique name
         * @param offset The offset applied, typically different that the base query
         * @param query The base query to copy
         */
        Query(String name, String offset, Query query) {
            super();

            this.name = name;
            setOffset(offset);

            setType(query.getType());
            setMetrics(query.getMetrics());
            setTags(query.getTags());
            setPercentiles(query.getPercentiles());
            setDuration(query.getDuration());
        }

        Query(String name, Set<String> metrics, String tags, Set<String> percentiles, String duration, String offset) {
            this(name, null, metrics, tags, percentiles, duration, offset);
        }

        Query(String name, String metricType, Set<String> metrics, String tags,
                Set<String> percentiles, String duration, String offset) {
            super();

            this.name = name;
            setType(metricType);
            setMetrics(metrics);
            setTags(tags);
            setPercentiles(percentiles);
            setDuration(duration);
            setOffset(offset);
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            if (null == type) {
                type = GAUGE.getText();
            }
            MetricType<?> mt = MetricType.fromTextCode(type);
            if (UNSUPPORTED_TYPES.contains(mt)) {
                throw new IllegalArgumentException("Unsupported type: [" + type + "]");
            }
            this.type = type;
            this.metricsType = MetricType.fromTextCode(type);
        }

        public MetricType<?> getMetricsType() {
            return metricsType;
        }

        public Set<String> getMetrics() {
            return metrics;
        }

        public void setMetrics(Set<String> metrics) {
            if (metrics != null && tags != null) {
                throw new IllegalArgumentException("Metrics and Tags can not both be set on the Query");
            }
            this.metrics = metrics;
        }

        public String getTags() {
            return tags;
        }

        public void setTags(String tags) {
            if (metrics != null && tags != null) {
                throw new IllegalArgumentException("Metrics and Tags can not both be set on the Query");
            }
            this.tags = tags;
        }

        private boolean isEmpty(String s) {
            return null == s || s.isEmpty();
        }

        private boolean isEmpty(Collection<?> c) {
            return null == c || c.isEmpty();
        }

        public Set<String> getPercentiles() {
            if (null == this.percentiles) {
                this.percentiles = new HashSet<>();
            }
            return percentiles;
        }

        public void setPercentiles(Set<String> percentiles) {
            this.percentiles = (null == percentiles) ? new HashSet<>() : percentiles;
            this.metricsPercentiles = percentilesToMetricsPercentiles();
        }

        private List<Percentile> percentilesToMetricsPercentiles() {
            if (isEmpty(percentiles)) {
                return Collections.emptyList();
            }

            return percentiles.stream().map(Percentile::new).collect(Collectors.toList());
        }

        public List<Percentile> getMetricsPercentiles() {
            return metricsPercentiles;
        }

        public String getDuration() {
            return duration;
        }

        public void setDuration(String duration) {
            this.duration = duration;
            this.metricsDuration = new Duration(isEmpty(duration) ? "0ms" : duration);
        }

        public Duration getMetricsDuration() {
            return metricsDuration;
        }

        public String getOffset() {
            return offset;
        }

        public void setOffset(String offset) {
            this.offset = offset;
            this.metricsOffset = new Duration(isEmpty(offset) ? "0ms" : offset);
        }

        public Duration getMetricsOffset() {
            return metricsOffset;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((duration == null) ? 0 : duration.hashCode());
            result = prime * result + ((metrics == null) ? 0 : metrics.hashCode());
            result = prime * result + ((name == null) ? 0 : name.hashCode());
            result = prime * result + ((offset == null) ? 0 : offset.hashCode());
            result = prime * result + ((percentiles == null) ? 0 : percentiles.hashCode());
            result = prime * result + ((tags == null) ? 0 : tags.hashCode());
            result = prime * result + ((type == null) ? 0 : type.hashCode());
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
            Query other = (Query) obj;
            if (duration == null) {
                if (other.duration != null)
                    return false;
            } else if (!duration.equals(other.duration))
                return false;
            if (metrics == null) {
                if (other.metrics != null)
                    return false;
            } else if (!metrics.equals(other.metrics))
                return false;
            if (name == null) {
                if (other.name != null)
                    return false;
            } else if (!name.equals(other.name))
                return false;
            if (offset == null) {
                if (other.offset != null)
                    return false;
            } else if (!offset.equals(other.offset))
                return false;
            if (percentiles == null) {
                if (other.percentiles != null)
                    return false;
            } else if (!percentiles.equals(other.percentiles))
                return false;
            if (tags == null) {
                if (other.tags != null)
                    return false;
            } else if (!tags.equals(other.tags))
                return false;
            if (type == null) {
                if (other.type != null)
                    return false;
            } else if (!type.equals(other.type))
                return false;
            return true;
        }

        @Override
        public String toString() {
            return "Query [name=" + name + ", type=" + type + ", metrics=" + metrics + ", tags=" + tags
                    + ", percentiles=" + percentiles + ", duration=" + duration + ", offset=" + offset + "]";
        }

        public static Builder builder(String name) {
            return new Builder(name);

        }

        public static Builder builder(String name, String offset, Query query) {
            return new Builder(name, offset, query);
        }

        public static class Builder {
            Query query;

            private Builder(String name) {
                this.query = new Query(name, null, null, null, null, null, null);
            }

            private Builder(String name, String offset, Query query) {
                this.query = new Query(name, offset, query);
            }

            public Builder duration(String duration) {
                this.query.setDuration(duration);
                return this;
            }

            public Builder metrics(Set<String> metrics) {
                this.query.setMetrics(metrics);
                return this;
            }

            public Builder metrics(String... metrics) {
                this.query.setMetrics(new HashSet<String>(Arrays.asList(metrics)));
                return this;
            }

            public Builder metric(String metric) {
                this.query.setMetrics(Collections.singleton(metric));
                return this;
            }

            public Builder offset(String offset) {
                this.query.setOffset(offset);
                return this;
            }

            public Builder percentiles(Set<String> percentiles) {
                this.query.setPercentiles(percentiles);
                return this;
            }

            public Builder tags(String tags) {
                this.query.setTags(tags);
                return this;
            }

            public Builder type(String type) {
                this.query.setType(type);
                return this;
            }

            public Builder type(MetricType<?> type) {
                this.query.setType(type.getText());
                return this;
            }

            public Query build() {
                return query;
            }

        }
    }

}
