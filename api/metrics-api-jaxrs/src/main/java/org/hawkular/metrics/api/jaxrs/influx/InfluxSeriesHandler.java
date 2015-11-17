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
package org.hawkular.metrics.api.jaxrs.influx;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN_TYPE;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

import static org.hawkular.metrics.core.api.MetricType.COUNTER;
import static org.hawkular.metrics.core.api.MetricType.GAUGE;
import static org.hawkular.metrics.core.api.MetricTypeFilter.COUNTER_FILTER;
import static org.hawkular.metrics.core.api.MetricTypeFilter.GAUGE_FILTER;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;

import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.hawkular.metrics.api.jaxrs.influx.query.InfluxQueryParseTreeWalker;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.InfluxQueryParser;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.InfluxQueryParser.ListSeriesContext;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.InfluxQueryParser.QueryContext;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.InfluxQueryParser.SelectQueryContext;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.InfluxQueryParserFactory;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.QueryParseException;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.AggregatedColumnDefinition;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.BooleanExpression;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.FunctionArgument;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.GroupByClause;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.ListSeriesDefinitionsParser;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.NumberFunctionArgument;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.RegularExpression;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.SelectQueryDefinitions;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.SelectQueryDefinitionsParser;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.type.QueryType;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.type.QueryTypeVisitor;
import org.hawkular.metrics.api.jaxrs.influx.query.translate.ToIntervalTranslator;
import org.hawkular.metrics.api.jaxrs.influx.query.validation.AggregationFunction;
import org.hawkular.metrics.api.jaxrs.influx.query.validation.IllegalQueryException;
import org.hawkular.metrics.api.jaxrs.influx.query.validation.QueryValidator;
import org.hawkular.metrics.api.jaxrs.influx.write.validation.InfluxObjectValidator;
import org.hawkular.metrics.api.jaxrs.influx.write.validation.InvalidObjectException;
import org.hawkular.metrics.core.api.Buckets;
import org.hawkular.metrics.core.api.DataPoint;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.MetricType;
import org.hawkular.metrics.core.api.MetricsService;
import org.jboss.logging.Logger;
import org.joda.time.Instant;
import org.joda.time.Interval;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import rx.Observable;
import rx.Observer;

/**
 * Some support for InfluxDB clients like Grafana.
 *
 * @author Heiko W. Rupp
 */
@Path("/db/{tenantId}/series")
@Produces(APPLICATION_JSON)
@ApplicationScoped
public class InfluxSeriesHandler {
    private static final Logger log = Logger.getLogger(InfluxSeriesHandler.class);

    private static final EnumSet<InfluxTimeUnit> TIME_PRECISION_ALLOWED =
            EnumSet.of(InfluxTimeUnit.SECONDS, InfluxTimeUnit.MILLISECONDS, InfluxTimeUnit.MICROSECONDS);

    private static final String GAUGE_PREFIX = "_gauge.";
    private static final String COUNTER_PREFIX = "_counter.";

    @Inject
    MetricsService metricsService;
    @Inject
    InfluxObjectValidator objectValidator;
    @Inject
    @InfluxQueryParseTreeWalker
    ParseTreeWalker parseTreeWalker;
    @Inject
    InfluxQueryParserFactory parserFactory;
    @Inject
    QueryValidator queryValidator;
    @Inject
    ToIntervalTranslator toIntervalTranslator;

    @POST
    @Consumes(APPLICATION_JSON)
    public void write(
            @Suspended AsyncResponse asyncResponse,
            @PathParam("tenantId") String tenantId,
            @QueryParam("time_precision") InfluxTimeUnit timePrecision,
            List<InfluxObject> influxObjects
    ) {
        if (influxObjects == null) {
            asyncResponse.resume(errorResponse(BAD_REQUEST, "Null objects"));
            return;
        }

        if (timePrecision != null && !TIME_PRECISION_ALLOWED.contains(timePrecision)) {
            asyncResponse.resume(errorResponse(BAD_REQUEST, "Invalid time precision: " + timePrecision));
            return;
        }

        try {
            objectValidator.validateInfluxObjects(influxObjects);
        } catch (InvalidObjectException e) {
            asyncResponse.resume(errorResponse(BAD_REQUEST, Throwables.getRootCause(e).getMessage()));
            return;
        }

        Map<MetricType<?>, List<Metric<?>>> metrics = influxObjects.stream()
                .map(influxObject -> influxToMetrics(tenantId, influxObject, timePrecision))
                .collect(groupingBy(metric -> metric.getId().getType()));

        Observable<Void> result = Observable.empty();
        if (metrics.containsKey(GAUGE)) {
            result = result.mergeWith(metricsService.addDataPoints(GAUGE, Observable.from(metrics.get(GAUGE))
                    .compose(GAUGE_FILTER)));
        }
        if (metrics.containsKey(COUNTER)) {
            result = result.mergeWith(metricsService.addDataPoints(COUNTER, Observable.from(metrics.get(COUNTER))
                    .compose(COUNTER_FILTER)));
        }
        result.subscribe(new WriteObserver(asyncResponse));
    }

    private static Metric<?> influxToMetrics(String tenantId, InfluxObject influxObject, InfluxTimeUnit timePrecision) {
        MetricTypeAndName metricTypeAndName = new MetricTypeAndName(influxObject.getName());
        MetricType<?> type = metricTypeAndName.getType();
        String name = metricTypeAndName.getName();

        List<String> influxObjectColumns = influxObject.getColumns();
        int valueColumnIndex = influxObjectColumns.indexOf("value");

        Stream<DataPoint<Number>> dataPoints = influxObject.getPoints().stream().map(objects -> {
            Number value;
            long timestamp;
            if (influxObjectColumns.size() == 1) {
                timestamp = System.currentTimeMillis();
                value = (Number) objects.get(0);
            } else {
                timestamp = ((Number) objects.get((valueColumnIndex + 1) % 2)).longValue();
                if (timePrecision != null) {
                    timestamp = timePrecision.convertTo(MILLISECONDS, timestamp);
                }
                value = (Number) objects.get(valueColumnIndex);
            }
            return new DataPoint<>(timestamp, value);
        });

        if (type == COUNTER) {
            List<DataPoint<Long>> counterPoints = dataPoints.map(p -> {
                return new DataPoint<>(p.getTimestamp(), p.getValue().longValue());
            }).collect(toList());
            return new Metric<>(new MetricId<>(tenantId, COUNTER, name), counterPoints);
        }
        List<DataPoint<Double>> gaugePoints = dataPoints.map(p -> {
            return new DataPoint<>(p.getTimestamp(), p.getValue().doubleValue());
        }).collect(toList());
        return new Metric<>(new MetricId<>(tenantId, GAUGE, name), gaugePoints);
    }

    @GET
    public void query(
            @Suspended AsyncResponse asyncResponse,
            @PathParam("tenantId") String tenantId,
            @QueryParam("q") String queryString,
            @QueryParam("time_precision") InfluxTimeUnit timePrecision
    ) {

        if (queryString == null || queryString.isEmpty()) {
            asyncResponse.resume(errorResponse(BAD_REQUEST, "Missing query"));
            return;
        }

        if (timePrecision != null && !TIME_PRECISION_ALLOWED.contains(timePrecision)) {
            asyncResponse.resume(errorResponse(BAD_REQUEST, "Invalid time precision: " + timePrecision));
            return;
        }

        InfluxQueryParser queryParser = parserFactory.newInstanceForQuery(queryString);

        QueryContext queryContext;
        QueryType queryType;
        try {
            queryContext = queryParser.query();
            queryType = new QueryTypeVisitor().visit(queryContext);
        } catch (QueryParseException e) {
            asyncResponse.resume(errorResponse(BAD_REQUEST, "Syntactically incorrect query: " + e.getMessage()));
            return;
        }

        switch (queryType) {
        case LIST_SERIES:
            listSeries(asyncResponse, tenantId, queryContext.listSeries());
            break;
        case SELECT:
            select(asyncResponse, tenantId, queryContext.selectQuery(), timePrecision);
            break;
        default:
            asyncResponse.resume(errorResponse(BAD_REQUEST, "Query not yet supported: " + queryString));
        }
    }

    private void listSeries(AsyncResponse asyncResponse, String tenantId, ListSeriesContext listSeriesContext) {
        ListSeriesDefinitionsParser definitionsParser = new ListSeriesDefinitionsParser();
        parseTreeWalker.walk(definitionsParser, listSeriesContext);

        RegularExpression regularExpression = definitionsParser.getRegularExpression();
        final Pattern pattern;
        if (regularExpression != null) {
            int flag = regularExpression.isCaseSensitive() ? 0 : Pattern.CASE_INSENSITIVE;
            try {
                pattern = Pattern.compile(regularExpression.getExpression(), flag);
            } catch (Exception e) {
                asyncResponse.resume(errorResponse(BAD_REQUEST, Throwables.getRootCause(e).getMessage()));
                return;
            }
        } else {
            pattern = null;
        }

        Observable.merge(metricsService.findMetrics(tenantId, GAUGE), metricsService.findMetrics(tenantId, COUNTER))
                .filter(metric -> pattern == null || pattern.matcher(metric.getId().getName()).find())
                .toList()
                .map(InfluxSeriesHandler::metricsListToListSeries)
                .subscribe(new ReadObserver(asyncResponse));
    }

    private static List<InfluxObject> metricsListToListSeries(List<? extends Metric<?>> metrics) {
        List<String> columns = ImmutableList.of("time", "name");
        InfluxObject.Builder builder = new InfluxObject.Builder("list_series_result", columns)
                .withForeseenPoints(metrics.size());
        for (Metric<?> metric : metrics) {
            String prefix;
            MetricType<?> type = metric.getId().getType();
            if (type == GAUGE) {
                prefix = GAUGE_PREFIX;
            } else if (type == COUNTER) {
                prefix = COUNTER_PREFIX;
            } else {
                log.tracef("List series query does not expect %s metric type", type);
                continue;
            }
            builder.addPoint(ImmutableList.of(0, prefix + metric.getId().getName()));
        }
        return ImmutableList.of(builder.createInfluxObject());
    }

    private void select(AsyncResponse asyncResponse, String tenantId, SelectQueryContext selectQueryContext,
            InfluxTimeUnit timePrecision) {

        SelectQueryDefinitionsParser definitionsParser = new SelectQueryDefinitionsParser();
        parseTreeWalker.walk(definitionsParser, selectQueryContext);

        SelectQueryDefinitions queryDefinitions = definitionsParser.getSelectQueryDefinitions();

        try {
            queryValidator.validateSelectQuery(queryDefinitions);
        } catch (IllegalQueryException e) {
            asyncResponse.resume(errorResponse(BAD_REQUEST, "Illegal query: " + e.getMessage()));
            return;
        }

        String influxObjectName = queryDefinitions.getFromClause().getName();
        MetricTypeAndName metricTypeAndName = new MetricTypeAndName(influxObjectName);
        MetricType<?> metricType = metricTypeAndName.getType();
        String metricName = metricTypeAndName.getName();

        BooleanExpression whereClause = queryDefinitions.getWhereClause();
        Interval timeInterval;
        if (whereClause == null) {
            timeInterval = new Interval(new Instant(0), Instant.now());
        } else {
            timeInterval = toIntervalTranslator.toInterval(whereClause);
        }
        if (timeInterval == null) {
            asyncResponse.resume(errorResponse(BAD_REQUEST, "Invalid time interval"));
            return;
        }
        String columnName = getColumnName(queryDefinitions);
        Buckets buckets;
        try {
            buckets = getBucketConfig(queryDefinitions, timeInterval);
        } catch (IllegalArgumentException e) {
            asyncResponse.resume(errorResponse(BAD_REQUEST, e.getMessage()));
            return;
        }

        metricsService.idExists(new MetricId<>(tenantId, metricType, metricName)).flatMap(idExists -> {
            if (idExists != Boolean.TRUE) {
                return Observable.just(null);
            }
            long start = timeInterval.getStartMillis();
            long end = timeInterval.getEndMillis();
            MetricId<? extends Number> metricId;
            if (metricType == GAUGE) {
                metricId = new MetricId<>(tenantId, GAUGE, metricName);
                return metricsService.findDataPoints(metricId, start, end).toList();
            }
            if (metricType == COUNTER) {
                metricId = new MetricId<>(tenantId, COUNTER, metricName);
                return metricsService.findDataPoints(metricId, start, end).toSortedList((dataPoint, dataPoint2) -> {
                    return Long.compare(dataPoint2.getTimestamp(), dataPoint.getTimestamp());
                });
            }
            return Observable.just(null);
        }).map(metrics -> {
            if (metrics == null) {
                return null;
            }

            if (buckets != null) {
                AggregatedColumnDefinition aggregatedColumnDefinition =
                        (AggregatedColumnDefinition) queryDefinitions
                                .getColumnDefinitions().get(0);
                metrics = applyMapping(
                        aggregatedColumnDefinition.getAggregationFunction(),
                        aggregatedColumnDefinition.getAggregationFunctionArguments(),
                        metrics,
                        buckets
                );
            }

            if (!queryDefinitions.isOrderDesc()) {
                metrics = Lists.reverse(metrics);
            }

            if (queryDefinitions.getLimitClause() != null) {
                metrics = metrics.subList(0, queryDefinitions.getLimitClause().getLimit());
            }

            List<InfluxObject> objects = new ArrayList<>(1);

            List<String> columns = new ArrayList<>(2);
            columns.add("time");
            columns.add(columnName);

            InfluxObject.Builder builder = new InfluxObject.Builder(influxObjectName, columns)
                    .withForeseenPoints(metrics.size());

            for (DataPoint<? extends Number> m : metrics) {
                List<Object> data = new ArrayList<>();
                if (timePrecision == null) {
                    data.add(m.getTimestamp());
                } else {
                    data.add(timePrecision.convert(m.getTimestamp(), InfluxTimeUnit.MILLISECONDS));
                }
                data.add(m.getValue());
                builder.addPoint(data);
            }

            objects.add(builder.createInfluxObject());

            return objects;
        }).subscribe(objects -> {
            if (objects == null) {
                String msg = "Metric with id [" + influxObjectName + "] not found. ";
                asyncResponse.resume(errorResponse(NOT_FOUND, msg));
            } else {
                ResponseBuilder builder = Response.ok(objects);
                asyncResponse.resume(builder.build());
            }
        }, asyncResponse::resume);
    }

    private String getColumnName(SelectQueryDefinitions queryDefinitions) {
        if (queryDefinitions.isStarColumn()) {
            return "value";
        }
        return queryDefinitions.getColumnDefinitions().get(0).getDisplayName();
    }

    private Buckets getBucketConfig(SelectQueryDefinitions queryDefinitions, Interval timeInterval) {
        if (queryDefinitions.isStarColumn()
                || !(queryDefinitions.getColumnDefinitions().get(0) instanceof AggregatedColumnDefinition)) {
            return null;
        }
        GroupByClause groupByClause = queryDefinitions.getGroupByClause();
        InfluxTimeUnit bucketSizeUnit = groupByClause.getBucketSizeUnit();
        long bucketSize = bucketSizeUnit.convertTo(MILLISECONDS, groupByClause.getBucketSize());
        return Buckets.fromStep(timeInterval.getStartMillis(), timeInterval.getEndMillis(), bucketSize);
    }

    private List<? extends DataPoint<? extends Number>> applyMapping(
            String aggregationFunction,
            List<FunctionArgument> aggregationFunctionArguments,
            List<? extends DataPoint<? extends Number>> in,
            Buckets buckets
    ) {
        Map<Integer, List<DataPoint<Double>>> tmpMap = new HashMap<>(buckets.getCount());

        // Bucketize
        for (DataPoint<? extends Number> rnm : in) {
            int pos = (int) ((rnm.getTimestamp() - buckets.getStart()) / buckets.getStep());
            List<DataPoint<Double>> bucket = tmpMap.get(pos);
            if (bucket == null) {
                bucket = new ArrayList<>();
                tmpMap.put(pos, bucket);
            }
            bucket.add(new DataPoint<>(rnm.getTimestamp(), rnm.getValue().doubleValue()));
        }

        List<DataPoint<Double>> out = new ArrayList<>(buckets.getCount());
        // Apply mapping to buckets to create final value
        SortedSet<Integer> keySet = new TreeSet<>(tmpMap.keySet());
        for (Integer pos : keySet) {
            List<DataPoint<Double>> list = tmpMap.get(pos);
            double retVal = 0.0;
            boolean isSingleValue = true;
            if (list != null) {
                int size = list.size();
                DataPoint<Double> lastElementInList = list.get(size - 1);
                DataPoint<Double> firstElementInList = list.get(0);
                AggregationFunction function = AggregationFunction.findByName(aggregationFunction);
                switch (function) {
                case MEAN:
                    for (DataPoint<Double> rnm : list) {
                        retVal += rnm.getValue();
                    }
                    log.debugf("Applying mean mapping, total = %f, size = %d", retVal, size);
                    retVal /= size;
                    break;
                case MAX:
                    retVal = Double.MIN_VALUE;
                    for (DataPoint<Double> rnm : list) {
                        if (rnm.getValue() > retVal) {
                            retVal = rnm.getValue();
                        }
                    }
                    break;
                case MIN:
                    retVal = Double.MAX_VALUE;
                    for (DataPoint<Double> rnm : list) {
                        if (rnm.getValue() < retVal) {
                            retVal = rnm.getValue();
                        }
                    }
                    break;
                case SUM:
                    for (DataPoint<Double> rnm : list) {
                        retVal += rnm.getValue();
                    }
                    break;
                case COUNT:
                    retVal = size;
                    break;
                case FIRST:
                    if (!list.isEmpty()) {
                        retVal = firstElementInList.getValue();
                    }
                    break;
                case LAST:
                    if (!list.isEmpty()) {
                        retVal = lastElementInList.getValue();
                    }
                    break;
                case DIFFERENCE:
                    if (!list.isEmpty()) {
                        retVal = (lastElementInList.getValue()) - (firstElementInList.getValue());
                    }
                    break;
                case DERIVATIVE:
                    if (!list.isEmpty()) {
                        double y = (lastElementInList.getValue()) - (firstElementInList.getValue());
                        long t = (lastElementInList.getTimestamp() - (firstElementInList.getTimestamp())) / 1000; // sec
                        retVal = y / (double) t;
                    }
                    break;
                case MEDIAN:
                    retVal = quantil(list, 50.0);
                    break;
                case PERCENTILE:
                    NumberFunctionArgument argument = (NumberFunctionArgument) aggregationFunctionArguments.get(1);
                    retVal = quantil(list, argument.getDoubleValue());
                    break;
                case TOP:
                    isSingleValue = false;
                    argument = (NumberFunctionArgument) aggregationFunctionArguments.get(1);
                    int numberOfTopElement = list.size() < (int) argument.getDoubleValue() ?
                            list.size() : (int) argument.getDoubleValue();
                    for (int elementPos = 0; elementPos < numberOfTopElement; elementPos++) {
                        out.add(list.get(elementPos));
                    }
                    break;
                case BOTTOM:
                    isSingleValue = false;
                    argument = (NumberFunctionArgument) aggregationFunctionArguments.get(1);
                    int numberOfBottomElement = list.size() < (int) argument.getDoubleValue() ?
                            list.size() : (int) argument.getDoubleValue();
                    for (int elementPos = 0; elementPos < numberOfBottomElement; elementPos++) {
                        out.add(list.get(list.size() - 1 - elementPos));
                    }
                    break;
                case HISTOGRAM:
                case MODE:
                    int maxCount = 0;
                    for (DataPoint<Double> rnm : list) {
                        int count = 0;
                        for (DataPoint<Double> rnm2 : list) {
                            if (rnm.getValue().doubleValue() == rnm2.getValue().doubleValue()) {
                                ++count;
                            }
                        }
                        if (count > maxCount) {
                            maxCount = count;
                            retVal = rnm.getValue();
                        }
                    }
                    break;
                case STDDEV:
                    double meanValue = 0.0;
                    double sd = 0.0;
                    for (DataPoint<Double> rnm : list) {
                        meanValue += rnm.getValue();
                    }
                    meanValue /= size;
                    for (DataPoint<Double> rnm : list) {
                        sd += Math.pow(rnm.getValue() - meanValue, 2) / (size - 1);
                    }
                    retVal = Math.sqrt(sd);
                    break;
                default:
                    log.warnf("Mapping of '%s' function not supported yet", function);
                }
                if (isSingleValue) {
                    out.add(new DataPoint<>(firstElementInList.getTimestamp(), retVal));
                }
            }
        }

        return out;
    }

    private double quantil(List<DataPoint<Double>> in, double quantil) {
        double[] values = new double[in.size()];
        for (int i = 0; i < in.size(); i++) {
            values[i] = in.get(i).getValue();
        }
        return new Percentile(quantil).evaluate(values);
    }

    private Response errorResponse(Status status, String message) {
        return Response.status(status).entity(message).type(TEXT_PLAIN_TYPE).build();
    }

    private class WriteObserver implements Observer<Void> {
        private final AsyncResponse asyncResponse;

        public WriteObserver(AsyncResponse asyncResponse) {
            this.asyncResponse = asyncResponse;
        }

        @Override
        public void onCompleted() {
            asyncResponse.resume(Response.ok().build());
        }

        @Override
        public void onError(Throwable t) {
            log.tracef(t, "Influx write query error");
            asyncResponse.resume(errorResponse(INTERNAL_SERVER_ERROR, Throwables.getRootCause(t).getMessage()));
        }

        @Override
        public void onNext(Void aVoid) {
        }
    }

    private class ReadObserver implements Observer<List<InfluxObject>> {
        private final AsyncResponse asyncResponse;

        public ReadObserver(AsyncResponse asyncResponse) {
            this.asyncResponse = asyncResponse;
        }

        @Override
        public void onCompleted() {

        }

        @Override
        public void onError(Throwable t) {
            asyncResponse.resume(errorResponse(INTERNAL_SERVER_ERROR, Throwables.getRootCause(t).getMessage()));
        }

        @Override
        public void onNext(List<InfluxObject> influxObjects) {
            asyncResponse.resume(Response.ok(influxObjects).build());
        }
    }

    private static class MetricTypeAndName {
        private final MetricType<?> type;
        private final String name;

        public MetricTypeAndName(String influxObjectName) {
            if (influxObjectName.startsWith(COUNTER_PREFIX)) {
                type = COUNTER;
                name = influxObjectName.substring(COUNTER_PREFIX.length());
            } else {
                type = GAUGE;
                if (influxObjectName.startsWith(GAUGE_PREFIX)) {
                    name = influxObjectName.substring(GAUGE_PREFIX.length());
                } else {
                    name = influxObjectName;
                }
            }
        }

        public MetricType<?> getType() {
            return type;
        }

        public String getName() {
            return name;
        }
    }
}
