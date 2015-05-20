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

import static java.util.concurrent.TimeUnit.SECONDS;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN_TYPE;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

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
import org.hawkular.metrics.api.jaxrs.influx.query.InfluxQueryParseTreeWalker;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.InfluxQueryParser;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.InfluxQueryParser.QueryContext;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.InfluxQueryParser.SelectQueryContext;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.InfluxQueryParserFactory;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.QueryParseException;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.AggregatedColumnDefinition;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.BooleanExpression;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.FunctionArgument;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.GroupByClause;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.InfluxTimeUnit;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.NumberFunctionArgument;
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
import org.hawkular.metrics.core.api.Gauge;
import org.hawkular.metrics.core.api.GaugeData;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.MetricType;
import org.hawkular.metrics.core.api.MetricsService;
import org.joda.time.Instant;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger LOG = LoggerFactory.getLogger(InfluxSeriesHandler.class);

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
            @Suspended AsyncResponse asyncResponse, @PathParam("tenantId") String tenantId,
            List<InfluxObject> influxObjects
    ) {

        if (influxObjects == null) {
            asyncResponse.resume(errorResponse(BAD_REQUEST, "Null objects"));
            return;
        }

        try {
            objectValidator.validateInfluxObjects(influxObjects);
        } catch (InvalidObjectException e) {
            asyncResponse.resume(errorResponse(BAD_REQUEST, Throwables.getRootCause(e).getMessage()));
            return;
        }

        Observable<Gauge> input = Observable.from(influxObjects)
                                            .map(InfluxSeriesHandler::influxObjectToGauge)
                                            .map(gauge -> (Gauge) gauge.setTenantId(tenantId));
        metricsService.addGaugeData(input).subscribe(new WriteObserver(asyncResponse));
    }

    private static Gauge influxObjectToGauge(InfluxObject influxObject) {
        List<String> influxObjectColumns = influxObject.getColumns();
        int valueColumnIndex = influxObjectColumns.indexOf("value");
        List<List<?>> influxObjectPoints = influxObject.getPoints();
        Gauge gaugeMetric = new Gauge(new MetricId(influxObject.getName()));
        for (List<?> point : influxObjectPoints) {
            double value;
            long timestamp;
            if (influxObjectColumns.size() == 1) {
                timestamp = System.currentTimeMillis();
                value = ((Number) point.get(0)).doubleValue();
            } else {
                timestamp = ((Number) point.get((valueColumnIndex + 1) % 2)).longValue();
                value = ((Number) point.get(valueColumnIndex)).doubleValue();
            }
            gaugeMetric.addData(new GaugeData(timestamp, value));
        }
        return gaugeMetric;
    }

    @GET
    public void query(
            @Suspended AsyncResponse asyncResponse, @PathParam("tenantId") String tenantId,
            @QueryParam("q") String queryString
    ) {

        if (queryString == null || queryString.isEmpty()) {
            asyncResponse.resume(errorResponse(BAD_REQUEST, "Missing query"));
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
        } catch (Exception e) {
            asyncResponse.resume(e);
            return;
        }

        switch (queryType) {
        case LIST_SERIES:
            listSeries(asyncResponse, tenantId);
            break;
        case SELECT:
            select(asyncResponse, tenantId, queryContext.selectQuery());
            break;
        default:
            asyncResponse.resume(errorResponse(BAD_REQUEST, "Query not yet supported: " + queryString));
        }
    }

    private void listSeries(AsyncResponse asyncResponse, String tenantId) {
        metricsService.findMetrics(tenantId, MetricType.GAUGE)
                      .toList()
                      .map(InfluxSeriesHandler::metricsListToListSeries)
                      .subscribe(new ReadObserver(asyncResponse));
    }

    private static List<InfluxObject> metricsListToListSeries(List<Metric<?>> metrics) {
        List<String> columns = ImmutableList.of("time", "name");
        InfluxObject.Builder builder = new InfluxObject.Builder("list_series_result", columns)
                .withForeseenPoints(metrics.size());
        for (Metric metric : metrics) {
            builder.addPoint(ImmutableList.of(0, metric.getId().getName()));
        }
        return ImmutableList.of(builder.createInfluxObject());
    }

    private void select(AsyncResponse asyncResponse, String tenantId, SelectQueryContext selectQueryContext) {

        SelectQueryDefinitionsParser definitionsParser = new SelectQueryDefinitionsParser();
        parseTreeWalker.walk(definitionsParser, selectQueryContext);

        SelectQueryDefinitions queryDefinitions = definitionsParser.getSelectQueryDefinitions();

        try {
            queryValidator.validateSelectQuery(queryDefinitions);
        } catch (IllegalQueryException e) {
            asyncResponse.resume(errorResponse(BAD_REQUEST, "Illegal query: " + e.getMessage()));
            return;
        }

        String metric = queryDefinitions.getFromClause().getName(); // metric to query from backend
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

        Observable.from(metricsService.idExists(metric))
                  .flatMap(
                          idExists -> {
                              if (idExists != Boolean.TRUE) {
                                  return Observable.just(null);
                              }
                              return metricsService.findGaugeData(
                                      tenantId, new MetricId(metric),
                                      timeInterval.getStartMillis(), timeInterval.getEndMillis()
                              ).toList();
                          }
                  ).map(
                metrics -> {
                    if (metrics == null) {
                        return null;
                    }

                    if (shouldApplyMapping(queryDefinitions)) {
                        GroupByClause groupByClause = queryDefinitions.getGroupByClause();
                        InfluxTimeUnit bucketSizeUnit = groupByClause.getBucketSizeUnit();
                        long bucketSizeSec = bucketSizeUnit.convertTo(
                                SECONDS,
                                groupByClause.getBucketSize()
                        );
                        AggregatedColumnDefinition aggregatedColumnDefinition =
                                (AggregatedColumnDefinition) queryDefinitions
                                        .getColumnDefinitions().get(0);
                        metrics = applyMapping(
                                aggregatedColumnDefinition.getAggregationFunction(),
                                aggregatedColumnDefinition.getAggregationFunctionArguments(),
                                metrics,
                                (int) bucketSizeSec,
                                timeInterval.getStartMillis(),
                                timeInterval.getEndMillis()
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

                    InfluxObject.Builder builder = new InfluxObject.Builder(metric, columns)
                            .withForeseenPoints(metrics.size());

                    for (GaugeData m : metrics) {
                        List<Object> data = new ArrayList<>();
                        data.add(m.getTimestamp());
                        data.add(m.getValue());
                        builder.addPoint(data);
                    }

                    objects.add(builder.createInfluxObject());

                    return objects;
                }
        ).subscribe(
                objects -> {
                    if (objects == null) {
                        String msg = "Metric with id [" + metric + "] not found. ";
                        asyncResponse.resume(errorResponse(NOT_FOUND, msg));
                    } else {
                        ResponseBuilder builder = Response.ok(objects);
                        asyncResponse.resume(builder.build());
                    }
                }, asyncResponse::resume
        );
    }

    private boolean shouldApplyMapping(SelectQueryDefinitions queryDefinitions) {
        return !queryDefinitions.isStarColumn()
               && queryDefinitions.getColumnDefinitions().get(0) instanceof AggregatedColumnDefinition
               && queryDefinitions.getGroupByClause() != null;
    }

    private String getColumnName(SelectQueryDefinitions queryDefinitions) {
        if (queryDefinitions.isStarColumn()) {
            return "value";
        }
        return queryDefinitions.getColumnDefinitions().get(0).getDisplayName();
    }

    /**
     * Apply a mapping function to the incoming data
     *
     * @param aggregationFunction
     * @param aggregationFunctionArguments
     * @param in                           Input list of data
     * @param bucketLengthSec              the length of the buckets
     * @param startTime                    Start time of the query
     * @param endTime                      End time of the query
     *
     * @return The mapped list of values, which could be the input or a longer or shorter list
     */
    private List<GaugeData> applyMapping(
            String aggregationFunction,
            List<FunctionArgument> aggregationFunctionArguments, List<GaugeData> in, int bucketLengthSec,
            long startTime,
            long endTime
    ) {

        long timeDiff = endTime - startTime; // Millis
        int numBuckets = (int) ((timeDiff / 1000) / bucketLengthSec);
        Map<Integer, List<GaugeData>> tmpMap = new HashMap<>(numBuckets);

        // Bucketize
        for (GaugeData rnm : in) {
            int pos = (int) ((rnm.getTimestamp() - startTime) / 1000) / bucketLengthSec;
            List<GaugeData> bucket = tmpMap.get(pos);
            if (bucket == null) {
                bucket = new ArrayList<>();
                tmpMap.put(pos, bucket);
            }
            bucket.add(rnm);
        }

        List<GaugeData> out = new ArrayList<>(numBuckets);
        // Apply mapping to buckets to create final value
        SortedSet<Integer> keySet = new TreeSet<>(tmpMap.keySet());
        for (Integer pos : keySet) {
            List<GaugeData> list = tmpMap.get(pos);
            double retVal = 0.0;
            boolean isSingleValue = true;
            if (list != null) {
                int size = list.size();
                GaugeData lastElementInList = list.get(size - 1);
                GaugeData firstElementInList = list.get(0);
                AggregationFunction function = AggregationFunction.findByName(aggregationFunction);
                switch (function) {
                case MEAN:
                    for (GaugeData rnm : list) {
                        retVal += rnm.getValue();
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Applying mean mapping, total = {}, size = {}", retVal, size);
                    }
                    retVal /= size;
                    break;
                case MAX:
                    retVal = Double.MIN_VALUE;
                    for (GaugeData rnm : list) {
                        if (rnm.getValue() > retVal) {
                            retVal = rnm.getValue();
                        }
                    }
                    break;
                case MIN:
                    retVal = Double.MAX_VALUE;
                    for (GaugeData rnm : list) {
                        if (rnm.getValue() < retVal) {
                            retVal = rnm.getValue();
                        }
                    }
                    break;
                case SUM:
                    for (GaugeData rnm : list) {
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
                    for (GaugeData rnm : list) {
                        int count = 0;
                        for (GaugeData rnm2 : list) {
                            if (rnm.getValue() == rnm2.getValue()) {
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
                    for (GaugeData rnm : list) {
                        meanValue += rnm.getValue();
                    }
                    meanValue /= size;
                    for (GaugeData rnm : list) {
                        sd += Math.pow(rnm.getValue() - meanValue, 2) / (size - 1);
                    }
                    retVal = Math.sqrt(sd);
                    break;
                default:
                    LOG.warn("Mapping of '{}' function not yet supported", function);
                }
                if (isSingleValue) {
                    out.add(new GaugeData(firstElementInList.getTimestamp(), retVal));
                }
            }
        }

        return out;
    }

    /**
     * Determine the quantil of the data
     *
     * @param in  data for computation
     * @param val a value between 0 and 100 to determine the <i>val</i>th quantil
     *
     * @return quantil from data
     */
    private double quantil(List<GaugeData> in, double val) {
        int n = in.size();
        List<Double> bla = new ArrayList<>(n);
        bla.addAll(in.stream().map(GaugeData::getValue).sorted().collect(Collectors.toList()));
        float x = (float) (n * (val / 100));
        if (Math.floor(x) == x) {
            return 0.5 * (bla.get((int) x - 1) + bla.get((int) (x)));
        } else {
            return bla.get((int) Math.ceil(x - 1));
        }
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
}
