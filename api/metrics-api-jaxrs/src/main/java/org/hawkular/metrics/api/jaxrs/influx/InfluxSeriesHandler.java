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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

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
import org.hawkular.metrics.api.jaxrs.callback.NoDataCallback;
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
import org.hawkular.metrics.api.jaxrs.util.StringValue;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.MetricType;
import org.hawkular.metrics.core.api.MetricsService;
import org.hawkular.metrics.core.api.NumericData;
import org.hawkular.metrics.core.api.NumericMetric;
import org.joda.time.Instant;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Some support for InfluxDB clients like Grafana.
 *
 * @author Heiko W. Rupp
 */
@Path("/tenants/{tenantId}/influx/series")
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
    public void write(@Suspended AsyncResponse asyncResponse, @PathParam("tenantId") String tenantId,
        List<InfluxObject> influxObjects) {
        if (influxObjects == null) {
            asyncResponse.resume(Response.status(Status.BAD_REQUEST).entity("Null objects").build());
            return;
        }
        try {
            objectValidator.validateInfluxObjects(influxObjects);
        } catch (InvalidObjectException e) {
            asyncResponse.resume(Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build());
            return;
        }
        List<NumericMetric> numericMetrics = FluentIterable.from(influxObjects) //
            .transform(influxObject -> {
                List<String> influxObjectColumns = influxObject.getColumns();
                int valueColumnIndex = influxObjectColumns.indexOf("value");
                List<List<?>> influxObjectPoints = influxObject.getPoints();
                NumericMetric numericMetric = new NumericMetric(tenantId, new MetricId(influxObject.getName()));
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
                        numericMetric.addData(new NumericData(timestamp, value));
                }
                return numericMetric;
            }).toList();
        ListenableFuture<Void> future = metricsService.addNumericData(numericMetrics);
        Futures.addCallback(future, new NoDataCallback<Void>(asyncResponse));
    }

    @GET
    public void query(@Suspended AsyncResponse asyncResponse, @PathParam("tenantId") String tenantId,
                       @QueryParam("q") String queryString) {

        if (queryString==null || queryString.isEmpty()) {
            asyncResponse.resume(Response.status(Status.BAD_REQUEST).entity("Missing query").build());
            return;
        }

        InfluxQueryParser queryParser = parserFactory.newInstanceForQuery(queryString);
        QueryContext queryContext = queryParser.query();

        QueryType queryType;
        try {
            queryType = new QueryTypeVisitor().visit(queryContext);
        } catch (QueryParseException e) {
            StringValue errMsg = new StringValue("Syntactically incorrect query: " + e.getMessage());
            asyncResponse.resume(Response.status(Status.BAD_REQUEST).entity(errMsg).build());
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
            StringValue errMsg = new StringValue("Query not yet supported: " + queryString);
            asyncResponse.resume(Response.status(Status.BAD_REQUEST).entity(errMsg).build());
        }
    }

    private void listSeries(AsyncResponse asyncResponse, String tenantId) {
        ListenableFuture<List<Metric<?>>> future = metricsService.findMetrics(tenantId, MetricType.NUMERIC);
        Futures.addCallback(future, new FutureCallback<List<Metric<?>>>() {
            @Override
            public void onSuccess(List<Metric<?>> result) {
                List<InfluxObject> objects = new ArrayList<>(result.size());

                for (Metric metric : result) {
                    List<String> columns = new ArrayList<>(2);
                    columns.add("time");
                    columns.add("sequence_number");
                    columns.add("val");
                    InfluxObject.Builder builder = new InfluxObject.Builder(metric.getId().getName(), columns)
                            .withForeseenPoints(0);
                    objects.add(builder.createInfluxObject());
                }

                ResponseBuilder builder = Response.ok(objects);

                asyncResponse.resume(builder.build());
            }

            @Override
            public void onFailure(Throwable t) {
                asyncResponse.resume(t);
            }
        });
    }

    private void select(AsyncResponse asyncResponse, String tenantId, SelectQueryContext selectQueryContext) {

        SelectQueryDefinitionsParser definitionsParser = new SelectQueryDefinitionsParser();
        parseTreeWalker.walk(definitionsParser, selectQueryContext);

        SelectQueryDefinitions queryDefinitions = definitionsParser.getSelectQueryDefinitions();

        try {
            queryValidator.validateSelectQuery(queryDefinitions);
        } catch (IllegalQueryException e) {
            StringValue errMsg = new StringValue("Illegal query: " + e.getMessage());
            asyncResponse.resume(Response.status(Status.BAD_REQUEST).entity(errMsg).build());
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
            StringValue errMsg = new StringValue("Invalid time interval");
            asyncResponse.resume(Response.status(Status.BAD_REQUEST).entity(errMsg).build());
            return;
        }
        String columnName = getColumnName(queryDefinitions);

        ListenableFuture<Boolean> idExistsFuture = metricsService.idExists(metric);
        ListenableFuture<List<NumericData>> loadMetricsFuture = Futures.transform(idExistsFuture,
                (AsyncFunction<Boolean, List<NumericData>>) idExists -> {
                    if (idExists != Boolean.TRUE) {
                        return Futures.immediateFuture(null);
                    }
                    return metricsService.findData(new NumericMetric(tenantId, new MetricId(metric)),
                        timeInterval.getStartMillis(), timeInterval.getEndMillis());
                });
        ListenableFuture<List<InfluxObject>> influxObjectTranslatorFuture = Futures.transform(loadMetricsFuture,
                (List<NumericData> metrics) -> {
                    if (metrics == null) {
                        return null;
                    }

                    if (shouldApplyMapping(queryDefinitions)) {
                        GroupByClause groupByClause = queryDefinitions.getGroupByClause();
                        InfluxTimeUnit bucketSizeUnit = groupByClause.getBucketSizeUnit();
                        long bucketSizeSec = bucketSizeUnit.convertTo(SECONDS, groupByClause.getBucketSize());
                        AggregatedColumnDefinition aggregatedColumnDefinition =
                                (AggregatedColumnDefinition) queryDefinitions
                                    .getColumnDefinitions().get(0);
                        metrics = applyMapping(aggregatedColumnDefinition.getAggregationFunction(),
                                aggregatedColumnDefinition.getAggregationFunctionArguments(), metrics,
                                (int) bucketSizeSec, timeInterval.getStartMillis(), timeInterval.getEndMillis());
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

                    for (NumericData m : metrics) {
                        List<Object> data = new ArrayList<>();
                        data.add(m.getTimestamp());
                        data.add(m.getValue());
                        builder.addPoint(data);
                    }

                    objects.add(builder.createInfluxObject());

                    return objects;
                });
        Futures.addCallback(influxObjectTranslatorFuture, new FutureCallback<List<InfluxObject>>() {
            @Override
            public void onSuccess(List<InfluxObject> objects) {
                if (objects == null) {
                    StringValue val = new StringValue("Metric with id [" + metric + "] not found. ");
                    asyncResponse.resume(Response.status(404).entity(val).build());
                } else {
                    ResponseBuilder builder = Response.ok(objects);
                    asyncResponse.resume(builder.build());
                }
            }

            @Override
            public void onFailure(Throwable t) {
                asyncResponse.resume(t);
            }
        });
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
     * @param aggregationFunction
     * @param aggregationFunctionArguments
     * @param in Input list of data
     * @param bucketLengthSec the length of the buckets
     * @param startTime Start time of the query
     * @param endTime  End time of the query
     * @return The mapped list of values, which could be the input or a longer or shorter list
     */
    private List<NumericData> applyMapping(String aggregationFunction,
        List<FunctionArgument> aggregationFunctionArguments, List<NumericData> in, int bucketLengthSec, long startTime,
        long endTime) {

        long timeDiff = endTime - startTime; // Millis
        int numBuckets = (int) ((timeDiff /1000 ) / bucketLengthSec);
        Map<Integer,List<NumericData>> tmpMap = new HashMap<>(numBuckets);

        // Bucketize
        for (NumericData rnm: in) {
            int pos = (int) ((rnm.getTimestamp()-startTime)/1000) /bucketLengthSec;
            List<NumericData> bucket = tmpMap.get(pos);
            if (bucket==null) {
                bucket = new ArrayList<>();
                tmpMap.put(pos, bucket);
            }
            bucket.add(rnm);
        }

        List<NumericData> out = new ArrayList<>(numBuckets);
        // Apply mapping to buckets to create final value
        SortedSet<Integer> keySet = new TreeSet<>(tmpMap.keySet());
        for (Integer pos: keySet ) {
            List<NumericData> list = tmpMap.get(pos);
            double retVal = 0.0;
            boolean isSingleValue = true;
            if (list!=null) {
                int size = list.size();
                NumericData lastElementInList = list.get(size - 1);
                NumericData firstElementInList = list.get(0);
                AggregationFunction function = AggregationFunction.findByName(aggregationFunction);
                switch (function) {
                case MEAN:
                    for (NumericData rnm : list) {
                        retVal += rnm.getValue();
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Applying mean mapping, total = {}, size = {}", retVal, size);
                    }
                    retVal /= size;
                    break;
                case MAX:
                    retVal = Double.MIN_VALUE;
                    for (NumericData rnm : list) {
                        if (rnm.getValue() > retVal) {
                            retVal = rnm.getValue();
                        }
                    }
                    break;
                case MIN:
                    retVal = Double.MAX_VALUE;
                    for (NumericData rnm : list) {
                        if (rnm.getValue() < retVal) {
                            retVal = rnm.getValue();
                        }
                    }
                    break;
                case SUM:
                    for (NumericData rnm : list) {
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
                        retVal = y/(double)t;
                    }
                    break;
                case MEDIAN:
                    retVal = quantil(list,50.0);
                    break;
                case PERCENTILE:
                    NumberFunctionArgument argument = (NumberFunctionArgument) aggregationFunctionArguments.get(1);
                    retVal = quantil(list, argument.getDoubleValue());
                    break;
                case TOP:
                    isSingleValue = false;
                    argument = (NumberFunctionArgument) aggregationFunctionArguments.get(1);
                    int numberOfTopElement = list.size() < (int)argument.getDoubleValue() ?
                            list.size() : (int)argument.getDoubleValue();
                    for(int elementPos =0; elementPos<numberOfTopElement; elementPos++){
                        out.add(list.get(elementPos));
                    }
                    break;
                case BOTTOM:
                    isSingleValue = false;
                    argument = (NumberFunctionArgument) aggregationFunctionArguments.get(1);
                    int numberOfBottomElement = list.size() < (int)argument.getDoubleValue() ?
                            list.size() : (int)argument.getDoubleValue();
                    for(int elementPos = 0; elementPos<numberOfBottomElement; elementPos++){
                        out.add(list.get(list.size() - 1 - elementPos));
                    }
                    break;
                case HISTOGRAM:
                case MODE:
                    int maxCount=0;
                    for (NumericData rnm : list) {
                        int count = 0;
                        for (NumericData rnm2 : list) {
                            if (rnm.getValue() == rnm2.getValue()){
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
                    for (NumericData rnm : list) {
                        meanValue += rnm.getValue();
                    }
                    meanValue /= size;
                    for (NumericData rnm : list) {
                        sd += Math.pow(rnm.getValue() - meanValue, 2) / (size - 1);
                    }
                    retVal = Math.sqrt(sd);
                    break;
                default:
                    LOG.warn("Mapping of '{}' function not yet supported", function);
                }
                if(isSingleValue){
                    out.add(new NumericData(firstElementInList.getTimestamp(), retVal));
                }
            }
        }

        return out;
    }

    /**
     * Determine the quantil of the data
     * @param in data for computation
     * @param val a value between 0 and 100 to determine the <i>val</i>th quantil
     * @return quantil from data
     */
    private double quantil (List<NumericData> in, double val) {
        int n = in.size();
        List<Double> bla = new ArrayList<>(n);
        for (NumericData rnm : in) {
            bla.add(rnm.getValue());
        }
        Collections.sort(bla);

        float x = (float) (n * (val/100));
        if (Math.floor(x)==x) {
            return 0.5*(bla.get((int) x-1) +  bla.get((int) (x)));
        }
        else {
            return bla.get((int) Math.ceil(x-1));
        }
    }
}
