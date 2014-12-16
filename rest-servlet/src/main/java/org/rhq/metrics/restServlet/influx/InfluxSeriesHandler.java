package org.rhq.metrics.restServlet.influx;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.rhq.metrics.core.MetricsService.DEFAULT_TENANT_ID;
import static org.rhq.metrics.restServlet.influx.query.parse.InfluxQueryParser.QueryContext;
import static org.rhq.metrics.restServlet.influx.query.parse.InfluxQueryParser.SelectQueryContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import javax.annotation.Resource;
import javax.enterprise.concurrent.ManagedExecutorService;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Response;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.joda.time.Instant;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.rhq.metrics.core.Metric;
import org.rhq.metrics.core.MetricId;
import org.rhq.metrics.core.MetricType;
import org.rhq.metrics.core.MetricsService;
import org.rhq.metrics.core.NumericData;
import org.rhq.metrics.core.NumericMetric2;
import org.rhq.metrics.restServlet.StringValue;
import org.rhq.metrics.restServlet.influx.query.InfluxQueryParseTreeWalker;
import org.rhq.metrics.restServlet.influx.query.parse.InfluxQueryParser;
import org.rhq.metrics.restServlet.influx.query.parse.InfluxQueryParserFactory;
import org.rhq.metrics.restServlet.influx.query.parse.QueryParseException;
import org.rhq.metrics.restServlet.influx.query.parse.definition.AggregatedColumnDefinition;
import org.rhq.metrics.restServlet.influx.query.parse.definition.BooleanExpression;
import org.rhq.metrics.restServlet.influx.query.parse.definition.FunctionArgument;
import org.rhq.metrics.restServlet.influx.query.parse.definition.GroupByClause;
import org.rhq.metrics.restServlet.influx.query.parse.definition.InfluxTimeUnit;
import org.rhq.metrics.restServlet.influx.query.parse.definition.NumberFunctionArgument;
import org.rhq.metrics.restServlet.influx.query.parse.definition.SelectQueryDefinitions;
import org.rhq.metrics.restServlet.influx.query.parse.definition.SelectQueryDefinitionsParser;
import org.rhq.metrics.restServlet.influx.query.parse.type.QueryType;
import org.rhq.metrics.restServlet.influx.query.parse.type.QueryTypeVisitor;
import org.rhq.metrics.restServlet.influx.query.translate.ToIntervalTranslator;
import org.rhq.metrics.restServlet.influx.query.validation.AggregationFunction;
import org.rhq.metrics.restServlet.influx.query.validation.IllegalQueryException;
import org.rhq.metrics.restServlet.influx.query.validation.QueryValidator;

/**
 * Some support for InfluxDB clients like Grafana.
 *
 * @author Heiko W. Rupp
 */
@Path("/tenants/{tenantId}/influx/series")
@Produces("application/json")
@ApplicationScoped
public class InfluxSeriesHandler {
    private static final Logger LOG = LoggerFactory.getLogger(InfluxSeriesHandler.class);

    @Inject
    private MetricsService metricsService;
    @Inject
    @InfluxQueryParseTreeWalker
    private ParseTreeWalker parseTreeWalker;
    @Inject
    private InfluxQueryParserFactory parserFactory;
    @Inject
    private QueryValidator queryValidator;
    @Inject
    private ToIntervalTranslator toIntervalTranslator;

    @Resource
    private ManagedExecutorService executor;

    @GET
    public void series(@Suspended AsyncResponse asyncResponse, @PathParam("tenantId") String tenantId,
                       @QueryParam("q") String queryString) {

        if (queryString==null || queryString.isEmpty()) {
            asyncResponse.resume(Response.status(Response.Status.BAD_REQUEST).entity("Missing query").build());
            return;
        }

        InfluxQueryParser queryParser = parserFactory.newInstanceForQuery(queryString);
        QueryContext queryContext = queryParser.query();

        QueryType queryType;
        try {
            queryType = new QueryTypeVisitor().visit(queryContext);
        } catch (QueryParseException e) {
            StringValue errMsg = new StringValue("Syntactically incorrect query: " + e.getMessage());
            asyncResponse.resume(Response.status(Response.Status.BAD_REQUEST).entity(errMsg).build());
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
            asyncResponse.resume(Response.status(Response.Status.BAD_REQUEST).entity(errMsg).build());
        }
    }

    private void listSeries(AsyncResponse asyncResponse, String tenantId) {
        ListenableFuture<List<Metric>> future = metricsService.findMetrics(tenantId, MetricType.NUMERIC);
        Futures.addCallback(future, new FutureCallback<List<Metric>>() {
            @Override
            public void onSuccess(List<Metric> result) {
                List<InfluxObject> objects = new ArrayList<>(result.size());

                for (Metric metric : result) {
                    InfluxObject obj = new InfluxObject(metric.getId().getName());
                    obj.columns = new ArrayList<>(2);
                    obj.columns.add("time");
                    obj.columns.add("sequence_number");
                    obj.columns.add("val");
                    obj.points = new ArrayList<>(1);
                    objects.add(obj);
                }

                Response.ResponseBuilder builder = Response.ok(objects);

                asyncResponse.resume(builder.build());
            }

            @Override
            public void onFailure(Throwable t) {
                asyncResponse.resume(t);
            }
        }, executor);
    }

    private void select(AsyncResponse asyncResponse, String tenantId, SelectQueryContext selectQueryContext) {

        SelectQueryDefinitionsParser definitionsParser = new SelectQueryDefinitionsParser();
        parseTreeWalker.walk(definitionsParser, selectQueryContext);

        SelectQueryDefinitions queryDefinitions = definitionsParser.getSelectQueryDefinitions();

        try {
            queryValidator.validateSelectQuery(queryDefinitions);
        } catch (IllegalQueryException e) {
            StringValue errMsg = new StringValue("Illegal query: " + e.getMessage());
            asyncResponse.resume(Response.status(Response.Status.BAD_REQUEST).entity(errMsg).build());
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
            asyncResponse.resume(Response.status(Response.Status.BAD_REQUEST).entity(errMsg).build());
            return;
        }
        String columnName = getColumnName(queryDefinitions);

        ListenableFuture<Boolean> idExistsFuture = metricsService.idExists(metric);
        ListenableFuture<List<NumericData>> loadMetricsFuture = Futures.transform(idExistsFuture,
                (AsyncFunction<Boolean, List<NumericData>>) idExists -> {
                    if (idExists != Boolean.TRUE) {
                        return Futures.immediateFuture(null);
                    }
                    return metricsService.findData(new NumericMetric2(tenantId, new MetricId(metric)),
                        timeInterval.getStartMillis(), timeInterval.getEndMillis());
                }, executor);
        ListenableFuture<List<InfluxObject>> influxObjectTranslatorFuture = Futures.transform(loadMetricsFuture,
                (List<NumericData> metrics) -> {
                    if (metrics == null) {
                        return null;
                    }

                    List<InfluxObject> objects = new ArrayList<>(1);

                    InfluxObject obj = new InfluxObject(metric);
                    obj.columns = new ArrayList<>(2);
                    obj.columns.add("time");
                    obj.columns.add(columnName);
                    obj.points = new ArrayList<>();

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

                    for (NumericData m : metrics) {
                        List<Object> data = new ArrayList<>();
                        data.add(m.getTimestamp());
                        data.add(m.getValue());
                        obj.points.add(data);
                    }

                    objects.add(obj);

                    return objects;
                }, executor);
        Futures.addCallback(influxObjectTranslatorFuture, new FutureCallback<List<InfluxObject>>() {
            @Override
            public void onSuccess(List<InfluxObject> objects) {
                if (objects == null) {
                    StringValue val = new StringValue("Metric with id [" + metric + "] not found. ");
                    asyncResponse.resume(Response.status(404).entity(val).build());
                } else {
                    Response.ResponseBuilder builder = Response.ok(objects);
                    asyncResponse.resume(builder.build());
                }
            }

            @Override
            public void onFailure(Throwable t) {
                asyncResponse.resume(t);
            }
        }, executor);
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
                default:
                    LOG.warn("Mapping of '{}' function not yet supported", function);
                }
                NumericMetric2 metric = new NumericMetric2(DEFAULT_TENANT_ID, firstElementInList.getMetric().getId());
                out.add(new NumericData(metric, firstElementInList.getTimestamp(), retVal));
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
