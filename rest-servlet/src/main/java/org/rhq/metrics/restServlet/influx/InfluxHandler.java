package org.rhq.metrics.restServlet.influx;

import static org.rhq.metrics.core.MetricsService.DEFAULT_TENANT_ID;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Response;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.rhq.metrics.core.MetricsService;
import org.rhq.metrics.core.NumericData;
import org.rhq.metrics.restServlet.ServiceKeeper;
import org.rhq.metrics.restServlet.StringValue;

/**
 * Some support for InfluxDB clients like Grafana.
 * This is very rough at the moment (to say it politely)
 * @author Heiko W. Rupp
 */
@Path("/influx")
@Produces("application/json")
public class InfluxHandler {
    private static final Logger LOG = LoggerFactory.getLogger(InfluxHandler.class);

    private static final String SELECT_FROM = "select ";

    @Inject
    private MetricsService metricsService;

    @GET
    @Path("/series")
    public void series(@Suspended final AsyncResponse asyncResponse,
                           @QueryParam("q") String queryString) {

        if (queryString==null || queryString.isEmpty()) {
            asyncResponse.cancel();
            return;
        }

        if (queryString.equals("list series")) {

            // Copied from MetricsHandler
            ListenableFuture<List<String>> future = ServiceKeeper.getInstance().service.listMetrics();
            Futures.addCallback(future, new FutureCallback<List<String>>() {
                @Override
                public void onSuccess(List<String> result) {

                    List<InfluxObject> objects = new ArrayList<>(result.size());

                    for (String name : result) {
                InfluxObject obj = new InfluxObject(name);
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
            });
        } else {
            // Example query: select  mean("value") as "value_mean" from "snert.cpu_user" where  time > now() - 6h     group by time(30s)  order asc
            final String query = queryString.toLowerCase();
            if (query.startsWith(SELECT_FROM)) {
                final InfluxQuery iq = new InfluxQuery(query);
                LOG.debug("Parsed Influx query: {}", iq);

                final String metric = iq.getMetric();  // metric to query from backend

//                ListenableFuture<Boolean> idExistsFuture = metricsService.idExists(metric);
                ListenableFuture<Boolean> idExistsFuture = Futures.immediateFuture(Boolean.TRUE);
                Futures.addCallback(idExistsFuture, new FutureCallback<Boolean>() {
                    @Override
                    public void onSuccess(Boolean result) {
                        if (!result) {
                            StringValue val = new StringValue("Metric with id [" + metric + "] not found. ");
                            asyncResponse.resume(Response.status(404).entity(val).build());
                        }

                        final ListenableFuture<List<NumericData>> future = metricsService.findData(DEFAULT_TENANT_ID,
                            metric, iq.getStart(), iq.getEnd());

                        Futures.addCallback(future, new FutureCallback<List<NumericData>>() {
                            @Override
                            public void onSuccess(List<NumericData> metrics) {

                                List<InfluxObject> objects = new ArrayList<>(1);

                                InfluxObject obj = new InfluxObject(metric);
                                obj.columns = new ArrayList<>(1);
                                obj.columns.add("time");
                                obj.columns.add(iq.getAlias());
                                obj.points = new ArrayList<>(1);

                                metrics = applyMapping(iq,metrics, iq.getBucketLengthSec(), iq.getStart(), iq.getEnd());

                                for (NumericData m : metrics) {
                                    List<Number> data = new ArrayList<>();
                                    data.add(m.getTimestamp()/1000);  // query param "time_precision
                                    data.add(m.getValue());
                                    obj.points.add(data);
                                }

                                objects.add(obj);

                                Response.ResponseBuilder builder = Response.ok(objects);

                                asyncResponse.resume(builder.build());
                            }

                            @Override
                            public void onFailure(Throwable t) {
                                asyncResponse.resume(t);
                            }

                        });
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        asyncResponse.resume(t);
                    }
                });
            }
            else {
                // Fallback if nothing matched
                StringValue errMsg = new StringValue("Query not yet supported: " + queryString);
                asyncResponse.resume(
                    Response.status(Response.Status.BAD_REQUEST).entity(errMsg).build());
            }
        }
    }

    /**
     * Apply a mapping function to the incoming data
     * @param query Object that describes the query
     * @param in Input list of data
     * @param bucketLengthSec the length of the buckets
     * @param startTime Start time of the query
     * @param endTime  End time of the query
     * @return The mapped list of values, which could be the input or a longer or shorter list
     */
    private List<NumericData> applyMapping(InfluxQuery query, final List<NumericData> in, int bucketLengthSec,
                                                long startTime, long endTime) {

        String mapping = query.getMapping();
        if (mapping==null || mapping.isEmpty() || mapping.equals("none")) {
            return  in;
        }

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
                switch (mapping) {
                case "mean":
                    for (NumericData rnm : list) {
                        retVal += rnm.getValue();
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Applying mean mapping, total = {}, size = {}", retVal, size);
                    }
                    retVal /= size;
                    break;
                case "max":
                    retVal = Double.MIN_VALUE;
                    for (NumericData rnm : list) {
                        if (rnm.getValue() > retVal) {
                            retVal = rnm.getValue();
                        }
                    }
                    break;
                case "min":
                    retVal = Double.MAX_VALUE;
                    for (NumericData rnm : list) {
                        if (rnm.getValue() < retVal) {
                            retVal = rnm.getValue();
                        }
                    }
                    break;
                case "sum":
                    for (NumericData rnm : list) {
                        retVal += rnm.getValue();
                    }
                    break;
                case "count":
                    retVal = size;
                    break;
                case "first":
                    if (!list.isEmpty()) {
                        retVal = firstElementInList.getValue();
                    }
                    break;
                case "last":
                    if (!list.isEmpty()) {
                        retVal = lastElementInList.getValue();
                    }
                    break;
                case "difference":
                    if (!list.isEmpty()) {
                        retVal = (lastElementInList.getValue()) - (firstElementInList.getValue());
                    }
                    break;
                case "derivative":
                    if (!list.isEmpty()) {
                        double y = (lastElementInList.getValue()) - (firstElementInList.getValue());
                        long t = (lastElementInList.getTimestamp() - (firstElementInList.getTimestamp())) / 1000; // sec
                        retVal = y/(double)t;
                    }
                    break;
                case "median":
                    retVal = quantil(list,50.0);
                    break;
                case "percentile":
                    retVal = quantil(list,Double.valueOf(query.getMappingArgs()));
                    break;
                default:
                    System.out.println("Mapping of " + query + " not yet supported");

                }
                Metric metric = new Metric().setTenantId(DEFAULT_TENANT_ID).setId(firstElementInList.getMetric().getId());
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
    public double quantil (List<NumericData> in, double val) {
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
