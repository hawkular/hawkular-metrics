package org.rhq.metrics.restServlet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Response;
import javax.xml.bind.annotation.XmlRootElement;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import org.rhq.metrics.core.MetricsService;
import org.rhq.metrics.core.RawNumericMetric;

/**
 * Some support for InfluxDB clients like Grafana.
 * This is very rough at the moment (to say it politely)
 * @author Heiko W. Rupp
 */
@Path("/influx")
@Produces("application/json")
public class InfluxHandler {

    private static final String SELECT_FROM = "select ";

    // TODO how often are they compiled? move those to a place where this happens only once
    static Pattern metricSelectPattern = Pattern.compile("select +(\\S+) +as +(\\S+) +from +(\\S+) +where +(.*?) +group by time\\((\\S+)\\).*");
    static Pattern timePattern = Pattern.compile("([0-9]+)([a-z])");


    @Inject
    private MetricsService metricsService;

    @GET
    @Path("/series")
    public void series(@Suspended final AsyncResponse asyncResponse,
                           @QueryParam("q") String queryString) {

        if (queryString.equals("list series")) {

            // Copied from MetricsHandler
            List<String> names = ServiceKeeper.getInstance().service.listMetrics();

            List<InfluxObject> objects = new ArrayList<>(names.size() + 2);

            for (String name : names) {
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

        } else {
            // Example query: select  mean("value") as "value_mean" from "snert.cpu_user" where  time > now() - 6h     group by time(30s)  order asc
            final String query = queryString.toLowerCase();
            if (query.startsWith(SELECT_FROM)) {
                final InfluxQuery iq = new InfluxQuery(query);

                Long start = iq.start;
                Long end = iq.end;

                final String metric = iq.metric;  // metric to query from backend
                final String alias = iq.alias;  // alias to return the data as

                if (!metricsService.idExists(metric)) {
                    StringValue val = new StringValue("Metric with Id [" + metric + "] not found. ");
                    asyncResponse.resume(Response.status(404).entity(val).build());
                }


                final ListenableFuture<List<RawNumericMetric>> future = metricsService.findData(metric, start, end);

                final Long finalStart = start;
                final Long finalEnd = end;
                Futures.addCallback(future, new FutureCallback<List<RawNumericMetric>>() {
                    @Override
                    public void onSuccess(List<RawNumericMetric> metrics) {

                        List<InfluxObject> objects = new ArrayList<>(1);

                        InfluxObject obj = new InfluxObject(metric);
                        obj.columns = new ArrayList<>(1);
                        obj.columns.add("time");
                        obj.columns.add(alias);
                        obj.points = new ArrayList<>(1);

                        metrics = applyMapping(iq.mapping,metrics,iq.bucketLengthSec, finalStart, finalEnd);

                        for (RawNumericMetric m : metrics) {
                            List<Number> data = new ArrayList<>();
                            data.add(m.getTimestamp()/1000);  // query param "time_precision
                            data.add(m.getAvg());
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
            else {
                // Fallback if nothing matched
                StringValue errMsg = new StringValue("Query not yet supported: " + queryString);
                asyncResponse.resume(
                    Response.status(Response.Status.BAD_REQUEST).entity(errMsg).build());
            }
        }
    }

    /**
     * Apply a mapping function to the incoming data. This can return the input if no
     * mapping is requested, but can also be a much shorter list of data if aggregation of
     * many data points into buckets happens.
     * @param mapping Name of the mapping
     * @param in Incoming list of raw data
     * @param bucketLengthSec Length of a bucket for aggregation
     * @param startTime Start time of the data, used for bucketing
     * @param endTime  End time of the data, used for bucketing
     * @return List of computed metrics, possibly the input if no mapping needed
     */
    private List<RawNumericMetric> applyMapping(String mapping, final List<RawNumericMetric> in, int bucketLengthSec,
                                                long startTime, long endTime) {

        if (mapping==null || mapping.isEmpty() || mapping.equals("none")) {
            return  in;
        }

        long timeDiff = endTime - startTime; // Millis
        int numBuckets = (int) ((timeDiff /1000 ) / bucketLengthSec);
        Map<Integer,List<RawNumericMetric>> tmpMap = new HashMap<>(numBuckets);

        // Bucketize
        for (RawNumericMetric rnm: in) {
            int pos = (int) ((rnm.getTimestamp()-startTime)/1000) /bucketLengthSec;
            List<RawNumericMetric> bucket = tmpMap.get(pos);
            if (bucket==null) {
                bucket = new ArrayList<>();
                tmpMap.put(pos, bucket);
            }
            bucket.add(rnm);
        }

        List<RawNumericMetric> out = new ArrayList<>(numBuckets);
        // Apply mapping to buckets to create final value
        SortedSet<Integer> keySet = new TreeSet<>(tmpMap.keySet());
        for (Integer pos: keySet ) {
            List<RawNumericMetric> list = tmpMap.get(pos);
            double retVal = 0.0;
            if (list!=null) {
                switch (mapping) {
                case "mean":
                    for (RawNumericMetric rnm : list) {
                        retVal += rnm.getAvg();
                    }
                    retVal /= list.size();
                    break;
                case "max":
                    retVal = Double.MIN_VALUE;
                    for (RawNumericMetric rnm : list) {
                        if (rnm.getAvg() > retVal) {
                            retVal = rnm.getAvg();
                        }
                    }
                    break;
                case "min":
                    retVal = Double.MAX_VALUE;
                    for (RawNumericMetric rnm : list) {
                        if (rnm.getAvg() < retVal) {
                            retVal = rnm.getAvg();
                        }
                    }
                    break;
                case "sum":
                    for (RawNumericMetric rnm : list) {
                        retVal += rnm.getAvg();
                    }
                    break;
                case "count":
                    retVal = list.size();
                    break;
                case "first":
                    if (!list.isEmpty()) {
                        retVal = list.get(0).getAvg();
                    }
                    break;
                case "last":
                    if (!list.isEmpty()) {
                        retVal = list.get(list.size() - 1).getAvg();
                    }
                    break;
                case "difference":
                    if (!list.isEmpty()) {
                        retVal = (list.get(list.size() - 1).getAvg()) - (list.get(0).getAvg());
                    }
                    break;
                default:
                    System.out.println("Mapping of " + mapping + " not yet supported");

                }
                RawNumericMetric outMetric = new RawNumericMetric(list.get(0).getId(),retVal,list.get(0).getTimestamp());
                out.add(outMetric);
            }
        }

        return out;
    }

    /**
     * The passed string may be surrounded by quotes, so we
     * need to remove them.
     * @param in String to de-quote
     * @return De-Quoted String
     */
    private String deQuote(String in) {

        if (in==null) {
            return null;
        }
        String out ;
        int start = 0;
        int end = in.length();
        if (in.startsWith("\"")) {
            start++;
        }
        if (in.endsWith("\"")) {
            end--;
        }
        out=in.substring(start,end);

        return out;
    }

    /**
     * Transfer object which is returned by Influx queries
     */
    @SuppressWarnings("unused")
    @XmlRootElement
    private class InfluxObject {

        private InfluxObject() {
        }

        private InfluxObject(String name) {
            this.name = name;
        }

        String name;
        List<String> columns;
        List<List<?>> points;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public List<String> getColumns() {
            return columns;
        }

        public void setColumns(List<String> columns) {
            this.columns = columns;
        }

        public List<List<?>> getPoints() {
            return points;
        }

        public void setPoints(List<List<?>> points) {
            this.points = points;
        }
    }

    private class InfluxQuery {
        // select  mean("value") as "value_mean" from "snert.cpu_user" where  time > now() - 6h     group by time(30s)  order asc
        // select  mean("value") as "value_mean" from "snert.cpu_user" where  time > 1402826660s and time < 1402934869s     group by time(1m)  order asc
        private String expr;
        private String alias;
        private String metric;

        // e.g.  time > 1402826660s and time < 1402934869s
        private String timeExpr;
        private String groupExpr;
        private String mapping;
        private long start;
        private long end;
        private int bucketLengthSec;

        private InfluxQuery(String query) {

            Matcher m = metricSelectPattern.matcher(query.toLowerCase());
            if (m.matches()) {
                expr = m.group(1);
                alias = m.group(2);
                metric = deQuote(m.group(3));
                timeExpr = m.group(4);
                groupExpr = m.group(5);

                if (timeExpr.contains("and")) {
                    int i = timeExpr.indexOf(" and ");
                    start = parseTime(timeExpr.substring(0,i));
                    end = parseTime(timeExpr.substring(i+5,timeExpr.length()));
                } else {
                    end = System.currentTimeMillis();
                    start = parseTime(timeExpr);
                }

                bucketLengthSec = (int) parseTime(groupExpr) / 1000;

                if (expr.contains("(")) {
                    mapping = expr.substring(0,expr.indexOf("("));
                } else {
                    mapping = expr;
                }
            }
            else if (query.toLowerCase().startsWith("select * from")) {
                // TODO
                System.out.println("Not yet supported: " + query);
            }
            else {
                throw new IllegalArgumentException("Can not parse " + query);
            }
        }

        /**
         * Parse the time input which looks like "time > now() - 6h"
         * @param timeExpr Expression to parse
         * @return Time in Milliseconds
         */
        private long parseTime(String timeExpr) {
            String tmp; // Skip over "time <"
            if (timeExpr.startsWith("time")) {
                tmp = timeExpr.substring(7);
            } else {
                tmp = timeExpr;
            }
            if (tmp.startsWith("now()")) {
                tmp = tmp.substring(8); // skip over "now() - "
                Matcher m = timePattern.matcher(tmp);
                if (m.matches()) {
                    long convertedOffset = getTimeFromExpr(m);
                    return System.currentTimeMillis() - convertedOffset; // Need to convert to ms -> *1000L
                }

            } else {
                Matcher m = timePattern.matcher(tmp);
                if (m.matches()) {
                    long convertedOffset = getTimeFromExpr(m);
                    return convertedOffset;
                }
            }
            return 0;  // TODO: Customise this generated block
        }

        private long getTimeFromExpr(Matcher m) {
            String val = m.group(1);
            String unit = m.group(2);
            long factor ;
            switch (unit) {
            case "h":
                factor = 3600;
                break;
            case "m":
                factor = 60;
                break;
            case "s":
                factor = 1;
                break;
            default:
                throw new IllegalArgumentException("Unknown unit " + unit);
            }
            long offset = Long.parseLong(val);
            return offset * factor * 1000L;
        }


    }

}
