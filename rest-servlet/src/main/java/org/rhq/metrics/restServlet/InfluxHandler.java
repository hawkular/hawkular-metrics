package org.rhq.metrics.restServlet;

import java.util.ArrayList;
import java.util.List;

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

            InfluxObject obj = new InfluxObject("bla");
            obj.columns = new ArrayList<>(2);
            obj.columns.add("time");
            obj.columns.add("sequence_number");
            obj.points = new ArrayList<>(1);
            objects.add(obj);

            Response.ResponseBuilder builder = Response.ok(objects);

            asyncResponse.resume(builder.build());

        } else {
            final String query = queryString.toLowerCase();
            if (query.startsWith(SELECT_FROM)) {

                Long start = null;
                Long end = null;

                final long EIGHT_HOURS = 8L * 60L * 60L * 1000L; // 8 Hours in milliseconds

                int i = query.indexOf("from") + 5;
                int j = query.indexOf(" ", i);
                final String metric = queryString.substring(i, j);  // metric to query from backend

                i = query.indexOf("as") + 3;
                j = query.indexOf(" ", i);
                final String alias = queryString.substring(i, j);  // alias to return the data as

                final long now = System.currentTimeMillis();
                if (start == null) {
                    start = now - EIGHT_HOURS;
                }
                if (end == null) {
                    end = now;
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

}
