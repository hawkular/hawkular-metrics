package org.rhq.metrics.restServlet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.Response;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.jboss.resteasy.annotations.GZIP;
import org.rhq.metrics.core.NumericMetric;
import org.rhq.metrics.core.RawNumericMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interface to deal with metrics
 * @author Heiko W. Rupp
 */
//@Stateless
@Path("/")
public class MetricHandler {

    private static final Logger logger = LoggerFactory.getLogger(MetricHandler.class);
    private static final long EIGHT_HOURS = 8L*60L*60L*1000L; // 8 Hours in milliseconds

    public MetricHandler() {
        if (logger.isDebugEnabled()) {
            logger.debug("MetricHandler instantiated");
        }
    }

	@GET
	@Path("/ping")
	@Consumes({ "application/json", "application/xml" })
	@Produces({ "application/json", "application/xml" })
	public Response ping() {
		Map<String, String> reply = new HashMap<String, String>();
		reply.put("pong", new Date().toString());

		Response.ResponseBuilder builder = Response.ok(reply);
		return builder.build();
	}

    @POST
    @Path("/{id}/data")
    @Consumes({"application/json","application/xml"})
    public void addMetric(@PathParam("id)") String id, IdDataPoint dataPoint) {

        RawNumericMetric rawMetric = new RawNumericMetric(dataPoint.getId(),dataPoint.getValue(),dataPoint.getTimestamp());
        Set<RawNumericMetric> rawSet = new HashSet<>(1);
        rawSet.add(rawMetric);
        addData(rawSet);
    }

    @POST
    @Path("/data")
    @Consumes({"application/json","application/xml"})
    public void addMetrics(Collection<IdDataPoint> dataPoints) {

        Set<RawNumericMetric> rawSet = new HashSet<>(dataPoints.size());
        for (IdDataPoint dataPoint : dataPoints) {
            RawNumericMetric rawMetric = new RawNumericMetric(dataPoint.getId(), dataPoint.getValue(),
                dataPoint.getTimestamp());
            rawSet.add(rawMetric);
        }

        addData(rawSet);
    }

    private void addData(Set<RawNumericMetric> rawData) {
        // The Futures.getUnchecked call is only being used temporarily until the REST
        // stuff is wired up to work with a Cassandra backend.
        ListenableFuture<Map<RawNumericMetric,Throwable>> future = ServiceKeeper.getInstance().service.addData(rawData);
        Futures.getUnchecked(future);
    }

    @GZIP
    @GET
    @Path("/{id}/data")
    @Produces({"application/json","text/json","application/xml"})
    public Response getDataForId(@PathParam("id") String id, @QueryParam("start") Long start, @QueryParam("end") Long end) {

        Response.ResponseBuilder builder;

        if (!ServiceKeeper.getInstance().service.idExists(id)) {
            builder = Response.status(404).entity("Id [" + id + "] not found. ");
            return builder.build();
        }

        long now = System.currentTimeMillis();
        if (start==null) {
            start= now -EIGHT_HOURS;
        }
        if (end==null) {
            end = now;
        }

        final ListenableFuture<List<RawNumericMetric>> future = ServiceKeeper.getInstance().service.findData(id, start,
            end);
        // The Futures.getUnchecked call is only being used temporarily until the REST
        // stuff is wired up to work with a Cassandra backend.
        List<RawNumericMetric> data = Futures.getUnchecked(future);
        final List<DataPoint> points = new ArrayList<>(data.size());
        for (NumericMetric item : data) {
            DataPoint point = new DataPoint(item.getTimestamp(),item.getAvg());
            points.add(point);
        }

        GenericEntity<List<DataPoint>> list = new GenericEntity<List<DataPoint>>(points) { };
        builder = Response.ok(list);

        return builder.build();

    }

    @GZIP
    @GET
    @Path("/list")
    @Produces({"application/json","text/json","application/xml"})
    public Response listMetrics(@QueryParam("q") String filter) {

        List<String> names = ServiceKeeper.getInstance().service.listMetrics();

        final List<SimpleLink> listWithLinks = new ArrayList<>(names.size());
        for (String name : names) {
            if ((filter == null || filter.isEmpty()) || (name.contains(filter))) {
                SimpleLink link = new SimpleLink("metrics", "/rhq-metrics/" + name + "/data", name);
                listWithLinks.add(link);
            }
        }

        GenericEntity<List<SimpleLink>> list = new GenericEntity<List<SimpleLink>>(listWithLinks) {} ;
        Response.ResponseBuilder builder = Response.ok(list);

        return builder.build();
    }
}
