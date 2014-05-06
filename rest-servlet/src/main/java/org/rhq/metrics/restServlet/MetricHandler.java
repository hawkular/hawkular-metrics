package org.rhq.metrics.restServlet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.ejb.Stateless;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.jboss.resteasy.annotations.GZIP;

import org.rhq.metrics.core.NumericMetric;
import org.rhq.metrics.core.RawNumericMetric;

/**
 * Interface to deal with metrics
 * @author Heiko W. Rupp
 */
@Stateless
@Path("metric")
public class MetricHandler {

    private static final Logger logger = LoggerFactory.getLogger(MetricHandler.class);
    private static final long EIGHT_HOURS = 8L*60L*60L*1000L; // 8 Hours in milliseconds

    public MetricHandler() {
        logger.info("MetricHandler instantiated");
    }

    @POST
    @Path("/")
    @Consumes({"application/json","application/xml"})
    public void addMetric(IdDataPoint dataPoint) {


        RawNumericMetric rawMetric = new RawNumericMetric(dataPoint.getId(),dataPoint.getValue(),dataPoint.getTimestamp());
        Set<RawNumericMetric> rawSet = new HashSet<>(1);
        rawSet.add(rawMetric);

        ServiceKeeper.getInstance().service.addData(rawSet);
    }

    @POST
    @Path("/s")
    @Consumes({"application/json","application/xml"})
    public void addMetrics(Collection<IdDataPoint> dataPoints) {

        Set<RawNumericMetric> rawSet = new HashSet<>(dataPoints.size());
        for (IdDataPoint dataPoint : dataPoints) {
            RawNumericMetric rawMetric = new RawNumericMetric(dataPoint.getId(), dataPoint.getValue(),
                dataPoint.getTimestamp());
            rawSet.add(rawMetric);
        }

        ServiceKeeper.getInstance().service.addData(rawSet);
    }

    @GZIP
    @GET
    @Path("/{id}")
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

        final List<NumericMetric> data = ServiceKeeper.getInstance().service.findData(id,start,end);
        final List<DataPoint> points = new ArrayList<>(data.size());
        for (NumericMetric item : data) {
            DataPoint point = new DataPoint(item.getTimestamp(),item.getAvg());
            points.add(point);
        }

        GenericEntity<List<DataPoint>> list = new GenericEntity<List<DataPoint>>(points) { };
        builder = Response.ok(list);

        return builder.build();

    }
}
