package org.rhq.metrics.restServlet;

import java.util.HashSet;
import java.util.Set;

import javax.ejb.Stateless;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.rhq.metrics.core.RawNumericMetric;

/**
 * Interface to deal with metrics
 * @author Heiko W. Rupp
 */
@Stateless
@Path("metric")
public class MetricHandler {

    private static final Logger logger = LoggerFactory.getLogger(MetricHandler.class);

    public MetricHandler() {
        logger.info("MetricHandler instantiated");
    }

    @POST
    @Path("/")
    public void addMetric(@QueryParam("id") String id, @QueryParam("time") long time, @QueryParam("value") Double value) {

        RawNumericMetric rawMetric = new RawNumericMetric(id,value,time);
        Set<RawNumericMetric> rawSet = new HashSet<>(1);
        rawSet.add(rawMetric);

        ServiceKeeper.getInstance().service.addData(rawSet);
    }

    @Produces("text/plain")
    @Path("/")
    @GET
    public String hello() {
        return "Hello World";
    }
}
