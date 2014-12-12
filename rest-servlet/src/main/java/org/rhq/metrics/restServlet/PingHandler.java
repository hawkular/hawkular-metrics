package org.rhq.metrics.restServlet;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.APPLICATION_XML;
import static org.rhq.metrics.restServlet.CustomMediaTypes.APPLICATION_VND_RHQ_WRAPPED_JSON;

import java.util.Date;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

import com.wordnik.swagger.annotations.ApiOperation;

import org.rhq.metrics.core.MetricsService;

/**
 * @author Thomas Segismont
 */
@Path("/ping")
public class PingHandler {

    @Inject
    private MetricsService metricsService;

    @GET
    @POST
    @Consumes({ APPLICATION_JSON, APPLICATION_XML })
    @Produces({ APPLICATION_JSON, APPLICATION_XML, APPLICATION_VND_RHQ_WRAPPED_JSON })
    @ApiOperation(value = "Returns the current time and serves to check for the availability of the api.",
            responseClass = "Map<String,String>")
    public Response ping() {
        return Response.ok(new StringValue(new Date().toString())).build();
    }
}
