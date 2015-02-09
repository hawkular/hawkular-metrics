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
package org.hawkular.metrics.restServlet;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.APPLICATION_XML;
import static org.hawkular.metrics.restServlet.CustomMediaTypes.APPLICATION_JAVASCRIPT;
import static org.hawkular.metrics.restServlet.CustomMediaTypes.APPLICATION_VND_RHQ_WRAPPED_JSON;

import java.util.Date;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

import com.wordnik.swagger.annotations.ApiOperation;

import org.hawkular.metrics.core.MetricsService;

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
    @Produces({ APPLICATION_JSON, APPLICATION_XML, APPLICATION_VND_RHQ_WRAPPED_JSON, APPLICATION_JAVASCRIPT })
    @ApiOperation(value = "Returns the current time and serves to check for the availability of the api.",
            responseClass = "Map<String,String>")
    public Response ping() {
        return Response.ok(new StringValue(new Date().toString())).build();
    }
}
