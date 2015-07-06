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
package org.hawkular.metrics.api.jaxrs.handler;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.APPLICATION_XHTML_XML;
import static javax.ws.rs.core.MediaType.TEXT_HTML;

import com.wordnik.swagger.annotations.ApiOperation;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.jboss.resteasy.spi.ResteasyProviderFactory;

/**
 * @author mwringe
 */
@Path("/")
public class BaseHandler {

    public static final String PATH = "/";

    @GET
    @Produces(APPLICATION_JSON)
    @ApiOperation(value = "Returns some basic information about the Hawkular Metrics service.",
                  response = String.class, responseContainer = "Map")
    public Response baseJSON(@Context ServletContext context) {

        String version = context.getInitParameter("hawkular.metrics.version");
        if (version == null) {
            version = "undefined";
        }

        HawkularMetricsBase hawkularMetrics = new HawkularMetricsBase();
        hawkularMetrics.version = version;

        return Response.ok(hawkularMetrics).build();
    }

    @GET
    @Produces({APPLICATION_XHTML_XML, TEXT_HTML})
    public void baseHTML(@Context ServletContext context) throws Exception {

        HttpServletRequest request = ResteasyProviderFactory.getContextData(HttpServletRequest.class);
        HttpServletResponse response = ResteasyProviderFactory.getContextData(HttpServletResponse.class);
        request.getRequestDispatcher("/static/index.html").forward(request,response);
    }

    private class HawkularMetricsBase {

        String name = "Hawkular-Metrics";
        String version;

        public String getName() {
            return name;
        }

        public void setVersion(String version) {
            this.version = version;
        }

        public String getVersion() {
            return version;
        }
    }
}