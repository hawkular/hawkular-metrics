/*
 * Copyright 2014-2017 Red Hat, Inc. and/or its affiliates
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

import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.ALLOWED_CORS_ACCESS_CONTROL_ALLOW_HEADERS;

import java.util.HashMap;
import java.util.Map;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.hawkular.jaxrs.filter.cors.Headers;
import org.hawkular.metrics.api.jaxrs.config.Configurable;
import org.hawkular.metrics.api.jaxrs.config.ConfigurationProperty;
import org.hawkular.metrics.api.jaxrs.util.ManifestInformation;
import org.jboss.resteasy.annotations.GZIP;
import org.jboss.resteasy.spi.ResteasyProviderFactory;

import io.swagger.annotations.ApiOperation;

/**
 * @author Matt Wringe
 */
@Path(BaseHandler.PATH)
@GZIP
@ApplicationScoped
public class BaseHandler {

    public static final String PATH = "/";
    private static final String STATIC_CACHE_CONTROL = "private, max-age=86400";    // 1 day

    @Inject
    ManifestInformation manifestInformation;

    @Inject
    @Configurable
    @ConfigurationProperty(ALLOWED_CORS_ACCESS_CONTROL_ALLOW_HEADERS)
    String extraAccesControlAllowHeaders;

    @GET
    @Produces(APPLICATION_JSON)
    @ApiOperation(value = "Returns some basic information about the Hawkular Metrics service.",
            response = String.class, responseContainer = "Map")
    public Response baseJSON() {
        Map<String, String> hawkularMetricsProperties = new HashMap<>();
        hawkularMetricsProperties.put("name", "Hawkular-Metrics");
        hawkularMetricsProperties.putAll(manifestInformation.getAttributes());
        return Response.ok(hawkularMetricsProperties).build();
    }

    @GET
    @Produces({APPLICATION_XHTML_XML, TEXT_HTML})
    public void baseHTML(@Context ServletContext context) throws Exception {
        HttpServletResponse response = ResteasyProviderFactory.getContextData(HttpServletResponse.class);
        addHeaders(response);
        response.sendRedirect("/hawkular/metrics" + ClientRouterDispatchingServlet.PATH_INDEX_HTML);
    }

    private void addHeaders(HttpServletResponse response) {
        response.addHeader("Cache-Control", STATIC_CACHE_CONTROL);
        response.addHeader("Vary", "Origin,Accept-Encoding");

        String requestOrigin = ResteasyProviderFactory.getContextData(HttpServletRequest.class).getHeader(Headers.ORIGIN);
        if (requestOrigin == null) {
            return;
        }

        // CORS validation already checked on request filter, see AbstractCorsRequestFilter
        response.setHeader(Headers.ACCESS_CONTROL_ALLOW_ORIGIN, requestOrigin);
        response.setHeader(Headers.ACCESS_CONTROL_ALLOW_CREDENTIALS, "true");
        response.setHeader(Headers.ACCESS_CONTROL_ALLOW_METHODS, Headers.DEFAULT_CORS_ACCESS_CONTROL_ALLOW_METHODS);
        response.setHeader(Headers.ACCESS_CONTROL_MAX_AGE, String.valueOf(72 * 60 * 60));

        if (extraAccesControlAllowHeaders != null) {
            response.setHeader(Headers.ACCESS_CONTROL_ALLOW_HEADERS,
                    Headers.DEFAULT_CORS_ACCESS_CONTROL_ALLOW_HEADERS + "," + extraAccesControlAllowHeaders.trim());
        } else {
            response.setHeader(Headers.ACCESS_CONTROL_ALLOW_HEADERS, Headers.DEFAULT_CORS_ACCESS_CONTROL_ALLOW_HEADERS);
        }
    }
}
