/*
 * Copyright 2014-2016 Red Hat, Inc. and/or its affiliates
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

import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;
import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.hawkular.jaxrs.filter.tenant.TenantRequired;
import org.hawkular.metrics.api.jaxrs.MetricsServiceLifecycle;
import org.hawkular.metrics.api.jaxrs.MetricsServiceLifecycle.State;
import org.hawkular.metrics.api.jaxrs.util.ManifestInformation;

import io.swagger.annotations.ApiOperation;

/**
 * @author Matt Wringe
 */
@Path(StatusHandler.PATH)
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
@TenantRequired(false)
public class StatusHandler {
    public static final String PATH = "/status";

    private static final String METRICSSERVICE_NAME = "MetricsService";

    @Inject
    MetricsServiceLifecycle metricsServiceLifecycle;
    @Inject
    ManifestInformation manifestInformation;

    @GET
    @ApiOperation(value = "Returns the current status for various components.",
            response = Map.class)
    public Response status(@Context ServletContext servletContext) {
        Map<String, String> status = new HashMap<>();
        State metricState = metricsServiceLifecycle.getState();
        status.put(METRICSSERVICE_NAME, metricState.toString());
        status.putAll(manifestInformation.getFrom(servletContext));
        return Response.ok(status).build();
    }
}
