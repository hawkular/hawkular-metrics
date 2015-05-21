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
package org.hawkular.metrics.api.jaxrs.filter;

import org.hawkular.metrics.api.jaxrs.ApiError;
import org.hawkular.metrics.core.api.MetricsService;

import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;
import java.io.IOException;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;

/**
 * Created by mwringe on 20/05/15.
 */
@Provider
public class CassandraAvailabilityFilter implements ContainerRequestFilter {

    private static final String NO_CASSANDRA_SESSION_MSG = "Service unavailable while initializing.";

    @Inject
    private MetricsService metricsService;


    @Override
    public void filter(ContainerRequestContext containerRequestContext) throws IOException {
        if (!metricsService.isStarted()) {
            // Fail since the Cassandra cluster is not yet up yet.
            Response response = Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .type(APPLICATION_JSON_TYPE)
                    .entity(new ApiError(NO_CASSANDRA_SESSION_MSG))
                    .build();
            containerRequestContext.abortWith(response);
        }

    }
}
