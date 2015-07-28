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

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.ws.rs.ext.Provider;

/**
 * @author Matt Wringe
 */
@Provider
public class MetricsServiceStateFilter implements Filter {

    private static final String STARTING = "Service unavailable while initializing.";
    private static final String FAILED = "Internal server error.";
    private static final String STOPPED = "The service is no longer running.";

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        // TODO Auto-generated method stub

    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        // TODO Auto-generated method stub

    }

    @Override
    public void destroy() {
        // TODO Auto-generated method stub

    }

    /**
    @Inject
    private MetricsServiceLifecycle metricsServiceLifecycle;

    @Override
    public void filter(ContainerRequestContext containerRequestContext) throws IOException {
        UriInfo uriInfo = containerRequestContext.getUriInfo();
        String path = uriInfo.getPath();

        if (path.startsWith(StatusHandler.PATH)) {
            // The status page does not require the MetricsService to be up and running.
            return;
        }

        if (metricsServiceLifecycle.getState() == State.STARTING) {
            // Fail since the Cassandra cluster is not yet up yet.
            Response response = Response.status(Status.SERVICE_UNAVAILABLE)
                    .type(APPLICATION_JSON_TYPE)
                    .entity(new ApiError(STARTING))
                    .build();
            containerRequestContext.abortWith(response);
        } else if (metricsServiceLifecycle.getState() == State.FAILED) {
            // Fail since an error has occured trying to start the Metrics service
            Response response = Response.status(Status.INTERNAL_SERVER_ERROR)
                    .type(APPLICATION_JSON_TYPE)
                    .entity(new ApiError(FAILED))
                    .build();
            containerRequestContext.abortWith(response);
        } else if (metricsServiceLifecycle.getState() == State.STOPPED ||
                   metricsServiceLifecycle.getState() == State.STOPPING) {
            Response response = Response.status(Status.SERVICE_UNAVAILABLE)
                    .type(APPLICATION_JSON_TYPE)
                    .entity(new ApiError(STOPPED))
                    .build();
            containerRequestContext.abortWith(response);
        }
    }
    */
}
