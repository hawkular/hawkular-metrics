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

import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;

import java.io.IOException;
import java.io.PrintWriter;

import javax.inject.Inject;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.Provider;

import org.hawkular.metrics.api.jaxrs.ApiError;
import org.hawkular.metrics.api.jaxrs.MetricsServiceLifecycle;
import org.hawkular.metrics.api.jaxrs.MetricsServiceLifecycle.State;
import org.hawkular.metrics.api.jaxrs.handler.StatusHandler;

/**
 * @author Matt Wringe
 */
@Provider
public class MetricsServiceStateFilter implements Filter {

    private static final String STARTING = "Service unavailable while initializing.";
    private static final String FAILED = "Internal server error.";
    private static final String STOPPED = "The service is no longer running.";

    @Inject
    private MetricsServiceLifecycle metricsServiceLifecycle;

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {

        final HttpServletRequest httpRequest = (HttpServletRequest) request;
        String path = httpRequest.getRequestURI();

        if (path.startsWith(StatusHandler.PATH)) {
            // The status page does not require the MetricsService to be up and running.
            chain.doFilter(request, response);
            return;
        }

        if (metricsServiceLifecycle.getState() == State.STARTING) {
            // Fail since the Cassandra cluster is not yet up yet.
            Response actualResponse = Response.status(Status.SERVICE_UNAVAILABLE)
                    .type(APPLICATION_JSON_TYPE)
                    .entity(new ApiError(STARTING))
                    .build();

            final HttpServletResponse httpResponse = (HttpServletResponse) response;
            httpResponse.setStatus(actualResponse.getStatus());
            httpResponse.setContentType(APPLICATION_JSON_TYPE.toString());
            PrintWriter out = response.getWriter();
            out.println(new ApiError(STARTING).toString());
            return;
        } else if (metricsServiceLifecycle.getState() == State.FAILED) {
            // Fail since an error has occured trying to start the Metrics service
            Response actualResponse = Response.status(Status.INTERNAL_SERVER_ERROR)
                    .type(APPLICATION_JSON_TYPE)
                    .entity(new ApiError(FAILED))
                    .build();

            final HttpServletResponse httpResponse = (HttpServletResponse) response;
            httpResponse.setStatus(actualResponse.getStatus());
            httpResponse.setContentType(APPLICATION_JSON_TYPE.toString());
            PrintWriter out = response.getWriter();
            out.println(new ApiError(FAILED).toString());
            return;
        } else if (metricsServiceLifecycle.getState() == State.STOPPED
                || metricsServiceLifecycle.getState() == State.STOPPING) {
            Response actualResponse = Response.status(Status.SERVICE_UNAVAILABLE)
                    .type(APPLICATION_JSON_TYPE)
                    .entity(new ApiError(STOPPED))
                    .build();

            final HttpServletResponse httpResponse = (HttpServletResponse) response;
            httpResponse.setStatus(actualResponse.getStatus());
            httpResponse.setContentType(APPLICATION_JSON_TYPE.toString());
            PrintWriter out = response.getWriter();
            out.println(new ApiError(STOPPED).toString());
            return;
        }

        chain.doFilter(request, response);
    }

    @Override
    public void destroy() {
    }
}
