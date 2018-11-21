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
package org.hawkular.metrics.api.jaxrs.dropwizard;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.ext.Provider;

import org.jboss.logging.Logger;

import com.codahale.metrics.Timer;

/**
 * This filter records DropWizard metrics for REST endpoints.
 *
 * @author jsanda
 */
@Provider
public class RecordMetricsFilter implements ContainerRequestFilter, ContainerResponseFilter {

    private static Logger logger = Logger.getLogger(RecordMetricsFilter.class);

    @Inject
    private RESTMetrics restMetrics;

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        String path = getPath(requestContext.getUriInfo());
        HTTPMethod method = HTTPMethod.fromString(requestContext.getMethod());
        RESTMetricName metricName = new RESTMetricName(method, path);
        Timer timer = restMetrics.getTimer(metricName.getName());
        if (timer != null) {
            Timer.Context context = timer.time();
            requestContext.setProperty("timerContext", context);
        }
    }

    @Override
    public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext)
            throws IOException {
        Timer.Context context = (Timer.Context) requestContext.getProperty("timerContext");
        if (context != null) {
            context.stop();
        }
    }

    private String getPath(UriInfo uriInfo) {
        MultivaluedMap<String, String> pathParameters = uriInfo.getPathParameters(true);
        final Map<String, String> valuesToParams = pathParameters.entrySet().stream()
                .map(entry -> entry.getValue().stream()
                        .collect(toMap(Function.identity(), value -> "{" + entry.getKey() + "}")))
                .reduce(new HashMap<>(), (m1, m2) -> {
                    m1.putAll(m2);
                    return m1;
                });
        return uriInfo.getPathSegments(true).stream()
                .map(PathSegment::getPath)
                .map(path -> {
                    if (valuesToParams.containsKey(path)) {
                        return valuesToParams.get(path);
                    }
                    return path;
                })
                .collect(joining("/"));
    }
}
