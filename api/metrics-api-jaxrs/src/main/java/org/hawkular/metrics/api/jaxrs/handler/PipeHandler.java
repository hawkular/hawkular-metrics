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

import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.OPTIONS;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.hawkular.metrics.api.jaxrs.log.RestLogger;
import org.hawkular.metrics.api.jaxrs.log.RestLogging;
import org.jboss.resteasy.client.ClientRequest;
import org.jboss.resteasy.client.ClientResponse;
import org.jboss.resteasy.client.core.executors.InMemoryClientExecutor;

/**
 * @author Stefan Negrea
 *
 * commands to test speed difference:
 * time `seq 1000 | parallel -j30 -P50 -q curl -s -w "%{time_total}\n" -o /dev/null \
 *      http://localhost:8080/hawkular/metrics/pipe/old -H "Hawkular-Tenant: 123"`
 */
@Path("/pipe")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
public class PipeHandler {

    private static final RestLogger log = RestLogging.getRestLogger(PipeHandler.class);

    private WebTarget target = ClientBuilder.newClient().target("http://localhost:8080/hawkular/metrics");

    @Path("/new")
    @DELETE
    @GET
    @HEAD
    @OPTIONS
    @POST
    public void pipeNew(@Context HttpHeaders headers, @Context UriInfo uriInfo,
            @Context final HttpServletRequest servletRequest,
            @Suspended final AsyncResponse asyncResponse) {
        WebTarget statusTarget = target.path("status");

        MultivaluedMap<String, String> params = uriInfo.getQueryParameters();
        if (params != null && !params.isEmpty()) {
            for (Map.Entry<String, List<String>> param : params.entrySet()) {
                String key = param.getKey();
                List<String> value = param.getValue();
                if (value != null && !value.isEmpty() && value.get(0) != null) {
                    statusTarget = statusTarget.queryParam(key, value.toArray());
                }
            }
        }

        Invocation.Builder requestBuilder = target.request();

        if (headers != null && !headers.getRequestHeaders().isEmpty()) {
            headers.getRequestHeaders().forEach((String t, List<String> u) -> {
                u.forEach((String v) -> {
                    requestBuilder.header(t, v);
                });
            });
        }

        if(headers.getAcceptableMediaTypes() != null && !headers.getAcceptableMediaTypes().isEmpty()) {
            MediaType[] mediaTypes = headers.getAcceptableMediaTypes()
                    .toArray(new MediaType[headers.getAcceptableMediaTypes().size()]);
            requestBuilder.accept(mediaTypes);
        }

        requestBuilder
                .async()
                .method(servletRequest.getMethod(), new InvocationCallback<Response>() {
                    @Override
                    public void completed(Response received) {
                        asyncResponse.resume(Response
                                .status(received.getStatus())
                                .type(received.getMediaType())
                                .entity(received.readEntity(Object.class))
                                .build());
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        log.error(throwable);
                        asyncResponse.resume(Response.serverError().status(500).build());
                    }
                });
    }

    @Path("/old")
    @DELETE
    @GET
    @HEAD
    @OPTIONS
    @POST
    public Response pipeOld(@Context HttpHeaders headers, @Context UriInfo uriInfo,
            @Context final HttpServletRequest servletRequest) throws Exception {
        InMemoryClientExecutor executor = new InMemoryClientExecutor();
        executor.getRegistry().addPerRequestResource(StatusHandler.class);

        try {
            ClientRequest requestBuilder = new ClientRequest("/status", executor);
            if (headers != null && !headers.getRequestHeaders().isEmpty()) {
                headers.getRequestHeaders().forEach((String t, List<String> u) -> {
                    u.forEach((String v) -> {
                        requestBuilder.header(t, v);
                    });
                });
            }

            MultivaluedMap<String, String> params = uriInfo.getQueryParameters();
            if (params != null && !params.isEmpty()) {
                for (Map.Entry<String, List<String>> param : params.entrySet()) {
                    String key = param.getKey();
                    List<String> value = param.getValue();
                    if (value != null && !value.isEmpty() && value.get(0) != null) {
                        requestBuilder.queryParameter(key, value.toArray());
                    }
                }
            }

            if (headers.getAcceptableMediaTypes() != null && !headers.getAcceptableMediaTypes().isEmpty()) {
                headers.getAcceptableMediaTypes().forEach((MediaType t) -> {
                    requestBuilder.accept(t);
                });
            }

            ClientResponse<Object> received = requestBuilder.httpMethod(servletRequest.getMethod(), Object.class);

            return Response.status(received.getStatus())
                    .type(received.getMediaType())
                    .entity(received.getEntity())
                    .build();
        } catch (Exception e) {
            log.error(e);
            return Response.serverError().status(500).build();
        }
    }
}
