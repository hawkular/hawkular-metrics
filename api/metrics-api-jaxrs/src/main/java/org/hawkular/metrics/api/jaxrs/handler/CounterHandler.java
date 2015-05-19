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

import static org.hawkular.metrics.api.jaxrs.filter.TenantFilter.TENANT_HEADER_NAME;
import static org.hawkular.metrics.core.api.MetricsService.DEFAULT_TENANT_ID;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Response;

import org.hawkular.metrics.api.jaxrs.ApiError;
import org.hawkular.metrics.core.api.Counter;
import org.hawkular.metrics.core.api.MetricsService;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

/**
 * @author Stefan Negrea
 *
 */
@Path("/counters")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
@Api(value = "", description = "Availability metrics interface")
public class CounterHandler {
    @Inject
    private MetricsService metricsService;

    @HeaderParam(TENANT_HEADER_NAME)
    private String tenantId;

    @POST
    @Path("/")
    @ApiOperation(value = "List of counter definitions", hidden = true)
    public void updateCountersForGroups(
            @Suspended final AsyncResponse asyncResponse,
            Collection<Counter> counters
    ) {
        ListenableFuture<Void> future = metricsService.updateCounters(counters);
        Futures.addCallback(future, new NoDataCallback<>(asyncResponse));
    }

    @POST
    @Path("/{group}")
    @ApiOperation(value = "Update multiple counters in a single counter group", hidden = true)
    public void updateCounterForGroup(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("group") String group,
            Collection<Counter> counters
    ) {
        for (Counter counter : counters) {
            counter.setGroup(group);
        }
        ListenableFuture<Void> future = metricsService.updateCounters(counters);
        Futures.addCallback(future, new NoDataCallback<>(asyncResponse));
    }

    @POST
    @Path("/{group}/{counter}")
    @ApiOperation(value = "Increase value of a counter", hidden = true)
    public void updateCounter(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("group") String group, @PathParam("counter") String counter
    ) {
        ListenableFuture<Void> future = metricsService
                .updateCounter(new Counter(DEFAULT_TENANT_ID, group, counter, 1L));
        Futures.addCallback(future, new NoDataCallback<>(asyncResponse));
    }

    @POST
    @Path("/{group}/{counter}/{value}")
    @ApiOperation(value = "Update value of a counter", hidden = true)
    public void updateCounter(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("group") String group, @PathParam("counter") String counter, @PathParam("value") Long value
    ) {
        ListenableFuture<Void> future = metricsService.updateCounter(new Counter(DEFAULT_TENANT_ID, group, counter,
                value));
        Futures.addCallback(future, new NoDataCallback<>(asyncResponse));
    }

    @GET
    @Path("/{group}")
    @ApiOperation(value = "Retrieve a list of counter values in this group", hidden = true,
        response = Counter.class, responseContainer = "List")
    @Produces({ APPLICATION_JSON })
    public void getCountersForGroup(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("group") String group
    ) {
        ListenableFuture<List<Counter>> future = metricsService.findCounters(group);
        Futures.addCallback(future, new SimpleDataCallback<>(asyncResponse));
    }

    @GET
    @Path("/{group}/{counter}")
    @ApiOperation(value = "Retrieve value of a counter", hidden = true,
        response = Counter.class, responseContainer = "List")
    public void getCounter(@Suspended final AsyncResponse asyncResponse, @PathParam("group") final String group,
            @PathParam("counter") final String counter) {
        ListenableFuture<List<Counter>> future = metricsService.findCounters(group, Collections.singletonList(counter));
        Futures.addCallback(future, new FutureCallback<List<Counter>>() {
            @Override
            public void onSuccess(List<Counter> counters) {
                if (counters.isEmpty()) {
                    asyncResponse.resume(Response.status(404)
                            .entity("Counter[group: " + group + ", name: " + counter + "] not found").build());
                } else {
                    Response jaxrs = Response.ok(counters.get(0)).build();
                    asyncResponse.resume(jaxrs);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                asyncResponse.resume(t);
            }
        });
    }

    /**
     * @author John Sanda
     */
    private static class NoDataCallback<T> implements FutureCallback<T> {

        protected AsyncResponse asyncResponse;

        public NoDataCallback(AsyncResponse asyncResponse) {
            this.asyncResponse = asyncResponse;
        }

        @Override
        public void onSuccess(Object result) {
            asyncResponse.resume(Response.ok().build());
        }

        @Override
        public void onFailure(Throwable t) {
            String msg = "Failed to perform operation due to an error: " + Throwables.getRootCause(t).getMessage();
            asyncResponse.resume(Response.serverError().entity(new ApiError(msg)).build());
        }
    }

    private static class SimpleDataCallback<T> extends NoDataCallback<T> {

        public SimpleDataCallback(AsyncResponse asyncResponse) {
            super(asyncResponse);
        }

        @Override
        public void onSuccess(Object responseData) {
            if (responseData == null) {
                asyncResponse.resume(Response.noContent().build());
            } else if (responseData instanceof Optional) {
                Optional optional = (Optional) responseData;
                if (optional.isPresent()) {
                    Object value = optional.get();
                    asyncResponse.resume(Response.ok(value).build());
                } else {
                    asyncResponse.resume(Response.noContent().build());
                }
            } else if (responseData instanceof Collection) {
                Collection collection = (Collection) responseData;
                if (collection.isEmpty()) {
                    asyncResponse.resume(Response.noContent().build());
                } else {
                    asyncResponse.resume(Response.ok(collection).build());
                }
            } else if (responseData instanceof Map) {
                Map map = (Map) responseData;
                if (map.isEmpty()) {
                    asyncResponse.resume(Response.noContent().build());
                } else {
                    asyncResponse.resume(Response.ok(map).build());
                }
            } else {
                asyncResponse.resume(Response.ok(responseData).build());
            }
        }
    }
}