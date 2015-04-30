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
import static org.hawkular.metrics.core.api.MetricsService.DEFAULT_TENANT_ID;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

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

import org.hawkular.metrics.api.jaxrs.callback.NoDataCallback;
import org.hawkular.metrics.api.jaxrs.callback.SimpleDataCallback;
import org.hawkular.metrics.core.api.Counter;
import org.hawkular.metrics.core.api.MetricsService;

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

    @POST
    @Path("/")
    @ApiOperation(value = "List of counter definitions", hidden = true)
    public void updateCountersForGroups(@Suspended final AsyncResponse asyncResponse,
            @HeaderParam("tenantId") String tenantId, Collection<Counter> counters) {
        ListenableFuture<Void> future = metricsService.updateCounters(counters);
        Futures.addCallback(future, new NoDataCallback<>(asyncResponse));
    }

    @POST
    @Path("/{group}")
    @ApiOperation(value = "Update multiple counters in a single counter group", hidden = true)
    public void updateCounterForGroup(@Suspended final AsyncResponse asyncResponse,
            @HeaderParam("tenantId") String tenantId, @PathParam("group") String group,
            Collection<Counter> counters) {
        for (Counter counter : counters) {
            counter.setGroup(group);
        }
        ListenableFuture<Void> future = metricsService.updateCounters(counters);
        Futures.addCallback(future, new NoDataCallback<>(asyncResponse));
    }

    @POST
    @Path("/{group}/{counter}")
    @ApiOperation(value = "Increase value of a counter", hidden = true)
    public void updateCounter(@Suspended final AsyncResponse asyncResponse, @HeaderParam("tenantId") String tenantId,
            @PathParam("group") String group, @PathParam("counter") String counter) {
        ListenableFuture<Void> future = metricsService
                .updateCounter(new Counter(DEFAULT_TENANT_ID, group, counter, 1L));
        Futures.addCallback(future, new NoDataCallback<>(asyncResponse));
    }

    @POST
    @Path("/{group}/{counter}/{value}")
    @ApiOperation(value = "Update value of a counter", hidden = true)
    public void updateCounter(@Suspended final AsyncResponse asyncResponse, @HeaderParam("tenantId") String tenantId,
            @PathParam("group") String group, @PathParam("counter") String counter, @PathParam("value") Long value) {
        ListenableFuture<Void> future = metricsService.updateCounter(new Counter(DEFAULT_TENANT_ID, group, counter,
                value));
        Futures.addCallback(future, new NoDataCallback<>(asyncResponse));
    }

    @GET
    @Path("/{group}")
    @ApiOperation(value = "Retrieve a list of counter values in this group", hidden = true,
        response = Counter.class, responseContainer = "List")
    @Produces({ APPLICATION_JSON })
    public void getCountersForGroup(@Suspended final AsyncResponse asyncResponse,
            @HeaderParam("tenantId") String tenantId, @PathParam("group") String group) {
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
}