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

import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

import com.google.common.collect.ImmutableMap;
import org.hawkular.metrics.api.jaxrs.param.Duration;
import org.hawkular.metrics.api.jaxrs.util.VirtualClock;
import org.hawkular.metrics.tasks.api.TaskScheduler;


/**
 * @author jsanda
 */
@Path("/clock")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
public class VirtualClockHandler {

    public static final String PATH = "/clock";

    @Inject
    private VirtualClock virtualClock;

    @Inject
    private TaskScheduler taskScheduler;

    private static boolean started;

    @GET
    public Response getTime() {
        return Response.ok(ImmutableMap.<String, Object>of("now", virtualClock.now())).build();
    }

    @PUT
    public Response setTime(Map<String, Object> params) {
        Long time = (Long) params.get("time");
        virtualClock.advanceTimeTo(time);
        if (!started) {
            taskScheduler.start();
            started = true;
        }
        return Response.ok().build();
    }

    @POST
    public Response incrementTime(Duration duration) {
        virtualClock.advanceTimeBy(duration.getValue(), duration.getTimeUnit());
        return Response.ok().build();
    }

}
