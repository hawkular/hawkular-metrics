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

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import static org.hawkular.metrics.api.jaxrs.util.ApiUtils.serverError;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

import org.hawkular.metrics.api.jaxrs.util.TestClock;
import org.hawkular.metrics.api.jaxrs.util.VirtualClock;
import org.hawkular.metrics.models.param.Duration;
import org.hawkular.metrics.tasks.api.TaskScheduler;
import org.jboss.logging.Logger;

import com.google.common.collect.ImmutableMap;

import rx.observers.TestSubscriber;
import rx.schedulers.Schedulers;


/**
 * @author jsanda
 */
@Path("/clock")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
public class VirtualClockHandler {
    private static Logger log = Logger.getLogger(VirtualClockHandler.class);

    public static final String PATH = "/clock";

    @Inject
    @TestClock
    private VirtualClock virtualClock;

    @Inject
    private TaskScheduler taskScheduler;

    @GET
    public Response getTime() {
        return Response.ok(ImmutableMap.<String, Object>of("now", virtualClock.now())).build();
    }

    @PUT
    public Response setTime(Map<String, Object> params) {
        Long time = (Long) params.get("time");
        virtualClock.advanceTimeTo(time);
        return Response.ok().build();
    }

    @POST
    public Response incrementTime(Duration duration) {
        virtualClock.advanceTimeBy(duration.getValue(), duration.getTimeUnit());
        return Response.ok().build();
    }

    @GET
    @Path("/wait")
    public Response waitForDuration(@QueryParam("duration") Duration duration) {
        int numMinutes = (int) TimeUnit.MINUTES.convert(duration.getValue(), duration.getTimeUnit());
        TestSubscriber<Long> timeSlicesSubscriber = new TestSubscriber<>();
        taskScheduler.getFinishedTimeSlices()
                .take(numMinutes)
                .observeOn(Schedulers.immediate())
                .subscribe(timeSlicesSubscriber);

        try {
            virtualClock.advanceTimeBy(numMinutes, MINUTES);
            long timeout = Long.parseLong(System.getProperty("hawkular.terminal-event.timeout", "10"));
            timeSlicesSubscriber.awaitTerminalEvent(timeout, SECONDS);
            timeSlicesSubscriber.assertNoErrors();
            timeSlicesSubscriber.assertTerminalEvent();

            return Response.ok().build();
        } catch (Exception e) {
            log.warn("Failed to wait " + numMinutes + " minutes for task scheduler to complete work", e);
            return serverError(e);
        }
    }

}
