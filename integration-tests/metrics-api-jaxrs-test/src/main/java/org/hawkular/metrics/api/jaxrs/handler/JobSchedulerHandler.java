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
import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

import org.hawkular.metrics.model.ApiError;
import org.hawkular.metrics.scheduler.impl.TestScheduler;
import org.jboss.logging.Logger;
import org.joda.time.DateTime;

import com.google.common.collect.ImmutableMap;

/**
 * @author jsanda
 */
@Path("/scheduler")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
@ApplicationScoped
public class JobSchedulerHandler {

    private Logger logger = Logger.getLogger(JobSchedulerHandler.class);

    @Inject
    private TestScheduler scheduler;

    @GET
    @Path("clock")
    public Response getTime() {
        return Response.ok(ImmutableMap.of("time", scheduler.now())).build();
    }

    @PUT
    @Path("clock")
    public Response setTime(Map<String, Object> params) {
        Long time = (Long) params.get("time");
        DateTime timeSlice = new DateTime(time);
        try {
            CountDownLatch timeSliceFinished = new CountDownLatch(1);
            scheduler.onTimeSliceFinished(finishedTimeSlice -> {
                if (finishedTimeSlice.equals(timeSlice)) {
                    timeSliceFinished.countDown();
                }
            });
            scheduler.advanceTimeTo(time);
            boolean finished = timeSliceFinished.await(30, TimeUnit.SECONDS);
            if (finished) {
                return Response.ok().build();
            } else {
                String msg = "The job scheduler did not finish its work for time slice [" + timeSlice.toDate() + "]";
                return Response.serverError().type(APPLICATION_JSON).entity(new ApiError(msg)).build();
            }
        } catch (InterruptedException e) {
            String msg = "There was an interrupt while waiting for the job scheduler to finish its work for time " +
                    "slice [" + timeSlice.toDate() + "]";
            logger.warn(msg, e);
            return Response.serverError().type(APPLICATION_JSON_TYPE).entity(new ApiError(msg)).build();
        } catch (Exception e) {
            String msg = "There was an unexpected error while waiting for the job scheduler to finish its work for " +
                    "[" + timeSlice.toDate() + "]";
            logger.warn(msg, e);
            return Response.serverError().type(APPLICATION_JSON_TYPE).entity(new ApiError(msg)).build();
        }
    }

}
