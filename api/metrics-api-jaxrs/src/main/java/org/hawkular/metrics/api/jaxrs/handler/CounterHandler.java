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

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.hawkular.metrics.api.jaxrs.filter.TenantFilter.TENANT_HEADER_NAME;
import static org.hawkular.metrics.api.jaxrs.util.ApiUtils.emptyPayload;
import static org.hawkular.metrics.api.jaxrs.util.ApiUtils.requestToCounterDataPoints;
import static org.hawkular.metrics.api.jaxrs.util.ApiUtils.requestToCounters;
import static org.hawkular.metrics.core.api.MetricType.COUNTER;

import java.net.URI;
import java.util.List;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;
import org.hawkular.metrics.api.jaxrs.ApiError;
import org.hawkular.metrics.api.jaxrs.handler.observer.MetricCreatedObserver;
import org.hawkular.metrics.api.jaxrs.handler.observer.ResultSetObserver;
import org.hawkular.metrics.api.jaxrs.model.Counter;
import org.hawkular.metrics.api.jaxrs.model.CounterDataPoint;
import org.hawkular.metrics.api.jaxrs.request.MetricDefinition;
import org.hawkular.metrics.api.jaxrs.util.ApiUtils;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.MetricsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

/**
 * @author Stefan Negrea
 *
 */
@Path("/counters")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
@Api(value = "", description = "Counter metrics interface. A counter is a metric whose value are monotonically " +
    "increasing or decreasing.")
public class CounterHandler {

    private static Logger logger = LoggerFactory.getLogger(CounterHandler.class);

    private static final long EIGHT_HOURS = MILLISECONDS.convert(8, HOURS);

    @Inject
    private MetricsService metricsService;

    @HeaderParam(TENANT_HEADER_NAME)
    private String tenantId;

    @POST
    @Path("/")
    @ApiOperation(
            value = "Create counter metric definition. This operation also causes the rate to be calculated and " +
                    "persisted periodically after raw count data is persisted.",
            notes = "Clients are not required to explicitly create a metric before storing data. Doing so however " +
                    "allows clients to prevent naming collisions and to specify tags and data retention.")
    @ApiResponses(value = {
            @ApiResponse(code = 201, message = "Metric definition created successfully"),
            @ApiResponse(code = 400, message = "Missing or invalid payload", response = ApiError.class),
            @ApiResponse(code = 409, message = "Counter metric with given id already exists",
                         response = ApiError.class),
            @ApiResponse(code = 500, message = "Metric definition creation failed due to an unexpected error",
                         response = ApiError.class) })
    public void createCounter (
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(required = true) MetricDefinition metricDefinition,
            @Context UriInfo uriInfo) {
        if (metricDefinition == null) {
            asyncResponse.resume(emptyPayload());
            return;
        }
        Metric<Double> metric = new Metric<>(tenantId, COUNTER, new MetricId(metricDefinition.getId()),
                metricDefinition.getTags(), metricDefinition.getDataRetention());
        URI location = uriInfo.getBaseUriBuilder().path("/counters/{id}").build(metric.getId().getName());
        metricsService.createMetric(metric).subscribe(new MetricCreatedObserver(asyncResponse, location));
    }

    @GET
    @Path("/{id}")
    @ApiOperation(value = "Retrieve a counter definition", response = MetricDefinition.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Metric's definition was successfully retrieved."),
            @ApiResponse(code = 204, message = "Query was successful, but no metrics definition is set."),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching metric's definition.",
                         response = ApiError.class) })
    public void getCounter(@Suspended final AsyncResponse asyncResponse, @PathParam("id") String id) {

        metricsService.findMetric(tenantId, COUNTER, new MetricId(id))
                .map(MetricDefinition::new)
                .map(metricDef -> Response.ok(metricDef).build())
                .switchIfEmpty(Observable.just(ApiUtils.noContent()))
                .subscribe(asyncResponse::resume, t -> asyncResponse.resume(ApiUtils.serverError(t)));
    }

    @POST
    @Path("/data")
    @ApiOperation(value = "Add data points for multiple counters")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Adding data points succeeded."),
            @ApiResponse(code = 400, message = "Missing or invalid payload", response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error happened while storing the data points",
                         response = ApiError.class) })
    public void addData(@Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "List of metrics", required = true) List<Counter> counters) {

        if (counters.isEmpty()) {
            asyncResponse.resume(emptyPayload());
        } else {
            Observable<Metric<Long>> metrics = requestToCounters(tenantId, counters);
            Observable<Void> observable = metricsService.addCounterData((metrics));
            observable.subscribe(new ResultSetObserver(asyncResponse));
        }
    }

    @POST
    @Path("/{id}/data")
    @ApiOperation(value = "Add data for a single counter")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Adding data succeeded."),
            @ApiResponse(code = 400, message = "Missing or invalid payload", response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error happened while storing the data",
                         response = ApiError.class), })
    public void addData(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("id") String id,
            @ApiParam(value = "List of data points containing timestamp and value", required = true)
            List<CounterDataPoint> data) {

        if (data.isEmpty()) {
            asyncResponse.resume(emptyPayload());
        } else {
            Metric<Long> metric = new Metric<>(tenantId, COUNTER, new MetricId(id), requestToCounterDataPoints(data));
            Observable<Void> observable = metricsService.addCounterData(Observable.just(metric));
            observable.subscribe(new ResultSetObserver(asyncResponse));
        }
    }

    @GET
    @Path("/{id}/data")
    @ApiOperation(value = "Retrieve counter data points.", response = List.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully fetched metric data."),
            @ApiResponse(code = 204, message = "No metric data was found."),
            @ApiResponse(code = 400, message = "start or end parameter is invalid.",
                         response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching metric data.",
                         response = ApiError.class) })
    public void findCounterData(
            @Suspended AsyncResponse asyncResponse,
            @PathParam("id") String id,
            @ApiParam(value = "Defaults to now - 8 hours") @QueryParam("start") final Long start,
            @ApiParam(value = "Defaults to now") @QueryParam("end") final Long end) {

        long now = System.currentTimeMillis();
        long startTime = start == null ? now - EIGHT_HOURS : start;
        long endTime = end == null ? now : end;

        metricsService.findCounterData(tenantId, new MetricId(id), startTime, endTime)
                .map(CounterDataPoint::new)
                .toList()
                .map(ApiUtils::collectionToResponse)
                .subscribe(
                        asyncResponse::resume,
                        t -> {
                            logger.warn("Failed to fetch counter data", t);
                            ApiUtils.serverError(t);
                        });
    }

}