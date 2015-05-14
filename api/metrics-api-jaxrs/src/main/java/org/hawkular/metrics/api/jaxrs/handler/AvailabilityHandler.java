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
import static org.hawkular.metrics.api.jaxrs.util.ApiUtils.badRequest;
import static org.hawkular.metrics.api.jaxrs.util.ApiUtils.executeAsync;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;
import org.hawkular.metrics.api.jaxrs.ApiError;
import org.hawkular.metrics.api.jaxrs.param.Duration;
import org.hawkular.metrics.api.jaxrs.param.Tags;
import org.hawkular.metrics.api.jaxrs.request.TagRequest;
import org.hawkular.metrics.api.jaxrs.util.ApiUtils;
import org.hawkular.metrics.core.api.Availability;
import org.hawkular.metrics.core.api.AvailabilityBucketDataPoint;
import org.hawkular.metrics.core.api.AvailabilityData;
import org.hawkular.metrics.core.api.BucketedOutput;
import org.hawkular.metrics.core.api.Buckets;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricAlreadyExistsException;
import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.MetricType;
import org.hawkular.metrics.core.api.MetricsService;
import rx.Observable;

/**
 * @author Stefan Negrea
 *
 */
@Path("/availability")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
@Api(value = "", description = "Availability metrics interface")
public class AvailabilityHandler {
    private static final long EIGHT_HOURS = MILLISECONDS.convert(8, HOURS);

    @Inject
    private MetricsService metricsService;

    @HeaderParam(TENANT_HEADER_NAME)
    private String tenantId;

    @POST
    @Path("/")
    @ApiOperation(value = "Create availability metric definition. Same notes as creating gauge metric apply.")
    @ApiResponses(value = {
            @ApiResponse(code = 201, message = "Metric definition created successfully"),
            @ApiResponse(code = 400, message = "Missing or invalid payload", response = ApiError.class),
            @ApiResponse(code = 409, message = "Availability metric with given id already exists",
                response = ApiError.class),
            @ApiResponse(code = 500, message = "Metric definition creation failed due to an unexpected error",
                response = ApiError.class) })
    public void createAvailabilityMetric(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(required = true) Availability metric,
            @Context UriInfo uriInfo
    ) {
        if (metric == null) {
            Response response = Response.status(Status.BAD_REQUEST).entity(new ApiError("Payload is empty")).build();
            asyncResponse.resume(response);
            return;
        }
        metric.setTenantId(tenantId);
        Observable<Void> metricCreated = metricsService.createMetric(metric);
        URI location = uriInfo.getBaseUriBuilder().path("/availability/{id}").build(metric.getId().getName());
//        MetricCreatedCallback metricCreatedCallback = new MetricCreatedCallback(asyncResponse, created);
//        Futures.addCallback(future, metricCreatedCallback);
        metricCreated.subscribe(
                nullArg -> {},
                t -> {
                    if (t instanceof MetricAlreadyExistsException) {
                        MetricAlreadyExistsException exception = (MetricAlreadyExistsException) t;
                        String message = "A metric with name [" + exception.getMetric().getId().getName() + "] " +
                                "already exists";
                        asyncResponse.resume(Response.status(Status.CONFLICT).entity(new ApiError(message)).build());
                    } else {
                        String message = "Failed to create metric due to an unexpected error: "
                                + Throwables.getRootCause(t).getMessage();
                        asyncResponse.resume(Response.serverError().entity(new ApiError(message)).build());
                    }
                },
                () -> asyncResponse.resume(Response.created(location).build())
        );
    }

    @GET
    @Path("/{id}")
    @ApiOperation(value = "Retrieve single metric definition.", response = Metric.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Metric's definition was successfully retrieved."),
            @ApiResponse(code = 204, message = "Query was successful, but no metrics definition is set."),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching metric's definition.",
                         response = ApiError.class) })
    public void getAvailabilityMetric(@Suspended final AsyncResponse asyncResponse, @PathParam("id") String id) {
        executeAsync(
                asyncResponse,
                () -> {
                ListenableFuture<Optional<Metric<?>>> future = metricsService.findMetric(tenantId,
                    MetricType.AVAILABILITY, new MetricId(id));
                    return Futures.transform(future, ApiUtils.MAP_VALUE);
                });
    }

    @GET
    @Path("/{id}/tags")
    @ApiOperation(value = "Retrieve tags associated with the metric definition.", response = String.class,
                  responseContainer = "Map")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Metric's tags were successfully retrieved."),
            @ApiResponse(code = 204, message = "Query was successful, but no metrics were found."),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching metric's tags.",
                response = ApiError.class) })
    public void getAvailabilityMetricTags(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("id") String id
    ) {
        executeAsync(
                asyncResponse,
                () -> {
                ListenableFuture<Map<String, String>> future = metricsService.getMetricTags(tenantId,
                    MetricType.AVAILABILITY, new MetricId(id));
                    return Futures.transform(future, ApiUtils.MAP_MAP);
                });
    }

    @PUT
    @Path("/{id}/tags")
    @ApiOperation(value = "Update tags associated with the metric definition.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Metric's tags were successfully updated."),
            @ApiResponse(code = 500, message = "Unexpected error occurred while updating metric's tags.",
                response = ApiError.class) })
    public void updateAvailabilityMetricTags(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("id") String id,
            @ApiParam(required = true) Map<String, String> tags
    ) {
        executeAsync(asyncResponse, () -> {
            Availability metric = new Availability(tenantId, new MetricId(id));
            ListenableFuture<Void> future = metricsService.addTags(metric, tags);
            return Futures.transform(future, ApiUtils.MAP_VOID);
        });
    }

    @DELETE
    @Path("/{id}/tags/{tags}")
    @ApiOperation(value = "Delete tags associated with the metric definition.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Metric's tags were successfully deleted."),
            @ApiResponse(code = 400, message = "Invalid tags", response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error occurred while trying to delete metric's tags.",
                response = ApiError.class) })
    public void deleteAvailabilityMetricTags(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("id") String id,
            @ApiParam("Tag list") @PathParam("tags") Tags tags
    ) {
        executeAsync(asyncResponse, () -> {
            Availability metric = new Availability(tenantId, new MetricId(id));
            ListenableFuture<Void> future = metricsService.deleteTags(metric, tags.getTags());
            return Futures.transform(future, ApiUtils.MAP_VOID);
        });
    }

    @POST
    @Path("/{id}/data")
    @ApiOperation(value = "Add data for a single availability metric.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Adding data succeeded."),
            @ApiResponse(code = 400, message = "Missing or invalid payload", response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error happened while storing the data",
                response = ApiError.class) })
    public void addAvailabilityForMetric(
            @Suspended final AsyncResponse asyncResponse, @PathParam("id") String id,
            @ApiParam(value = "List of availability datapoints", required = true) List<AvailabilityData> data
    ) {
        executeAsync(asyncResponse, () -> {
            if (data == null) {
                return ApiUtils.emptyPayload();
            }
            Availability metric = new Availability(tenantId, new MetricId(id));
            metric.getData().addAll(data);
            ListenableFuture<Void> future = metricsService.addAvailabilityData(Collections.singletonList(metric));
            return Futures.transform(future, ApiUtils.MAP_VOID);
        });
    }

    @POST
    @Path("/data")
    @ApiOperation(value = "Add metric data for multiple availability metrics in a single call.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Adding data succeeded."),
            @ApiResponse(code = 500, message = "Unexpected error happened while storing the data",
                response = ApiError.class) })
    public void addAvailabilityData(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "List of availability metrics", required = true) List<Availability> metrics
    ) {
        executeAsync(asyncResponse, () -> {
            if (metrics.isEmpty()) {
                return Futures.immediateFuture(Response.ok().build());
            }
            metrics.forEach(m -> m.setTenantId(tenantId));
            ListenableFuture<Void> future = metricsService.addAvailabilityData(metrics);
            return Futures.transform(future, ApiUtils.MAP_VOID);
        });
    }

    @GET
    @Path("/")
    @ApiOperation(value = "Find availabilities metrics data by their tags.", response = Map.class,
        responseContainer = "List")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Successfully fetched data."),
            @ApiResponse(code = 204, message = "No matching data found."),
            @ApiResponse(code = 400, message = "Missing or invalid tags query", response = ApiError.class),
            @ApiResponse(code = 500, message = "Any error in the query.", response = ApiError.class), })
    public void findAvailabilityDataByTags(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Tag list", required = true) @QueryParam("tags") Tags tags
    ) {
        executeAsync(asyncResponse, () -> {
            if (tags == null) {
                return badRequest(new ApiError("Missing tags query"));
            }
            ListenableFuture<Map<MetricId, Set<AvailabilityData>>> future;
            future = metricsService.findAvailabilityByTags(tenantId, tags.getTags());
            return Futures.transform(future, ApiUtils.MAP_MAP);
        });
    }

    @GET
    @Path("/{id}/data")
    @ApiOperation(value = "Retrieve availability data. When buckets or bucketDuration query parameter is used, "
            + "the time range between start and end will be divided in buckets of equal duration, and availability "
            + "statistics will be computed for each bucket.", response = List.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully fetched availability data."),
            @ApiResponse(code = 204, message = "No availability data was found."),
            @ApiResponse(code = 400, message = "buckets or bucketDuration parameter is invalid, or both are used.",
                response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching availability data.",
                response = ApiError.class), })
    public void findAvailabilityData(
            @Suspended AsyncResponse asyncResponse,
            @PathParam("id") String id,
            @ApiParam(value = "Defaults to now - 8 hours") @QueryParam("start") final Long start,
            @ApiParam(value = "Defaults to now") @QueryParam("end") final Long end,
            @ApiParam(value = "Total number of buckets") @QueryParam("buckets") Integer bucketsCount,
            @ApiParam(value = "Bucket duration") @QueryParam("bucketDuration") Duration bucketDuration,
            @ApiParam(value = "Set to true to return only distinct, contiguous values")
            @QueryParam("distinct") Boolean distinct
    ) {
        executeAsync(
                asyncResponse,
                () -> {
                    long now = System.currentTimeMillis();
                    Long startTime = start == null ? now - EIGHT_HOURS : start;
                    Long endTime = end == null ? now : end;

                    Availability metric = new Availability(tenantId, new MetricId(id));
                    ListenableFuture<List<AvailabilityData>> queryFuture;

                    if (distinct != null && distinct.equals(Boolean.TRUE)) {
                        queryFuture = metricsService.findAvailabilityData(tenantId, metric.getId(), startTime, endTime,
                                distinct);
                        return Futures.transform(queryFuture, ApiUtils.MAP_COLLECTION);
                    }

                    if (bucketsCount == null && bucketDuration == null) {
                        queryFuture = metricsService.findAvailabilityData(tenantId, metric.getId(), startTime, endTime);
                        return Futures.transform(queryFuture, ApiUtils.MAP_COLLECTION);
                    }

                    if (bucketsCount != null && bucketDuration != null) {
                        return badRequest(new ApiError("Both buckets and bucketDuration parameters are used"));
                    }

                    Buckets buckets;
                    try {
                        if (bucketsCount != null) {
                            buckets = Buckets.fromCount(startTime, endTime, bucketsCount);
                        } else {
                            buckets = Buckets.fromStep(startTime, endTime, bucketDuration.toMillis());
                        }
                    } catch (IllegalArgumentException e) {
                        return badRequest(new ApiError("Bucket: " + e.getMessage()));
                    }
                    ListenableFuture<BucketedOutput<AvailabilityBucketDataPoint>> dataFuture;
                    dataFuture = metricsService.findAvailabilityStats(metric, startTime, endTime, buckets);

                    ListenableFuture<List<AvailabilityBucketDataPoint>> outputFuture;
                    outputFuture = Futures.transform(dataFuture, BucketedOutput<AvailabilityBucketDataPoint>::getData);
                    return Futures.transform(outputFuture, ApiUtils.MAP_COLLECTION);
                });
    }

    @POST
    @Path("/{id}/tag")
    @ApiOperation(value = "Add or update availability metric's tags.")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Tags were modified successfully.") })
    public void tagAvailabilityData(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("id") final String id,
            @ApiParam(required = true) TagRequest params
    ) {
        executeAsync(
                asyncResponse,
                () -> {
                    ListenableFuture<List<AvailabilityData>> future;
                    Availability metric = new Availability(tenantId, new MetricId(id));
                    if (params.getTimestamp() != null) {
                        future = metricsService.tagAvailabilityData(metric, params.getTags(), params.getTimestamp());
                    } else {
                        future = metricsService.tagAvailabilityData(metric, params.getTags(), params.getStart(),
                                params.getEnd());
                    }
                    return Futures.transform(future, (List<AvailabilityData> data) -> Response.ok().build());
                });
    }

    @GET
    @Path("/tags/{tags}")
    @ApiOperation(value = "Find availability metric data with given tags.", response = Map.class,
        responseContainer = "List")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Availability values fetched successfully"),
            @ApiResponse(code = 204, message = "No matching data found."),
            @ApiResponse(code = 400, message = "Invalid tags", response = ApiError.class),
            @ApiResponse(code = 500, message = "Any error while fetching data.", response = ApiError.class), })
    public void findTaggedAvailabilityData(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam("Tag list") @PathParam("tags") Tags tags
    ) {
        executeAsync(asyncResponse, () -> {
            ListenableFuture<Map<MetricId, Set<AvailabilityData>>> future;
            future = metricsService.findAvailabilityByTags(tenantId, tags.getTags());
            return Futures.transform(future, ApiUtils.MAP_MAP);
        });
    }

}