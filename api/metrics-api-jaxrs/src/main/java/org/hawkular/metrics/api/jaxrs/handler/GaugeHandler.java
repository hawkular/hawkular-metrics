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
import static org.hawkular.metrics.api.jaxrs.util.ApiUtils.emptyPayload;
import static org.hawkular.metrics.api.jaxrs.util.ApiUtils.executeAsync;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

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
import javax.ws.rs.core.UriInfo;

import com.datastax.driver.core.ResultSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;

import org.hawkular.metrics.api.jaxrs.ApiError;
import org.hawkular.metrics.api.jaxrs.ResultSetObserver;
import org.hawkular.metrics.api.jaxrs.param.Duration;
import org.hawkular.metrics.api.jaxrs.param.Tags;
import org.hawkular.metrics.api.jaxrs.request.TagRequest;
import org.hawkular.metrics.api.jaxrs.util.ApiUtils;
import org.hawkular.metrics.core.api.BucketedOutput;
import org.hawkular.metrics.core.api.Buckets;
import org.hawkular.metrics.core.api.Gauge;
import org.hawkular.metrics.core.api.GaugeBucketDataPoint;
import org.hawkular.metrics.core.api.GaugeData;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.MetricType;
import org.hawkular.metrics.core.api.MetricsService;

import rx.Observable;

/**
 * @author Stefan Negrea
 *
 */
@Path("/gauges")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
@Api(value = "", description = "Gauge metrics interface")
public class GaugeHandler {
    private static final long EIGHT_HOURS = MILLISECONDS.convert(8, HOURS);

    @Inject
    private MetricsService metricsService;

    @HeaderParam(TENANT_HEADER_NAME)
    private String tenantId;

    @POST
    @Path("/")
    @ApiOperation(value = "Create gauge metric definition.", notes = "Clients are not required to explicitly create "
            + "a metric before storing data. Doing so however allows clients to prevent naming collisions and to "
            + "specify tags and data retention.")
    @ApiResponses(value = {
            @ApiResponse(code = 201, message = "Metric definition created successfully"),
            @ApiResponse(code = 400, message = "Missing or invalid payload", response = ApiError.class),
            @ApiResponse(code = 409, message = "Gauge metric with given id already exists",
                response = ApiError.class),
            @ApiResponse(code = 500, message = "Metric definition creation failed due to an unexpected error",
                response = ApiError.class) })
    public void createGaugeMetric(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(required = true) Gauge metric,
            @Context UriInfo uriInfo
    ) {
        if (metric == null) {
            asyncResponse.resume(emptyPayload());
            return;
        }
        metric.setTenantId(tenantId);
        URI location = uriInfo.getBaseUriBuilder().path("/gauges/{id}").build(metric.getId().getName());
        metricsService.createMetric(metric).subscribe(new MetricCreatedObserver(asyncResponse, location));
    }

    @GET
    @Path("/{id}")
    @ApiOperation(value = "Retrieve single metric definition.", response = Metric.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Metric's definition was successfully retrieved."),
            @ApiResponse(code = 204, message = "Query was successful, but no metrics definition is set."),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching metric's definition.",
                         response = ApiError.class) })
    public void getGaugeMetric(@Suspended final AsyncResponse asyncResponse, @PathParam("id") String id) {

        metricsService.findMetric(tenantId, MetricType.GAUGE, new MetricId(id))
                .subscribe(
                        optional -> asyncResponse.resume(ApiUtils.valueToResponse(optional)),
                        t -> asyncResponse.resume(ApiUtils.serverError(t))
                );
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
    public void getGaugeMetricTags(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("id") String id
    ) {
        metricsService.getMetricTags(tenantId, MetricType.GAUGE, new MetricId(id))
                .subscribe(
                        optional -> asyncResponse.resume(ApiUtils.valueToResponse(optional)),
                        t ->asyncResponse.resume(ApiUtils.serverError(t))
                );
    }

    @PUT
    @Path("/{id}/tags")
    @ApiOperation(value = "Update tags associated with the metric definition.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Metric's tags were successfully updated."),
            @ApiResponse(code = 500, message = "Unexpected error occurred while updating metric's tags.",
                response = ApiError.class) })
    public void updateGaugeMetricTags(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("id") String id,
            @ApiParam(required = true) Map<String, String> tags
    ) {
        Gauge metric = new Gauge(tenantId, new MetricId(id));
        metricsService.addTags(metric, tags).subscribe(new ResultSetObserver(asyncResponse));
    }

    @DELETE
    @Path("/{id}/tags/{tags}")
    @ApiOperation(value = "Delete tags associated with the metric definition.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Metric's tags were successfully deleted."),
            @ApiResponse(code = 400, message = "Invalid tags", response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error occurred while trying to delete metric's tags.",
                response = ApiError.class) })
    public void deleteGaugeMetricTags(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("id") String id,
            @ApiParam("Tag list") @PathParam("tags") Tags tags
    ) {
        Gauge metric = new Gauge(tenantId, new MetricId(id));
        metricsService.deleteTags(metric, tags.getTags()).subscribe(new ResultSetObserver(asyncResponse));
    }

    @POST
    @Path("/{id}/data")
    @ApiOperation(value = "Add data for a single gauge metric.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Adding data succeeded."),
            @ApiResponse(code = 400, message = "Missing or invalid payload", response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error happened while storing the data",
                response = ApiError.class), })
    public void addDataForMetric(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("id") String id,
            @ApiParam(value = "List of datapoints containing timestamp and value", required = true)
            List<GaugeData> data
    ) {
        if (data.isEmpty()) {
            asyncResponse.resume(emptyPayload());
        } else {
            Gauge metric = new Gauge(tenantId, new MetricId(id));
            metric.getData().addAll(data);
            Observable<Void> observable = metricsService.addGaugeData(Observable.just(metric));
            observable.subscribe(
                    noArg -> {},
                    t -> asyncResponse.resume(Response.serverError().entity(new ApiError(t.getMessage())).build()),
                    () -> asyncResponse.resume(Response.ok().build()));
        }
    }

    @POST
    @Path("/data")
    @ApiOperation(value = "Add data for multiple gauge metrics in a single call.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Adding data succeeded."),
            @ApiResponse(code = 500, message = "Unexpected error happened while storing the data",
                response = ApiError.class) })
    public void addGaugeData(@Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "List of metrics", required = true) List<Gauge> metrics) {

        if (metrics.isEmpty()) {
            asyncResponse.resume(emptyPayload());
        } else {
            metrics.forEach(m -> m.setTenantId(tenantId));
            Observable<Void> observable = metricsService.addGaugeData(Observable.from(metrics));
            observable.subscribe(
                    noArg -> {},
                    t -> asyncResponse.resume(Response.serverError().entity(new ApiError(t.getMessage())).build()),
                    () -> asyncResponse.resume(Response.ok().build()));
        }
    }

    @GET
    @Path("/")
    @ApiOperation(value = "Find gauge metrics data by their tags.", response = Map.class, responseContainer = "List")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Successfully fetched data."),
            @ApiResponse(code = 204, message = "No matching data found."),
            @ApiResponse(code = 400, message = "Missing or invalid tags query", response = ApiError.class),
            @ApiResponse(code = 500, message = "Any error in the query.", response = ApiError.class), })
    public void findGaugeDataByTags(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Tag list", required = true) @QueryParam("tags") Tags tags
    ) {
        if (tags == null) {
            asyncResponse.resume(badRequest(new ApiError("Missing tags query")));
        } else {
            metricsService.findGaugeDataByTags(tenantId, tags.getTags()).subscribe(m -> {
                if (m.isEmpty()) {
                    asyncResponse.resume(Response.noContent());
                } else {
                    asyncResponse.resume(Response.ok(m).build());
                }
            }, t -> asyncResponse.resume(Response.serverError().entity(new ApiError(t.getMessage())).build()));
        }
    }

    @GET
    @Path("/{id}/data")
    @ApiOperation(value = "Retrieve gauge data. When buckets or bucketDuration query parameter is used, the time "
            + "range between start and end will be divided in buckets of equal duration, and metric "
            + "statistics will be computed for each bucket.", response = List.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully fetched metric data."),
            @ApiResponse(code = 204, message = "No metric data was found."),
            @ApiResponse(code = 400, message = "buckets or bucketDuration parameter is invalid, or both are used.",
                response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching metric data.",
                response = ApiError.class) })
    public void findGaugeData(
            @Suspended AsyncResponse asyncResponse,
            @PathParam("id") String id,
            @ApiParam(value = "Defaults to now - 8 hours") @QueryParam("start") final Long start,
            @ApiParam(value = "Defaults to now") @QueryParam("end") final Long end,
            @ApiParam(value = "Total number of buckets") @QueryParam("buckets") Integer bucketsCount,
            @ApiParam(value = "Bucket duration") @QueryParam("bucketDuration") Duration bucketDuration) {

        if (bucketsCount == null && bucketDuration == null) {
            long now = System.currentTimeMillis();
            long startTime = start == null ? now - EIGHT_HOURS : start;
            long endTime = end == null ? now : end;

            metricsService.findGaugeData(tenantId, new MetricId(id), startTime, endTime)
                    .reduce(new ArrayList<>(), ApiUtils::addToCollection)
                    .map(ApiUtils::collectionToResponse)
                    .subscribe(asyncResponse::resume, t -> asyncResponse.resume(ApiUtils.serverError(t)));
        } else {
            executeAsync(
                    asyncResponse,
                    () -> {
                        long now = System.currentTimeMillis();
                        long startTime = start == null ? now - EIGHT_HOURS : start;
                        long endTime = end == null ? now : end;

                        Gauge metric = new Gauge(tenantId, new MetricId(id));

                        if (bucketsCount != null && bucketDuration != null) {
                            return Futures.immediateFuture(
                                    badRequest(
                                            new ApiError(
                                                    "Both buckets and bucketDuration parameters are used"
                                            )
                                    )
                            );
                        }

                        Buckets buckets;
                        try {
                            if (bucketsCount != null) {
                                buckets = Buckets.fromCount(startTime, endTime, bucketsCount);
                            } else {
                                buckets = Buckets.fromStep(startTime, endTime, bucketDuration.toMillis());
                            }
                        } catch (IllegalArgumentException e) {
                            return Futures.immediateFuture(badRequest(new ApiError("Bucket: " + e.getMessage())));
                        }

                        ListenableFuture<BucketedOutput<GaugeBucketDataPoint>> dataFuture;
                        dataFuture = metricsService.findGaugeStats(metric, startTime, endTime, buckets);

                        ListenableFuture<List<GaugeBucketDataPoint>> outputFuture;
                        outputFuture = Futures.transform(dataFuture, BucketedOutput<GaugeBucketDataPoint>::getData);

                        return Futures.transform(outputFuture, ApiUtils.MAP_COLLECTION);
                    });
        }
    }

    @GET
    @Path("/{id}/periods")
    @ApiOperation(value = "Retrieve periods for which the condition holds true for each consecutive data point.",
        response = List.class)
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Successfully fetched periods."),
            @ApiResponse(code = 204, message = "No data was found."),
            @ApiResponse(code = 400, message = "Missing or invalid query parameters") })
    public void findPeriods(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("id") String id,
            @ApiParam(value = "Defaults to now - 8 hours", required = false) @QueryParam("start") final Long start,
            @ApiParam(value = "Defaults to now", required = false) @QueryParam("end") final Long end,
            @ApiParam(value = "A threshold against which values are compared", required = true)
            @QueryParam("threshold") double threshold,
            @ApiParam(value = "A comparison operation to perform between values and the threshold."
                              + " Supported operations include ge, gte, lt, lte, and eq", required = true)
            @QueryParam("op") String operator
    ) {
        executeAsync(
                asyncResponse,
                () -> {
                    long now = System.currentTimeMillis();
                    Long startTime = start;
                    Long endTime = end;
                    if (start == null) {
                        startTime = now - EIGHT_HOURS;
                    }
                    if (end == null) {
                        endTime = now;
                    }

                    Predicate<Double> predicate;
                    switch (operator) {
                    case "lt":
                        predicate = d -> d < threshold;
                        break;
                    case "lte":
                        predicate = d -> d <= threshold;
                        break;
                    case "eq":
                        predicate = d -> d == threshold;
                        break;
                    case "neq":
                        predicate = d -> d != threshold;
                        break;
                    case "gt":
                        predicate = d -> d > threshold;
                        break;
                    case "gte":
                        predicate = d -> d >= threshold;
                        break;
                    default:
                        predicate = null;
                    }

                    if (predicate == null) {
                        return Futures.immediateFuture(
                                badRequest(
                                        new ApiError(
                                                "Invalid value for op parameter. Supported values are lt, "
                                                + "lte, eq, gt, gte."
                                        )
                                )
                        );
                    } else {
                        ListenableFuture<List<long[]>> future = metricsService.getPeriods(tenantId, new MetricId(id),
                                predicate, startTime, endTime);
                        return Futures.transform(future, ApiUtils.MAP_COLLECTION);
                    }
                });
    }

    @GET
    @Path("/tags/{tags}")
    @ApiOperation(value = "Find metric data with given tags.", response = Map.class, responseContainer = "List")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Me values fetched successfully"),
            @ApiResponse(code = 204, message = "No matching data found."),
            @ApiResponse(code = 400, message = "Invalid tags", response = ApiError.class),
            @ApiResponse(code = 500, message = "Any error while fetching data.", response = ApiError.class), })
    public void findTaggedGaugeData(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam("Tag list") @PathParam("tags") Tags tags
    ) {
        Observable<Map<MetricId, Set<GaugeData>>> gaugeDataByTags = metricsService.findGaugeDataByTags(tenantId,
            tags.getTags());

        gaugeDataByTags.map(input -> {
            Map<String, Set<GaugeData>> result = new HashMap<>(input.size());
            for (Map.Entry<MetricId, Set<GaugeData>> entry : input.entrySet()) {
                result.put(entry.getKey().getName(), entry.getValue());
            }
            return result;
        }).subscribe(m -> { // @TODO Repeated code, refactor and use Optional?
                if (m.isEmpty()) {
                    asyncResponse.resume(Response.noContent());
                } else {
                    asyncResponse.resume(Response.ok(m).build());
                }
            }, t -> asyncResponse.resume(Response.serverError().entity(new ApiError(t.getMessage())).build()));

    }

    @POST
    @Path("/{id}/tag")
    @ApiOperation(value = "Add or update gauge metric's tags.")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Tags were modified successfully."),
        @ApiResponse(code = 500, message = "Processing tags failed") })
    public void tagGaugeData(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("id") final String id, @ApiParam(required = true) TagRequest params
    ) {
        Observable<ResultSet> resultSetObservable;
        Gauge metric = new Gauge(tenantId, new MetricId(id));
        if (params.getTimestamp() != null) {
            resultSetObservable = metricsService.tagGaugeData(metric, params.getTags(), params.getTimestamp());
        } else {
            resultSetObservable = metricsService.tagGaugeData(metric, params.getTags(), params.getStart(), params
                    .getEnd());
        }

        resultSetObservable.subscribe(new ResultSetObserver(asyncResponse));
    }
}
