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
package org.hawkular.metrics.api.jaxrs;

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.hawkular.metrics.core.api.MetricsService.DEFAULT_TENANT_ID;

import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
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

import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;

import org.hawkular.metrics.api.jaxrs.callback.MetricCreatedCallback;
import org.hawkular.metrics.api.jaxrs.callback.NoDataCallback;
import org.hawkular.metrics.api.jaxrs.callback.SimpleDataCallback;
import org.hawkular.metrics.api.jaxrs.param.Duration;
import org.hawkular.metrics.core.api.Availability;
import org.hawkular.metrics.core.api.AvailabilityBucketDataPoint;
import org.hawkular.metrics.core.api.AvailabilityMetric;
import org.hawkular.metrics.core.api.BucketedOutput;
import org.hawkular.metrics.core.api.Buckets;
import org.hawkular.metrics.core.api.Counter;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.MetricType;
import org.hawkular.metrics.core.api.MetricsService;
import org.hawkular.metrics.core.api.NumericBucketDataPoint;
import org.hawkular.metrics.core.api.NumericData;
import org.hawkular.metrics.core.api.NumericMetric;
import org.hawkular.metrics.core.impl.cassandra.MetricUtils;
import org.hawkular.metrics.core.impl.request.TagRequest;

/**
 * Interface to deal with metrics
 * @author Heiko W. Rupp
 */
@Path("/")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
@Api(value = "/", description = "Metrics related REST interface")
public class MetricHandler {
    private static final long EIGHT_HOURS = MILLISECONDS.convert(8, HOURS);

    @Inject
    private MetricsService metricsService;

    @POST
    @Path("/{tenantId}/metrics/numeric")
    @ApiOperation(value = "Create numeric metric definition.", notes = "Clients are not required to explicitly create "
            + "a metric before storing data. Doing so however allows clients to prevent naming collisions and to "
            + "specify tags and data retention.")
    @ApiResponses(value = {
            @ApiResponse(code = 201, message = "Metric definition created successfully"),
            @ApiResponse(code = 400, message = "Missing or invalid payload", response = ApiError.class),
            @ApiResponse(code = 409, message = "Numeric metric with given id already exists",
                         response = ApiError.class),
            @ApiResponse(code = 500, message = "Metric definition creation failed due to an unexpected error",
                         response = ApiError.class)
    })
    public void createNumericMetric(@Suspended final AsyncResponse asyncResponse,
                                    @PathParam("tenantId") String tenantId,
                                    @ApiParam(required = true) NumericMetric metric,
                                    @Context UriInfo uriInfo
    ) {
        if (metric == null) {
            Response response = Response.status(Status.BAD_REQUEST).entity(new ApiError("Payload is empty")).build();
            asyncResponse.resume(response);
            return;
        }
        metric.setTenantId(tenantId);
        ListenableFuture<Void> future = metricsService.createMetric(metric);
        URI created = uriInfo.getBaseUriBuilder()
                             .path("/{tenantId}/metrics/numeric/{id}")
                             .build(tenantId, metric.getId().getName());
        MetricCreatedCallback metricCreatedCallback = new MetricCreatedCallback(asyncResponse, created);
        Futures.addCallback(future, metricCreatedCallback);
    }

    @POST
    @Path("/{tenantId}/metrics/availability")
    @ApiOperation(value = "Create availability metric definition. Same notes as creating numeric metric apply.")
    @ApiResponses(value = {
            @ApiResponse(code = 201, message = "Metric definition created successfully"),
            @ApiResponse(code = 400, message = "Missing or invalid payload", response = ApiError.class),
            @ApiResponse(code = 409, message = "Numeric metric with given id already exists",
                         response = ApiError.class),
            @ApiResponse(code = 500, message = "Metric definition creation failed due to an unexpected error",
                         response = ApiError.class)
    })
    public void createAvailabilityMetric(@Suspended final AsyncResponse asyncResponse,
                                         @PathParam("tenantId") String tenantId,
                                         @ApiParam(required = true) AvailabilityMetric metric,
                                         @Context UriInfo uriInfo) {
        if (metric == null) {
            Response response = Response.status(Status.BAD_REQUEST).entity(new ApiError("Payload is empty")).build();
            asyncResponse.resume(response);
            return;
        }
        metric.setTenantId(tenantId);
        ListenableFuture<Void> future = metricsService.createMetric(metric);
        URI created = uriInfo.getBaseUriBuilder()
                             .path("/{tenantId}/metrics/availability/{id}")
                             .build(tenantId, metric.getId().getName());
        MetricCreatedCallback metricCreatedCallback = new MetricCreatedCallback(asyncResponse, created);
        Futures.addCallback(future, metricCreatedCallback);
    }

    @GET
    @Path("/{tenantId}/metrics/numeric/{id}/tags")
    @ApiOperation(value = "Retrieve tags associated with the metric definition.", response = Metric.class)
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Metric's tags were successfully retrieved."),
            @ApiResponse(code = 204, message = "Query was successful, but no metrics were found."),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching metric's tags.",
                         response = ApiError.class)
    })
    public void getNumericMetricTags(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenantId") String tenantId, @PathParam("id") String id) {
        ListenableFuture<Metric<?>> future = metricsService.findMetric(tenantId, MetricType.NUMERIC,
            new MetricId(id));
        Futures.addCallback(future, new SimpleDataCallback<Metric<?>>(asyncResponse));
    }

    @PUT
    @Path("/{tenantId}/metrics/numeric/{id}/tags")
    @ApiOperation(value = "Update tags associated with the metric definition.")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Metric's tags were successfully updated."),
            @ApiResponse(code = 500, message = "Unexpected error occurred while updating metric's tags.",
                         response = ApiError.class)
    })
    public void updateNumericMetricTags(@Suspended final AsyncResponse asyncResponse,
                                        @PathParam("tenantId") String tenantId, @PathParam("id") String id,
                                        @ApiParam(required = true) Map<String, String> tags) {
        NumericMetric metric = new NumericMetric(tenantId, new MetricId(id));
        ListenableFuture<Void> future = metricsService.addTags(metric, tags);
        Futures.addCallback(future, new NoDataCallback<Void>(asyncResponse));
    }

    @DELETE
    @Path("/{tenantId}/metrics/numeric/{id}/tags/{tags}")
    @ApiOperation(value = "Delete tags associated with the metric definition.")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Metric's tags were successfully deleted."),
            @ApiResponse(code = 500, message = "Unexpected error occurred while trying to delete metric's tags.",
                         response = ApiError.class)
    })
    public void deleteNumericMetricTags(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenantId") String tenantId,
            @PathParam("id") String id,
        @ApiParam(allowMultiple = true, required = true, value = "A list of tags in the format of name:value")
        @PathParam("tags") String encodedTags) {
        NumericMetric metric = new NumericMetric(tenantId, new MetricId(id));
        ListenableFuture<Void> future = metricsService.deleteTags(metric, MetricUtils.decodeTags(encodedTags));
        Futures.addCallback(future, new NoDataCallback<Void>(asyncResponse));
    }

    @GET
    @Path("/{tenantId}/metrics/availability/{id}/tags")
    @ApiOperation(value = "Retrieve tags associated with the metric definition.", response = Map.class)
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Metric's tags were successfully retrieved."),
            @ApiResponse(code = 204, message = "Query was successful, but no metrics were found."),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching metric's tags.",
                         response = ApiError.class)
    })
    public void getAvailabilityMetricTags(@Suspended final AsyncResponse asyncResponse,
        @PathParam("tenantId") String tenantId, @PathParam("id") String id) {
        ListenableFuture<Metric<?>> future = metricsService.findMetric(tenantId, MetricType.AVAILABILITY,
            new MetricId(id));
        Futures.addCallback(future, new SimpleDataCallback<Metric<?>>(asyncResponse));
    }

    @PUT
    @Path("/{tenantId}/metrics/availability/{id}/tags")
    @ApiOperation(value = "Update tags associated with the metric definition.")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Metric's tags were successfully updated."),
            @ApiResponse(code = 500, message = "Unexpected error occurred while updating metric's tags.",
                         response = ApiError.class)
    })
    public void updateAvailabilityMetricTags(@Suspended final AsyncResponse asyncResponse,
        @PathParam("tenantId") String tenantId, @PathParam("id") String id,
        @ApiParam(required = true) Map<String, String> tags) {
        AvailabilityMetric metric = new AvailabilityMetric(tenantId, new MetricId(id));
        ListenableFuture<Void> future = metricsService.addTags(metric, tags);
        Futures.addCallback(future, new NoDataCallback<Void>(asyncResponse));
    }

    @DELETE
    @Path("/{tenantId}/metrics/availability/{id}/tags/{tags}")
    @ApiOperation(value = "Delete tags associated with the metric definition.")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Metric's tags were successfully deleted."),
            @ApiResponse(code = 500, message = "Unexpected error occurred while trying to delete metric's tags.",
                         response = ApiError.class)
    })
    public void deleteAvailabilityMetricTags(
            @Suspended final AsyncResponse asyncResponse,
        @PathParam("tenantId") String tenantId, @PathParam("id") String id,
        @ApiParam(allowMultiple = true, required = true, value = "A list of tags in the format of name:value")
        @PathParam("tags") String encodedTags) {
        AvailabilityMetric metric = new AvailabilityMetric(tenantId, new MetricId(id));
        ListenableFuture<Void> future = metricsService.deleteTags(metric, MetricUtils.decodeTags(encodedTags));
        Futures.addCallback(future, new NoDataCallback<Void>(asyncResponse));
    }

    @POST
    @Path("/{tenantId}/metrics/numeric/{id}/data")
    @ApiOperation(value = "Add data for a single numeric metric.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Adding data succeeded."),
            @ApiResponse(code = 400, message = "Missing or invalid payload",
                         response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error happened while storing the data",
                         response = ApiError.class),
    })
    public void addDataForMetric(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenantId") final String tenantId, @PathParam("id") String id,
            @ApiParam(value = "List of datapoints containing timestamp and value", required = true)
            List<NumericData> data
    ) {
        if (data == null) {
            Response response = Response.status(Status.BAD_REQUEST).entity(new ApiError("Payload is empty")).build();
            asyncResponse.resume(response);
            return;
        }
        NumericMetric metric = new NumericMetric(tenantId, new MetricId(id));
        data.forEach(metric::addData);
        ListenableFuture<Void> future = metricsService.addNumericData(Collections.singletonList(metric));
        Futures.addCallback(future, new NoDataCallback<Void>(asyncResponse));
    }

    @POST
    @Path("/{tenantId}/metrics/availability/{id}/data")
    @ApiOperation(value = "Add data for a single availability metric.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Adding data succeeded."),
            @ApiResponse(code = 400, message = "Missing or invalid payload",
                         response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error happened while storing the data",
                         response = ApiError.class)
    })
    public void addAvailabilityForMetric(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenantId") final String tenantId, @PathParam("id") String id,
            @ApiParam(value = "List of availability datapoints", required = true) List<Availability> data
    ) {
        if (data == null) {
            Response response = Response.status(Status.BAD_REQUEST).entity(new ApiError("Payload is empty")).build();
            asyncResponse.resume(response);
            return;
        }
        AvailabilityMetric metric = new AvailabilityMetric(tenantId, new MetricId(id));
        data.forEach(metric::addData);
        ListenableFuture<Void> future = metricsService.addAvailabilityData(Collections.singletonList(metric));
        Futures.addCallback(future, new NoDataCallback<Void>(asyncResponse));
    }

    @POST
    @Path("/{tenantId}/metrics/numeric/data")
    @ApiOperation(value = "Add metric data for multiple numeric metrics in a single call.")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Adding data succeeded."),
            @ApiResponse(code = 500, message = "Unexpected error happened while storing the data",
                         response = ApiError.class)
    })
    public void addNumericData(@Suspended final AsyncResponse asyncResponse,
                               @PathParam("tenantId") String tenantId,
                               @ApiParam(value = "List of metrics", required = true)
                               List<NumericMetric> metrics) {
        if (metrics.isEmpty()) {
            asyncResponse.resume(Response.ok().build());
        }

        for (NumericMetric metric : metrics) {
            metric.setTenantId(tenantId);
        }

        ListenableFuture<Void> future = metricsService.addNumericData(metrics);
        Futures.addCallback(future, new NoDataCallback<Void>(asyncResponse));
    }

    @POST
    @Path("/{tenantId}/metrics/availability/data")
    @ApiOperation(value = "Add metric data for multiple availability metrics in a single call.")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Adding data succeeded."),
            @ApiResponse(code = 500, message = "Unexpected error happened while storing the data",
                         response = ApiError.class)
    })
    public void addAvailabilityData(@Suspended final AsyncResponse asyncResponse,
        @PathParam("tenantId") String tenantId, @ApiParam(value = "List of availability metrics", required = true)
        List<AvailabilityMetric> metrics) {
        if (metrics.isEmpty()) {
            asyncResponse.resume(Response.ok().build());
        }

        for (AvailabilityMetric metric : metrics) {
            metric.setTenantId(tenantId);
        }

        ListenableFuture<Void> future = metricsService.addAvailabilityData(metrics);
        Futures.addCallback(future, new NoDataCallback<Void>(asyncResponse));
    }

    @GET
    @Path("/{tenantId}/numeric")
    @ApiOperation(value = "Find numeric metrics data by their tags.", response = Map.class,
            responseContainer = "List")
    @ApiResponses(value = { @ApiResponse(code = 200, message = ""),
                            @ApiResponse(code = 500, message = "Any error in the query.", response = ApiError.class)
    })
    public void findNumericDataByTags(@Suspended final AsyncResponse asyncResponse,
        @PathParam("tenantId") String tenantId,
        @ApiParam(allowMultiple = true, required = true, value = "A list of tags in the format of name:value")
        @QueryParam("tags") String encodedTags) {
        ListenableFuture<Map<MetricId, Set<NumericData>>> queryFuture = metricsService.findNumericDataByTags(
            tenantId, MetricUtils.decodeTags(encodedTags));
        Futures.addCallback(queryFuture, new SimpleDataCallback<Map<MetricId, Set<NumericData>>>(asyncResponse));
    }

    @GET
    @Path("/{tenantId}/availability")
    @ApiOperation(value = "Find availabilities metrics data by their tags.", response = Map.class,
            responseContainer = "List")
    // See above method and HWKMETRICS-26 for fixes.
    @ApiResponses(value = { @ApiResponse(code = 200, message = ""),
            @ApiResponse(code = 204, message = "No matching availability metrics were found."),
            @ApiResponse(code = 500, message = "Any error in the query.", response = ApiError.class)
    })
    public void findAvailabilityDataByTags(@Suspended final AsyncResponse asyncResponse,
        @PathParam("tenantId") String tenantId,
        @ApiParam(allowMultiple = true, required = true, value = "A list of tags in the format of name:value")
        @QueryParam("tags") String encodedTags) {
        ListenableFuture<Map<MetricId, Set<Availability>>> queryFuture = metricsService.findAvailabilityByTags(
            tenantId, MetricUtils.decodeTags(encodedTags));
        Futures.addCallback(queryFuture, new SimpleDataCallback<Map<MetricId, Set<Availability>>>(asyncResponse));
    }

    @GET
    @Path("/{tenantId}/metrics/numeric/{id}/data")
    @ApiOperation(value = "Retrieve numeric data. When buckets or bucketDuration query parameter is used, the time "
                          + "range between start and end will be divided in buckets of equal duration, and metric "
                          + "statistics will be computed for each bucket.", response = List.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully fetched numeric data."),
            @ApiResponse(code = 204, message = "No numeric data was found."),
            @ApiResponse(code = 400, message = "buckets or bucketDuration parameter is invalid, or both are used.",
                         response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching numeric data.",
                         response = ApiError.class)
    })
    public void findNumericData(
            @Suspended AsyncResponse asyncResponse,
            @PathParam("tenantId") String tenantId,
            @PathParam("id") String id,
            @ApiParam(value = "Defaults to now - 8 hours") @QueryParam("start") Long start,
            @ApiParam(value = "Defaults to now") @QueryParam("end") Long end,
            @ApiParam(value = "Total number of buckets") @QueryParam("buckets") Integer bucketsCount,
            @ApiParam(value = "Bucket duration") @QueryParam("bucketDuration") Duration bucketDuration
    ) {
        long now = System.currentTimeMillis();
        start = start == null ? now - EIGHT_HOURS : start;
        end = end == null ? now : end;

        NumericMetric metric = new NumericMetric(tenantId, new MetricId(id));

        if (bucketsCount == null && bucketDuration == null) {
            ListenableFuture<NumericMetric> dataFuture = metricsService.findNumericData(metric, start, end);
            ListenableFuture<List<NumericData>> outputFuture = Futures.transform(
                    dataFuture, new Function<NumericMetric, List<NumericData>>() {
                        @Override
                        public List<NumericData> apply(NumericMetric input) {
                            if (input == null) {
                                return null;
                            }
                            return input.getData();
                        }
                    }
            );
            Futures.addCallback(outputFuture, new SimpleDataCallback<Object>(asyncResponse));
            return;
        }

        if (bucketsCount != null && bucketDuration != null) {
            ApiError apiError = new ApiError("Both buckets and bucketDuration parameters are used");
            Response response = Response.status(Status.BAD_REQUEST).entity(apiError).build();
            asyncResponse.resume(response);
            return;
        }

        Buckets buckets;
        try {
            if (bucketsCount != null) {
                buckets = Buckets.fromCount(start, end, bucketsCount);
            } else {
                buckets = Buckets.fromStep(start, end, bucketDuration.toMillis());
            }
        } catch (IllegalArgumentException e) {
            ApiError apiError = new ApiError("Bucket: " + e.getMessage());
            Response response = Response.status(Status.BAD_REQUEST).entity(apiError).build();
            asyncResponse.resume(response);
            return;
        }
        ListenableFuture<BucketedOutput<NumericBucketDataPoint>> dataFuture = metricsService.findNumericStats(
                metric, start, end, buckets
        );
        ListenableFuture<List<NumericBucketDataPoint>> outputFuture = Futures.transform(
                dataFuture, new Function<BucketedOutput<NumericBucketDataPoint>, List<NumericBucketDataPoint>>() {
                    @Override
                    public List<NumericBucketDataPoint> apply(BucketedOutput<NumericBucketDataPoint> input) {
                        if (input == null) {
                            return null;
                        }
                        return input.getData();
                    }
                }
        );
        Futures.addCallback(outputFuture, new SimpleDataCallback<Object>(asyncResponse));
    }

    @GET
    @Path("/{tenantId}/metrics/numeric/{id}/periods")
    @ApiOperation(value = "Retrieve periods for which the condition holds true for each consecutive data point.",
        response = List.class)
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "Successfully fetched periods."),
        @ApiResponse(code = 204, message = "No numeric data was found."),
        @ApiResponse(code = 400, message = "Missing or invalid query parameters")})
    public void findPeriods(
        @Suspended final AsyncResponse response, @PathParam("tenantId") String tenantId,
        @PathParam("id") String id,
        @ApiParam(value = "Defaults to now - 8 hours", required = false) @QueryParam("start") Long start,
        @ApiParam(value = "Defaults to now", required = false) @QueryParam("end") Long end,
        @ApiParam(value = "A threshold against which values are compared", required = true) @QueryParam("threshold")
            double threshold,
        @ApiParam(value = "A comparison operation to perform between values and the threshold. Supported operations " +
            "include ge, gte, lt, lte, and eq", required = true) @QueryParam("op") String operator) {

        long now = System.currentTimeMillis();
        if (start == null) {
            start = now - EIGHT_HOURS;
        }
        if (end == null) {
            end = now;
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
            response.resume(Response.status(Status.BAD_REQUEST).entity(new ApiError("Invalid value for op parameter. "
                + "Supported values are lt, lte, eq, gt, gte")).build());
        } else {
            ListenableFuture<List<long[]>> future = metricsService.getPeriods(tenantId, new MetricId(id), predicate,
                start, end);
            // We need to transform empty results to null because SimpleDataCallback returns a 204 status for null
            // data.
            future = Futures.transform(future, (List<long[]> periods) -> periods.isEmpty() ? null : periods);
            Futures.addCallback(future, new SimpleDataCallback<>(response));
        }
    }

    @GET
    @Path("/{tenantId}/metrics/availability/{id}/data")
    @ApiOperation(
            value = "Retrieve availability data. When buckets or bucketDuration query parameter is used, the time "
                    + "range between start and end will be divided in buckets of equal duration, and availability "
                    + "statistics will be computed for each bucket.", response = List.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully fetched availability data."),
            @ApiResponse(code = 204, message = "No availability data was found."),
            @ApiResponse(code = 400, message = "buckets or bucketDuration parameter is invalid, or both are used.",
                         response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching availability data.",
                         response = ApiError.class),
    })
    public void findAvailabilityData(
            @Suspended AsyncResponse asyncResponse,
            @PathParam("tenantId") String tenantId,
            @PathParam("id") String id,
            @ApiParam(value = "Defaults to now - 8 hours") @QueryParam("start") Long start,
            @ApiParam(value = "Defaults to now") @QueryParam("end") Long end,
            @ApiParam(value = "Total number of buckets") @QueryParam("buckets") Integer bucketsCount,
            @ApiParam(value = "Bucket duration") @QueryParam("bucketDuration") Duration bucketDuration
    ) {
        long now = System.currentTimeMillis();
        start = start == null ? now - EIGHT_HOURS : start;
        end = end == null ? now : end;

        AvailabilityMetric metric = new AvailabilityMetric(tenantId, new MetricId(id));

        if (bucketsCount == null && bucketDuration == null) {
            ListenableFuture<AvailabilityMetric> dataFuture = metricsService.findAvailabilityData(metric, start, end);
            ListenableFuture<List<Availability>> outputFuture = Futures.transform(
                    dataFuture, new Function<AvailabilityMetric, List<Availability>>() {
                        @Override
                        public List<Availability> apply(AvailabilityMetric input) {
                            if (input == null) {
                                return null;
                            }
                            return input.getData();
                        }
                    }
            );
            Futures.addCallback(outputFuture, new SimpleDataCallback<Object>(asyncResponse));
            return;
        }

        if (bucketsCount != null && bucketDuration != null) {
            ApiError apiError = new ApiError("Both buckets and bucketDuration parameters are used");
            Response response = Response.status(Status.BAD_REQUEST).entity(apiError).build();
            asyncResponse.resume(response);
            return;
        }

        Buckets buckets;
        try {
            if (bucketsCount != null) {
                buckets = Buckets.fromCount(start, end, bucketsCount);
            } else {
                buckets = Buckets.fromStep(start, end, bucketDuration.toMillis());
            }
        } catch (IllegalArgumentException e) {
            ApiError apiError = new ApiError("Bucket: " + e.getMessage());
            Response response = Response.status(Status.BAD_REQUEST).entity(apiError).build();
            asyncResponse.resume(response);
            return;
        }
        ListenableFuture<BucketedOutput<AvailabilityBucketDataPoint>> dataFuture = metricsService.findAvailabilityStats(
                metric, start, end, buckets
        );
        ListenableFuture<List<AvailabilityBucketDataPoint>> outputFuture = Futures.transform(
                dataFuture,
                new Function<BucketedOutput<AvailabilityBucketDataPoint>, List<AvailabilityBucketDataPoint>>() {
                    @Override
                    public List<AvailabilityBucketDataPoint> apply(BucketedOutput<AvailabilityBucketDataPoint> input) {
                        if (input == null) {
                            return null;
                        }
                        return input.getData();
                    }
                }
        );
        Futures.addCallback(outputFuture, new SimpleDataCallback<Object>(asyncResponse));
    }

    @POST
    @Path("/{tenantId}/metrics/numeric/{id}/tag")
    @ApiOperation(value = "Add or update numeric metric's tags.")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Tags were modified successfully.")})
    public void tagNumericData(@Suspended final AsyncResponse asyncResponse, @PathParam("tenantId") String tenantId,
            @PathParam("id") final String id, @ApiParam(required = true) TagRequest params) {
        ListenableFuture<List<NumericData>> future;
        NumericMetric metric = new NumericMetric(tenantId, new MetricId(id));
        if (params.getTimestamp() != null) {
            future = metricsService.tagNumericData(metric, params.getTags(), params.getTimestamp());
        } else {
            future = metricsService.tagNumericData(metric, params.getTags(), params.getStart(), params.getEnd());
        }
        Futures.addCallback(future, new NoDataCallback<List<NumericData>>(asyncResponse));
    }

    @POST
    @Path("/{tenantId}/metrics/availability/{id}/tag")
    @ApiOperation(value = "Add or update availability metric's tags.")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Tags were modified successfully.")})
    public void tagAvailabilityData(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenantId") String tenantId, @PathParam("id") final String id,
            @ApiParam(required = true) TagRequest params) {
        ListenableFuture<List<Availability>> future;
        AvailabilityMetric metric = new AvailabilityMetric(tenantId, new MetricId(id));
        if (params.getTimestamp() != null) {
            future = metricsService.tagAvailabilityData(metric, params.getTags(), params.getTimestamp());
        } else {
            future = metricsService.tagAvailabilityData(metric, params.getTags(), params.getStart(), params.getEnd());
        }
        Futures.addCallback(future, new NoDataCallback<List<Availability>>(asyncResponse));
    }

    @GET
    @Path("/{tenantId}/tags/numeric/{tag}")
    @ApiOperation(value = "Find numeric metric data with given tags.", response = Map.class,
            responseContainer = "List")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Numeric values fetched successfully"),
                            @ApiResponse(code = 500, message = "Any error while fetching data.",
                                         response = ApiError.class)
    })
    public void findTaggedNumericData(@Suspended final AsyncResponse asyncResponse,
        @PathParam("tenantId") String tenantId,
        @ApiParam(allowMultiple = true, required = true, value = "A list of tags in the format of name:value")
        @PathParam("tag") String encodedTag) {
        ListenableFuture<Map<MetricId, Set<NumericData>>> queryFuture = metricsService.findNumericDataByTags(
                tenantId, MetricUtils.decodeTags(encodedTag));
        ListenableFuture<Map<String, Set<NumericData>>> resultFuture = Futures.transform(
                queryFuture,
                new Function<Map<MetricId, Set<NumericData>>, Map<String, Set<NumericData>>>() {
                    @Override
                    public Map<String, Set<NumericData>> apply(Map<MetricId, Set<NumericData>> input) {
                        Map<String, Set<NumericData>> result = new HashMap<String, Set<NumericData>>(input.size());
                        for (Map.Entry<MetricId, Set<NumericData>> entry : input.entrySet()) {
                            result.put(entry.getKey().getName(), entry.getValue());
                        }
                        return result;
                    }
                }
        );
        Futures.addCallback(resultFuture, new SimpleDataCallback<Map<String, Set<NumericData>>>(asyncResponse));
    }

    @GET
    @Path("/{tenantId}/tags/availability/{tag}")
    @ApiOperation(value = "Find availability metric data with given tags.", response = Map.class,
            responseContainer = "List")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Availability values fetched successfully"),
                            @ApiResponse(code = 500, message = "Any error while fetching data.",
                                         response = ApiError.class)
    })
    public void findTaggedAvailabilityData(@Suspended final AsyncResponse asyncResponse,
        @PathParam("tenantId") String tenantId,
        @ApiParam(allowMultiple = true, required = true, value = "A list of tags in the format of name:value")
        @PathParam("tag") String encodedTag) {
        ListenableFuture<Map<MetricId, Set<Availability>>> queryFuture = metricsService.findAvailabilityByTags(tenantId,
            MetricUtils.decodeTags(encodedTag));
        Futures.addCallback(queryFuture, new SimpleDataCallback<Map<MetricId, Set<Availability>>>(asyncResponse));
    }

    @POST
    @Path("/counters")
    @ApiOperation(value = "List of counter definitions", hidden = true)
    public void updateCountersForGroups(@Suspended final AsyncResponse asyncResponse, Collection<Counter> counters) {
        ListenableFuture<Void> future = metricsService.updateCounters(counters);
        Futures.addCallback(future, new NoDataCallback<Void>(asyncResponse));
    }

    @POST
    @Path("/counters/{group}")
    @ApiOperation(value = "Update multiple counters in a single counter group", hidden = true)
    public void updateCounterForGroup(@Suspended final AsyncResponse asyncResponse, @PathParam("group") String group,
        Collection<Counter> counters) {
        for (Counter counter : counters) {
            counter.setGroup(group);
        }
        ListenableFuture<Void> future = metricsService.updateCounters(counters);
        Futures.addCallback(future, new NoDataCallback<Void>(asyncResponse));
    }

    @POST
    @Path("/counters/{group}/{counter}")
    @ApiOperation(value = "Increase value of a counter", hidden = true)
    public void updateCounter(@Suspended final AsyncResponse asyncResponse, @PathParam("group") String group,
        @PathParam("counter") String counter) {
        ListenableFuture<Void> future = metricsService
                .updateCounter(new Counter(DEFAULT_TENANT_ID, group, counter, 1L));
        Futures.addCallback(future, new NoDataCallback<Void>(asyncResponse));
    }

    @POST
    @Path("/counters/{group}/{counter}/{value}")
    @ApiOperation(value = "Update value of a counter", hidden = true)
    public void updateCounter(@Suspended final AsyncResponse asyncResponse, @PathParam("group") String group,
        @PathParam("counter") String counter, @PathParam("value") Long value) {
        ListenableFuture<Void> future = metricsService.updateCounter(new Counter(DEFAULT_TENANT_ID, group, counter,
                value));
        Futures.addCallback(future, new NoDataCallback<Void>(asyncResponse));
    }

    @GET
    @Path("/counters/{group}")
    @ApiOperation(value = "Retrieve a list of counter values in this group", hidden = true, response = Counter.class,
            responseContainer = "List")
    @Produces({ APPLICATION_JSON })
    public void getCountersForGroup(@Suspended final AsyncResponse asyncResponse, @PathParam("group") String group) {
        ListenableFuture<List<Counter>> future = metricsService.findCounters(group);
        Futures.addCallback(future, new SimpleDataCallback<List<Counter>>(asyncResponse));
    }

    @GET
    @Path("/counters/{group}/{counter}")
    @ApiOperation(value = "Retrieve value of a counter", hidden = true, response = Counter.class,
            responseContainer = "List")
    public void getCounter(@Suspended final AsyncResponse asyncResponse, @PathParam("group") final String group,
        @PathParam("counter") final String counter) {
        ListenableFuture<List<Counter>> future = metricsService.findCounters(group, Collections.singletonList(counter));
        Futures.addCallback(future, new FutureCallback<List<Counter>>() {
            @Override
            public void onSuccess(List<Counter> counters) {
                if (counters.isEmpty()) {
                    asyncResponse.resume(Response.status(404).entity("Counter[group: " + group + ", name: " +
                            counter + "] not found").build());
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

    @GET
    @Path("/{tenantId}/metrics")
    @ApiOperation(value = "Find tenant's metric definitions.", notes = "Does not include any metric values. ",
            response = List.class, responseContainer = "List")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Successfully retrieved at least one metric "
            + "definition."),
            @ApiResponse(code = 204, message = "No metrics found."),
            @ApiResponse(code = 400, message = "Given type is not a valid type.", response = ApiError.class),
            @ApiResponse(code = 500, message = "Failed to retrieve metrics due to unexpected error.",
                         response = ApiError.class)
    })
    public void findMetrics(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenantId") final String tenantId,
        @ApiParam(value = "Queried metric type", required = true, allowableValues = "[num, avail, log]")
        @QueryParam("type") String type) {
        MetricType metricType = null;
        try {
            metricType = MetricType.fromTextCode(type);
        } catch (IllegalArgumentException e) {
            ApiError errors = new ApiError("[" + type + "] is not a valid type. Accepted values are num|avail|log");
            asyncResponse.resume(Response.status(Status.BAD_REQUEST).entity(errors).build());
        }
        ListenableFuture<List<Metric<?>>> future = metricsService.findMetrics(tenantId, metricType);
        Futures.addCallback(future, new SimpleDataCallback<List<Metric<?>>>(asyncResponse));
    }

}
