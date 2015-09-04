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
import static org.hawkular.metrics.api.jaxrs.util.ApiUtils.requestToGaugeDataPoints;
import static org.hawkular.metrics.api.jaxrs.util.ApiUtils.requestToGauges;
import static org.hawkular.metrics.api.jaxrs.validation.Validate.validate;
import static org.hawkular.metrics.core.api.MetricType.GAUGE;

import java.net.URI;
import java.util.List;
import java.util.Map;
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
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.hawkular.metrics.api.jaxrs.ApiError;
import org.hawkular.metrics.api.jaxrs.model.Gauge;
import org.hawkular.metrics.api.jaxrs.model.GaugeDataPoint;
import org.hawkular.metrics.api.jaxrs.param.Duration;
import org.hawkular.metrics.api.jaxrs.param.Tags;
import org.hawkular.metrics.api.jaxrs.request.MetricDefinition;
import org.hawkular.metrics.api.jaxrs.request.TagRequest;
import org.hawkular.metrics.api.jaxrs.util.ApiUtils;
import org.hawkular.metrics.core.api.Buckets;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricAlreadyExistsException;
import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.MetricType;
import org.hawkular.metrics.core.api.MetricsService;

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;

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
    @Produces(APPLICATION_JSON)
    @Path("/")
    @ApiOperation(value = "Create gauge metric.", notes = "Clients are not required to explicitly create "
            + "a metric before storing data. Doing so however allows clients to prevent naming collisions and to "
            + "specify tags and data retention.")
    @ApiResponses(value = {
            @ApiResponse(code = 201, message = "Metric created successfully"),
            @ApiResponse(code = 400, message = "Missing or invalid payload", response = ApiError.class),
            @ApiResponse(code = 409, message = "Gauge metric with given id already exists",
                    response = ApiError.class),
            @ApiResponse(code = 500, message = "Metric creation failed due to an unexpected error",
                    response = ApiError.class)
    })
    public Response createGaugeMetric(
            @ApiParam(required = true) MetricDefinition metricDefinition,
            @Context UriInfo uriInfo
    ) {
        if(metricDefinition.getType() != null && MetricType.GAUGE != metricDefinition.getType()) {
            return ApiUtils.badRequest(new ApiError("MetricDefinition type does not match " + MetricType
                    .GAUGE.getText()));
        }
        Metric<Double> metric = new Metric<>(new MetricId<>(tenantId, GAUGE, metricDefinition.getId()),
                metricDefinition.getTags(), metricDefinition.getDataRetention());
        URI location = uriInfo.getBaseUriBuilder().path("/gauges/{id}").build(metric.getId().getName());
        try {
            Observable<Void> observable = metricsService.createMetric(metric);
            observable.toBlocking().lastOrDefault(null);
            return Response.created(location).build();
        } catch (MetricAlreadyExistsException e) {
            String message = "A metric with name [" + e.getMetric().getId().getName() + "] already exists";
            return Response.status(Response.Status.CONFLICT).entity(new ApiError(message)).build();
        } catch (Exception e) {
            return ApiUtils.serverError(e);
        }
    }

    @GET
    @Produces(APPLICATION_JSON)
    @Path("/{id}")
    @ApiOperation(value = "Retrieve single metric definition.", response = MetricDefinition.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Metric's definition was successfully retrieved."),
            @ApiResponse(code = 204, message = "Query was successful, but no metrics definition is set."),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching metric's definition.",
                         response = ApiError.class) })
    public Response getGaugeMetric(@PathParam("id") String id) {
        try {
            return metricsService.findMetric(new MetricId<>(tenantId, GAUGE, id))
                .map(MetricDefinition::new)
                .map(metricDef -> Response.ok(metricDef).build())
                .switchIfEmpty(Observable.just(ApiUtils.noContent())).toBlocking().lastOrDefault(null);
        } catch (Exception e) {
            return ApiUtils.serverError(e);
        }
    }

    @GET
    @Produces(APPLICATION_JSON)
    @Path("/{id}/tags")
    @ApiOperation(value = "Retrieve tags associated with the metric definition.", response = String.class,
                  responseContainer = "Map")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Metric's tags were successfully retrieved."),
            @ApiResponse(code = 204, message = "Query was successful, but no metrics were found."),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching metric's tags.",
                response = ApiError.class) })
    public Response getGaugeMetricTags(@PathParam("id") String id) {
        try {
            return metricsService.getMetricTags(new MetricId<>(tenantId, GAUGE, id))
                    .map(ApiUtils::valueToResponse)
                    .toBlocking().lastOrDefault(null);
        } catch (Exception e) {
            return ApiUtils.serverError(e);
        }
    }

    @PUT
    @Produces(APPLICATION_JSON)
    @Path("/{id}/tags")
    @ApiOperation(value = "Update tags associated with the metric definition.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Metric's tags were successfully updated."),
            @ApiResponse(code = 500, message = "Unexpected error occurred while updating metric's tags.",
                response = ApiError.class) })
    public Response updateGaugeMetricTags(
            @PathParam("id") String id,
            @ApiParam(required = true) Map<String, String> tags
    ) {
        Metric<Double> metric = new Metric<>(new MetricId<>(tenantId, GAUGE, id));
        try {
            metricsService.addTags(metric, tags).toBlocking().lastOrDefault(null);
            return Response.ok().build();
        } catch (Exception e) {
            return ApiUtils.serverError(e);
        }
    }

    @DELETE
    @Produces(APPLICATION_JSON)
    @Path("/{id}/tags/{tags}")
    @ApiOperation(value = "Delete tags associated with the metric definition.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Metric's tags were successfully deleted."),
            @ApiResponse(code = 400, message = "Invalid tags", response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error occurred while trying to delete metric's tags.",
                response = ApiError.class) })
    public Response deleteGaugeMetricTags(
            @PathParam("id") String id,
            @ApiParam("Tag list") @PathParam("tags") Tags tags
    ) {
        try {
            Metric<Double> metric = new Metric<>(new MetricId<>(tenantId, GAUGE, id));
            metricsService.deleteTags(metric, tags.getTags()).toBlocking().lastOrDefault(null);
            return Response.ok().build();
        } catch (Exception e) {
            return ApiUtils.serverError(e);
        }
    }

    @POST
    @Produces(APPLICATION_JSON)
    @Path("/{id}/data")
    @ApiOperation(value = "Add data for a single gauge metric.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Adding data succeeded."),
            @ApiResponse(code = 400, message = "Missing or invalid payload", response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error happened while storing the data",
                    response = ApiError.class),
    })
    public Response addDataForMetric(
            @PathParam("id") String id,
            @ApiParam(value = "List of datapoints containing timestamp and value", required = true)
            List<GaugeDataPoint> data
    ) {
        if (!validate(data).toBlocking().lastOrDefault(false)) {
            return badRequest(new ApiError("Timestamp not provided for data point"));
        }

        Metric<Double> metric = new Metric<>(new MetricId<>(tenantId, GAUGE, id), requestToGaugeDataPoints(data));
        try {
            metricsService.addDataPoints(GAUGE, Observable.just(metric)).toBlocking().lastOrDefault(null);
            return Response.ok().build();
        } catch (Exception e) {
            return ApiUtils.serverError(e);
        }
    }

    @POST
    @Produces(APPLICATION_JSON)
    @Path("/data")
    @ApiOperation(value = "Add data for multiple gauge metrics in a single call.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Adding data succeeded."),
            @ApiResponse(code = 400, message = "Missing or invalid payload", response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error happened while storing the data",
                    response = ApiError.class)
    })
    public Response addGaugeData(@ApiParam(value = "List of metrics", required = true) List<Gauge> gauges
    ) {
        if (!validate(gauges).toBlocking().lastOrDefault(false)) {
            return badRequest(new ApiError("Timestamp not provided for data point"));
        }

        Observable<Metric<Double>> metrics = requestToGauges(tenantId, gauges);
        try {
            metricsService.addDataPoints(GAUGE, metrics).toBlocking().lastOrDefault(null);
            return Response.ok().build();
        } catch (Exception e) {
            return ApiUtils.serverError(e);
        }
    }

    @GET
    @Produces(APPLICATION_JSON)
    @Path("/")
    @ApiOperation(value = "Find gauge metrics data by their tags.", response = Map.class, responseContainer = "List")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Successfully fetched data."),
            @ApiResponse(code = 204, message = "No matching data found."),
            @ApiResponse(code = 400, message = "Missing or invalid tags query", response = ApiError.class),
            @ApiResponse(code = 500, message = "Any error in the query.", response = ApiError.class), })
    public Response findGaugeDataByTags(@ApiParam(value = "Tag list", required = true) @QueryParam("tags") Tags tags) {
        if (tags == null) {
            return badRequest(new ApiError("Missing tags query"));
        } else {
            try{
                return metricsService.findGaugeDataByTags(tenantId, tags.getTags()).map(m -> {
                    if (m.isEmpty()) {
                        return ApiUtils.noContent();
                    } else {
                        return Response.ok(m).build();
                    }
                }).toBlocking().lastOrDefault(null);
            } catch (Exception e) {
                return Response.serverError().entity(new ApiError(e.getMessage())).build();
            }
        }
    }

    @GET
    @Path("/{id}/data")
    @Produces(APPLICATION_JSON)
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
    public Response findGaugeData(
            @PathParam("id") String id,
            @ApiParam(value = "Defaults to now - 8 hours") @QueryParam("start") final Long start,
            @ApiParam(value = "Defaults to now") @QueryParam("end") final Long end,
            @ApiParam(value = "Total number of buckets") @QueryParam("buckets") Integer bucketsCount,
            @ApiParam(value = "Bucket duration") @QueryParam("bucketDuration") Duration bucketDuration) {

        long now = System.currentTimeMillis();
        long startTime = start == null ? now - EIGHT_HOURS : start;
        long endTime = end == null ? now : end;

        MetricId<Double> metricId = new MetricId<>(tenantId, GAUGE, id);

        if (bucketsCount == null && bucketDuration == null) {
            try {
                return metricsService
                        .findDataPoints(metricId, startTime, endTime)
                        .map(GaugeDataPoint::new)
                        .toList()
                        .map(ApiUtils::collectionToResponse)
                        .toBlocking()
                        .lastOrDefault(null);
            } catch (Exception e) {
                return ApiUtils.serverError(e);
            }
        } else if (bucketsCount != null && bucketDuration != null) {
            return badRequest(new ApiError("Both buckets and bucketDuration parameters are used"));
        } else {
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

            try {
                return metricsService
                        .findGaugeStats(metricId, startTime, endTime, buckets)
                        .map(ApiUtils::collectionToResponse)
                        .toBlocking()
                        .lastOrDefault(null);
            } catch (Exception e) {
                return ApiUtils.serverError(e);
            }
        }
    }

    @GET
    @Produces(APPLICATION_JSON)
    @Path("/{id}/periods")
    @ApiOperation(value = "Retrieve periods for which the condition holds true for each consecutive data point.",
        response = List.class)
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Successfully fetched periods."),
            @ApiResponse(code = 204, message = "No data was found."),
            @ApiResponse(code = 400, message = "Missing or invalid query parameters") })
    public Response findPeriods(
            @PathParam("id") String id,
            @ApiParam(value = "Defaults to now - 8 hours", required = false) @QueryParam("start") final Long start,
            @ApiParam(value = "Defaults to now", required = false) @QueryParam("end") final Long end,
            @ApiParam(value = "A threshold against which values are compared", required = true)
            @QueryParam("threshold") double threshold,
            @ApiParam(value = "A comparison operation to perform between values and the threshold."
                              + " Supported operations include ge, gte, lt, lte, and eq", required = true,
                      allowableValues = "[ge, gte, lt, lte, eq, neq]")
            @QueryParam("op") String operator
    ) {
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
        switch (operator) { // Why not enum?
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
            return badRequest(
                    new ApiError("Invalid value for op parameter. Supported values are lt, lte, eq, gt, gte."));
        } else {
            try {
                return metricsService.getPeriods(new MetricId<>(tenantId, GAUGE, id), predicate, startTime, endTime)
                        .map(ApiUtils::collectionToResponse).toBlocking().lastOrDefault(null);
            } catch (Exception e) {
                return ApiUtils.serverError(e);
            }
        }
    }

    @GET
    @Produces(APPLICATION_JSON)
    @Path("/tags/{tags}")
    @ApiOperation(value = "Find metric data with given tags.", response = Map.class, responseContainer = "List")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Me values fetched successfully"),
            @ApiResponse(code = 204, message = "No matching data found."),
            @ApiResponse(code = 400, message = "Invalid tags", response = ApiError.class),
            @ApiResponse(code = 500, message = "Any error while fetching data.", response = ApiError.class), })
    public Response findTaggedGaugeData(@ApiParam("Tag list") @PathParam("tags") Tags tags) {
        try {
            return metricsService.findGaugeDataByTags(tenantId, tags.getTags())
                    .flatMap(input -> Observable.from(input.entrySet()))
                    .toMap(e -> e.getKey().getName(), Map.Entry::getValue)
                    .map(m -> {
                        if (m.isEmpty()) {
                        return ApiUtils.noContent();
                        } else {
                            return Response.ok(m).build();
                        }
                    }).toBlocking().lastOrDefault(null);
        } catch (Exception e) {
            return ApiUtils.serverError(e);
        }
    }

    @POST
    @Produces(APPLICATION_JSON)
    @Path("/{id}/tag")
    @ApiOperation(value = "Add or update gauge metric's tags.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Tags were modified successfully."),
            @ApiResponse(code = 400, message = "Missing or invalid payload", response = ApiError.class),
            @ApiResponse(code = 500, message = "Processing tags failed")
    })
    public Response tagGaugeData(@PathParam("id") final String id, @ApiParam(required = true) TagRequest params) {
        Observable<Void> resultSetObservable;
        Metric<Double> metric = new Metric<>(new MetricId<>(tenantId, GAUGE, id));
        if (params.getTimestamp() != null) {
            resultSetObservable = metricsService.tagGaugeData(metric, params.getTags(), params.getTimestamp());
        } else {
            resultSetObservable = metricsService.tagGaugeData(metric, params.getTags(), params.getStart(), params
                    .getEnd());
        }
        try {
            resultSetObservable.toBlocking().lastOrDefault(null);
            return Response.ok().build();
        } catch (Exception e) {
            return ApiUtils.serverError(e);
        }
    }
}
