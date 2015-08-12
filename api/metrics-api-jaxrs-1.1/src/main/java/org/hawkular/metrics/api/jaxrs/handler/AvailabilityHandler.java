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

import static org.hawkular.metrics.api.jaxrs.util.ApiUtils.noContent;
import static org.hawkular.metrics.api.jaxrs.util.ApiUtils.requestToAvailabilities;
import static org.hawkular.metrics.api.jaxrs.util.ApiUtils.requestToAvailabilityDataPoints;
import static org.hawkular.metrics.api.jaxrs.util.ApiUtils.valueToResponse;
import static org.hawkular.metrics.core.api.MetricType.AVAILABILITY;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
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
import org.hawkular.metrics.api.jaxrs.filter.TenantFilter;
import org.hawkular.metrics.api.jaxrs.model.Availability;
import org.hawkular.metrics.api.jaxrs.model.AvailabilityDataPoint;
import org.hawkular.metrics.api.jaxrs.param.Duration;
import org.hawkular.metrics.api.jaxrs.param.Tags;
import org.hawkular.metrics.api.jaxrs.request.MetricDefinition;
import org.hawkular.metrics.api.jaxrs.request.TagRequest;
import org.hawkular.metrics.api.jaxrs.util.ApiUtils;
import org.hawkular.metrics.core.api.AvailabilityType;
import org.hawkular.metrics.core.api.BucketedOutput;
import org.hawkular.metrics.core.api.Buckets;
import org.hawkular.metrics.core.api.DataPoint;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricAlreadyExistsException;
import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.MetricsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
@Path("/availability")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
@Api(value = "", description = "Availability metrics interface")
public class AvailabilityHandler {
    private static final long EIGHT_HOURS = MILLISECONDS.convert(8, HOURS);

    private static final Logger logger = LoggerFactory.getLogger(AvailabilityHandler.class);

    @Inject
    private MetricsService metricsService;

    @HeaderParam(TenantFilter.TENANT_HEADER_NAME)
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
                    response = ApiError.class)
    })
    public Response createAvailabilityMetric(
            @ApiParam(required = true) MetricDefinition metricDefinition,
            @Context UriInfo uriInfo
    ) {
        URI location = uriInfo.getBaseUriBuilder().path("/availability/{id}").build(metricDefinition.getId());
        Metric<?> metric = new Metric<DataPoint<?>>(new MetricId(tenantId, AVAILABILITY, metricDefinition.getId()),
                metricDefinition.getTags(), metricDefinition.getDataRetention());
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
    @Path("/{id}")
    @ApiOperation(value = "Retrieve single metric definition.", response = MetricDefinition.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Metric's definition was successfully retrieved."),
            @ApiResponse(code = 204, message = "Query was successful, but no metrics definition is set."),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching metric's definition.",
                         response = ApiError.class) })
    public Response getAvailabilityMetric(@HeaderParam("tenantId") String tenantId, @PathParam("id") String id) {
        try {
            return metricsService.findMetric(new MetricId(tenantId, AVAILABILITY, id))
                .map(MetricDefinition::new)
                .map(metricDef -> Response.ok(metricDef).build())
                .switchIfEmpty(Observable.just(noContent()))
                .toBlocking()
                .firstOrDefault(null);
        } catch (Exception e) {
            return ApiUtils.serverError(e);
        }
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
    public Response getAvailabilityMetricTags(
            @PathParam("id") String id
    ) {
        Observable<Optional<Map<String, String>>> something = metricsService
                .getMetricTags(new MetricId(tenantId, AVAILABILITY, id));
        try {
            return something
                    .map(optional -> valueToResponse(optional))
                    .toBlocking()
                    .lastOrDefault(null);
        } catch (Exception e) {
            return ApiUtils.serverError(e);
        }
    }


    @PUT
    @Path("/{id}/tags")
    @ApiOperation(value = "Update tags associated with the metric definition.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Metric's tags were successfully updated."),
            @ApiResponse(code = 500, message = "Unexpected error occurred while updating metric's tags.",
                response = ApiError.class) })
    public Response updateAvailabilityMetricTags(
            @PathParam("id") String id,
            @ApiParam(required = true) Map<String, String> tags
    ) {
        Metric<AvailabilityType> metric = new Metric<>(new MetricId(tenantId, AVAILABILITY, id));
        try {
            metricsService.addTags(metric, tags).toBlocking().lastOrDefault(null);
            return Response.ok().build();
        } catch (Exception e) {
            return ApiUtils.serverError(e);
        }
    }

    @DELETE
    @Path("/{id}/tags/{tags}")
    @ApiOperation(value = "Delete tags associated with the metric definition.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Metric's tags were successfully deleted."),
            @ApiResponse(code = 400, message = "Invalid tags", response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error occurred while trying to delete metric's tags.",
                response = ApiError.class) })
    public Response deleteAvailabilityMetricTags(
            @PathParam("id") String id,
            @ApiParam("Tag list") @PathParam("tags") Tags tags
    ) {
        Metric<AvailabilityType> metric = new Metric<>(new MetricId(tenantId, AVAILABILITY, id));

        try {
            metricsService.deleteTags(metric, tags.getTags()).toBlocking().lastOrDefault(null);
            return Response.ok().build();
        } catch (Exception e) {
            return ApiUtils.serverError(e);
        }
    }

    @POST
    @Path("/{id}/data")
    @ApiOperation(value = "Add data for a single availability metric.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Adding data succeeded."),
            @ApiResponse(code = 400, message = "Missing or invalid payload", response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error happened while storing the data",
                    response = ApiError.class)
    })
    public Response addAvailabilityForMetric(
            @PathParam("id") String id,
            @ApiParam(value = "List of availability datapoints", required = true) List<AvailabilityDataPoint> data
    ) {
        Metric<AvailabilityType> metric = new Metric<>(new MetricId(tenantId, AVAILABILITY, id),
                requestToAvailabilityDataPoints(data));

        try {
            metricsService.addAvailabilityData(Observable.just(metric)).toBlocking().lastOrDefault(null);
            return Response.ok().build();
        } catch (Exception e) {
            return ApiUtils.serverError(e);
        }
    }

    @POST
    @Path("/data")
    @ApiOperation(value = "Add metric data for multiple availability metrics in a single call.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Adding data succeeded."),
            @ApiResponse(code = 400, message = "Missing or invalid payload", response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error happened while storing the data",
                    response = ApiError.class)
    })
    public Response addAvailabilityData(
            @ApiParam(value = "List of availability metrics", required = true)
            List<Availability> availabilities
    ) {
        try {
            metricsService.addAvailabilityData(requestToAvailabilities(tenantId, availabilities)).toBlocking()
                    .lastOrDefault(null);
            return Response.ok().build();
        } catch (Exception e) {
            return ApiUtils.serverError(e);
        }
    }

    @GET
    @Path("/")
    @ApiOperation(value = "Find availabilities metrics data by their tags.", response = Map.class,
        responseContainer = "List")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Successfully fetched data."),
            @ApiResponse(code = 204, message = "No matching data found."),
            @ApiResponse(code = 400, message = "Missing or invalid tags query", response = ApiError.class),
            @ApiResponse(code = 500, message = "Any error in the query.", response = ApiError.class), })
    public Response findAvailabilityDataByTags(
            @ApiParam(value = "Tag list", required = true) @QueryParam("tags") Tags tags
    ) {
        if (tags == null) {
            return ApiUtils.badRequest(new ApiError("Missing tags query"));
        } else {
            try {
                return metricsService.findAvailabilityByTags(tenantId, tags.getTags()).map(m -> {
                    if (m.isEmpty()) {
                        return ApiUtils.noContent();
                    } else {
                        return Response.ok(m).build();
                    }
                })
                        .toBlocking()
                        .lastOrDefault(null);
            } catch (Exception e) {
                return Response.serverError().entity(new ApiError(e.getMessage())).build();
            }
        }
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
    public Response findAvailabilityData(
            @PathParam("id") String id,
            @ApiParam(value = "Defaults to now - 8 hours") @QueryParam("start") final Long start,
            @ApiParam(value = "Defaults to now") @QueryParam("end") final Long end,
            @ApiParam(value = "Total number of buckets") @QueryParam("buckets") Integer bucketsCount,
            @ApiParam(value = "Bucket duration") @QueryParam("bucketDuration") Duration bucketDuration,
            @ApiParam(value = "Set to true to return only distinct, contiguous values")
            @QueryParam("distinct") @DefaultValue("false") Boolean distinct
    ) {
        long now = System.currentTimeMillis();
        Long startTime = start == null ? now - EIGHT_HOURS : start;
        Long endTime = end == null ? now : end;

        Metric<AvailabilityType> metric = new Metric<>(new MetricId(tenantId, AVAILABILITY, id));
        if (bucketsCount == null && bucketDuration == null) {
            try {
                return metricsService.findAvailabilityData(metric.getId(), startTime, endTime, distinct)
                    .map(AvailabilityDataPoint::new)
                    .toList()
                    .map(ApiUtils::collectionToResponse)
                    .toBlocking()
                    .lastOrDefault(null);
            } catch (Exception e) {
                logger.warn("Failed to fetch availability data", e);
                return ApiUtils.serverError(e);
            }
        } else if (bucketsCount != null && bucketDuration != null) {
            return ApiUtils.badRequest(new ApiError("Both buckets and bucketDuration parameters are used"));
        } else {
            Buckets buckets;
            try {
                if (bucketsCount != null) {
                    buckets = Buckets.fromCount(startTime, endTime, bucketsCount);
                } else {
                    buckets = Buckets.fromStep(startTime, endTime, bucketDuration.toMillis());
                }
            } catch (IllegalArgumentException e) {
                return ApiUtils.badRequest(new ApiError("Bucket: " + e.getMessage()));
            }
            try {
                return metricsService.findAvailabilityStats(metric, startTime, endTime, buckets)
                        .map(BucketedOutput::getData).map(ApiUtils::collectionToResponse).toBlocking()
                        .lastOrDefault(null);
            } catch (Exception e) {
                return ApiUtils.serverError(e);
            }
        }
    }

    @POST
    @Path("/{id}/tag")
    @ApiOperation(value = "Add or update availability metric's tags.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Tags were modified successfully."),
            @ApiResponse(code = 400, message = "Missing or invalid payload", response = ApiError.class),
    })
    public Response tagAvailabilityData(
            @PathParam("id") final String id,
            @ApiParam(required = true) TagRequest params
    ) {
        Observable<Void> resultSetObservable;
        Metric<AvailabilityType> metric = new Metric<>(new MetricId(tenantId, AVAILABILITY, id));
        if (params.getTimestamp() != null) {
            resultSetObservable = metricsService.tagAvailabilityData(metric, params.getTags(), params.getTimestamp());
        } else {
            resultSetObservable = metricsService.tagAvailabilityData(metric, params.getTags(), params.getStart(),
                params.getEnd());
        }

        try {
            resultSetObservable.toBlocking().lastOrDefault(null);
            return Response.ok().build();
        } catch (Exception e) {
            return ApiUtils.serverError(e);
        }
    }

    @GET
    @Path("/tags/{tags}")
    @ApiOperation(value = "Find availability metric data with given tags.", response = Map.class,
        responseContainer = "List")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Availability values fetched successfully"),
            @ApiResponse(code = 204, message = "No matching data found."),
            @ApiResponse(code = 400, message = "Invalid tags", response = ApiError.class),
            @ApiResponse(code = 500, message = "Any error while fetching data.", response = ApiError.class), })
    public Response findTaggedAvailabilityData(
            @ApiParam("Tag list") @PathParam("tags") Tags tags
    ) {
        try {
        return metricsService.findAvailabilityByTags(tenantId, tags.getTags()).map(m -> {
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
