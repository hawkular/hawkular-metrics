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
import static org.hawkular.metrics.api.jaxrs.util.ApiUtils.noContent;
import static org.hawkular.metrics.api.jaxrs.util.ApiUtils.requestToAvailabilities;
import static org.hawkular.metrics.api.jaxrs.util.ApiUtils.requestToAvailabilityDataPoints;
import static org.hawkular.metrics.api.jaxrs.util.ApiUtils.serverError;
import static org.hawkular.metrics.api.jaxrs.util.ApiUtils.valueToResponse;
import static org.hawkular.metrics.api.jaxrs.validation.Validate.validate;
import static org.hawkular.metrics.core.api.MetricType.AVAILABILITY;

import java.net.URI;
import java.util.List;
import java.util.Map;

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
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.hawkular.metrics.api.jaxrs.ApiError;
import org.hawkular.metrics.api.jaxrs.handler.observer.MetricCreatedObserver;
import org.hawkular.metrics.api.jaxrs.handler.observer.ResultSetObserver;
import org.hawkular.metrics.api.jaxrs.model.Availability;
import org.hawkular.metrics.api.jaxrs.model.AvailabilityDataPoint;
import org.hawkular.metrics.api.jaxrs.param.Duration;
import org.hawkular.metrics.api.jaxrs.param.Tags;
import org.hawkular.metrics.api.jaxrs.request.MetricDefinition;
import org.hawkular.metrics.api.jaxrs.request.TagRequest;
import org.hawkular.metrics.api.jaxrs.util.ApiUtils;
import org.hawkular.metrics.core.api.AvailabilityType;
import org.hawkular.metrics.core.api.Buckets;
import org.hawkular.metrics.core.api.Metric;
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
                    response = ApiError.class)
    })
    public void createAvailabilityMetric(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(required = true) MetricDefinition metricDefinition,
            @Context UriInfo uriInfo
    ) {
        URI location = uriInfo.getBaseUriBuilder().path("/availability/{id}").build(metricDefinition.getId());
        Metric<AvailabilityType> metric = new Metric<>(new MetricId<>(tenantId, AVAILABILITY, metricDefinition.getId()),
                metricDefinition.getTags(), metricDefinition.getDataRetention());
        metricsService.createMetric(metric).subscribe(new MetricCreatedObserver(asyncResponse, location));
    }

    @GET
    @Path("/{id}")
    @ApiOperation(value = "Retrieve single metric definition.", response = MetricDefinition.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Metric's definition was successfully retrieved."),
            @ApiResponse(code = 204, message = "Query was successful, but no metrics definition is set."),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching metric's definition.",
                         response = ApiError.class) })
    public void getAvailabilityMetric(@Suspended final AsyncResponse asyncResponse, @PathParam("id") String id) {

        metricsService.findMetric(new MetricId<>(tenantId, AVAILABILITY, id))
                .map(MetricDefinition::new)
                .map(metricDef -> Response.ok(metricDef).build())
                .switchIfEmpty(Observable.just(noContent()))
                .subscribe(asyncResponse::resume, t -> asyncResponse.resume(serverError(t)));
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
        metricsService.getMetricTags(new MetricId<>(tenantId, AVAILABILITY, id)).subscribe(
                optional -> asyncResponse.resume(valueToResponse(optional)),
                t -> asyncResponse.resume(serverError(t)));
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
        Metric<AvailabilityType> metric = new Metric<>(new MetricId<>(tenantId, AVAILABILITY, id));
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
    public void deleteAvailabilityMetricTags(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("id") String id,
            @ApiParam("Tag list") @PathParam("tags") Tags tags
    ) {
        Metric<AvailabilityType> metric = new Metric<>(new MetricId<>(tenantId, AVAILABILITY, id));
        metricsService.deleteTags(metric, tags.getTags()).subscribe(new ResultSetObserver(asyncResponse));
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
    public void addAvailabilityForMetric(
            @Suspended final AsyncResponse asyncResponse, @PathParam("id") String id,
            @ApiParam(value = "List of availability datapoints", required = true) List<AvailabilityDataPoint> data
    ) {
        validate(data).lastOrDefault(false).subscribe(valid -> {
            if (!valid) {
                asyncResponse.resume(badRequest(new ApiError("Timestamp not provided for data point")));
                return;
            }

            Metric<AvailabilityType> metric = new Metric<>(new MetricId<>(tenantId, AVAILABILITY, id),
                    requestToAvailabilityDataPoints(data));
            metricsService.addDataPoints(AVAILABILITY, Observable.just(metric))
                    .subscribe(new ResultSetObserver(asyncResponse));
        });
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
    public void addAvailabilityData(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "List of availability metrics", required = true)
            List<Availability> availabilities
    ) {
        validate(availabilities).lastOrDefault(false).subscribe(valid -> {
            if (!valid) {
                asyncResponse.resume(badRequest(new ApiError("Timestamp not provided for data point")));
                return;
            }

            metricsService.addDataPoints(AVAILABILITY, requestToAvailabilities(tenantId, availabilities))
                .subscribe(new ResultSetObserver(asyncResponse));
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
        if (tags == null) {
            asyncResponse.resume(badRequest(new ApiError("Missing tags query")));
        } else {
            // @TODO Repeated code, refactor (in GaugeHandler also)
            metricsService.findAvailabilityByTags(tenantId, tags.getTags()).subscribe(m -> {
                if (m.isEmpty()) {
                    asyncResponse.resume(Response.noContent().build());
                } else {
                    asyncResponse.resume(Response.ok(m).build());
                }
            }, t -> asyncResponse.resume(Response.serverError().entity(new ApiError(t.getMessage())).build()));

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
    public void findAvailabilityData(
            @Suspended AsyncResponse asyncResponse,
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

        MetricId<AvailabilityType> metricId = new MetricId<>(tenantId, AVAILABILITY, id);

        if (bucketsCount == null && bucketDuration == null) {
            metricsService.findAvailabilityData(metricId, startTime, endTime, distinct)
                    .map(AvailabilityDataPoint::new)
                    .toList()
                    .map(ApiUtils::collectionToResponse)
                    .subscribe(
                            asyncResponse::resume,
                            t -> {
                                logger.warn("Failed to fetch availability data", t);
                                serverError(t);
                            });
        } else if (bucketsCount != null && bucketDuration != null) {
            asyncResponse.resume(badRequest(new ApiError("Both buckets and bucketDuration parameters are used")));
        } else {
            Buckets buckets;
            try {
                if (bucketsCount != null) {
                    buckets = Buckets.fromCount(startTime, endTime, bucketsCount);
                } else {
                    buckets = Buckets.fromStep(startTime, endTime, bucketDuration.toMillis());
                }
            } catch (IllegalArgumentException e) {
                asyncResponse.resume(badRequest(new ApiError("Bucket: " + e.getMessage())));
                return;
            }

            metricsService.findAvailabilityStats(metricId, startTime, endTime, buckets)
                .map(ApiUtils::collectionToResponse)
                .subscribe(asyncResponse::resume, ApiUtils::serverError);
        }
    }

    @POST
    @Path("/{id}/tag")
    @ApiOperation(value = "Add or update availability metric's tags.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Tags were modified successfully."),
            @ApiResponse(code = 400, message = "Missing or invalid payload", response = ApiError.class),
    })
    public void tagAvailabilityData(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("id") final String id,
            @ApiParam(required = true) TagRequest params
    ) {
        Observable<Void> resultSetObservable;
        Metric<AvailabilityType> metric = new Metric<>(new MetricId<>(tenantId, AVAILABILITY, id));
        if (params.getTimestamp() != null) {
            resultSetObservable = metricsService.tagAvailabilityData(metric, params.getTags(), params.getTimestamp());
        } else {
            resultSetObservable = metricsService.tagAvailabilityData(metric, params.getTags(), params.getStart(),
                params.getEnd());
        }
        resultSetObservable.subscribe(new ResultSetObserver(asyncResponse));
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
        metricsService.findAvailabilityByTags(tenantId, tags.getTags())
        .subscribe(m -> { // @TODO Repeated code, refactor and use Optional?
            if (m.isEmpty()) {
                asyncResponse.resume(Response.noContent().build());
            } else {
                asyncResponse.resume(Response.ok(m).build());
            }
        }, t -> asyncResponse.resume(Response.serverError().entity(new ApiError(t.getMessage())).build()));
    }

}
