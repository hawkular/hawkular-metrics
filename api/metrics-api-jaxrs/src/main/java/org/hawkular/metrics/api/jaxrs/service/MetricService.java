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

package org.hawkular.metrics.api.jaxrs.service;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import java.util.Collection;
import java.util.List;
import java.util.Map;

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
import javax.ws.rs.core.UriInfo;

import org.hawkular.metrics.api.jaxrs.ApiError;
import org.hawkular.metrics.api.jaxrs.param.Duration;
import org.hawkular.metrics.api.jaxrs.param.Tags;
import org.hawkular.metrics.core.api.Availability;
import org.hawkular.metrics.core.api.AvailabilityMetric;
import org.hawkular.metrics.core.api.Counter;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.NumericData;
import org.hawkular.metrics.core.api.NumericMetric;
import org.hawkular.metrics.core.impl.request.TagRequest;

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;

/**
 * Interface to deal with metrics
 *
 * @author Heiko W. Rupp
 */
@Path("/")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
@Api(value = "/", description = "Metrics related REST interface")
public interface MetricService {

    @POST
    @Path("/{tenantId}/metrics/numeric")
    @ApiOperation(value = "Create numeric metric definition.", notes = "Clients are not required to explicitly create "
                                                                       + "a metric before storing data. Doing so "
                                                                       + "however allows clients to prevent naming "
                                                                       + "collisions and to "
                                                                       + "specify tags and data retention.")
    @ApiResponses(value = {
            @ApiResponse(code = 201, message = "Metric definition created successfully"),
            @ApiResponse(code = 400, message = "Missing or invalid payload", response = ApiError.class),
            @ApiResponse(code = 409, message = "Numeric metric with given id already exists",
                         response = ApiError.class),
            @ApiResponse(code = 500, message = "Metric definition creation failed due to an unexpected error",
                         response = ApiError.class)
    })
    void createNumericMetric(
            @Suspended AsyncResponse asyncResponse,
            @PathParam("tenantId") String tenantId,
            @ApiParam(required = true) NumericMetric metric,
            @Context UriInfo uriInfo
    );

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
    void createAvailabilityMetric(
            @Suspended AsyncResponse asyncResponse,
            @PathParam("tenantId") String tenantId,
            @ApiParam(required = true) AvailabilityMetric metric,
            @Context UriInfo uriInfo
    );

    @GET
    @Path("/{tenantId}/metrics/numeric/{id}/tags")
    @ApiOperation(value = "Retrieve tags associated with the metric definition.", response = Metric.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Metric's tags were successfully retrieved."),
            @ApiResponse(code = 204, message = "Query was successful, but no metrics were found."),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching metric's tags.",
                         response = ApiError.class)
    })
    void getNumericMetricTags(
            @Suspended AsyncResponse asyncResponse,
            @PathParam("tenantId") String tenantId, @PathParam("id") String id
    );

    @PUT
    @Path("/{tenantId}/metrics/numeric/{id}/tags")
    @ApiOperation(value = "Update tags associated with the metric definition.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Metric's tags were successfully updated."),
            @ApiResponse(code = 500, message = "Unexpected error occurred while updating metric's tags.",
                         response = ApiError.class)
    })
    void updateNumericMetricTags(
            @Suspended AsyncResponse asyncResponse,
            @PathParam("tenantId") String tenantId, @PathParam("id") String id,
            @ApiParam(required = true) Map<String, String> tags
    );

    @DELETE
    @Path("/{tenantId}/metrics/numeric/{id}/tags/{tags}")
    @ApiOperation(value = "Delete tags associated with the metric definition.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Metric's tags were successfully deleted."),
            @ApiResponse(code = 400, message = "Invalid tags", response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error occurred while trying to delete metric's tags.",
                         response = ApiError.class)
    })
    void deleteNumericMetricTags(
            @Suspended AsyncResponse asyncResponse,
            @PathParam("tenantId") String tenantId,
            @PathParam("id") String id,
            @ApiParam("Tag list") @PathParam("tags") Tags tags
    );

    @GET
    @Path("/{tenantId}/metrics/availability/{id}/tags")
    @ApiOperation(value = "Retrieve tags associated with the metric definition.", response = Map.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Metric's tags were successfully retrieved."),
            @ApiResponse(code = 204, message = "Query was successful, but no metrics were found."),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching metric's tags.",
                         response = ApiError.class)
    })
    void getAvailabilityMetricTags(
            @Suspended AsyncResponse asyncResponse,
            @PathParam("tenantId") String tenantId, @PathParam("id") String id
    );

    @PUT
    @Path("/{tenantId}/metrics/availability/{id}/tags")
    @ApiOperation(value = "Update tags associated with the metric definition.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Metric's tags were successfully updated."),
            @ApiResponse(code = 500, message = "Unexpected error occurred while updating metric's tags.",
                         response = ApiError.class)
    })
    void updateAvailabilityMetricTags(
            @Suspended AsyncResponse asyncResponse,
            @PathParam("tenantId") String tenantId, @PathParam("id") String id,
            @ApiParam(required = true) Map<String, String> tags
    );

    @DELETE
    @Path("/{tenantId}/metrics/availability/{id}/tags/{tags}")
    @ApiOperation(value = "Delete tags associated with the metric definition.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Metric's tags were successfully deleted."),
            @ApiResponse(code = 400, message = "Invalid tags", response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error occurred while trying to delete metric's tags.",
                         response = ApiError.class)
    })
    void deleteAvailabilityMetricTags(
            @Suspended AsyncResponse asyncResponse,
            @PathParam("tenantId") String tenantId, @PathParam("id") String id,
            @ApiParam("Tag list") @PathParam("tags") Tags tags
    );

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
    void addDataForMetric(
            @Suspended AsyncResponse asyncResponse,
            @PathParam("tenantId") String tenantId, @PathParam("id") String id,
            @ApiParam(value = "List of datapoints containing timestamp and value", required = true)
            List<NumericData> data
    );

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
    void addAvailabilityForMetric(
            @Suspended AsyncResponse asyncResponse,
            @PathParam("tenantId") String tenantId, @PathParam("id") String id,
            @ApiParam(value = "List of availability datapoints", required = true) List<Availability> data
    );

    @POST
    @Path("/{tenantId}/metrics/numeric/data")
    @ApiOperation(value = "Add metric data for multiple numeric metrics in a single call.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Adding data succeeded."),
            @ApiResponse(code = 500, message = "Unexpected error happened while storing the data",
                         response = ApiError.class)
    })
    void addNumericData(
            @Suspended AsyncResponse asyncResponse,
            @PathParam("tenantId") String tenantId,
            @ApiParam(value = "List of metrics", required = true)
            List<NumericMetric> metrics
    );

    @POST
    @Path("/{tenantId}/metrics/availability/data")
    @ApiOperation(value = "Add metric data for multiple availability metrics in a single call.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Adding data succeeded."),
            @ApiResponse(code = 500, message = "Unexpected error happened while storing the data",
                         response = ApiError.class)
    })
    void addAvailabilityData(
            @Suspended AsyncResponse asyncResponse,
            @PathParam("tenantId") String tenantId, @ApiParam(value = "List of availability metrics", required = true)
            List<AvailabilityMetric> metrics
    );

    @GET
    @Path("/{tenantId}/numeric")
    @ApiOperation(value = "Find numeric metrics data by their tags.", response = Map.class,
                  responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully fetched data."),
            @ApiResponse(code = 204, message = "No matching data found."),
            @ApiResponse(code = 400, message = "Missing or invalid tags query", response = ApiError.class),
            @ApiResponse(code = 500, message = "Any error in the query.", response = ApiError.class),
    })
    void findNumericDataByTags(
            @Suspended AsyncResponse asyncResponse,
            @PathParam("tenantId") String tenantId,
            @ApiParam(value = "Tag list", required = true) @QueryParam("tags") Tags tags
    );

    @GET
    @Path("/{tenantId}/availability")
    @ApiOperation(value = "Find availabilities metrics data by their tags.", response = Map.class,
                  responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully fetched data."),
            @ApiResponse(code = 204, message = "No matching data found."),
            @ApiResponse(code = 400, message = "Missing or invalid tags query", response = ApiError.class),
            @ApiResponse(code = 500, message = "Any error in the query.", response = ApiError.class),
    })
    void findAvailabilityDataByTags(
            @Suspended AsyncResponse asyncResponse,
            @PathParam("tenantId") String tenantId,
            @ApiParam(value = "Tag list", required = true) @QueryParam("tags") Tags tags
    );

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
    void findNumericData(
            @Suspended AsyncResponse asyncResponse,
            @PathParam("tenantId") String tenantId,
            @PathParam("id") String id,
            @ApiParam(value = "Defaults to now - 8 hours") @QueryParam("start") Long start,
            @ApiParam(value = "Defaults to now") @QueryParam("end") Long end,
            @ApiParam(value = "Total number of buckets") @QueryParam("buckets") Integer bucketsCount,
            @ApiParam(value = "Bucket duration") @QueryParam("bucketDuration") Duration bucketDuration
    );

    @GET
    @Path("/{tenantId}/metrics/numeric/{id}/periods")
    @ApiOperation(value = "Retrieve periods for which the condition holds true for each consecutive data point.",
                  response = List.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully fetched periods."),
            @ApiResponse(code = 204, message = "No numeric data was found."),
            @ApiResponse(code = 400, message = "Missing or invalid query parameters")
    })
    void findPeriods(
            @Suspended AsyncResponse asyncResponse, @PathParam("tenantId") String tenantId,
            @PathParam("id") String id,
            @ApiParam(value = "Defaults to now - 8 hours", required = false) @QueryParam("start") Long start,
            @ApiParam(value = "Defaults to now", required = false) @QueryParam("end") Long end,
            @ApiParam(value = "A threshold against which values are compared", required = true) @QueryParam("threshold")
            double threshold,
            @ApiParam(
                    value = "A comparison operation to perform between values and the threshold. Supported operations "
                            +
                            "include ge, gte, lt, lte, and eq", required = true) @QueryParam("op") String operator
    );

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
    void findAvailabilityData(
            @Suspended AsyncResponse asyncResponse,
            @PathParam("tenantId") String tenantId,
            @PathParam("id") String id,
            @ApiParam(value = "Defaults to now - 8 hours") @QueryParam("start") Long start,
            @ApiParam(value = "Defaults to now") @QueryParam("end") Long end,
            @ApiParam(value = "Total number of buckets") @QueryParam("buckets") Integer bucketsCount,
            @ApiParam(value = "Bucket duration") @QueryParam("bucketDuration") Duration bucketDuration
    );

    @POST
    @Path("/{tenantId}/metrics/numeric/{id}/tag")
    @ApiOperation(value = "Add or update numeric metric's tags.")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "Tags were modified successfully.")})
    void tagNumericData(
            @Suspended AsyncResponse asyncResponse, @PathParam("tenantId") String tenantId,
            @PathParam("id") String id, @ApiParam(required = true) TagRequest params
    );

    @POST
    @Path("/{tenantId}/metrics/availability/{id}/tag")
    @ApiOperation(value = "Add or update availability metric's tags.")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "Tags were modified successfully.")})
    void tagAvailabilityData(
            @Suspended AsyncResponse asyncResponse,
            @PathParam("tenantId") String tenantId, @PathParam("id") String id,
            @ApiParam(required = true) TagRequest params
    );

    @GET
    @Path("/{tenantId}/tags/numeric/{tags}")
    @ApiOperation(value = "Find numeric metric data with given tags.", response = Map.class,
                  responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Numeric values fetched successfully"),
            @ApiResponse(code = 204, message = "No matching data found."),
            @ApiResponse(code = 400, message = "Invalid tags", response = ApiError.class),
            @ApiResponse(code = 500, message = "Any error while fetching data.", response = ApiError.class),
    })
    void findTaggedNumericData(
            @Suspended AsyncResponse asyncResponse,
            @PathParam("tenantId") String tenantId,
            @ApiParam("Tag list") @PathParam("tags") Tags tags
    );

    @GET
    @Path("/{tenantId}/tags/availability/{tags}")
    @ApiOperation(value = "Find availability metric data with given tags.", response = Map.class,
                  responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Availability values fetched successfully"),
            @ApiResponse(code = 204, message = "No matching data found."),
            @ApiResponse(code = 400, message = "Invalid tags", response = ApiError.class),
            @ApiResponse(code = 500, message = "Any error while fetching data.", response = ApiError.class),
    })
    void findTaggedAvailabilityData(
            @Suspended AsyncResponse asyncResponse,
            @PathParam("tenantId") String tenantId,
            @ApiParam("Tag list") @PathParam("tags") Tags tags
    );

    @POST
    @Path("/counters")
    @ApiOperation(value = "List of counter definitions", hidden = true)
    void updateCountersForGroups(@Suspended AsyncResponse asyncResponse, Collection<Counter> counters);

    @POST
    @Path("/counters/{group}")
    @ApiOperation(value = "Update multiple counters in a single counter group", hidden = true)
    void updateCounterForGroup(
            @Suspended AsyncResponse asyncResponse, @PathParam("group") String group,
            Collection<Counter> counters
    );

    @POST
    @Path("/counters/{group}/{counter}")
    @ApiOperation(value = "Increase value of a counter", hidden = true)
    void updateCounter(
            @Suspended AsyncResponse asyncResponse, @PathParam("group") String group,
            @PathParam("counter") String counter
    );

    @POST
    @Path("/counters/{group}/{counter}/{value}")
    @ApiOperation(value = "Update value of a counter", hidden = true)
    void updateCounter(
            @Suspended AsyncResponse asyncResponse, @PathParam("group") String group,
            @PathParam("counter") String counter, @PathParam("value") Long value
    );

    @GET
    @Path("/counters/{group}")
    @ApiOperation(value = "Retrieve a list of counter values in this group", hidden = true, response = Counter.class,
                  responseContainer = "List")
    @Produces({APPLICATION_JSON})
    void getCountersForGroup(@Suspended AsyncResponse asyncResponse, @PathParam("group") String group);

    @GET
    @Path("/counters/{group}/{counter}")
    @ApiOperation(value = "Retrieve value of a counter", hidden = true, response = Counter.class,
                  responseContainer = "List")
    void getCounter(
            @Suspended AsyncResponse asyncResponse, @PathParam("group") String group,
            @PathParam("counter") String counter
    );

    @GET
    @Path("/{tenantId}/metrics")
    @ApiOperation(value = "Find tenant's metric definitions.", notes = "Does not include any metric values. ",
                  response = List.class, responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved at least one metric "
                                               + "definition."),
            @ApiResponse(code = 204, message = "No metrics found."),
            @ApiResponse(code = 400, message = "Given type is not a valid type.", response = ApiError.class),
            @ApiResponse(code = 500, message = "Failed to retrieve metrics due to unexpected error.",
                         response = ApiError.class)
    })
    void findMetrics(
            @Suspended AsyncResponse asyncResponse,
            @PathParam("tenantId") String tenantId,
            @ApiParam(value = "Queried metric type", required = true, allowableValues = "[num, avail, log]")
            @QueryParam("type") String type
    );
}
