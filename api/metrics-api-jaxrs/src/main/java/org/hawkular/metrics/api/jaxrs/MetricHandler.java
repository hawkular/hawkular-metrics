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

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;
import static org.hawkular.metrics.core.api.MetricsService.DEFAULT_TENANT_ID;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.hawkular.metrics.api.jaxrs.callback.NoDataCallback;
import org.hawkular.metrics.api.jaxrs.callback.SimpleDataCallback;
import org.hawkular.metrics.core.api.Availability;
import org.hawkular.metrics.core.api.AvailabilityMetric;
import org.hawkular.metrics.core.api.Counter;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.MetricType;
import org.hawkular.metrics.core.api.MetricsService;
import org.hawkular.metrics.core.api.NumericData;
import org.hawkular.metrics.core.api.NumericMetric;
import org.hawkular.metrics.core.impl.cassandra.MetricUtils;
import org.hawkular.metrics.core.impl.mapper.CreateSimpleBuckets;
import org.hawkular.metrics.core.impl.mapper.MetricOut;
import org.hawkular.metrics.core.impl.mapper.NoResultsException;
import org.hawkular.metrics.core.impl.request.TagRequest;

import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;

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
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Metric definition created successfully"),
            @ApiResponse(code = 400, message = "Metric with given id already exists or request is otherwise incorrect",
                    response = Error.class),
            @ApiResponse(code = 500, message = "Metric definition creation failed due to an unexpected error",
                    response = Error.class)})
    public void createNumericMetric(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenantId") String tenantId, @ApiParam(required = true) NumericMetric metric) {
        ListenableFuture<Void> future = metricsService.createMetric(metric);
        Futures.addCallback(future, new NoDataCallback<Void>(asyncResponse));
    }

    @POST
    @Path("/{tenantId}/metrics/availability")
    @ApiOperation(value = "Create availability metric definition. Same notes as creating numeric metric apply.")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Metric definition created successfully"),
            @ApiResponse(code = 400, message = "Metric with given id already exists", response = Error.class),
            @ApiResponse(code = 500, message = "Metric definition creation failed due to an unexpected error",
                    response = Error.class)})
    public void createAvailabilityMetric(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenantId") String tenantId, @ApiParam(required = true) AvailabilityMetric metric) {
        ListenableFuture<Void> future = metricsService.createMetric(metric);
        Futures.addCallback(future, new NoDataCallback<Void>(asyncResponse));
    }

    @GET
    @Path("/{tenantId}/metrics/numeric/{id}/tags")
    @ApiOperation(value = "Retrieve tags associated with the metric definition.", response = Metric.class)
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Metric's tags were successfully retrieved."),
            @ApiResponse(code = 204, message = "Query was successful, but no metrics were found."),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching metric's tags.",
                    response = Error.class)})
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
                    response = Error.class)})
    public void updateNumericMetricTags(@Suspended final AsyncResponse asyncResponse,
                                        @PathParam("tenantId") String tenantId, @PathParam("id") String id,
                                        @ApiParam(required = true) Map<String, String> tags) {
        NumericMetric metric = new NumericMetric(tenantId, new MetricId(id));
        ListenableFuture<Void> future = metricsService.addTags(metric, MetricUtils.getTags(tags));
        Futures.addCallback(future, new NoDataCallback<Void>(asyncResponse));
    }

    @DELETE
    @Path("/{tenantId}/metrics/numeric/{id}/tags/{tags}")
    @ApiOperation(value = "Delete tags associated with the metric definition.")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Metric's tags were successfully deleted."),
            @ApiResponse(code = 500, message = "Unexpected error occurred while trying to delete metric's tags.",
                    response = Error.class)})
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
                    response = Error.class)})
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
                    response = Error.class)})
    public void updateAvailabilityMetricTags(@Suspended final AsyncResponse asyncResponse,
        @PathParam("tenantId") String tenantId, @PathParam("id") String id,
        @ApiParam(required = true) Map<String, String> tags) {
        AvailabilityMetric metric = new AvailabilityMetric(tenantId, new MetricId(id));
        ListenableFuture<Void> future = metricsService.addTags(metric, MetricUtils.getTags(tags));
        Futures.addCallback(future, new NoDataCallback<Void>(asyncResponse));
    }

    @DELETE
    @Path("/{tenantId}/metrics/availability/{id}/tags/{tags}")
    @ApiOperation(value = "Delete tags associated with the metric definition.")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Metric's tags were successfully deleted."),
            @ApiResponse(code = 500, message = "Unexpected error occurred while trying to delete metric's tags.",
                    response = Error.class)})
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
    @ApiResponses(value = { @ApiResponse(code = 500, message = "Unexpected error happened while storing the data",
            response = Error.class)})
    public void addDataForMetric(@Suspended final AsyncResponse asyncResponse,
                                 @PathParam("tenantId") final String tenantId, @PathParam("id") String id,
                                 @ApiParam(value = "List of datapoints containing timestamp and value", required = true)
                                 List<NumericData> dataPoints) {

        NumericMetric metric = new NumericMetric(tenantId, new MetricId(id));
        for (NumericData p : dataPoints) {
            metric.addData(p);
        }
        ListenableFuture<Void> future = metricsService.addNumericData(asList(metric));
        Futures.addCallback(future, new NoDataCallback<Void>(asyncResponse));
    }

    @POST
    @Path("/{tenantId}/metrics/availability/{id}/data")
    @ApiOperation(value = "Add data for a single availability metric.")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Adding data succeeded."),
            @ApiResponse(code = 500, message = "Unexpected error happened while storing the data",
                    response = Error.class)})
    public void addAvailabilityForMetric(@Suspended final AsyncResponse asyncResponse,
        @PathParam("tenantId") final String tenantId, @PathParam("id") String id,
            @ApiParam(value = "List of availability datapoints", required = true) List<Availability> data) {
        AvailabilityMetric metric = new AvailabilityMetric(tenantId, new MetricId(id));

        for (Availability p : data) {
            metric.addData(p);
        }

        ListenableFuture<Void> future = metricsService.addAvailabilityData(asList(metric));
        Futures.addCallback(future, new NoDataCallback<Void>(asyncResponse));
    }

    @POST
    @Path("/{tenantId}/metrics/numeric/data")
    @ApiOperation(value = "Add metric data for multiple numeric metrics in a single call.")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Adding data succeeded."),
            @ApiResponse(code = 500, message = "Unexpected error happened while storing the data",
                    response = Error.class)})
    public void addNumericData(@Suspended final AsyncResponse asyncResponse,
                               @PathParam("tenantId") String tenantId,
                               @ApiParam(value = "List of metrics", required = true)
                               List<NumericMetric> metrics) {
        if (metrics.isEmpty()) {
            asyncResponse.resume(Response.ok().type(APPLICATION_JSON_TYPE).build());
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
                    response = Error.class)})
    public void addAvailabilityData(@Suspended final AsyncResponse asyncResponse,
        @PathParam("tenantId") String tenantId, @ApiParam(value = "List of availability metrics", required = true)
        List<AvailabilityMetric> metrics) {
        if (metrics.isEmpty()) {
            asyncResponse.resume(Response.ok().type(APPLICATION_JSON_TYPE).build());
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
            @ApiResponse(code = 500, message = "Any error in the query.", response = Error.class)})
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
            @ApiResponse(code = 500, message = "Any error in the query.", response = Error.class)})
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
    @ApiOperation(value = "Retrieve numeric data.", response = MetricOut.class)
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Successfully fetched numeric data."),
            @ApiResponse(code = 204, message = "No numeric data was found."),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching numeric data.",
                    response = Error.class)})
    public void findNumericData(
            @Suspended final AsyncResponse asyncResponse,
        @PathParam("tenantId") String tenantId,
        @PathParam("id") final String id,
        @ApiParam(value = "Defaults to now - 8 hours", required = false) @QueryParam("start") Long start,
        @ApiParam(value = "Defaults to now", required = false) @QueryParam("end") Long end,
        @ApiParam(value = "The number of buckets or intervals in which to divide the time range. A value of 60 for "
                + "example will return 60 equally spaced buckets for the time period between start and end times, "
                    + "having max/min/avg calculated for each bucket.") @QueryParam("buckets") final int numberOfBuckets) {
        long now = System.currentTimeMillis();
        if (start == null) {
            start = now - EIGHT_HOURS;
        }
        if (end == null) {
            end = now;
        }

        NumericMetric metric = new NumericMetric(tenantId, new MetricId(id));
        ListenableFuture<NumericMetric> dataFuture = metricsService.findNumericData(metric, start, end);
        ListenableFuture<? extends Object> outputFuture = null;
        if (numberOfBuckets == 0) {
            outputFuture = Futures.transform(dataFuture, new Function<NumericMetric, List<NumericData>>() {

                @Override
                public List<NumericData> apply(NumericMetric metric) {
                    if (metric == null) {
                        throw new NoResultsException();
                    }

                    return metric.getData();
                }
            });
        } else {
            outputFuture = Futures.transform(dataFuture, new CreateSimpleBuckets(start, end, numberOfBuckets, false));
        }

        Futures.addCallback(outputFuture, new NoDataCallback<Object>(asyncResponse));
    }

    @GET
    @Path("/{tenantId}/metrics/availability/{id}/data")
    @ApiOperation(value = "Retrieve availability data.", response = Availability.class)
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Successfully fetched availability data."),
            @ApiResponse(code = 204, message = "No availability data was found.")})
    public void findAvailabilityData(@Suspended final AsyncResponse asyncResponse,
        @PathParam("tenantId") String tenantId, @PathParam("id") final String id,
        @ApiParam(value = "Defaults to now - 8 hours", required = false) @QueryParam("start") Long start,
        @ApiParam(value = "Defaults to now", required = false) @QueryParam("end") Long end) {

        long now = System.currentTimeMillis();
        if (start == null) {
            start = now - EIGHT_HOURS;
        }
        if (end == null) {
            end = now;
        }

        AvailabilityMetric metric = new AvailabilityMetric(tenantId, new MetricId(id));
        ListenableFuture<AvailabilityMetric> future = metricsService.findAvailabilityData(metric, start, end);

        ListenableFuture<List<Availability>> outputfuture = Futures.transform(future,
                new Function<AvailabilityMetric, List<Availability>>() {
                    @Override
                    public List<Availability> apply(AvailabilityMetric input) {
                        if (input == null) {
                            return null;
                        }
                        return input.getData();
                    }
        });

        Futures.addCallback(outputfuture, new SimpleDataCallback<List<Availability>>(asyncResponse));
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
            future = metricsService.tagNumericData(metric, MetricUtils.getTags(params.getTags()),
                params.getTimestamp());
        } else {
            future = metricsService.tagNumericData(metric, MetricUtils.getTags(params.getTags()), params.getStart(),
                params.getEnd());
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
            future = metricsService.tagAvailabilityData(metric, MetricUtils.getTags(params.getTags()),
                params.getTimestamp());
        } else {
            future = metricsService.tagAvailabilityData(metric, MetricUtils.getTags(params.getTags()),
                params.getStart(), params.getEnd());
        }
        Futures.addCallback(future, new NoDataCallback<List<Availability>>(asyncResponse));
    }

    @GET
    @Path("/{tenantId}/tags/numeric/{tag}")
    @ApiOperation(value = "Find numeric metric data with given tags.", response = Map.class,
            responseContainer = "List")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Numeric values fetched successfully"),
            @ApiResponse(code = 500, message = "Any error while fetching data.", response = Error.class)})
    public void findTaggedNumericData(@Suspended final AsyncResponse asyncResponse,
        @PathParam("tenantId") String tenantId,
        @ApiParam(allowMultiple = true, required = true, value = "A list of tags in the format of name:value")
        @PathParam("tag") String encodedTag) {
        ListenableFuture<Map<MetricId, Set<NumericData>>> queryFuture = metricsService.findNumericDataByTags(
                tenantId, MetricUtils.decodeTags(encodedTag));
        Futures.addCallback(queryFuture, new SimpleDataCallback<Map<MetricId, Set<NumericData>>>(asyncResponse));
    }

    @GET
    @Path("/{tenantId}/tags/availability/{tag}")
    @ApiOperation(value = "Find availability metric data with given tags.", response = Map.class,
            responseContainer = "List")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Availability values fetched successfully"),
            @ApiResponse(code = 500, message = "Any error while fetching data.", response = Error.class)})
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
        ListenableFuture<List<Counter>> future = metricsService.findCounters(group, asList(counter));
        Futures.addCallback(future, new FutureCallback<List<Counter>>() {
            @Override
            public void onSuccess(List<Counter> counters) {
                if (counters.isEmpty()) {
                    asyncResponse.resume(Response.status(404).entity("Counter[group: " + group + ", name: " +
                            counter + "] not found").build());
                } else {
                    Response jaxrs = Response.ok(counters.get(0)).type(APPLICATION_JSON_TYPE).build();
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
            @ApiResponse(code = 400, message = "Given type is not a valid type.", response = Error.class),
            @ApiResponse(code = 500, message = "Failed to retrieve metrics due to unexpected error.",
                    response = Error.class)})
    public void findMetrics(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenantId") final String tenantId,
        @ApiParam(value = "Queried metric type", required = true, allowableValues = "[num, avail, log]")
        @QueryParam("type") String type) {
        MetricType metricType = null;
        try {
            metricType = MetricType.fromTextCode(type);
        } catch (IllegalArgumentException e) {
            Error errors = new Error("[" + type + "] is not a valid type. Accepted values are num|avail|log");
            asyncResponse
                    .resume(Response.status(Status.BAD_REQUEST).entity(errors).type(APPLICATION_JSON_TYPE).build());
        }
        ListenableFuture<List<Metric<?>>> future = metricsService.findMetrics(tenantId, metricType);
        Futures.addCallback(future, new SimpleDataCallback<List<Metric<?>>>(asyncResponse));
    }

}
