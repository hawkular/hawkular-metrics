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

import static java.lang.Double.NaN;
import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.summarizingDouble;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;
import static javax.ws.rs.core.Response.Status;
import static org.hawkular.metrics.api.jaxrs.util.CustomMediaTypes.APPLICATION_VND_HAWKULAR_WRAPPED_JSON;
import static org.hawkular.metrics.core.api.MetricsService.DEFAULT_TENANT_ID;

import java.util.ArrayList;
import java.util.Collection;
import java.util.DoubleSummaryStatistics;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.LongStream;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
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

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;

import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;

import org.hawkular.metrics.core.api.Availability;
import org.hawkular.metrics.core.api.AvailabilityMetric;
import org.hawkular.metrics.core.api.Counter;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricAlreadyExistsException;
import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.MetricType;
import org.hawkular.metrics.core.api.MetricsService;
import org.hawkular.metrics.core.api.NumericData;
import org.hawkular.metrics.core.api.NumericMetric;
import org.hawkular.metrics.core.impl.cassandra.MetricUtils;
import org.hawkular.metrics.core.impl.mapper.AvailabilityDataParams;
import org.hawkular.metrics.core.impl.mapper.AvailabilityDataPoint;
import org.hawkular.metrics.core.impl.mapper.BucketDataPoint;
import org.hawkular.metrics.core.impl.mapper.BucketedOutput;
import org.hawkular.metrics.core.impl.mapper.DataPointOut;
import org.hawkular.metrics.core.impl.mapper.MetricMapper;
import org.hawkular.metrics.core.impl.mapper.MetricOut;
import org.hawkular.metrics.core.impl.mapper.MetricParams;
import org.hawkular.metrics.core.impl.mapper.NoResultsException;
import org.hawkular.metrics.core.impl.mapper.NumericDataParams;
import org.hawkular.metrics.core.impl.mapper.NumericDataPoint;
import org.hawkular.metrics.core.impl.mapper.TagParams;

/**
 * Interface to deal with metrics
 * @author Heiko W. Rupp
 */
@Api(value = "/", description = "Metrics related REST interface")
@Path("/")
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
            @ApiResponse(code = 400, message = "Metric with given id already exists or request is otherwise incorrect"),
            @ApiResponse(code = 500, message = "Metric definition creation failed due to an unexpected error")})
    @Consumes(APPLICATION_JSON)
    public void createNumericMetric(@Suspended AsyncResponse asyncResponse, @PathParam("tenantId") String tenantId,
                                    @ApiParam(required = true) MetricParams params) {
        NumericMetric metric = new NumericMetric(tenantId, new MetricId(params.getName()), MetricUtils.getTags(
            params.getTags()), params.getDataRetention());
        ListenableFuture<Void> future = metricsService.createMetric(metric);
        Futures.addCallback(future, new MetricCreatedCallback(asyncResponse, params));
    }

    @POST
    @Path("/{tenantId}/metrics/availability")
    @ApiOperation(value = "Create availability metric definition. Same notes as creating numeric metric apply.")
    @ApiResponses(value = { @ApiResponse(code = 400, message = "Metric with given id already exists"),
            @ApiResponse(code = 200, message = "Metric definition created successfully"),
            @ApiResponse(code = 500, message = "Metric definition creation failed due to an unexpected error")})
    @Consumes(APPLICATION_JSON)
    public void createAvailabilityMetric(@Suspended AsyncResponse asyncResponse, @PathParam("tenantId") String tenantId,
                                         @ApiParam(required = true) MetricParams params) {
        AvailabilityMetric metric = new AvailabilityMetric(tenantId, new MetricId(params.getName()),
            MetricUtils.getTags(params.getTags()), params.getDataRetention());
        ListenableFuture<Void> future = metricsService.createMetric(metric);
        Futures.addCallback(future, new MetricCreatedCallback(asyncResponse, params));
    }

    private class MetricCreatedCallback implements FutureCallback<Void> {

        AsyncResponse response;
        MetricParams params;

        public MetricCreatedCallback(AsyncResponse response, MetricParams params) {
            this.response = response;
            this.params = params;
        }

        @Override
        public void onSuccess(Void result) {
            response.resume(Response.ok().type(APPLICATION_JSON_TYPE).build());
        }

        @Override
        public void onFailure(Throwable t) {
            if (t instanceof MetricAlreadyExistsException) {
                Map<String, String> errors = ImmutableMap.of("errorMsg", "A metric with name [" + params.getName() +
                    "] already exists");
                response.resume(Response.status(Status.BAD_REQUEST).entity(errors).type(APPLICATION_JSON_TYPE).build());
            } else {
                Map<String, String> errors = ImmutableMap.of("errorMsg", "Failed to create metric due to an " +
                    "unexpected error: " + Throwables.getRootCause(t).getMessage());
                response.resume(Response.status(Status.INTERNAL_SERVER_ERROR).entity(errors)
                    .type(APPLICATION_JSON_TYPE).build());
            }
        }
    }

    @GET
    @Path("/{tenantId}/metrics/numeric/{id}/tags")
    @ApiOperation(value = "Retrieve tags associated with the metric definition.", response = MetricOut.class)
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Metric's tags were successfully retrieved."),
            @ApiResponse(code = 204, message = "Query was successful, but no metrics were found."),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching metric's tags.")})
    public void getNumericMetricTags(@Suspended AsyncResponse response, @PathParam("tenantId") String tenantId,
        @PathParam("id") String id) {
        ListenableFuture<Metric> future = metricsService.findMetric(tenantId, MetricType.NUMERIC,
            new MetricId(id));
        Futures.addCallback(future, new GetMetricTagsCallback(response));
    }

    @PUT
    @Path("/{tenantId}/metrics/numeric/{id}/tags")
    @ApiOperation(value = "Update tags associated with the metric definition.")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Metric's tags were successfully updated."),
            @ApiResponse(code = 500, message = "Unexpected error occurred while updating metric's tags.")})
    public void updateNumericMetricTags(@Suspended final AsyncResponse response,
                                        @PathParam("tenantId") String tenantId, @PathParam("id") String id,
                                        @ApiParam(required = true) Map<String, String> tags) {
        NumericMetric metric = new NumericMetric(tenantId, new MetricId(id));
        ListenableFuture<Void> future = metricsService.addTags(metric, MetricUtils.getTags(tags));
        Futures.addCallback(future, new DataInsertedCallback(response, "Failed to update tags"));
    }

    @DELETE
    @Path("/{tenantId}/metrics/numeric/{id}/tags/{tags}")
    @ApiOperation(value = "Delete tags associated with the metric definition.")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Metric's tags were successfully deleted."),
            @ApiResponse(code = 500, message = "Unexpected error occurred while trying to delete metric's tags.")})
    public void deleteNumericMetricTags(@Suspended final AsyncResponse response,
        @PathParam("tenantId") String tenantId, @PathParam("id") String id,
        @ApiParam(allowMultiple = true, required = true, value = "A list of tags in the format of name:value")
        @PathParam("tags") String encodedTags) {
        NumericMetric metric = new NumericMetric(tenantId, new MetricId(id));
        ListenableFuture<Void> future = metricsService.deleteTags(metric, MetricUtils.decodeTags(encodedTags));
        Futures.addCallback(future, new DataInsertedCallback(response, "Failed to delete tags"));
    }

    @GET
    @Path("/{tenantId}/metrics/availability/{id}/tags")
    @ApiOperation(value = "Retrieve tags associated with the metric definition.", response = MetricOut.class)
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Metric's tags were successfully retrieved."),
            @ApiResponse(code = 204, message = "Query was successful, but no metrics were found."),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching metric's tags.")})
    public void getAvailabilityMetricTags(@Suspended AsyncResponse response,
        @PathParam("tenantId") String tenantId, @PathParam("id") String id) {
        ListenableFuture<Metric> future = metricsService.findMetric(tenantId, MetricType.AVAILABILITY,
            new MetricId(id));
        Futures.addCallback(future, new GetMetricTagsCallback(response));
    }

    @PUT
    @Path("/{tenantId}/metrics/availability/{id}/tags")
    @ApiOperation(value = "Update tags associated with the metric definition.")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Metric's tags were successfully updated."),
            @ApiResponse(code = 500, message = "Unexpected error occurred while updating metric's tags.")})
    public void updateAvailabilityMetricTags(@Suspended final AsyncResponse response,
        @PathParam("tenantId") String tenantId, @PathParam("id") String id,
        @ApiParam(required = true) Map<String, String> tags) {
        AvailabilityMetric metric = new AvailabilityMetric(tenantId, new MetricId(id));
        ListenableFuture<Void> future = metricsService.addTags(metric, MetricUtils.getTags(tags));
        Futures.addCallback(future, new DataInsertedCallback(response, "Failed to update tags"));
    }

    @DELETE
    @Path("/{tenantId}/metrics/availability/{id}/tags/{tags}")
    @ApiOperation(value = "Delete tags associated with the metric definition.")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Metric's tags were successfully deleted."),
            @ApiResponse(code = 500, message = "Unexpected error occurred while trying to delete metric's tags.")})
    public void deleteAvailabilityMetricTags(@Suspended final AsyncResponse response,
        @PathParam("tenantId") String tenantId, @PathParam("id") String id,
        @ApiParam(allowMultiple = true, required = true, value = "A list of tags in the format of name:value")
        @PathParam("tags") String encodedTags) {
        AvailabilityMetric metric = new AvailabilityMetric(tenantId, new MetricId(id));
        ListenableFuture<Void> future = metricsService.deleteTags(metric, MetricUtils.decodeTags(encodedTags));
        Futures.addCallback(future, new DataInsertedCallback(response, "Failed to delete tags"));
    }

    private class GetMetricTagsCallback implements FutureCallback<Metric> {

        AsyncResponse response;

        public GetMetricTagsCallback(AsyncResponse response) {
            this.response = response;
        }

        @Override
        public void onSuccess(Metric metric) {
            if (metric == null) {
                response.resume(Response.status(Status.NO_CONTENT).type(APPLICATION_JSON_TYPE).build());
            } else {
                response.resume(Response.ok(new MetricOut(metric.getTenantId(), metric.getId().getName(),
                    MetricUtils.flattenTags(metric.getTags()), metric.getDataRetention())).type(APPLICATION_JSON_TYPE)
                    .build());
            }
        }

        @Override
        public void onFailure(Throwable t) {
            Map<String, String> errors = ImmutableMap.of("errorMsg", "Failed to retrieve tags due to " +
                "an unexpected error: " + Throwables.getRootCause(t).getMessage());
            response.resume(Response.status(Status.INTERNAL_SERVER_ERROR).entity(errors).type(APPLICATION_JSON_TYPE)
                .build());
        }
    }

    @POST
    @Path("/{tenantId}/metrics/numeric/{id}/data")
    @ApiOperation(value = "Add data for a single numeric metric.")
    @ApiResponses(value = { @ApiResponse(code = 500, message = "Unexpected error happened while storing the data")})
    @Consumes(APPLICATION_JSON)
    public void addDataForMetric(@Suspended final AsyncResponse asyncResponse,
                                 @PathParam("tenantId") final String tenantId, @PathParam("id") String id,
                                 @ApiParam(value = "List of datapoints containing timestamp and value", required = true)
                                 List<NumericDataPoint> dataPoints) {

        NumericMetric metric = new NumericMetric(tenantId, new MetricId(id));
        for (NumericDataPoint p : dataPoints) {
            metric.addData(p.getTimestamp(), p.getValue());
        }
        ListenableFuture<Void> future = metricsService.addNumericData(asList(metric));
        Futures.addCallback(future, new DataInsertedCallback(asyncResponse, "Failed to insert data"));
    }

    @POST
    @Path("/{tenantId}/metrics/availability/{id}/data")
    @Consumes(APPLICATION_JSON)
    @ApiOperation(value = "Add data for a single availability metric.")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Adding data succeeded."),
            @ApiResponse(code = 500, message = "Unexpected error happened while storing the data")})
    public void addAvailabilityForMetric(@Suspended final AsyncResponse asyncResponse,
        @PathParam("tenantId") final String tenantId, @PathParam("id") String id,
        @ApiParam(value = "List of availability datapoints", required = true) List<AvailabilityDataPoint> data) {
        AvailabilityMetric metric = new AvailabilityMetric(tenantId, new MetricId(id));

        for (AvailabilityDataPoint p : data) {
            metric.addData(new Availability(metric, p.getTimestamp(), p.getValue()));
        }

        ListenableFuture<Void> future = metricsService.addAvailabilityData(asList(metric));
        Futures.addCallback(future, new DataInsertedCallback(asyncResponse, "Failed to insert data"));
    }

    @POST
    @Path("/{tenantId}/metrics/numeric/data")
    @ApiOperation(value = "Add metric data for multiple numeric metrics in a single call.")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Adding data succeeded."),
            @ApiResponse(code = 500, message = "Unexpected error happened while storing the data")})
    @Consumes(APPLICATION_JSON)
    public void addNumericData(@ApiParam(access = "internal") @Suspended final AsyncResponse asyncResponse,
                               @PathParam("tenantId") String tenantId,
                               @ApiParam(value = "List of metrics", required = true)
                               List<NumericDataParams> paramsList) {
        if (paramsList.isEmpty()) {
            asyncResponse.resume(Response.ok().type(APPLICATION_JSON_TYPE).build());
        }

        List<NumericMetric> metrics = new ArrayList<>(paramsList.size());

        for (NumericDataParams params : paramsList) {
            NumericMetric metric = new NumericMetric(tenantId, new MetricId(params.getName()),
                MetricUtils.getTags(params.getTags()));
            for (NumericDataPoint p : params.getData()) {
                metric.addData(p.getTimestamp(), p.getValue());
            }
            metrics.add(metric);
        }
        ListenableFuture<Void> future = metricsService.addNumericData(metrics);
        Futures.addCallback(future, new DataInsertedCallback(asyncResponse, "Failed to insert data"));
    }

    @POST
    @Path("/{tenantId}/metrics/availability/data")
    @ApiOperation(value = "Add metric data for multiple availability metrics in a single call.")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Adding data succeeded."),
            @ApiResponse(code = 500, message = "Unexpected error happened while storing the data")})
    @Consumes(APPLICATION_JSON)
    public void addAvailabilityData(@Suspended final AsyncResponse asyncResponse,
        @PathParam("tenantId") String tenantId, @ApiParam(value = "List of availability metrics", required = true)
        List<AvailabilityDataParams> paramsList) {
        if (paramsList.isEmpty()) {
            asyncResponse.resume(Response.ok().type(APPLICATION_JSON_TYPE).build());
        }

        List<AvailabilityMetric> metrics = new ArrayList<>(paramsList.size());

        for (AvailabilityDataParams params : paramsList) {
            AvailabilityMetric metric = new AvailabilityMetric(tenantId, new MetricId(params.getName()),
                MetricUtils.getTags(params.getTags()));
            for (AvailabilityDataPoint p : params.getData()) {
                metric.addData(new Availability(metric, p.getTimestamp(), p.getValue()));
            }
            metrics.add(metric);
        }
        ListenableFuture<Void> future = metricsService.addAvailabilityData(metrics);
        Futures.addCallback(future, new DataInsertedCallback(asyncResponse, "Failed to insert data"));
    }

    @GET
    @ApiOperation(value = "Find numeric metrics data by their tags.", response = MetricOut.class,
            responseContainer = "List")
    @ApiResponses(value = { @ApiResponse(code = 200, message = ""),
            @ApiResponse(code = 500, message = "Any error in the query.")})
    @Path("/{tenantId}/numeric")
    public void findNumericDataByTags(@Suspended final AsyncResponse asyncResponse,
        @PathParam("tenantId") String tenantId,
        @ApiParam(allowMultiple = true, required = true, value = "A list of tags in the format of name:value")
        @QueryParam("tags") String encodedTags) {
        ListenableFuture<Map<MetricId, Set<NumericData>>> queryFuture = metricsService.findNumericDataByTags(
            tenantId, MetricUtils.decodeTags(encodedTags));
        Futures.addCallback(queryFuture, new FutureCallback<Map<MetricId, Set<NumericData>>>() {
            @Override
            public void onSuccess(Map<MetricId, Set<NumericData>> taggedDataMap) {
                Map<String, MetricOut> results = new HashMap<>();
                MetricOut dataOut = null;
                for (MetricId id : taggedDataMap.keySet()) {
                    List<DataPointOut> dataPoints = new ArrayList<>();
                    for (NumericData d : taggedDataMap.get(id)) {
                        if (dataOut == null) {
                            dataOut = new MetricOut(d.getMetric().getTenantId(), d.getMetric().getId().getName(), null);
                        }
                        dataPoints.add(new DataPointOut(d.getTimestamp(), d.getValue()));
                    }
                    dataOut.setData(dataPoints);
                    results.put(id.getName(), dataOut);
                    dataOut = null;
                }
                asyncResponse.resume(Response.ok(results).type(APPLICATION_JSON_TYPE).build());
            }

            @Override
            public void onFailure(Throwable t) {
                asyncResponse.resume(t);
            }
        });
    }

    @GET
    @ApiOperation(value = "Find availabilities metrics data by their tags.", response = MetricOut.class,
            responseContainer = "List")
    // See above method and HWKMETRICS-26 for fixes.
    @ApiResponses(value = { @ApiResponse(code = 200, message = ""),
            @ApiResponse(code = 204, message = "No matching availability metrics were found."),
            @ApiResponse(code = 500, message = "Any error in the query.")})
    @Path("/{tenantId}/availability")
    public void findAvailabilityDataByTags(@Suspended final AsyncResponse asyncResponse,
        @PathParam("tenantId") String tenantId,
        @ApiParam(allowMultiple = true, required = true, value = "A list of tags in the format of name:value")
        @QueryParam("tags") String encodedTags) {
        ListenableFuture<Map<MetricId, Set<Availability>>> queryFuture = metricsService.findAvailabilityByTags(
            tenantId, MetricUtils.decodeTags(encodedTags));
        Futures.addCallback(queryFuture, new FutureCallback<Map<MetricId, Set<Availability>>>() {
            @Override
            public void onSuccess(Map<MetricId, Set<Availability>> taggedDataMap) {
                if (taggedDataMap.isEmpty()) {
                    asyncResponse.resume(Response.ok().status(Status.NO_CONTENT).build());
                } else {
                    Map<String, MetricOut> results = new HashMap<>();
                    MetricOut dataOut = null;
                    for (MetricId id : taggedDataMap.keySet()) {
                        List<DataPointOut> dataPoints = new ArrayList<>();
                        for (Availability a : taggedDataMap.get(id)) {
                            if (dataOut == null) {
                                dataOut = new MetricOut(a.getMetric().getTenantId(), a.getMetric().getId().getName(),
                                    null);
                            }
                            dataPoints.add(new DataPointOut(a.getTimestamp(), a.getType().getText()));
                        }
                        dataOut.setData(dataPoints);
                        results.put(id.getName(), dataOut);
                        dataOut = null;
                    }
                    asyncResponse.resume(Response.ok(results).type(APPLICATION_JSON_TYPE).build());
                }
            }

            @Override
            public void onFailure(Throwable t) {
                asyncResponse.resume(t);
            }
        });
    }

    @GET
    @ApiOperation(value = "Retrieve numeric data.", response = MetricOut.class)
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Successfully fetched numeric data."),
            @ApiResponse(code = 204, message = "No numeric data was found."),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching numeric data.")})
    @Path("/{tenantId}/metrics/numeric/{id}/data")
    public void findNumericData(
        @Suspended final AsyncResponse response,
        @PathParam("tenantId") String tenantId,
        @PathParam("id") final String id,
        @ApiParam(value = "Defaults to now - 8 hours", required = false) @QueryParam("start") Long start,
        @ApiParam(value = "Defaults to now", required = false) @QueryParam("end") Long end,
        @ApiParam(value = "The number of buckets or intervals in which to divide the time range. A value of 60 for "
                + "example will return 60 equally spaced buckets for the time period between start and end times, "
                + "having max/min/avg calculated for each bucket.") @QueryParam("buckets") final int numberOfBuckets,
        @QueryParam("bucketWidthSeconds") final int bucketWidthSeconds,
        @QueryParam("skipEmpty") @DefaultValue("false") final boolean skipEmpty,
        @QueryParam("bucketCluster") @DefaultValue("true") final boolean bucketCluster) {

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
            outputFuture = Futures.transform(dataFuture, new MetricOutMapper());
        } else {
            if (bucketWidthSeconds == 0) {
                outputFuture = Futures.transform(dataFuture, new CreateSimpleBuckets(start, end, numberOfBuckets,
                    skipEmpty));
            } else {
                ListenableFuture<List<? extends Object>> bucketsFuture = Futures.transform(dataFuture,
                    new CreateFixedNumberOfBuckets(numberOfBuckets, bucketWidthSeconds));
                if (bucketCluster) {
                    outputFuture = Futures.transform(bucketsFuture, new FlattenBuckets(numberOfBuckets,
                        bucketWidthSeconds, skipEmpty));
                } else {
                    outputFuture = Futures.transform(bucketsFuture, new ClusterBucketData(numberOfBuckets,
                        bucketWidthSeconds));
                }
            }
        }
        Futures.addCallback(outputFuture, new FutureCallback<Object>() {
            @Override
            public void onSuccess(Object output) {
                response.resume(Response.ok(output).type(APPLICATION_JSON_TYPE).build());
            }

            @Override
            public void onFailure(Throwable t) {
                if (t instanceof NoResultsException) {
                    response.resume(Response.ok().status(Status.NO_CONTENT).build());
                } else {
                    Map<String, String> errors = ImmutableMap.of("errorMsg", "Failed to retrieve data due to " +
                        "an unexpected error: " + Throwables.getRootCause(t).getMessage());
                    response.resume(Response.status(Status.INTERNAL_SERVER_ERROR).entity(errors)
                        .type(APPLICATION_JSON_TYPE).build());
                }
            }
        });
    }

    private class MetricOutMapper extends MetricMapper<MetricOut> {
        @Override
        public MetricOut doApply(NumericMetric metric) {
            MetricOut output = new MetricOut(metric.getTenantId(), metric.getId().getName(),
                MetricUtils.flattenTags(metric.getTags()), metric.getDataRetention());
            List<DataPointOut> dataPoints = new ArrayList<>();
            for (NumericData d : metric.getData()) {
                dataPoints.add(new DataPointOut(d.getTimestamp(), d.getValue(), MetricUtils.flattenTags(d.getTags())));
            }
            output.setData(dataPoints);

            return output;
        }
    }

    private class CreateSimpleBuckets extends MetricMapper<BucketedOutput> {

        private long startTime;
        private long endTime;
        private int numberOfBuckets;
        private boolean skipEmpty;

        public CreateSimpleBuckets(long startTime, long endTime, int numberOfBuckets, boolean skipEmpty) {
            this.startTime = startTime;
            this.endTime = endTime;
            this.numberOfBuckets = numberOfBuckets;
            this.skipEmpty = skipEmpty;
        }

        @Override
        public BucketedOutput doApply(NumericMetric metric) {
            // we will have numberOfBuckets buckets over the whole time span
            BucketedOutput output = new BucketedOutput(metric.getTenantId(), metric.getId().getName(),
                MetricUtils.flattenTags(metric.getTags()));
            long bucketSize = (endTime - startTime) / numberOfBuckets;

            long[] buckets = LongStream.iterate(0, i -> i + 1).limit(numberOfBuckets)
                .map(i -> startTime + (i * bucketSize)).toArray();

            Map<Long, List<NumericData>> map = metric.getData().stream().collect(
                groupingBy(dataPoint -> findBucket(buckets, bucketSize, dataPoint.getTimestamp())));

            Map<Long, DoubleSummaryStatistics> statsMap = map.entrySet().stream()
                .collect(toMap(Map.Entry::getKey, entry -> entry.getValue().stream().collect(summarizingDouble(
                    NumericData::getValue))));

            for (Long bucket : buckets) {
                statsMap.computeIfAbsent(bucket, key -> new DoubleSummaryStatistics());
            }

            output.setData(statsMap.entrySet().stream()
                .sorted((left, right) -> left.getKey().compareTo(right.getKey()))
                .filter(e -> !skipEmpty || e.getValue().getCount() > 0)
                .map(e -> getBucketedDataPoint(metric.getId(), e.getKey(), e.getValue()))
                .collect(toList()));

            return output;
        }
    }

    private BucketDataPoint getBucketedDataPoint(MetricId id, long timestamp, DoubleSummaryStatistics stats) {
        // Currently, if a bucket does not contain any data, we set max/min/avg to Double.NaN.
        // DoubleSummaryStatistics however uses Double.Infinity for max/min and 0.0 for avg.
        if (stats.getCount() > 0) {
            return new BucketDataPoint(id.getName(), timestamp, stats.getMin(), stats.getAverage(), stats.getMax());
        }
        return new BucketDataPoint(id.getName(), timestamp, Double.NaN, Double.NaN, Double.NaN);
    }

    private class CreateFixedNumberOfBuckets extends MetricMapper<List<? extends Object>> {

        private int numberOfBuckets;
        private int bucketWidthSeconds;

        public CreateFixedNumberOfBuckets(int numberOfBuckets, int bucketWidthSeconds) {
            this.numberOfBuckets = numberOfBuckets;
            this.bucketWidthSeconds = bucketWidthSeconds;
        }

        @Override
        public List<? extends Object> doApply(NumericMetric metric) {
            long totalLength = (long) numberOfBuckets * bucketWidthSeconds * 1000L;
            long minTs = Long.MAX_VALUE;
            for (NumericData d : metric.getData()) {
                if (d.getTimestamp() < minTs) {
                    minTs = d.getTimestamp();
                }
            }
            TLongObjectMap<List<NumericData>> buckets = new TLongObjectHashMap<>(numberOfBuckets);
            for (NumericData d : metric.getData()) {
                long bucket = d.getTimestamp() - minTs;
                bucket = bucket % totalLength;
                bucket = bucket / (bucketWidthSeconds * 1000L);
                List<NumericData> tmpList = buckets.get(bucket);
                if (tmpList == null) {
                    tmpList = new ArrayList<>();
                    buckets.put(bucket, tmpList);
                }
                tmpList.add(d);
            }
            return asList(metric, buckets);
        }
    }

    private class FlattenBuckets implements Function<List<? extends Object>, BucketedOutput> {

        private int numberOfBuckets;
        private boolean skipEmpty;
        private int bucketWidthSeconds;

        public FlattenBuckets(int numberOfBuckets, int bucketWidthSeconds, boolean skipEmpty) {
            this.numberOfBuckets = numberOfBuckets;
            this.bucketWidthSeconds = bucketWidthSeconds;
            this.skipEmpty = skipEmpty;
        }

        @Override
        public BucketedOutput apply(List<? extends Object> args) {
            // Now that stuff is in buckets - we need to "flatten" them out.
            // As we collapse stuff from a lot of input timestamps into some
            // buckets, we only use a relative time for the bucket timestamps.
            NumericMetric metric = (NumericMetric) args.get(0);
            TLongObjectMap<List<NumericData>> buckets = (TLongObjectMap<List<NumericData>>) args.get(1);
            BucketedOutput output = new BucketedOutput(metric.getTenantId(), metric.getId().getName(),
                MetricUtils.flattenTags(metric.getTags()));
            for (int i = 0; i < numberOfBuckets; ++i) {
                List<NumericData> tmpList = buckets.get(i);
                if (tmpList == null) {
                    if (!skipEmpty) {
                        output.add(new BucketDataPoint(metric.getId().getName(), 1000 * i * bucketWidthSeconds, NaN,
                            NaN, NaN));
                    }
                } else {
                    output.add(getBucketDataPoint(tmpList.get(0).getMetric().getId().getName(),
                        1000L * i * bucketWidthSeconds, tmpList));
                }
            }
            return output;
        }
    }

    private class ClusterBucketData implements Function<List<? extends Object>, BucketedOutput> {

        private int numberOfBuckets;
        private int bucketWidthSeconds;

        public ClusterBucketData(int numberOfBuckets, int bucketWidthSeconds) {
            this.numberOfBuckets = numberOfBuckets;
            this.bucketWidthSeconds = bucketWidthSeconds;
        }

        @Override
        public BucketedOutput apply(List<? extends Object> args) {
            // We want to keep the raw values, but put them into clusters anyway
            // without collapsing them into a single min/avg/max tuple
            NumericMetric metric = (NumericMetric) args.get(0);
            TLongObjectMap<List<NumericData>> buckets = (TLongObjectMap<List<NumericData>>) args.get(1);
            BucketedOutput output = new BucketedOutput(metric.getTenantId(), metric.getId().getName(),
                MetricUtils.flattenTags(metric.getTags()));
            for (int i = 0; i < numberOfBuckets; ++i) {
                List<NumericData> tmpList = buckets.get(i);
                if (tmpList != null) {
                    for (NumericData d : tmpList) {
                        BucketDataPoint p = new BucketDataPoint(metric.getId().getName(),
                            1000L * i * bucketWidthSeconds, NaN, d.getValue(), NaN);
                        p.setValue(d.getValue());
                        output.add(p);
                    }
                }
            }
            return output;
        }
    }

    @GET
    @ApiOperation(value = "Retrieve availability data.", response = MetricOut.class)
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Successfully fetched availability data."),
            @ApiResponse(code = 204, message = "No availability data was found.")})
//            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching availability data.")
    @Path("/{tenantId}/metrics/availability/{id}/data")
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
        Futures.addCallback(future, new FutureCallback<AvailabilityMetric>() {
            @Override
            public void onSuccess(AvailabilityMetric metric) {
                if (metric == null) {
                    asyncResponse.resume(Response.ok().status(Status.NO_CONTENT).build());
                } else {
                    MetricOut output = new MetricOut(metric.getTenantId(), metric.getId().getName(),
                        MetricUtils.flattenTags(metric.getTags()), metric.getDataRetention());
                    List<DataPointOut> dataPoints = new ArrayList<>(metric.getData().size());
                    for (Availability a : metric.getData()) {
                        dataPoints.add(new DataPointOut(a.getTimestamp(), a.getType().getText(),
                            MetricUtils.flattenTags(a.getTags())));
                    }
                    output.setData(dataPoints);

                    asyncResponse.resume(Response.ok(output).type(APPLICATION_JSON_TYPE).build());
                }
            }

            @Override
            public void onFailure(Throwable t) {
                asyncResponse.resume(t);
            }
        });
    }

    @POST
    @ApiOperation(value = "Add or update numeric metric's tags.")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Tags were modified successfully.")})
    @Path("/{tenantId}/tags/numeric")
    public void tagNumericData(@Suspended final AsyncResponse asyncResponse, @PathParam("tenantId") String tenantId,
        @ApiParam(required = true) TagParams params) {
        ListenableFuture<List<NumericData>> future;
        NumericMetric metric = new NumericMetric(tenantId, new MetricId(params.getMetric()));
        if (params.getTimestamp() != null) {
            future = metricsService.tagNumericData(metric, MetricUtils.getTags(params.getTags()),
                params.getTimestamp());
        } else {
            future = metricsService.tagNumericData(metric, MetricUtils.getTags(params.getTags()), params.getStart(),
                params.getEnd());
        }
        Futures.addCallback(future, new FutureCallback<List<NumericData>>() {
            @Override
            public void onSuccess(List<NumericData> data) {
                asyncResponse.resume(Response.ok().type(APPLICATION_JSON_TYPE).build());
            }

            @Override
            public void onFailure(Throwable t) {
                asyncResponse.resume(t);
            }
        });
    }

    @POST
    @ApiOperation(value = "Add or update availability metric's tags.")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Tags were modified successfully.")})
    @Path("/{tenantId}/tags/availability")
    public void tagAvailabilityData(@Suspended final AsyncResponse asyncResponse,
        @PathParam("tenantId") String tenantId, @ApiParam(required = true) TagParams params) {
        ListenableFuture<List<Availability>> future;
        AvailabilityMetric metric = new AvailabilityMetric(tenantId, new MetricId(params.getMetric()));
        if (params.getTimestamp() != null) {
            future = metricsService.tagAvailabilityData(metric, MetricUtils.getTags(params.getTags()),
                params.getTimestamp());
        } else {
            future = metricsService.tagAvailabilityData(metric, MetricUtils.getTags(params.getTags()),
                params.getStart(), params.getEnd());
        }
        Futures.addCallback(future, new FutureCallback<List<Availability>>() {
            @Override
            public void onSuccess(List<Availability> data) {
                asyncResponse.resume(Response.ok().type(APPLICATION_JSON_TYPE).build());
            }

            @Override
            public void onFailure(Throwable t) {
                asyncResponse.resume(t);
            }
        });
    }

    @GET
    @ApiOperation(value = "Find numeric metric data with given tags.", response = MetricOut.class,
            responseContainer = "List")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Numeric values fetched successfully"),
            @ApiResponse(code = 500, message = "Any error while fetching data.")})
    @Path("/{tenantId}/tags/numeric/{tag}")
    public void findTaggedNumericData(@Suspended final AsyncResponse asyncResponse,
        @PathParam("tenantId") String tenantId,
        @ApiParam(allowMultiple = true, required = true, value = "A list of tags in the format of name:value")
        @PathParam("tag") String encodedTag) {
        ListenableFuture<Map<MetricId, Set<NumericData>>> future = metricsService.findNumericDataByTags(
                tenantId, MetricUtils.decodeTags(encodedTag));
        Futures.addCallback(future, new FutureCallback<Map<MetricId, Set<NumericData>>>() {
            @Override
            public void onSuccess(Map<MetricId, Set<NumericData>> taggedDataMap) {
                if (taggedDataMap.isEmpty()) {
                    asyncResponse.resume(Response.ok().status(Status.NO_CONTENT).build());
                } else {
                    // TODO Should we return something other than NumericDataOutput?
                    // Currently we only query the tags table which does not include meta data.
                    // There is a metadata property in NumericDataOutput so the resulting json
                    // will always have a null metadata field, which might misleading. We may
                    // want to use a different return type that does not have a meta data property.

                    Map<String, MetricOut> results = new HashMap<>();
                    MetricOut dataOut = null;
                    for (MetricId id : taggedDataMap.keySet()) {
                        List<DataPointOut> dataPoints = new ArrayList<>();
                        for (NumericData d : taggedDataMap.get(id)) {
                            if (dataOut == null) {
                                dataOut = new MetricOut(d.getMetric().getTenantId(), d.getMetric().getId().getName(),
                                        null);
                            }
                            dataPoints.add(new DataPointOut(d.getTimestamp(), d.getValue()));
                        }
                        dataOut.setData(dataPoints);
                        results.put(id.getName(), dataOut);
                        dataOut = null;
                    }
                    asyncResponse.resume(Response.ok(results).type(APPLICATION_JSON_TYPE).build());
                }
            }

            @Override
            public void onFailure(Throwable t) {
                asyncResponse.resume(t);
            }
        });
    }

    @GET
    @ApiOperation(value = "Find availability metric data with given tags.", response = MetricOut.class,
            responseContainer = "List")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Availability values fetched successfully"),
            @ApiResponse(code = 500, message = "Any error while fetching data.")})
    @Path("/{tenantId}/tags/availability/{tag}")
    public void findTaggedAvailabilityData(@Suspended final AsyncResponse asyncResponse,
        @PathParam("tenantId") String tenantId,
        @ApiParam(allowMultiple = true, required = true, value = "A list of tags in the format of name:value")
        @PathParam("tag") String encodedTag) {
        ListenableFuture<Map<MetricId, Set<Availability>>> future = metricsService.findAvailabilityByTags(tenantId,
            MetricUtils.decodeTags(encodedTag));
        Futures.addCallback(future, new FutureCallback<Map<MetricId, Set<Availability>>>() {
            @Override
            public void onSuccess(Map<MetricId, Set<Availability>> taggedDataMap) {
                if (taggedDataMap.isEmpty()) {
                    asyncResponse.resume(Response.ok().status(Status.NO_CONTENT).build());
                } else {
                    Map<String, MetricOut> results = new HashMap<>();
                    MetricOut dataOut = null;
                    for (MetricId id : taggedDataMap.keySet()) {
                        List<DataPointOut> dataPoints = new ArrayList<>();
                        for (Availability a : taggedDataMap.get(id)) {
                            if (dataOut == null) {
                                dataOut = new MetricOut(a.getMetric().getTenantId(), a.getMetric().getId().getName(),
                                        null);
                            }
                            dataPoints.add(new DataPointOut(a.getTimestamp(), a.getType().getText()));
                        }
                        dataOut.setData(dataPoints);
                        results.put(id.getName(), dataOut);
                        dataOut = null;
                    }
                    asyncResponse.resume(Response.ok(results).type(APPLICATION_JSON_TYPE).build());
                }
            }

            @Override
            public void onFailure(Throwable t) {
                asyncResponse.resume(t);
            }
        });
    }

    @POST
    @ApiOperation(value = "List of counter definitions", hidden = true)
    @Path("/counters")
    @Produces({APPLICATION_JSON})
    public void updateCountersForGroups(@Suspended final AsyncResponse asyncResponse, Collection<Counter> counters) {
        updateCounters(asyncResponse, counters);
    }

    @POST
    @ApiOperation(value = "Update multiple counters in a single counter group", hidden = true)
    @Path("/counters/{group}")
    @Produces(APPLICATION_JSON)
    public void updateCounterForGroup(@Suspended final AsyncResponse asyncResponse, @PathParam("group") String group,
        Collection<Counter> counters) {
        for (Counter counter : counters) {
            counter.setGroup(group);
        }
        updateCounters(asyncResponse, counters);
    }

    private void updateCounters(final AsyncResponse asyncResponse, Collection<Counter> counters) {
        ListenableFuture<Void> future = metricsService.updateCounters(counters);
        Futures.addCallback(future, new FutureCallback<Void>() {
            @Override
            public void onSuccess(Void result) {
                Response jaxrs = Response.ok().type(APPLICATION_JSON_TYPE).build();
                asyncResponse.resume(jaxrs);
            }

            @Override
            public void onFailure(Throwable t) {
                asyncResponse.resume(t);
            }
        });
    }

    @POST
    @ApiOperation(value = "Increase value of a counter", hidden = true)
    @Path("/counters/{group}/{counter}")
    public void updateCounter(@Suspended final AsyncResponse asyncResponse, @PathParam("group") String group,
        @PathParam("counter") String counter) {
        updateCounterValue(asyncResponse, group, counter, 1L);
    }

    @POST
    @ApiOperation(value = "Update value of a counter", hidden = true)
    @Path("/counters/{group}/{counter}/{value}")
    public void updateCounter(@Suspended final AsyncResponse asyncResponse, @PathParam("group") String group,
        @PathParam("counter") String counter, @PathParam("value") Long value) {
        updateCounterValue(asyncResponse, group, counter, value);
    }

    private void updateCounterValue(final AsyncResponse asyncResponse, String group, String counter, Long value) {
        ListenableFuture<Void> future = metricsService.updateCounter(new Counter(DEFAULT_TENANT_ID, group, counter,
                value));
        Futures.addCallback(future, new FutureCallback<Void>() {
            @Override
            public void onSuccess(Void result) {
                Response jaxrs = Response.ok().type(APPLICATION_JSON_TYPE).build();
                asyncResponse.resume(jaxrs);
            }

            @Override
            public void onFailure(Throwable t) {
                asyncResponse.resume(t);
            }
        });
    }

    @GET
    @ApiOperation(value = "Retrieve a list of counter values in this group", hidden = true, response = Counter.class,
            responseContainer = "List")
    @Path("/counters/{group}")
    @Produces({APPLICATION_JSON,APPLICATION_VND_HAWKULAR_WRAPPED_JSON})
    public void getCountersForGroup(@Suspended final AsyncResponse asyncResponse, @PathParam("group") String group) {
        ListenableFuture<List<Counter>> future = metricsService.findCounters(group);
        Futures.addCallback(future, new FutureCallback<List<Counter>>() {
            @Override
            public void onSuccess(List<Counter> counters) {
                Response jaxrs = Response.ok(counters).type(APPLICATION_JSON_TYPE).build();
                asyncResponse.resume(jaxrs);
            }

            @Override
            public void onFailure(Throwable t) {
                asyncResponse.resume(t);
            }
        });
    }

    @GET
    @ApiOperation(value = "Retrieve value of a counter", hidden = true, response = Counter.class,
            responseContainer = "List")
    @Path("/counters/{group}/{counter}")
    @Produces({APPLICATION_JSON,APPLICATION_VND_HAWKULAR_WRAPPED_JSON})
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
    @Produces(APPLICATION_JSON)
    @ApiOperation(value = "Find tenant's metric definitions.", notes = "Does not include any metric values. ",
            response = MetricOut.class, responseContainer = "List")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Successfully retrieved at least one metric "
            + "definition."),
            @ApiResponse(code = 204, message = "No metrics found."),
            @ApiResponse(code = 400, message = "Given type is not a valid type."),
            @ApiResponse(code = 500, message = "Failed to retrieve metrics due to unexpected error.")})
    public void findMetrics(@Suspended final AsyncResponse response, @PathParam("tenantId") final String tenantId,
        @ApiParam(value = "Queried metric type", required = true, allowableValues = "[num, avail, log]")
        @QueryParam("type") String type) {
        MetricType metricType = null;
        try {
            metricType = MetricType.fromTextCode(type);
        } catch (IllegalArgumentException e) {
            ImmutableMap<String, String> errors = ImmutableMap.of("errorMsg", "[" + type + "] is not a valid type. " +
                "Accepted values are num|avail|log");
            response.resume(Response.status(Status.BAD_REQUEST).entity(errors).type(APPLICATION_JSON_TYPE).build());
        }
        ListenableFuture<List<Metric>> future = metricsService.findMetrics(tenantId, metricType);
        Futures.addCallback(future, new FutureCallback<List<Metric>>() {
            @Override
            public void onSuccess(List<Metric> metrics) {
                if (metrics.isEmpty()) {
                    response.resume(Response.status(Status.NO_CONTENT).type(APPLICATION_JSON_TYPE).build());
                } else {
                    List<MetricOut> output = new ArrayList<>();
                    for (Metric metric : metrics) {
                        output.add(new MetricOut(tenantId, metric.getId().getName(), MetricUtils.flattenTags(
                            metric.getTags()), metric.getDataRetention()));
                    }
                    response.resume(Response.status(Status.OK).entity(output).type(APPLICATION_JSON_TYPE).build());
                }
            }

            @Override
            public void onFailure(Throwable t) {
                Map<String, String> errors = ImmutableMap.of("errorMsg", "Failed to retrieve metrics due to " +
                    "an unexpected error: " + Throwables.getRootCause(t).getMessage());
                response.resume(Response.status(Status.INTERNAL_SERVER_ERROR).entity(errors)
                    .type(APPLICATION_JSON_TYPE).build());
            }
        });
    }

//    static BucketDataPoint createPointInSimpleBucket(String id, long startTime, long bucketsize,
//        List<NumericData> metrics) {
//        List<NumericData> bucketMetrics = new ArrayList<>(metrics.size());
//        // Find matching metrics
//        for (NumericData raw : metrics) {
//            if (raw.getTimestamp() >= startTime && raw.getTimestamp() < startTime + bucketsize) {
//                bucketMetrics.add((NumericData) raw);
//            }
//        }
//
//        return getBucketDataPoint(id, startTime, bucketMetrics);
//    }

    static long findBucket(long[] buckets, long bucketSize, long timestamp) {
        return stream(buckets).filter(bucket -> timestamp >= bucket && timestamp < bucket + bucketSize)
            .findFirst().getAsLong();
    }

    static BucketDataPoint getBucketDataPoint(String id, long startTime, List<NumericData> bucketMetrics) {

        Double min = null;
        Double max = null;
        double sum = 0;
        for (NumericData raw : bucketMetrics) {
            if (max==null || raw.getValue() > max) {
                max = raw.getValue();
            }
            if (min==null || raw.getValue() < min) {
                min = raw.getValue();
            }
            sum += raw.getValue();
        }
        double avg = bucketMetrics.size()>0 ? sum / bucketMetrics.size() : NaN;
        if (min == null) {
            min = NaN;
        }
        if (max == null) {
            max = NaN;
        }
        BucketDataPoint result = new BucketDataPoint(id,startTime,min, avg,max);

        return result;
    }

}
