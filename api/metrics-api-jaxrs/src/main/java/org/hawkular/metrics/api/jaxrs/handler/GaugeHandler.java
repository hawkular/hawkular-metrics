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

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import static org.hawkular.metrics.api.jaxrs.filter.TenantFilter.TENANT_HEADER_NAME;
import static org.hawkular.metrics.api.jaxrs.util.ApiUtils.badRequest;
import static org.hawkular.metrics.api.jaxrs.util.ApiUtils.serverError;
import static org.hawkular.metrics.model.MetricType.GAUGE;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.PatternSyntaxException;

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

import org.hawkular.metrics.api.jaxrs.handler.observer.MetricCreatedObserver;
import org.hawkular.metrics.api.jaxrs.handler.observer.ResultSetObserver;
import org.hawkular.metrics.api.jaxrs.util.ApiUtils;
import org.hawkular.metrics.core.service.Functions;
import org.hawkular.metrics.core.service.MetricsService;
import org.hawkular.metrics.model.ApiError;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.MetricType;
import org.hawkular.metrics.model.NumericBucketPoint;
import org.hawkular.metrics.model.exception.RuntimeApiError;
import org.hawkular.metrics.model.param.BucketConfig;
import org.hawkular.metrics.model.param.Duration;
import org.hawkular.metrics.model.param.Percentiles;
import org.hawkular.metrics.model.param.Tags;
import org.hawkular.metrics.model.param.TimeRange;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import rx.Observable;

/**
 * @author Stefan Negrea
 *
 */
@Path("/gauges")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
@Api(tags = "Gauge")
public class GaugeHandler {

    @Inject
    private MetricsService metricsService;

    @HeaderParam(TENANT_HEADER_NAME)
    private String tenantId;

    @POST
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
    public void createGaugeMetric(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(required = true) Metric<Double> metric,
            @Context UriInfo uriInfo
    ) {
        if (metric.getType() != null
                && MetricType.UNDEFINED != metric.getType()
                && MetricType.GAUGE != metric.getType()) {
            asyncResponse.resume(badRequest(new ApiError("Metric type does not match " + MetricType
                    .GAUGE.getText())));
        }
        metric = new Metric<>(new MetricId<>(tenantId, GAUGE, metric.getId()), metric.getTags(),
                metric.getDataRetention());
        URI location = uriInfo.getBaseUriBuilder().path("/gauges/{id}").build(metric.getMetricId().getName());
        metricsService.createMetric(metric).subscribe(new MetricCreatedObserver(asyncResponse, location));
    }

    @GET
    @Path("/")
    @ApiOperation(value = "Find tenant's metric definitions.",
                    notes = "Does not include any metric values. ",
                    response = Metric.class, responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved at least one metric definition."),
            @ApiResponse(code = 204, message = "No metrics found."),
            @ApiResponse(code = 400, message = "Invalid type parameter type.", response = ApiError.class),
            @ApiResponse(code = 500, message = "Failed to retrieve metrics due to unexpected error.",
                    response = ApiError.class)
    })
    public void findGaugeMetrics(
            @Suspended AsyncResponse asyncResponse,
            @ApiParam(value = "List of tags filters", required = false) @QueryParam("tags") Tags tags) {

        Observable<Metric<Double>> metricObservable = (tags == null)
                ? metricsService.findMetrics(tenantId, GAUGE)
                : metricsService.findMetricsWithFilters(tenantId, tags.getTags(), GAUGE);

        metricObservable
                .toList()
                .map(ApiUtils::collectionToResponse)
                .subscribe(asyncResponse::resume, t -> {
                    if (t instanceof PatternSyntaxException) {
                        asyncResponse.resume(badRequest(t));
                    } else {
                        asyncResponse.resume(serverError(t));
                    }
                });
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
        metricsService.findMetric(new MetricId<>(tenantId, GAUGE, id))
                .map(metricDef -> Response.ok(metricDef).build())
                .switchIfEmpty(Observable.just(ApiUtils.noContent()))
                .subscribe(asyncResponse::resume, t -> asyncResponse.resume(ApiUtils.serverError(t)));
    }

    @GET
    @Path("/{id}/tags")
    @ApiOperation(value = "Retrieve tags associated with the metric definition.", response = Map.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Metric's tags were successfully retrieved."),
            @ApiResponse(code = 204, message = "Query was successful, but no metrics were found."),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching metric's tags.",
                response = ApiError.class) })
    public void getMetricTags(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("id") String id
    ) {
        metricsService.getMetricTags(new MetricId<>(tenantId, GAUGE, id))
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
    public void updateMetricTags(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("id") String id,
            @ApiParam(required = true) Map<String, String> tags
    ) {
        Metric<Double> metric = new Metric<>(new MetricId<>(tenantId, GAUGE, id));
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
    public void deleteMetricTags(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("id") String id,
            @ApiParam("Tag list") @PathParam("tags") Tags tags
    ) {
        Metric<Double> metric = new Metric<>(new MetricId<>(tenantId, GAUGE, id));
        metricsService.deleteTags(metric, tags.getTags()).subscribe(new ResultSetObserver(asyncResponse));
    }

    @POST
    @Path("/{id}/data")
    @ApiOperation(value = "Add data for a single gauge metric.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Adding data succeeded."),
            @ApiResponse(code = 400, message = "Missing or invalid payload", response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error happened while storing the data",
                    response = ApiError.class),
    })
    public void addDataForMetric(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("id") String id,
            @ApiParam(value = "List of datapoints containing timestamp and value", required = true)
            List<DataPoint<Double>> data
    ) {
        Observable<Metric<Double>> metrics = Functions.dataPointToObservable(tenantId, id, data, GAUGE);
        Observable<Void> observable = metricsService.addDataPoints(GAUGE, metrics);
        observable.subscribe(new ResultSetObserver(asyncResponse));
    }

    @POST
    @Path("/data")
    @ApiOperation(value = "Add data for multiple gauge metrics in a single call.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Adding data succeeded."),
            @ApiResponse(code = 400, message = "Missing or invalid payload", response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error happened while storing the data",
                    response = ApiError.class)
    })
    public void addGaugeData(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "List of metrics", required = true) List<Metric<Double>> gauges
    ) {
        Observable<Metric<Double>> metrics = Functions.metricToObservable(tenantId, gauges, GAUGE);
        Observable<Void> observable = metricsService.addDataPoints(GAUGE, metrics);
        observable.subscribe(new ResultSetObserver(asyncResponse));
    }

    @GET
    @Path("/{id}/data")
    @ApiOperation(value = "Retrieve gauge data.", notes = "When buckets or bucketDuration query parameter is used, " +
            "the time range between start and end will be divided in buckets of equal duration, and metric statistics" +
            " will be computed for each bucket.", response = DataPoint.class, responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully fetched metric data."),
            @ApiResponse(code = 204, message = "No metric data was found."),
            @ApiResponse(code = 400, message = "buckets or bucketDuration parameter is invalid, or both are used.",
                    response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching metric data.",
                    response = ApiError.class)
    })
    public void findGaugeData(
            @Suspended AsyncResponse asyncResponse,
            @PathParam("id") String id,
            @ApiParam(value = "Defaults to now - 8 hours") @QueryParam("start") Long start,
            @ApiParam(value = "Defaults to now") @QueryParam("end") Long end,
            @ApiParam(value = "Use data from earliest received, subject to retention period")
                @QueryParam("fromEarliest") Boolean fromEarliest,
            @ApiParam(value = "Total number of buckets") @QueryParam("buckets") Integer bucketsCount,
            @ApiParam(value = "Bucket duration") @QueryParam("bucketDuration") Duration bucketDuration,
            @ApiParam(value = "Percentiles to calculate") @QueryParam("percentiles") Percentiles percentiles) {

        MetricId<Double> metricId = new MetricId<>(tenantId, GAUGE, id);

        if (bucketsCount == null && bucketDuration == null && !Boolean.TRUE.equals(fromEarliest)) {
            TimeRange timeRange = new TimeRange(start, end);
            if (!timeRange.isValid()) {
                asyncResponse.resume(badRequest(new ApiError(timeRange.getProblem())));
                return;
            }

            metricsService.findDataPoints(metricId, timeRange.getStart(), timeRange.getEnd())
                    .toList()
                    .map(ApiUtils::collectionToResponse)
                    .subscribe(asyncResponse::resume, t -> asyncResponse.resume(ApiUtils.serverError(t)));
            return;
        }

        Observable<BucketConfig> observableConfig = null;

        if (Boolean.TRUE.equals(fromEarliest)) {
            if (start != null || end != null) {
                asyncResponse.resume(badRequest(new ApiError("fromEarliest can only be used without start & end")));
                return;
            }

            if (bucketsCount == null && bucketDuration == null) {
                asyncResponse.resume(badRequest(new ApiError("fromEarliest can only be used with bucketed results")));
                return;
            }

            observableConfig = metricsService.findMetric(metricId).first().map((metric) -> {
                long dataRetention = metric.getDataRetention() != null && metric.getDataRetention() != 0
                        ? metric.getDataRetention() * 24 * 60 * 60 * 1000L
                        : metricsService.getDefaultTTL() * 1000L;

                long now = System.currentTimeMillis();
                long earliest = now - dataRetention;

                BucketConfig bucketConfig = new BucketConfig(bucketsCount, bucketDuration,
                        new TimeRange(earliest, now));

                if (!bucketConfig.isValid()) {
                    throw new RuntimeApiError(bucketConfig.getProblem());
                }

                return bucketConfig;
            });
        } else {
            TimeRange timeRange = new TimeRange(start, end);
            if (!timeRange.isValid()) {
                asyncResponse.resume(badRequest(new ApiError(timeRange.getProblem())));
                return;
            }

            BucketConfig bucketConfig = new BucketConfig(bucketsCount, bucketDuration, timeRange);
            if (!bucketConfig.isValid()) {
                asyncResponse.resume(badRequest(new ApiError(bucketConfig.getProblem())));
                return;
            }

            observableConfig = Observable.just(bucketConfig);
        }

        final Percentiles lPercentiles = percentiles != null ? percentiles
                : new Percentiles(Collections.<Double> emptyList());

        observableConfig
                .flatMap((config) -> metricsService.findGaugeStats(metricId,
                        config.getTimeRange().getStart(),
                        config.getTimeRange().getEnd(),
                        config.getBuckets(), lPercentiles.getPercentiles()))
                .flatMap(Observable::from)
                .skipWhile(bucket -> Boolean.TRUE.equals(fromEarliest) && bucket.isEmpty())
                .toList()
                .map(ApiUtils::collectionToResponse)
                .subscribe(asyncResponse::resume, t -> asyncResponse.resume(ApiUtils.error(t)));
    }

    @GET
    @Path("/data")
    @ApiOperation(value = "Find stats for multiple metrics.", notes = "Fetches data points from one or more metrics"
            + " that are determined using either a tags filter or a list of metric names. The time range between " +
            "start and end is divided into buckets of equal size (i.e., duration) using either the buckets or " +
            "bucketDuration parameter. Functions are applied to the data points in each bucket to produce statistics " +
            "or aggregated metrics.",
            response = NumericBucketPoint.class, responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully fetched metric data."),
            @ApiResponse(code = 204, message = "No metric data was found."),
            @ApiResponse(code = 400, message = "The tags parameter is required. Either the buckets or the " +
                    "bucketDuration parameter is required but not both.",
                    response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching metric data.",
                    response = ApiError.class) })
    public void findGaugeData(
            @Suspended AsyncResponse asyncResponse,
            @ApiParam(value = "Defaults to now - 8 hours") @QueryParam("start") final Long start,
            @ApiParam(value = "Defaults to now") @QueryParam("end") final Long end,
            @ApiParam(value = "Total number of buckets") @QueryParam("buckets") Integer bucketsCount,
            @ApiParam(value = "Bucket duration") @QueryParam("bucketDuration") Duration bucketDuration,
            @ApiParam(value = "Percentiles to calculate") @QueryParam("percentiles") Percentiles percentiles,
            @ApiParam(value = "List of tags filters", required = false) @QueryParam("tags") Tags tags,
            @ApiParam(value = "List of metric names", required = false) @QueryParam("metrics") List<String> metricNames,
            @ApiParam(value = "Downsample method (if true then sum of stacked individual stats; defaults to false)",
                    required = false) @DefaultValue("false") @QueryParam("stacked") Boolean stacked) {

        TimeRange timeRange = new TimeRange(start, end);
        if (!timeRange.isValid()) {
            asyncResponse.resume(badRequest(new ApiError(timeRange.getProblem())));
            return;
        }
        BucketConfig bucketConfig = new BucketConfig(bucketsCount, bucketDuration, timeRange);
        if (bucketConfig.isEmpty()) {
            asyncResponse.resume(badRequest(new ApiError(
                    "Either the buckets or bucketDuration parameter must be used")));
            return;
        }
        if (!bucketConfig.isValid()) {
            asyncResponse.resume(badRequest(new ApiError(bucketConfig.getProblem())));
            return;
        }
        if (metricNames.isEmpty() && (tags == null || tags.getTags().isEmpty())) {
            asyncResponse.resume(badRequest(new ApiError("Either metrics or tags parameter must be used")));
            return;
        }
        if (!metricNames.isEmpty() && !(tags == null || tags.getTags().isEmpty())) {
            asyncResponse.resume(badRequest(new ApiError("Cannot use both the metrics and tags parameters")));
            return;
        }

        if(percentiles == null) {
            percentiles = new Percentiles(Collections.<Double>emptyList());
        }

        if (metricNames.isEmpty()) {
            metricsService.findNumericStats(tenantId, MetricType.GAUGE, tags.getTags(), timeRange.getStart(),
                    timeRange.getEnd(), bucketConfig.getBuckets(), percentiles.getPercentiles(), stacked)
                    .map(ApiUtils::collectionToResponse)
                    .subscribe(asyncResponse::resume, t -> asyncResponse.resume(ApiUtils.serverError(t)));
        } else {
            metricsService.findNumericStats(tenantId, MetricType.GAUGE, metricNames, timeRange.getStart(),
                    timeRange.getEnd(), bucketConfig.getBuckets(), percentiles.getPercentiles(), stacked)
                    .map(ApiUtils::collectionToResponse)
                    .subscribe(asyncResponse::resume, t -> asyncResponse.resume(ApiUtils.serverError(t)));
        }
    }

    @GET
    @Path("/{id}/periods")
    @ApiOperation(value = "Find condition periods.", notes = "Retrieve periods for which the condition holds true for" +
            " each consecutive data point.", response = List.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully fetched periods."),
            @ApiResponse(code = 204, message = "No data was found."),
            @ApiResponse(code = 400, message = "Missing or invalid query parameters", response = ApiError.class)
    })
    public void findPeriods(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("id") String id,
            @ApiParam(value = "Defaults to now - 8 hours", required = false) @QueryParam("start") Long start,
            @ApiParam(value = "Defaults to now", required = false) @QueryParam("end") Long end,
            @ApiParam(value = "A threshold against which values are compared", required = true)
            @QueryParam("threshold") double threshold,
            @ApiParam(value = "A comparison operation to perform between values and the threshold.", required = true,
                    allowableValues = "ge, gte, lt, lte, eq, neq")
            @QueryParam("op") String operator
    ) {
        TimeRange timeRange = new TimeRange(start, end);
        if (!timeRange.isValid()) {
            asyncResponse.resume(badRequest(new ApiError(timeRange.getProblem())));
            return;
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
            asyncResponse.resume(badRequest(
                    new ApiError(
                            "Invalid value for op parameter. Supported values are lt, "
                                    + "lte, eq, gt, gte."
                    )
            ));
        } else {
            MetricId<Double> metricId = new MetricId<>(tenantId, GAUGE, id);
            metricsService.getPeriods(metricId, predicate, timeRange.getStart(), timeRange.getEnd())
                    .map(ApiUtils::collectionToResponse)
                    .subscribe(asyncResponse::resume, t -> asyncResponse.resume(ApiUtils.serverError(t)));
        }
    }
}
