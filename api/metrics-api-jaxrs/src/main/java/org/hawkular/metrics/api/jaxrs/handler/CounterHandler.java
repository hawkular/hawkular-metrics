/*
 * Copyright 2014-2017 Red Hat, Inc. and/or its affiliates
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

import static org.hawkular.metrics.api.jaxrs.util.ApiUtils.badRequest;
import static org.hawkular.metrics.api.jaxrs.util.ApiUtils.noContent;
import static org.hawkular.metrics.api.jaxrs.util.ApiUtils.serverError;
import static org.hawkular.metrics.model.MetricType.COUNTER;
import static org.hawkular.metrics.model.MetricType.COUNTER_RATE;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.PatternSyntaxException;

import javax.enterprise.context.ApplicationScoped;
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
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.hawkular.metrics.api.jaxrs.AggregatedStatsQueryRequest;
import org.hawkular.metrics.api.jaxrs.QueryRequest;
import org.hawkular.metrics.api.jaxrs.handler.observer.MetricCreatedObserver;
import org.hawkular.metrics.api.jaxrs.handler.observer.ResultSetObserver;
import org.hawkular.metrics.api.jaxrs.handler.template.IMetricsHandler;
import org.hawkular.metrics.api.jaxrs.param.TimeAndBucketParams;
import org.hawkular.metrics.api.jaxrs.param.TimeAndSortParams;
import org.hawkular.metrics.api.jaxrs.util.ApiUtils;
import org.hawkular.metrics.api.jaxrs.util.Logged;
import org.hawkular.metrics.core.service.Functions;
import org.hawkular.metrics.core.service.Order;
import org.hawkular.metrics.core.service.transformers.MinMaxTimestampTransformer;
import org.hawkular.metrics.model.ApiError;
import org.hawkular.metrics.model.Buckets;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.MetricType;
import org.hawkular.metrics.model.NumericBucketPoint;
import org.hawkular.metrics.model.exception.RuntimeApiError;
import org.hawkular.metrics.model.param.BucketConfig;
import org.hawkular.metrics.model.param.Duration;
import org.hawkular.metrics.model.param.Percentiles;
import org.hawkular.metrics.model.param.TagNames;
import org.hawkular.metrics.model.param.Tags;
import org.hawkular.metrics.model.param.TimeRange;
import org.jboss.resteasy.annotations.GZIP;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import rx.Observable;
import rx.schedulers.Schedulers;

/**
 * @author Stefan Negrea
 *
 */
@Path("/counters")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
@GZIP
@Api(tags = "Counter")
@ApplicationScoped
@Logged
public class CounterHandler extends MetricsServiceHandler implements IMetricsHandler<Long> {

    @POST
    @Path("/")
    @ApiOperation(
            value = "Create counter metric.", notes = "This operation also causes the rate to be calculated and " +
            "persisted periodically after raw count data is persisted. Clients are not required to explicitly create " +
            "a metric before storing data. Doing so however allows clients to prevent naming collisions and to " +
            "specify tags and data retention.")
    @ApiResponses(value = {
            @ApiResponse(code = 201, message = "Metric created successfully"),
            @ApiResponse(code = 400, message = "Missing or invalid payload", response = ApiError.class),
            @ApiResponse(code = 409, message = "Counter metric with given id already exists", response = ApiError
                    .class),
            @ApiResponse(code = 500, message = "Metric creation failed due to an unexpected error",
                    response = ApiError.class)
    })
    public void createMetric(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(required = true) Metric<Long> metric,
            @ApiParam(value = "Overwrite previously created metric configuration if it exists. "
                    + "Only data retention and tags are overwriten; existing data points are unnafected. Defaults to false.",
                    required = false) @DefaultValue("false") @QueryParam("overwrite") Boolean overwrite,
            @Context UriInfo uriInfo
    ) {
        if (metric.getType() != null
                && MetricType.UNDEFINED != metric.getType()
                && MetricType.COUNTER != metric.getType()) {
            asyncResponse
                    .resume(badRequest(new ApiError("Metric type does not match " + MetricType
                    .COUNTER.getText())));
        }
        metric = new Metric<>(new MetricId<>(getTenant(), COUNTER, metric.getId()),
                metric.getTags(), metric.getDataRetention());
        URI location = uriInfo.getBaseUriBuilder().path("/counters/{id}").build(metric.getMetricId().getName());
        metricsService.createMetric(metric, overwrite).subscribe(new MetricCreatedObserver(asyncResponse, location));
    }

    @GET
    @Path("/")
    @ApiOperation(value = "Find tenant's counter metric definitions.",
                    notes = "Does not include any metric values. ",
                    response = Metric.class, responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved at least one metric definition."),
            @ApiResponse(code = 204, message = "No metrics found."),
            @ApiResponse(code = 400, message = "Invalid type parameter type.", response = ApiError.class),
            @ApiResponse(code = 500, message = "Failed to retrieve metrics due to unexpected error.",
                    response = ApiError.class)
    })
    public void getMetrics(
            @Suspended AsyncResponse asyncResponse,
            @ApiParam(value = "List of tags filters", required = false) @QueryParam("tags") String tags,
            @ApiParam(value = "Fetch min and max timestamps of available datapoints") @DefaultValue("false")
            @QueryParam("timestamps") Boolean fetchTimestamps) {

        Observable<Metric<Long>> metricObservable = null;
        if (tags != null) {
            metricObservable = metricsService.findMetricIdentifiersWithFilters(getTenant(), COUNTER, tags)
                    .flatMap(metricsService::findMetric);
        } else {
            metricObservable = metricsService.findMetrics(getTenant(), COUNTER);
        }

        if(fetchTimestamps) {
            metricObservable = metricObservable
                    .compose(new MinMaxTimestampTransformer<>(metricsService));
        }

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
    @ApiOperation(value = "Retrieve a counter definition.", response = Metric.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Metric's definition was successfully retrieved."),
            @ApiResponse(code = 204, message = "Query was successful, but no metrics definition is set."),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching metric's definition.",
                         response = ApiError.class) })
    public void getMetric(@Suspended final AsyncResponse asyncResponse, @PathParam("id") String id) {

        metricsService.findMetric(new MetricId<>(getTenant(), COUNTER, id))
                .compose(new MinMaxTimestampTransformer<>(metricsService))
                .map(metric -> Response.ok(metric).build())
                .switchIfEmpty(Observable.just(noContent()))
                .subscribe(asyncResponse::resume, t -> asyncResponse.resume(serverError(t)));
    }

    @DELETE
    @Path("/{id}")
    @ApiOperation(value = "Deletes the metric and associated uncompressed data points, and updates internal indexes."
            + " Note: compressed data will not be deleted immediately. It is deleted as part of the normal"
            + " data expiration as defined by the data retention settings. Consequently, compressed data will"
            + " be accessible until it automatically expires.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Metric deletion was successful."),
            @ApiResponse(code = 500, message = "Unexpected error occurred trying to delete the metric.")
    })
    public void deleteMetric(@Suspended AsyncResponse asyncResponse, @PathParam("id") String id) {
        MetricId<Long> metric = new MetricId<>(getTenant(), COUNTER, id);
        metricsService.deleteMetric(metric).subscribe(new ResultSetObserver(asyncResponse));
    }

    @GET
    @Path("/tags/{tags}")
    @ApiOperation(value = "Retrieve counter type's tag values", response = Map.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Tags successfully retrieved."),
            @ApiResponse(code = 204, message = "No matching tags were found"),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching tags.",
                    response = ApiError.class)
    })
    public void getTags(@Suspended final AsyncResponse asyncResponse,
                        @ApiParam("Tag query") @PathParam("tags") Tags tags) {
        metricsService.getTagValues(getTenant(), COUNTER, tags.getTags())
                .map(ApiUtils::mapToResponse)
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
            @PathParam("id") String id) {
        metricsService.getMetricTags(new MetricId<>(getTenant(), COUNTER, id))
                .map(ApiUtils::mapToResponse)
                .subscribe(asyncResponse::resume, t -> asyncResponse.resume(ApiUtils.serverError(t)));
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
            @ApiParam(required = true) Map<String, String> tags) {
        Metric<Long> metric = new Metric<>(new MetricId<>(getTenant(), COUNTER, id));
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
            @ApiParam(value = "Tag names", allowableValues = "Comma-separated list of tag names")
            @PathParam("tags") TagNames tags
    ) {
        Metric<Long> metric = new Metric<>(new MetricId<>(getTenant(), COUNTER, id));
        metricsService.deleteTags(metric, tags.getNames()).subscribe(new ResultSetObserver(asyncResponse));
    }

    @POST
    @Path("/raw")
    @ApiOperation(value = "Add data points for multiple counters.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Adding data points succeeded."),
            @ApiResponse(code = 400, message = "Missing or invalid payload", response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error happened while storing the data points",
                    response = ApiError.class)
    })
    public void addData(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "List of metrics", required = true) List<Metric<Long>> counters
    ) {
        Observable<Metric<Long>> metrics = Functions.metricToObservable(getTenant(), counters, COUNTER);
        Observable<Void> observable = metricsService.addDataPoints(COUNTER, metrics);
        observable.subscribe(new ResultSetObserver(asyncResponse));
    }

    @POST
    @Path("/raw/query")
    @ApiOperation(value = "Fetch raw data points for multiple metrics. This endpoint is experimental and may " +
            "undergo non-backwards compatible changes in future releases.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully fetched metric data points."),
            @ApiResponse(code = 204, message = "Query was successful, but no data was found."),
            @ApiResponse(code = 400, message = "No metric ids are specified", response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching metric data.",
                    response = ApiError.class)
    })
    @Override
    public void getData(
            @Suspended AsyncResponse asyncResponse,
            @ApiParam(required = true, value = "Query parameters that minimally must include a list of metric ids or " +
                    "tags. The standard start, end, order, and limit query parameters are supported as well.")
                    QueryRequest query) {
        findMetricsByNameOrTag(query.getIds(), query.getTags(), COUNTER)
                .toList()
                .flatMap(metricIds -> TimeAndSortParams.<Long>deferredBuilder(query.getStart(), query.getEnd())
                            .fromEarliest(query.getFromEarliest(), metricIds, this::findTimeRange)
                            .sortOptions(query.getLimit(), query.getOrder())
                            .toObservable()
                            .flatMap(p -> metricsService.findDataPoints(metricIds, p.getTimeRange().getStart(),
                                    p.getTimeRange().getEnd(), p.getLimit(), p.getOrder())
                                .observeOn(Schedulers.io())))
                .subscribe(createNamedDataPointObserver(asyncResponse, COUNTER));
    }

    @POST
    @Path("/rate/query")
    @ApiOperation(value = "Fetch rate data points for multiple metrics. This endpoint is experimental and may " +
            "undergo non-backwards compatible changes in future releases.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully fetched metric rate data points."),
            @ApiResponse(code = 204, message = "Query was successful, but no data was found."),
            @ApiResponse(code = 400, message = "No metric ids are specified", response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching metric data.",
                    response = ApiError.class)
    })
    public void getRateData(
            @Suspended AsyncResponse asyncResponse,
            @ApiParam(required = true, value = "Query parameters that minimally must include a list of metric ids or " +
                    "tags. The standard start, end, order, and limit query parameters are supported as well.")
                    QueryRequest query) {
        findMetricsByNameOrTag(query.getIds(), query.getTags(), COUNTER)
                .toList()
                .flatMap(metricIds -> TimeAndSortParams.<Long>deferredBuilder(query.getStart(), query.getEnd())
                        .fromEarliest(query.getFromEarliest(), metricIds, this::findTimeRange)
                        .sortOptions(query.getLimit(), query.getOrder())
                        .toObservable()
                        .flatMap(p -> metricsService.findRateData(metricIds, p.getTimeRange().getStart(),
                                p.getTimeRange().getEnd(), p.getLimit(), p.getOrder())
                            .observeOn(Schedulers.io())))
                .subscribe(createNamedDataPointObserver(asyncResponse, COUNTER_RATE));
    }

    @Deprecated
    @POST
    @Path("/data")
    @ApiOperation(value = "Deprecated. Please use /raw endpoint.")
    public void deprecatedAddData(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "List of metrics", required = true) List<Metric<Long>> counters
    ) {
        addData(asyncResponse, counters);
    }

    @POST
    @Path("/{id}/raw")
    @ApiOperation(value = "Add data for a single counter.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Adding data succeeded."),
            @ApiResponse(code = 400, message = "Missing or invalid payload", response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error happened while storing the data",
                    response = ApiError.class),
    })
    public void addMetricData(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("id") String id,
            @ApiParam(value = "List of data points containing timestamp and value", required = true)
            List<DataPoint<Long>> data
    ) {
        Observable<Metric<Long>> metrics = Functions.dataPointToObservable(getTenant(), id, data, COUNTER);
        Observable<Void> observable = metricsService.addDataPoints(COUNTER, metrics);
        observable.subscribe(new ResultSetObserver(asyncResponse));
    }

    @Deprecated
    @POST
    @Path("/{id}/data")
    @ApiOperation(value = "Deprecated. Please use /raw endpoint.")
    public void deprecatedAddData(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("id") String id,
            @ApiParam(value = "List of data points containing timestamp and value", required = true)
                List<DataPoint<Long>> data) {
        addMetricData(asyncResponse, id, data);
    }

    @Deprecated
    @GET
    @Path("/{id}/data")
    @ApiOperation(value = "Deprecated. Please use /raw or /stats endpoints",
                    response = DataPoint.class, responseContainer = "List")
    public void deprecatedFindCounterData(
            @Suspended AsyncResponse asyncResponse,
            @PathParam("id") String id,
            @ApiParam(value = "Defaults to now - 8 hours") @QueryParam("start") String start,
            @ApiParam(value = "Defaults to now") @QueryParam("end") String end,
            @ApiParam(value = "Use data from earliest received, subject to retention period")
                @QueryParam("fromEarliest") Boolean fromEarliest,
            @ApiParam(value = "Total number of buckets") @QueryParam("buckets") Integer bucketsCount,
            @ApiParam(value = "Bucket duration") @QueryParam("bucketDuration") Duration bucketDuration,
            @ApiParam(value = "Percentiles to calculate") @QueryParam("percentiles") Percentiles percentiles,
            @ApiParam(value = "Limit the number of data points returned") @QueryParam("limit") Integer limit,
            @ApiParam(value = "Data point sort order, based on timestamp") @QueryParam("order") Order order
    ) {
        MetricId<Long> metricId = new MetricId<>(getTenant(), COUNTER, id);

        if ((bucketsCount != null || bucketDuration != null) &&
                (limit != null || order != null)) {
            asyncResponse.resume(badRequest(new ApiError("Limit and order cannot be used with bucketed results")));
            return;
        }

        if (bucketsCount == null && bucketDuration == null && !Boolean.TRUE.equals(fromEarliest)) {
            TimeRange timeRange = new TimeRange(start, end);
            if (!timeRange.isValid()) {
                asyncResponse.resume(badRequest(new ApiError(timeRange.getProblem())));
                return;
            }

            if (limit == null) {
                limit = 0;
            }
            if (order == null) {
                order = Order.defaultValue(limit, start, end);
            }

            metricsService.findDataPoints(metricId, timeRange.getStart(), timeRange.getEnd(), limit, order)
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

            observableConfig = metricsService.findMetric(metricId).map((metric) -> {
                long dataRetention = metric.getDataRetention() * 24 * 60 * 60 * 1000L;
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
                : new Percentiles(Collections.emptyList());

        observableConfig
                .flatMap((config) -> metricsService.findCounterStats(metricId,
                        config, lPercentiles.getPercentiles()))
                .flatMap(Observable::from)
                .skipWhile(bucket -> Boolean.TRUE.equals(fromEarliest) && bucket.isEmpty())
                .toList()
                .map(ApiUtils::collectionToResponse)
                .subscribe(asyncResponse::resume, t -> asyncResponse.resume(ApiUtils.error(t)));
    }

    @GET
    @Path("/{id}/raw")
    @ApiOperation(value = "Retrieve counter data points.", response = DataPoint.class, responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully fetched metric data."),
            @ApiResponse(code = 204, message = "No metric data was found."),
            @ApiResponse(code = 400, message = "buckets or bucketDuration parameter is invalid, or both are used.",
                    response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching metric data.",
                    response = ApiError.class)
    })
    public void getMetricData(
            @Suspended AsyncResponse asyncResponse,
            @PathParam("id") String id,
            @ApiParam(value = "Defaults to now - 8 hours") @QueryParam("start") String start,
            @ApiParam(value = "Defaults to now") @QueryParam("end") String end,
            @ApiParam(value = "Use data from earliest received, subject to retention period")
                @QueryParam("fromEarliest") Boolean fromEarliest,
            @ApiParam(value = "Limit the number of data points returned") @QueryParam("limit") Integer limit,
            @ApiParam(value = "Data point sort order, based on timestamp") @QueryParam("order") Order order
    ) {
        MetricId<Long> metricId = new MetricId<>(getTenant(), COUNTER, id);
        TimeAndSortParams.<Long>deferredBuilder(start, end)
                .fromEarliest(fromEarliest, metricId, this::findTimeRange)
                .sortOptions(limit, order)
                .toObservable()
                .flatMap(p -> metricsService.findDataPoints(metricId, p.getTimeRange().getStart(), p.getTimeRange()
                        .getEnd(), p.getLimit(), p.getOrder()))
                .toList()
                .map(ApiUtils::collectionToResponse)
                .subscribe(asyncResponse::resume, t -> asyncResponse.resume(ApiUtils.error(t)));
    }

    @GET
    @Path("/{id}/stats")
    @ApiOperation(value = "Retrieve counter data points.", notes = "When buckets or bucketDuration query parameter " +
            "is used, the time range between start and end will be divided in buckets of equal duration, and metric " +
            "statistics will be computed for each bucket.", response = NumericBucketPoint.class, responseContainer =
            "List")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully fetched metric data."),
            @ApiResponse(code = 204, message = "No metric data was found."),
            @ApiResponse(code = 400, message = "buckets or bucketDuration parameter is invalid, or both are used.",
                    response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching metric data.",
                    response = ApiError.class)
    })
    public void getMetricStats(
            @Suspended AsyncResponse asyncResponse,
            @PathParam("id") String id,
            @ApiParam(value = "Defaults to now - 8 hours") @QueryParam("start") String start,
            @ApiParam(value = "Defaults to now") @QueryParam("end") String end,
            @ApiParam(value = "Use data from earliest received, subject to retention period")
                @QueryParam("fromEarliest") Boolean fromEarliest,
            @ApiParam(value = "Total number of buckets") @QueryParam("buckets") Integer bucketsCount,
            @ApiParam(value = "Bucket duration") @QueryParam("bucketDuration") Duration bucketDuration,
            @ApiParam(value = "Percentiles to calculate") @QueryParam("percentiles") Percentiles percentiles
    ) {
        MetricId<Long> metricId = new MetricId<>(getTenant(), COUNTER, id);
        TimeAndBucketParams.<Long>deferredBuilder(start, end)
                .fromEarliest(fromEarliest, metricId, this::findTimeRange)
                .bucketConfig(bucketsCount, bucketDuration)
                .percentiles(percentiles)
                .toObservable()
                .flatMap(p -> metricsService.findCounterStats(metricId, p.getBucketConfig(), p.getPercentiles()))
                .flatMap(Observable::from)
                .skipWhile(bucket -> Boolean.TRUE.equals(fromEarliest) && bucket.isEmpty())
                .toList()
                .map(ApiUtils::collectionToResponse)
                .subscribe(asyncResponse::resume, t -> asyncResponse.resume(ApiUtils.error(t)));
    }

    @GET
    @Path("/{id}/rate")
    @ApiOperation(
            value = "Retrieve counter rate data points.", notes = "Bucket related parameters as well as percentiles " +
            "are deprecated, please use rate/stats for bucketed results. " +
            "When buckets or bucketDuration query parameter is " +
            "used, the time range between start and end will be divided in buckets of equal duration, and metric " +
            "statistics will be computed for each bucket. Reset events are detected and data points that immediately " +
            "follow such events are filtered out prior to calculating the rates. This avoid misleading or inaccurate " +
            "rates when resets occur.", response = NumericBucketPoint.class, responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully fetched metric data."),
            @ApiResponse(code = 204, message = "No metric data was found."),
            @ApiResponse(code = 400, message = "buckets or bucketDuration parameter is invalid, or both are used.",
                    response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching metric data.",
                    response = ApiError.class)
    })
    public void getMetricRate(
            @Suspended AsyncResponse asyncResponse,
            @PathParam("id") String id,
            @ApiParam(value = "Defaults to now - 8 hours") @QueryParam("start") String start,
            @ApiParam(value = "Defaults to now") @QueryParam("end") String end,
            @ApiParam(value = "Limit the number of data points returned") @QueryParam("limit") Integer limit,
            @ApiParam(value = "Data point sort order, based on timestamp") @QueryParam("order") Order order,
            @Deprecated @ApiParam(value = "Total number of buckets") @QueryParam("buckets") Integer bucketsCount,
            @Deprecated @ApiParam(value = "Bucket duration") @QueryParam("bucketDuration") Duration bucketDuration,
            @Deprecated @ApiParam(value = "Percentiles to calculate") @QueryParam("percentiles") Percentiles percentiles
    ) {
        if ((bucketsCount != null || bucketDuration != null) &&
                (limit != null || order != null)) {
            asyncResponse.resume(badRequest(new ApiError("Limit and order cannot be used with bucketed results")));
            return;
        }

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

        MetricId<Long> metricId = new MetricId<>(getTenant(), COUNTER, id);
        Buckets buckets = bucketConfig.getBuckets();
        if (buckets == null) {

            if (limit == null) {
                limit = 0;
            }
            if (order == null) {
                order = Order.defaultValue(limit, start, end);
            }

            metricsService.findRateData(metricId, timeRange.getStart(), timeRange.getEnd(), limit, order)
                    .toList()
                    .map(ApiUtils::collectionToResponse)
                    .subscribe(asyncResponse::resume, t -> asyncResponse.resume(serverError(t)));
        } else {
            if(percentiles == null) {
                percentiles = new Percentiles(Collections.emptyList());
            }

            metricsService.findRateStats(metricId, bucketConfig, percentiles.getPercentiles())
                    .map(ApiUtils::collectionToResponse)
                    .subscribe(asyncResponse::resume, t -> asyncResponse.resume(serverError(t)));
        }
    }

    @GET
    @Path("/{id}/rate/stats")
    @ApiOperation(
            value = "Retrieve stats for counter rate data points.", notes = "The time range between start and end " +
            "will be divided in buckets of equal duration, and metric " +
            "statistics will be computed for each bucket. Reset events are detected and data points that immediately " +
            "follow such events are filtered out prior to calculating the rates. This avoid misleading or inaccurate " +
            "rates when resets occur.", response = NumericBucketPoint.class, responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully fetched metric data."),
            @ApiResponse(code = 204, message = "No metric data was found."),
            @ApiResponse(code = 400, message = "buckets or bucketDuration parameter is invalid, or both are used.",
                    response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching metric data.",
                    response = ApiError.class)
    })
    public void getMetricStatsRate(
            @Suspended AsyncResponse asyncResponse,
            @PathParam("id") String id,
            @ApiParam(value = "Defaults to now - 8 hours") @QueryParam("start") String start,
            @ApiParam(value = "Defaults to now") @QueryParam("end") String end,
            @ApiParam(value = "Use data from earliest received, subject to retention period") @QueryParam
                    ("fromEarliest") Boolean fromEarliest,
            @ApiParam(value = "Total number of buckets") @QueryParam("buckets") Integer bucketsCount,
            @ApiParam(value = "Bucket duration") @QueryParam("bucketDuration") Duration bucketDuration,
            @ApiParam(value = "Percentiles to calculate") @QueryParam("percentiles") Percentiles percentiles
    ) {
        MetricId<Long> metricId = new MetricId<>(getTenant(), COUNTER, id);
        TimeAndBucketParams.<Long>deferredBuilder(start, end)
                .fromEarliest(fromEarliest, metricId, this::findTimeRange)
                .bucketConfig(bucketsCount, bucketDuration)
                .percentiles(percentiles)
                .toObservable()
                .flatMap(p -> metricsService.findRateStats(metricId, p.getBucketConfig(), p.getPercentiles()))
                .flatMap(Observable::from)
                .skipWhile(bucket -> Boolean.TRUE.equals(fromEarliest) && bucket.isEmpty())
                .toList()
                .map(ApiUtils::collectionToResponse)
                .subscribe(asyncResponse::resume, t -> asyncResponse.resume(ApiUtils.error(t)));
    }

    @GET
    @Path("/stats")
    @ApiOperation(value = "Fetches data points from one or more metrics that are determined using either a tags " +
            "filter or a list of metric names. The time range between start and end is divided into buckets of " +
            "equal size (i.e., duration) using either the buckets or bucketDuration parameter. Functions  " +
            " are applied tothe data points in each bucket to produce statistics or aggregated metrics.",
            response = NumericBucketPoint.class, responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully fetched metric data."),
            @ApiResponse(code = 204, message = "No metric data was found."),
            @ApiResponse(code = 400, message = "The tags parameter is required. Either the buckets or the " +
                    "bucketDuration parameter is required but not both.", response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching metric data.",
                response = ApiError.class) })
    public void getStats(
            @Suspended AsyncResponse asyncResponse,
            @ApiParam(value = "Defaults to now - 8 hours") @QueryParam("start") final String start,
            @ApiParam(value = "Defaults to now") @QueryParam("end") final String end,
            @ApiParam(value = "Use data from earliest received, subject to retention period") @QueryParam
                    ("fromEarliest") Boolean fromEarliest,
            @ApiParam(value = "Total number of buckets") @QueryParam("buckets") Integer bucketsCount,
            @ApiParam(value = "Bucket duration") @QueryParam("bucketDuration") Duration bucketDuration,
            @ApiParam(value = "Percentiles to calculate") @QueryParam("percentiles") Percentiles percentiles,
            @ApiParam(value = "List of tags filters", required = false) @QueryParam("tags") String tags,
            @ApiParam(value = "List of metric names", required = false) @QueryParam("metrics") List<String> metricNames,
            @ApiParam(value = "Downsample method (if true then sum of stacked individual stats; defaults to false)",
                required = false) @DefaultValue("false") @QueryParam("stacked") Boolean stacked) {

        findMetricsByNameOrTag(metricNames, tags, MetricType.COUNTER)
                .toList()
                .flatMap(metricIds -> TimeAndBucketParams.<Long>deferredBuilder(start, end)
                        .fromEarliest(fromEarliest, metricIds, this::findTimeRange)
                        .bucketConfig(bucketsCount, bucketDuration)
                        .percentiles(percentiles)
                        .toObservable()
                        .flatMap(p -> metricsService.findNumericStats(metricIds, p.getTimeRange().getStart(),
                                    p.getTimeRange().getEnd(), p.getBucketConfig().getBuckets(), p.getPercentiles(),
                                    stacked, false)))
                .flatMap(Observable::from)
                .skipWhile(bucket -> Boolean.TRUE.equals(fromEarliest) && bucket.isEmpty())
                .toList()
                .map(ApiUtils::collectionToResponse)
                .subscribe(asyncResponse::resume, t -> asyncResponse.resume(ApiUtils.error(t)));
    }

    @POST
    @Path("/stats/query")
    @ApiOperation(value = "Find stats for multiple metrics. These metrics are aggregated into a single statistics " +
            "series.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully fetched metric data."),
            @ApiResponse(code = 204, message = "Query was successful, but no data was found."),
            @ApiResponse(code = 400, message = "Either tags or metric ids is required but not both. Either the " +
                    "buckets or the bucketDuration parameter is required but not both.", response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching metric data.",
                    response = ApiError.class)
    })
    public void getStats(
            @Suspended AsyncResponse asyncResponse,
            @ApiParam(required = true, value = "Query parameters that minimally must include a list of metric ids. " +
                    "The standard start, end, order, and limit query parameters are supported as well.")
                    AggregatedStatsQueryRequest query) {
        findMetricsByNameOrTag(query.getMetrics(), query.getTags(), MetricType.COUNTER)
                .toList()
                .flatMap(metricIds -> TimeAndBucketParams.<Long>deferredBuilder(query.getStart(), query.getEnd())
                        .fromEarliest(query.getFromEarliest(), metricIds, this::findTimeRange)
                        .bucketConfig(query.getBuckets(), query.getBucketDuration())
                        .percentiles(query.getPercentiles())
                        .toObservable()
                        .flatMap(p -> metricsService.findNumericStats(metricIds, p.getTimeRange().getStart(),
                                    p.getTimeRange().getEnd(), p.getBucketConfig().getBuckets(), p.getPercentiles(),
                                    query.isStacked(), false)))
                .flatMap(Observable::from)
                .skipWhile(bucket -> Boolean.TRUE.equals(query.getFromEarliest()) && bucket.isEmpty())
                .toList()
                .map(ApiUtils::collectionToResponse)
                .subscribe(asyncResponse::resume, t -> asyncResponse.resume(ApiUtils.error(t)));
    }

    @Deprecated
    @GET
    @Path("/data")
    @ApiOperation(value = "Deprecated. Please use /stats endpoint.",
            response = NumericBucketPoint.class, responseContainer = "List")
    public void deprecatedFindCounterDataStats(
            @Suspended AsyncResponse asyncResponse,
            @ApiParam(value = "Defaults to now - 8 hours") @QueryParam("start") final String start,
            @ApiParam(value = "Defaults to now") @QueryParam("end") final String end,
            @ApiParam(value = "Total number of buckets") @QueryParam("buckets") Integer bucketsCount,
            @ApiParam(value = "Bucket duration") @QueryParam("bucketDuration") Duration bucketDuration,
            @ApiParam(value = "Percentiles to calculate") @QueryParam("percentiles") Percentiles percentiles,
            @ApiParam(value = "List of tags filters", required = false) @QueryParam("tags") String tags,
            @ApiParam(value = "List of metric names", required = false) @QueryParam("metrics") List<String> metricNames,
            @ApiParam(value = "Downsample method (if true then sum of stacked individual stats; defaults to false)",
                required = false) @DefaultValue("false") @QueryParam("stacked") Boolean stacked) {
        getStats(asyncResponse, start, end, null, bucketsCount, bucketDuration, percentiles, tags, metricNames,
                stacked);
    }

    @GET
    @Path("/rate/stats")
    @ApiOperation(value = "Fetches data points from one or more metrics that are determined using either a tags " +
            "filter or a list of metric names. The time range between start and end is divided into buckets of " +
            "equal size (i.e., duration) using either the buckets or bucketDuration parameter. Functions are " +
            "applied to the data points in each bucket to produce statistics or aggregated metrics.",
                response = NumericBucketPoint.class, responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully fetched metric data."),
            @ApiResponse(code = 204, message = "No metric data was found."),
            @ApiResponse(code = 400, message = "The tags parameter is required. Either the buckets or the " +
                    "bucketDuration parameter is required but not both.", response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching metric data.",
                response = ApiError.class) })
    public void getRateStats(
            @Suspended AsyncResponse asyncResponse,
            @ApiParam(value = "Defaults to now - 8 hours") @QueryParam("start") final String start,
            @ApiParam(value = "Defaults to now") @QueryParam("end") final String end,
            @ApiParam(value = "Use data from earliest received, subject to retention period") @QueryParam
                    ("fromEarliest") Boolean fromEarliest,
            @ApiParam(value = "Total number of buckets") @QueryParam("buckets") Integer bucketsCount,
            @ApiParam(value = "Bucket duration") @QueryParam("bucketDuration") Duration bucketDuration,
            @ApiParam(value = "Percentiles to calculate") @QueryParam("percentiles") Percentiles percentiles,
            @ApiParam(value = "List of tags filters", required = false) @QueryParam("tags") String tags,
            @ApiParam(value = "List of metric names", required = false) @QueryParam("metrics") List<String> metricNames,
            @ApiParam(value = "Downsample method (if true then sum of stacked individual stats; defaults to false)",
                required = false) @DefaultValue("false") @QueryParam("stacked") Boolean stacked) {

        findMetricsByNameOrTag(metricNames, tags, MetricType.COUNTER)
                .toList()
                .flatMap(metricIds -> TimeAndBucketParams.<Long>deferredBuilder(start, end)
                        .fromEarliest(fromEarliest, metricIds, this::findTimeRange)
                        .bucketConfig(bucketsCount, bucketDuration)
                        .percentiles(percentiles)
                        .toObservable()
                        .flatMap(p -> metricsService.findNumericStats(metricIds, p.getTimeRange().getStart(),
                                    p.getTimeRange().getEnd(), p.getBucketConfig().getBuckets(), p.getPercentiles(),
                                    stacked, true)))
                .flatMap(Observable::from)
                .skipWhile(bucket -> Boolean.TRUE.equals(fromEarliest) && bucket.isEmpty())
                .toList()
                .map(ApiUtils::collectionToResponse)
                .subscribe(asyncResponse::resume, t -> asyncResponse.resume(ApiUtils.error(t)));
    }

    @Deprecated
    @GET
    @Path("/rate")
    @ApiOperation(value = "Deprecated. Please use /rate/stats endpoint.",
                    response = NumericBucketPoint.class, responseContainer = "List")
    public void deprecatedFindCounterRateDataStats(
            @Suspended AsyncResponse asyncResponse,
            @ApiParam(value = "Defaults to now - 8 hours") @QueryParam("start") final String start,
            @ApiParam(value = "Defaults to now") @QueryParam("end") final String end,
            @ApiParam(value = "Total number of buckets") @QueryParam("buckets") Integer bucketsCount,
            @ApiParam(value = "Bucket duration") @QueryParam("bucketDuration") Duration bucketDuration,
            @ApiParam(value = "Percentiles to calculate") @QueryParam("percentiles") Percentiles percentiles,
            @ApiParam(value = "List of tags filters", required = false) @QueryParam("tags") String tags,
            @ApiParam(value = "List of metric names", required = false) @QueryParam("metrics") List<String> metricNames,
            @ApiParam(value = "Downsample method (if true then sum of stacked individual stats; defaults to false)",
                    required = false) @DefaultValue("false") @QueryParam("stacked") Boolean stacked) {
        getStats(asyncResponse, start, end, null, bucketsCount, bucketDuration, percentiles, tags, metricNames,
                stacked);
    }

    @GET
    @Path("/{id}/stats/tags/{tags}")
    @ApiOperation(value = "Fetches data points and groups them into buckets based on one or more tag filters. The " +
            "data points in each bucket are then transformed into aggregated (i.e., bucket) data points.",
            response = DataPoint.class, responseContainer = "Map")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully fetched metric data."),
            @ApiResponse(code = 204, message = "No metric data was found."),
            @ApiResponse(code = 400, message = "Tags are invalid",
                    response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching metric data.",
                    response = ApiError.class)
    })
    public void getMetricStatsByTags(
            @Suspended AsyncResponse asyncResponse,
            @PathParam("id") String id,
            @ApiParam(value = "Defaults to now - 8 hours") @QueryParam("start") String start,
            @ApiParam(value = "Defaults to now") @QueryParam("end") String end,
            @ApiParam(value = "Percentiles to calculate") @QueryParam("percentiles") Percentiles percentiles,
            @ApiParam(value = "Tags") @PathParam("tags") Tags tags
    ) {
        TimeRange timeRange = new TimeRange(start, end);
        if (!timeRange.isValid()) {
            asyncResponse.resume(badRequest(new ApiError(timeRange.getProblem())));
            return;
        }
        MetricId<Long> metricId = new MetricId<>(getTenant(), COUNTER, id);
        final Percentiles lPercentiles = percentiles != null ? percentiles
                : new Percentiles(Collections.emptyList());
        metricsService.findCounterStats(metricId, tags.getTags(), timeRange.getStart(), timeRange.getEnd(),
                lPercentiles.getPercentiles())
                .map(ApiUtils::mapToResponse)
                .subscribe(asyncResponse::resume, t -> asyncResponse.resume(ApiUtils.serverError(t)));
    }

    @GET
    @Path("/tags/{tags}/raw")
    @ApiOperation(value = "Retrieve counter data points on multiple metrics by tags.", response = DataPoint.class,
            responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully fetched metric data points."),
            @ApiResponse(code = 204, message = "Query was successful, but no data was found."),
            @ApiResponse(code = 400, message = "No metric ids are specified", response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching metric data.",
                    response = ApiError.class)
    })
    public void getRawDataByTag(
            @Suspended AsyncResponse asyncResponse,
            @PathParam("tags") String tags,
            @ApiParam(value = "Defaults to now - 8 hours") @QueryParam("start") String start,
            @ApiParam(value = "Defaults to now") @QueryParam("end") String end,
            @ApiParam(value = "Use data from earliest received, subject to retention period")
            @QueryParam("fromEarliest") Boolean fromEarliest,
            @ApiParam(value = "Limit the number of data points returned") @QueryParam("limit") Integer limit,
            @ApiParam(value = "Data point sort order, based on timestamp") @QueryParam("order") Order order
    ) {
        metricsService.findMetricIdentifiersWithFilters(getTenant(), COUNTER, tags)
                .toList()
                .flatMap(metricIds -> TimeAndSortParams.<Long>deferredBuilder(start, end)
                        .fromEarliest(fromEarliest, metricIds, this::findTimeRange)
                        .sortOptions(limit, order)
                        .toObservable()
                        .flatMap(p -> metricsService.findDataPoints(metricIds, p.getTimeRange().getStart(),
                                p.getTimeRange().getEnd(), p.getLimit(), p.getOrder())
                                .observeOn(Schedulers.io())))
                .subscribe(createNamedDataPointObserver(asyncResponse, COUNTER));
    }
}
