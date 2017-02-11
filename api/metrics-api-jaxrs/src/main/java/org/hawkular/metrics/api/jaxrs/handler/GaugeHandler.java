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
import static org.hawkular.metrics.api.jaxrs.util.ApiUtils.serverError;
import static org.hawkular.metrics.model.MetricType.GAUGE;
import static org.hawkular.metrics.model.MetricType.GAUGE_RATE;
import static org.hawkular.metrics.model.MetricType.UNDEFINED;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
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
import org.hawkular.metrics.api.jaxrs.handler.transformer.MinMaxTimestampTransformer;
import org.hawkular.metrics.api.jaxrs.param.TimeAndBucketParams;
import org.hawkular.metrics.api.jaxrs.param.TimeAndSortParams;
import org.hawkular.metrics.api.jaxrs.util.ApiUtils;
import org.hawkular.metrics.core.service.Functions;
import org.hawkular.metrics.core.service.Order;
import org.hawkular.metrics.model.ApiError;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.MetricType;
import org.hawkular.metrics.model.NumericBucketPoint;
import org.hawkular.metrics.model.Percentile;
import org.hawkular.metrics.model.TaggedBucketPoint;
import org.hawkular.metrics.model.exception.RuntimeApiError;
import org.hawkular.metrics.model.param.BucketConfig;
import org.hawkular.metrics.model.param.Duration;
import org.hawkular.metrics.model.param.Percentiles;
import org.hawkular.metrics.model.param.TagNames;
import org.hawkular.metrics.model.param.Tags;
import org.hawkular.metrics.model.param.TimeRange;
import org.jboss.logging.Logger;

import io.reactivex.CompletableObserver;
import io.reactivex.Flowable;
import io.reactivex.disposables.Disposable;
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
@Path("/gauges")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
@Api(tags = "Gauge")
@ApplicationScoped
public class GaugeHandler extends MetricsServiceHandler implements IMetricsHandler<Double> {

    private Logger logger = Logger.getLogger(GaugeHandler.class);

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
    public void createMetric(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(required = true) Metric<Double> metric,
            @ApiParam(value = "Overwrite previously created metric configuration if it exists. "
                    + "Only data retention and tags are overwriten; existing data points are unnafected. "
                    + "Defaults to false."
            ) @DefaultValue("false") @QueryParam("overwrite") Boolean overwrite,
            @Context UriInfo uriInfo
    ) {
        if (metric.getType() != null && UNDEFINED != metric.getType() && GAUGE != metric.getType()) {
            asyncResponse.resume(badRequest(new ApiError("Metric type does not match " + GAUGE.getText())));
        }
        metric = new Metric<>(new MetricId<>(getTenant(), GAUGE, metric.getId()), metric.getTags(),
                metric.getDataRetention());
        URI location = uriInfo.getBaseUriBuilder().path("/gauges/{id}").build(metric.getMetricId().getName());
        metricsService.createMetric(metric, overwrite).subscribe(new MetricCreatedObserver(asyncResponse, location));
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
    public void getMetrics(
            @Suspended AsyncResponse asyncResponse,
            @ApiParam(value = "List of tags filters") @QueryParam("tags") String tags) {

        Observable<Metric<Double>> metricObservable = null;
        if (tags != null) {
            metricObservable = metricsService.findMetricsWithFilters(getTenant(), GAUGE, tags);
        } else {
            metricObservable = metricsService.findMetrics(getTenant(), GAUGE);
        }

        metricObservable
                .compose(new MinMaxTimestampTransformer<>(metricsService))
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
    public void getMetric(@Suspended final AsyncResponse asyncResponse, @PathParam("id") String id) {
        metricsService.findMetric(new MetricId<>(getTenant(), GAUGE, id))
                .compose(new MinMaxTimestampTransformer<>(metricsService))
                .map(metric -> Response.ok(metric).build())
                .switchIfEmpty(Observable.just(ApiUtils.noContent()))
                .subscribe(asyncResponse::resume, t -> asyncResponse.resume(ApiUtils.serverError(t)));
    }

    @GET
    @Path("/tags/{tags}")
    @ApiOperation(value = "Retrieve gauge type's tag values", response = Map.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Tags successfully retrieved."),
            @ApiResponse(code = 204, message = "No matching tags were found"),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching tags.",
                    response = ApiError.class)
    })
    public void getTags(@Suspended final AsyncResponse asyncResponse,
                        @ApiParam("Tag query") @PathParam("tags") Tags tags) {
        metricsService.getTagValues(getTenant(), GAUGE, tags.getTags())
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
            @PathParam("id") String id
    ) {
        metricsService.getMetricTags(new MetricId<>(getTenant(), GAUGE, id))
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
            @ApiParam(required = true) Map<String, String> tags
    ) {
        Metric<Double> metric = new Metric<>(new MetricId<>(getTenant(), GAUGE, id));
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
        Metric<Double> metric = new Metric<>(new MetricId<>(getTenant(), GAUGE, id));
        metricsService.deleteTags(metric, tags.getNames()).subscribe(new ResultSetObserver(asyncResponse));
    }

    @POST
    @Path("/{id}/raw")
    @ApiOperation(value = "Add data for a single gauge metric.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Adding data succeeded."),
            @ApiResponse(code = 400, message = "Missing or invalid payload", response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error happened while storing the data",
                    response = ApiError.class),
    })
    public void addMetricData(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("id") String id,
            @ApiParam(value = "List of datapoints containing timestamp and value", required = true)
            List<DataPoint<Double>> data
    ) {
        Observable<Metric<Double>> metrics = Functions.dataPointToObservable(getTenant(), id, data, GAUGE);
        Observable<Void> observable = metricsService.addDataPoints(GAUGE, metrics);
        observable.subscribe(new ResultSetObserver(asyncResponse));
    }

    @Deprecated
    @POST
    @Path("/{id}/data")
    @ApiOperation(value = "Deprecated. Please use /raw endpoint.")
    public void deprecatedAddDataForMetric(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("id") String id,
            @ApiParam(value = "List of datapoints containing timestamp and value", required = true)
            List<DataPoint<Double>> data
    ) {
        addMetricData(asyncResponse, id, data);
    }

    @POST
    @Path("/raw")
    @ApiOperation(value = "Add data for multiple gauge metrics in a single call.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Adding data succeeded."),
            @ApiResponse(code = 400, message = "Missing or invalid payload", response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error happened while storing the data",
                response = ApiError.class)
    })
    public void addData(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "List of metrics", required = true) List<Metric<Double>> gauges) {

        metricsService.addGaugePoints(Flowable.fromIterable(gauges).map(g -> new Metric<>(new MetricId<>(getTenant(),
                GAUGE, g.getMetricId().getName()), g.getDataPoints())))
                .subscribe(new CompletableObserver() {
                    @Override public void onSubscribe(Disposable disposable) {
                    }

                    @Override public void onComplete() {
                        asyncResponse.resume(Response.ok().build());
                    }

                    @Override public void onError(Throwable throwable) {

                    }
                });
//        Observable<Metric<Double>> metrics = Functions.metricToObservable(getTenant(), gauges, GAUGE);
//        Observable<Void> observable = metricsService.addDataPoints(GAUGE, metrics);
//        observable.subscribe(new ResultSetObserver(asyncResponse));
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
        findMetricsByNameOrTag(query.getIds(), query.getTags(), GAUGE)
                .toList()
                .flatMap(metricIds -> TimeAndSortParams.<Double>deferredBuilder(query.getStart(), query.getEnd())
                        .fromEarliest(query.getFromEarliest(), metricIds, this::findTimeRange)
                        .sortOptions(query.getLimit(), query.getOrder())
                        .toObservable()
                        .flatMap(p -> metricsService.findDataPoints(metricIds, p.getTimeRange().getStart(),
                                p.getTimeRange().getEnd(), p.getLimit(), p.getOrder())
                                .observeOn(Schedulers.io())))
                .subscribe(createNamedDataPointObserver(asyncResponse, GAUGE));
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
        findMetricsByNameOrTag(query.getIds(), query.getTags(), GAUGE)
                .toList()
                .flatMap(metricIds -> TimeAndSortParams.<Double>deferredBuilder(query.getStart(), query.getEnd())
                        .fromEarliest(query.getFromEarliest(), metricIds, this::findTimeRange)
                        .sortOptions(query.getLimit(), query.getOrder())
                        .toObservable()
                        .flatMap(p -> metricsService.findRateData(metricIds, p.getTimeRange().getStart(),
                                p.getTimeRange().getEnd(), p.getLimit(), p.getOrder())
                                .observeOn(Schedulers.io())))
                .subscribe(createNamedDataPointObserver(asyncResponse, GAUGE_RATE));
    }

    @Deprecated
    @POST
    @Path("/data")
    @ApiOperation(value = "Deprecated. Please use /raw endpoint.")
    public void deprecatedAddGaugeData(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "List of metrics", required = true) List<Metric<Double>> gauges
    ) {
        addData(asyncResponse, gauges);
    }

    @Deprecated
    @GET
    @Path("/{id}/data")
    @ApiOperation(value = "Deprecated. Please use /raw or /stats endpoints.",
                    response = DataPoint.class, responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully fetched metric data."),
            @ApiResponse(code = 204, message = "No metric data was found."),
            @ApiResponse(code = 400, message = "buckets or bucketDuration parameter is invalid, or both are used.",
                    response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching metric data.",
                    response = ApiError.class)
    })
    public void deprecatedFindGaugeData(
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

        MetricId<Double> metricId = new MetricId<>(getTenant(), GAUGE, id);

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

        Observable<BucketConfig> observableConfig;

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

        observableConfig
                .flatMap((config) -> {
                    List<Percentile> perc = percentiles == null ? Collections.emptyList() : percentiles.getPercentiles();
                    return metricsService.findGaugeStats(metricId, config, perc);
                })
                .flatMap(Observable::from)
                .skipWhile(bucket -> Boolean.TRUE.equals(fromEarliest) && bucket.isEmpty())
                .toList()
                .map(ApiUtils::collectionToResponse)
                .subscribe(asyncResponse::resume, t -> asyncResponse.resume(ApiUtils.error(t)));
    }

    @GET
    @Path("/{id}/raw")
    @ApiOperation(value = "Retrieve raw gauge data.", response = DataPoint.class, responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully fetched metric data."),
            @ApiResponse(code = 204, message = "No metric data was found."),
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
        MetricId<Double> metricId = new MetricId<>(getTenant(), GAUGE, id);
        TimeAndSortParams.<Double>deferredBuilder(start, end)
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
    @ApiOperation(value = "Retrieve gauge data.", notes = "The time range between start and end will be divided "
            + "in buckets of equal duration, and metric statistics will be computed for each bucket.",
                    response = NumericBucketPoint.class, responseContainer = "List")
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
            @ApiParam(value = "Percentiles to calculate") @QueryParam("percentiles") Percentiles percentiles) {

        MetricId<Double> metricId = new MetricId<>(getTenant(), GAUGE, id);
        TimeAndBucketParams.<Double>deferredBuilder(start, end)
                .fromEarliest(fromEarliest, metricId, this::findTimeRange)
                .bucketConfig(bucketsCount, bucketDuration)
                .percentiles(percentiles)
                .toObservable()
                .flatMap(p -> metricsService.findGaugeStats(metricId, p.getBucketConfig(), p.getPercentiles()))
                .flatMap(Observable::from)
                .skipWhile(bucket -> Boolean.TRUE.equals(fromEarliest) && bucket.isEmpty())
                .toList()
                .map(ApiUtils::collectionToResponse)
                .subscribe(asyncResponse::resume, t -> asyncResponse.resume(ApiUtils.error(t)));
    }

    @GET
    @Path("/stats")
    @ApiOperation(value = "Find stats for multiple metrics.", notes = "Fetches data points from one or more metrics"
            + " that are determined using either a tags filter or a list of metric names. The time range between " +
            "start and end is divided into buckets of equal size (i.e., duration) using either the buckets or " +
            "bucketDuration parameter. Functions are applied to the data points in each bucket to produce statistics " +
            "on aggregated metrics.",
            response = NumericBucketPoint.class, responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully fetched metric data."),
            @ApiResponse(code = 204, message = "No metric data was found."),
            @ApiResponse(code = 400, message = "The tags parameter is required. Either the buckets or the " +
                    "bucketDuration parameter is required but not both.",
                    response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching metric data.",
                    response = ApiError.class) })
    public void getStats(
            @Suspended AsyncResponse asyncResponse,
            @ApiParam(value = "Defaults to now - 8 hours") @QueryParam("start") final String start,
            @ApiParam(value = "Defaults to now") @QueryParam("end") final String end,
            @ApiParam(value = "Use data from earliest received, subject to retention period")
                                                @QueryParam("fromEarliest") Boolean fromEarliest,
            @ApiParam(value = "Total number of buckets") @QueryParam("buckets") Integer bucketsCount,
            @ApiParam(value = "Bucket duration") @QueryParam("bucketDuration") Duration bucketDuration,
            @ApiParam(value = "Percentiles to calculate") @QueryParam("percentiles") Percentiles percentiles,
            @ApiParam(value = "List of tags filters") @QueryParam("tags") String tags,
            @ApiParam(value = "List of metric names") @QueryParam("metrics") List<String> metricNames,
            @ApiParam(value = "Downsample method (if true then sum of stacked individual stats; defaults to false)")
            @DefaultValue("false") @QueryParam("stacked") Boolean stacked) {

        findMetricsByNameOrTag(metricNames, tags, MetricType.GAUGE)
                .toList()
                .flatMap(metricIds -> TimeAndBucketParams.<Double>deferredBuilder(start, end)
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
            @ApiResponse(code = 400, message = "Either tags or metric ids is required but not both. Either the buckets or the " +
                    "bucketDuration parameter is required but not both.", response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching metric data.",
                    response = ApiError.class)
    })
    public void getStats(
            @Suspended AsyncResponse asyncResponse,
            @ApiParam(required = true, value = "Query parameters that minimally must include a list of metric ids. " +
                    "The standard start, end, order, and limit query parameters are supported as well.")
                    AggregatedStatsQueryRequest query) {
        findMetricsByNameOrTag(query.getMetrics(), query.getTags(), MetricType.GAUGE)
                .toList()
                .flatMap(metricIds -> TimeAndBucketParams.<Double>deferredBuilder(query.getStart(), query.getEnd())
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

    @GET
    @Path("/{id}/stats/tags/{tags}")
    @ApiOperation(value = "Fetches data points and groups them into buckets based on one or more tag filters. The " +
            "data points in each bucket are then transformed into aggregated (i.e., bucket) data points.",
            response = TaggedBucketPoint.class, responseContainer = "Map")
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
        MetricId<Double> metricId = new MetricId<>(getTenant(), GAUGE, id);
        if (percentiles == null) {
            percentiles = new Percentiles(Collections.emptyList());
        }
        metricsService.findGaugeStats(metricId, tags.getTags(), timeRange.getStart(), timeRange.getEnd(),
                percentiles.getPercentiles())
                .map(ApiUtils::mapToResponse)
                .subscribe(asyncResponse::resume, t -> asyncResponse.resume(ApiUtils.serverError(t)));
    }

    @Deprecated
    @GET
    @Path("/data")
    @ApiOperation(value = "Deprecated. Please use /stats endpoint.",
            response = NumericBucketPoint.class, responseContainer = "List")
    public void deprecatedFindData(
            @Suspended AsyncResponse asyncResponse,
            @ApiParam(value = "Defaults to now - 8 hours") @QueryParam("start") final String start,
            @ApiParam(value = "Defaults to now") @QueryParam("end") final String end,
            @ApiParam(value = "Total number of buckets") @QueryParam("buckets") Integer bucketsCount,
            @ApiParam(value = "Bucket duration") @QueryParam("bucketDuration") Duration bucketDuration,
            @ApiParam(value = "Percentiles to calculate") @QueryParam("percentiles") Percentiles percentiles,
            @ApiParam(value = "List of tags filters") @QueryParam("tags") String tags,
            @ApiParam(value = "List of metric names") @QueryParam("metrics") List<String> metricNames,
            @ApiParam(value = "Downsample method (if true then sum of stacked individual stats; defaults to false)")
            @DefaultValue("false") @QueryParam("stacked") Boolean stacked) {

        getStats(asyncResponse, start, end, null, bucketsCount, bucketDuration, percentiles, tags, metricNames,
                stacked);
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
    public void getMetricPeriods(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("id") String id,
            @ApiParam(value = "Defaults to now - 8 hours") @QueryParam("start") String start,
            @ApiParam(value = "Defaults to now") @QueryParam("end") String end,
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
            MetricId<Double> metricId = new MetricId<>(getTenant(), GAUGE, id);
            metricsService.getPeriods(metricId, predicate, timeRange.getStart(), timeRange.getEnd())
                    .map(ApiUtils::collectionToResponse)
                    .subscribe(asyncResponse::resume, t -> asyncResponse.resume(ApiUtils.serverError(t)));
        }
    }

    @GET
    @Path("/{id}/rate")
    @ApiOperation(value = "Retrieve gauge rate data points.", response = DataPoint.class, responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully fetched metric data."),
            @ApiResponse(code = 204, message = "No metric data was found."),
            @ApiResponse(code = 400, message = "Time range is invalid.", response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching metric data.",
                    response = ApiError.class)
    })
    public void getMetricRate(
            @Suspended AsyncResponse asyncResponse,
            @PathParam("id") String id,
            @ApiParam(value = "Defaults to now - 8 hours") @QueryParam("start") String start,
            @ApiParam(value = "Defaults to now") @QueryParam("end") String end,
            @ApiParam(value = "Use data from earliest received, subject to retention period") @QueryParam
                    ("fromEarliest") Boolean fromEarliest,
            @ApiParam(value = "Limit the number of data points returned") @QueryParam("limit") Integer limit,
            @ApiParam(value = "Data point sort order, based on timestamp") @QueryParam("order") Order order
    ) {
        MetricId<Double> metricId = new MetricId<>(getTenant(), GAUGE, id);
        TimeAndSortParams.<Double>deferredBuilder(start, end)
                .fromEarliest(fromEarliest, metricId, this::findTimeRange)
                .sortOptions(limit, order)
                .toObservable()
                .flatMap(p -> metricsService.findRateData(metricId, p.getTimeRange().getStart(), p.getTimeRange()
                        .getEnd(), p.getLimit(), p.getOrder()))
                .toList()
                .map(ApiUtils::collectionToResponse)
                .subscribe(asyncResponse::resume, t -> asyncResponse.resume(ApiUtils.error(t)));
    }

    @GET
    @Path("/{id}/rate/stats")
    @ApiOperation(
            value = "Retrieve stats for gauge rate data points.", notes = "The time range between start and end " +
            "will be divided in buckets of equal duration, and metric statistics will be computed for each bucket.",
            response = NumericBucketPoint.class, responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully fetched metric data."),
            @ApiResponse(code = 204, message = "No metric data was found."),
            @ApiResponse(code = 400, message = "buckets or bucketDuration parameter is invalid, or both are used.",
                    response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching metric data.",
                    response = ApiError.class)
    })
    public void getMetricRateStats(
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
        MetricId<Double> metricId = new MetricId<>(getTenant(), GAUGE, id);
        TimeAndBucketParams.<Double>deferredBuilder(start, end)
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
                    response = ApiError.class)})
    public void getRateStats(
            @Suspended AsyncResponse asyncResponse,
            @ApiParam(value = "Defaults to now - 8 hours") @QueryParam("start") final String start,
            @ApiParam(value = "Defaults to now") @QueryParam("end") final String end,
            @ApiParam(value = "Use data from earliest received, subject to retention period") @QueryParam
                    ("fromEarliest") Boolean fromEarliest,
            @ApiParam(value = "Total number of buckets") @QueryParam("buckets") Integer bucketsCount,
            @ApiParam(value = "Bucket duration") @QueryParam("bucketDuration") Duration bucketDuration,
            @ApiParam(value = "Percentiles to calculate") @QueryParam("percentiles") Percentiles percentiles,
            @ApiParam(value = "List of tags filters") @QueryParam("tags") String tags,
            @ApiParam(value = "List of metric names") @QueryParam("metrics") List<String> metricNames,
            @ApiParam(value = "Downsample method (if true then sum of stacked individual stats; defaults to false)")
            @DefaultValue("false") @QueryParam("stacked") Boolean stacked) {

        findMetricsByNameOrTag(metricNames, tags, MetricType.GAUGE)
                .toList()
                .flatMap(metricIds -> TimeAndBucketParams.<Double>deferredBuilder(start, end)
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
}
