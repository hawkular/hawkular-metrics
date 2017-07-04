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

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import static org.hawkular.metrics.api.jaxrs.filter.TenantFilter.TENANT_HEADER_NAME;
import static org.hawkular.metrics.api.jaxrs.util.ApiUtils.badRequest;
import static org.hawkular.metrics.api.jaxrs.util.ApiUtils.emptyPayload;
import static org.hawkular.metrics.api.jaxrs.util.ApiUtils.serverError;
import static org.hawkular.metrics.model.MetricType.AVAILABILITY;
import static org.hawkular.metrics.model.MetricType.COUNTER;
import static org.hawkular.metrics.model.MetricType.COUNTER_RATE;
import static org.hawkular.metrics.model.MetricType.GAUGE;
import static org.hawkular.metrics.model.MetricType.GAUGE_RATE;
import static org.hawkular.metrics.model.MetricType.STRING;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.hawkular.metrics.api.jaxrs.StatsQueryRequest;
import org.hawkular.metrics.api.jaxrs.handler.observer.MetricCreatedObserver;
import org.hawkular.metrics.api.jaxrs.param.DurationConverter;
import org.hawkular.metrics.api.jaxrs.param.PercentilesConverter;
import org.hawkular.metrics.api.jaxrs.util.ApiUtils;
import org.hawkular.metrics.api.jaxrs.util.Logged;
import org.hawkular.metrics.api.jaxrs.util.MetricTypeTextConverter;
import org.hawkular.metrics.core.service.Functions;
import org.hawkular.metrics.core.service.MetricsService;
import org.hawkular.metrics.core.service.transformers.MinMaxTimestampTransformer;
import org.hawkular.metrics.model.ApiError;
import org.hawkular.metrics.model.AvailabilityType;
import org.hawkular.metrics.model.BucketPoint;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.MetricType;
import org.hawkular.metrics.model.MixedMetricsRequest;
import org.hawkular.metrics.model.Percentile;
import org.hawkular.metrics.model.param.BucketConfig;
import org.hawkular.metrics.model.param.Duration;
import org.hawkular.metrics.model.param.Tags;
import org.hawkular.metrics.model.param.TimeRange;
import org.jboss.resteasy.annotations.GZIP;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import rx.Observable;


/**
 * Interface to deal with metrics
 *
 * @author Heiko W. Rupp
 */
@Path("/{dual_path:metrics|m}")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
@GZIP
@Api(tags = "Metric")
@ApplicationScoped
@Logged
public class MetricHandler {
    @Inject
    private MetricsService metricsService;

    @Context
    private HttpHeaders httpHeaders;

    private String getTenant() {
        return httpHeaders.getRequestHeaders().getFirst(TENANT_HEADER_NAME);
    }

    @POST
    @Path("/")
    @ApiOperation(value = "Create metric.", notes = "Clients are not required to explicitly create "
            + "a metric before storing data. Doing so however allows clients to prevent naming collisions and to "
            + "specify tags and data retention.")
    @ApiResponses(value = {
            @ApiResponse(code = 201, message = "Metric created successfully"),
            @ApiResponse(code = 400, message = "Missing or invalid payload", response = ApiError.class),
            @ApiResponse(code = 409, message = "Metric with given id already exists",
                    response = ApiError.class),
            @ApiResponse(code = 500, message = "Metric creation failed due to an unexpected error",
                    response = ApiError.class)
    })
    public <T> void createMetric(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(required = true) Metric<T> metric,
            @ApiParam(value = "Overwrite previously created metric if it exists. Defaults to false.",
                    required = false) @DefaultValue("false") @QueryParam("overwrite") Boolean overwrite,
            @Context UriInfo uriInfo
    ) {
        if (metric.getType() == null || !metric.getType().isUserType()) {
            asyncResponse.resume(badRequest(new ApiError("Metric type is invalid")));
        }
        MetricId<T> id = new MetricId<>(getTenant(), metric.getMetricId().getType(), metric.getId());
        metric = new Metric<>(id, metric.getTags(), metric.getDataRetention());
        URI location = uriInfo.getBaseUriBuilder().path("/{type}/{id}").build(MetricTypeTextConverter.getLongForm(id
                .getType()), id.getName());
        metricsService.createMetric(metric, overwrite).subscribe(new MetricCreatedObserver(asyncResponse, location));
    }

    @GET
    @Path("/tags")
    @ApiOperation(value = "Retrieve available tag names", response = List.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Tags successfully retrieved."),
            @ApiResponse(code = 204, message = "No tags were found"),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching tags.",
                    response = ApiError.class)
    })
    public <T> void getTagNames(@Suspended final AsyncResponse asyncResponse,
                            @ApiParam(value = "Tag name regexp filter") @QueryParam("filter") String tagNameFilter,
                            @ApiParam(value = "Tags applied to defined metric type", allowableValues = "gauge, " +
                                    "availability, counter, string") @QueryParam("type") MetricType<T> metricType) {
        metricsService.getTagNames(getTenant(), metricType, tagNameFilter)
                .toList()
                .map(ApiUtils::collectionToResponse)
                .subscribe(asyncResponse::resume, t -> asyncResponse.resume(ApiUtils.serverError(t)));
    }

    @GET
    @Path("/tags/{tags}")
    @ApiOperation(value = "Retrieve metrics' tag values", response = Map.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Tags successfully retrieved."),
            @ApiResponse(code = 204, message = "No matching tags were found"),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching tags.",
                    response = ApiError.class)
    })
    public <T> void getTags(@Suspended final AsyncResponse asyncResponse,
                            @ApiParam(value = "Queried metric type", allowableValues = "gauge, availability, counter," +
                                    " string")
                            @QueryParam("type") MetricType<T> metricType,
                            @ApiParam("Tag query") @PathParam("tags") Tags tags) {
        metricsService.getTagValues(getTenant(), metricType, tags.getTags())
                .map(ApiUtils::mapToResponse)
                .subscribe(asyncResponse::resume, t -> asyncResponse.resume(ApiUtils.serverError(t)));
    }

    @GET
    @Path("/")
    @ApiOperation(value = "Find tenant's metric definitions.", notes = "Does not include any metric values. ",
                    response = Metric.class, responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved at least one metric definition."),
            @ApiResponse(code = 204, message = "No metrics found."),
            @ApiResponse(code = 400, message = "Invalid type parameter type.", response = ApiError.class),
            @ApiResponse(code = 500, message = "Failed to retrieve metrics due to unexpected error.",
                    response = ApiError.class)
    })
    public <T> void findMetrics(
            @Suspended
            AsyncResponse asyncResponse,
            @ApiParam(value = "Queried metric type", required = false, allowableValues = "gauge, availability, " +
                    "counter, string")
            @QueryParam("type") MetricType<T> metricType,
            @ApiParam(value = "Fetch min and max timestamps of available datapoints") @DefaultValue("false")
            @QueryParam("timestamps") Boolean fetchTimestamps,
            @ApiParam(value = "List of tags filters", required = false) @QueryParam("tags") String tags,
            @ApiParam(value = "Regexp to match metricId if tags filtering is used, otherwise exact matching",
                    required = false) @QueryParam("id") String id) {
        if (metricType != null && !metricType.isUserType()) {
            asyncResponse.resume(badRequest(new ApiError("Incorrect type param " + metricType.toString())));
            return;
        }

        Observable<Metric<T>> metricObservable;

        if (tags != null) {
            metricObservable = metricsService.findMetricIdentifiersWithFilters(getTenant(), metricType, tags)
                    .filter(metricsService.idFilter(id))
                    .flatMap(metricsService::findMetric);
        } else {
            if(!Strings.isNullOrEmpty(id)) {
                // HWKMETRICS-461
                if(metricType == null) {
                    asyncResponse.resume(badRequest(new ApiError("Exact id search requires type to be set")));
                    return;
                }
                String[] ids = id.split("\\|");
                metricObservable = Observable.from(ids)
                        .map(idPart -> new MetricId<>(getTenant(), metricType, idPart))
                        .flatMap(metricsService::findMetric);
            } else {
                metricObservable = metricsService.findMetrics(getTenant(), metricType);
            }
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

    @Deprecated
    @POST
    @Path("/data")
    @ApiOperation(value = "Deprecated. Please use /raw endpoint.")
    public void deprecatedAddMetricsData(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "List of metrics", required = true) MixedMetricsRequest metricsRequest) {
        addMetricsData(asyncResponse, metricsRequest);
    }

    @POST
    @Path("/raw")
    @ApiOperation(value = "Add data points for multiple metrics in a single call.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Adding data points succeeded."),
            @ApiResponse(code = 400, message = "Missing or invalid payload.", response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error happened while storing the data",
                    response = ApiError.class)
    })
    public void addMetricsData(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "List of metrics", required = true) MixedMetricsRequest metricsRequest
    ) {
        if (metricsRequest.isEmpty()) {
            asyncResponse.resume(emptyPayload());
            return;
        }

        Observable<Metric<Double>> gauges =
                Functions.metricToObservable(getTenant(), metricsRequest.getGauges(), GAUGE);
        Observable<Metric<AvailabilityType>> availabilities = Functions.metricToObservable(getTenant(),
                metricsRequest.getAvailabilities(), AVAILABILITY);
        Observable<Metric<Long>> counters = Functions.metricToObservable(getTenant(), metricsRequest.getCounters(),
                COUNTER);
        Observable<Metric<String>> strings = Functions.metricToObservable(getTenant(), metricsRequest.getStrings(),
                STRING);

        metricsService.addDataPoints(GAUGE, gauges)
                .mergeWith(metricsService.addDataPoints(AVAILABILITY, availabilities))
                .mergeWith(metricsService.addDataPoints(COUNTER, counters))
                .mergeWith(metricsService.addDataPoints(STRING, strings))
                .subscribe(
                        aVoid -> {
                        },
                        t -> asyncResponse.resume(serverError(t)),
                        () -> asyncResponse.resume(Response.ok().build())
                );
    }

    @POST
    @Path("/stats/query")
    public void findStats(@Suspended AsyncResponse asyncResponse, StatsQueryRequest query) {
        try {
            checkRequiredParams(query);
            doStatsQuery(query)
                    .map(ApiUtils::mapToResponse)
                    .subscribe(asyncResponse::resume, t -> asyncResponse.resume(ApiUtils.error(t)));
        } catch (IllegalArgumentException e) {
            asyncResponse.resume(badRequest(new ApiError(e.getMessage())));
        }
    }

    @POST
    @Path("/stats/batch/query")
    @SuppressWarnings("unchecked")
    public void findStatsBatched(@Suspended AsyncResponse asyncResponse, Map<String, StatsQueryRequest> queries) {
        try {
            queries.values().forEach(this::checkRequiredParams);
            Map<String, Observable<Map<String, Map<String, List<? extends BucketPoint>>>>> results = new HashMap<>();
            queries.entrySet().forEach(entry -> results.put(entry.getKey(), doStatsQuery(entry.getValue())));
            Observable.from(results.entrySet())
                    .flatMap(entry -> entry.getValue()
                            .map(map -> ImmutableMap.of(entry.getKey(), map)))
                    .collect(HashMap::new, HashMap::putAll)
                    .map(ApiUtils::mapToResponse)
                    .subscribe(asyncResponse::resume, t -> asyncResponse.resume(ApiUtils.error(t)));
        } catch (IllegalArgumentException e) {
            asyncResponse.resume(badRequest(new ApiError(e.getMessage())));
        }
    }

    private Observable<Map<String, Map<String, List<? extends BucketPoint>>>> doStatsQuery(StatsQueryRequest query) {
        Duration duration;
        if (query.getBucketDuration() == null) {
            duration = null;
        } else {
            duration = new DurationConverter().fromString(query.getBucketDuration());
        }
        TimeRange timeRange = new TimeRange(query.getStart(), query.getEnd());
        BucketConfig bucketsConfig = new BucketConfig(query.getBuckets(), duration, timeRange);

        List<Percentile> percentiles;
        if (query.getPercentiles() == null) {
            percentiles = emptyList();
        } else {
            percentiles = new PercentilesConverter().fromString(query.getPercentiles()).getPercentiles();
        }

        List<MetricType<?>> types = emptyList();
        if (query.getTypes() != null) {
            types = query.getTypes().stream().map(MetricType::fromTextCode).collect(Collectors.toList());
        }

        Observable<Map<String, List<? extends BucketPoint>>> gaugeStats = Observable.just(emptyMap());
        Observable<Map<String, List<? extends BucketPoint>>> counterStats = Observable.just(emptyMap());
        Observable<Map<String, List<? extends BucketPoint>>> availabilityStats = Observable.just(emptyMap());
        Observable<Map<String, List<? extends BucketPoint>>> gaugeRateStats = Observable.just(emptyMap());
        Observable<Map<String, List<? extends BucketPoint>>> counterRateStats = Observable.just(emptyMap());

        // TODO Eliminate duplicate queries when fetching and rates
        // When we return gauge and gauge rates and/or counter and counter rate data points we are doing extra queries
        // that can be avoided. The code below can be optimized such that we fetch gauge/counter raw data, and then
        // reuse that observable to compute stats and rate stats.

        if (!query.getMetrics().isEmpty() && (query.getMetrics().containsKey(GAUGE.getText()) ||
                query.getMetrics().containsKey(COUNTER.getText()) ||
                query.getMetrics().containsKey(AVAILABILITY.getText())
        )) {
            if (!isMetricsEmpty(query, GAUGE)) {
                if (types.isEmpty()) {
                    gaugeStats = getGaugeStats(getMetricIds(query, GAUGE), bucketsConfig, percentiles);
                } else if (types.contains(GAUGE_RATE)) {
                    if (types.contains(GAUGE)) {
                        gaugeStats = getGaugeStats(query, bucketsConfig, percentiles);
                        gaugeRateStats = getRateStats(getMetricIds(query, GAUGE), bucketsConfig, percentiles);
                    } else {
                        gaugeRateStats = getRateStats(getMetricIds(query, GAUGE), bucketsConfig, percentiles);
                    }
                } else {
                    gaugeStats = getGaugeStats(getMetricIds(query, GAUGE), bucketsConfig, percentiles);
                }
            }

            if (!isMetricsEmpty(query, COUNTER)) {
                if (types.isEmpty()) {
                    counterStats = getCounterStats(getMetricIds(query, COUNTER), bucketsConfig, percentiles);
                } else if (types.contains(COUNTER_RATE)) {
                    if (types.contains(COUNTER)) {
                        counterStats = getCounterStats(getMetricIds(query, COUNTER), bucketsConfig, percentiles);
                        counterRateStats = getRateStats(getMetricIds(query, COUNTER), bucketsConfig, percentiles);
                    } else {
                        counterRateStats = getRateStats(getMetricIds(query, COUNTER), bucketsConfig, percentiles);
                    }
                } else {
                    counterStats = getCounterStats(query, bucketsConfig, percentiles);
                }
            }

            if (!isMetricsEmpty(query, AVAILABILITY)) {
                availabilityStats = Observable.from(query.getMetrics().get("availability"))
                        .flatMap(id -> metricsService.findAvailabilityStats(new MetricId<>(getTenant(), AVAILABILITY,
                                id), timeRange.getStart(), timeRange.getEnd(), bucketsConfig.getBuckets())
                                .map(bucketPoints -> new NamedBucketPoints<>(id, bucketPoints)))
                        .collect(HashMap::new, (statsMap, namedBucketPoints) -> statsMap.put(namedBucketPoints.id,
                                namedBucketPoints.bucketPoints));
            }
        } else {
            Observable<MetricId<Double>> gauges;
            Observable<MetricId<Long>> counters;

            if (types.isEmpty()) {
                gaugeStats = getGaugeStatsFromTags(bucketsConfig, percentiles, query.getTags());
                counterStats = getCounterStatsFromTags(bucketsConfig, percentiles, query.getTags());
                availabilityStats = getAvailabilityStatsFromTags(bucketsConfig, query.getTags());
            } else {
                if (types.contains(GAUGE) && types.contains(GAUGE_RATE)) {
                    gauges = metricsService.findMetricIdentifiersWithFilters(getTenant(), GAUGE, query.getTags()).cache();
                    gaugeStats = gauges.flatMap(gauge ->
                            metricsService.findGaugeStats(gauge, bucketsConfig, percentiles)
                            .map(bucketPoints -> new NamedBucketPoints<>(gauge.getName(),
                                            bucketPoints)))
                            .collect(HashMap::new, (statsMap, namedBucketPoints) ->
                                    statsMap.put(namedBucketPoints.id, namedBucketPoints.bucketPoints));
                    gaugeRateStats = getRateStats(gauges, bucketsConfig, percentiles);
                } else if (types.contains(GAUGE)) {
                    gaugeStats = getGaugeStatsFromTags(bucketsConfig, percentiles, query.getTags());
                } else {
                    gauges = metricsService.findMetricIdentifiersWithFilters(getTenant(), GAUGE, query.getTags());
                    gaugeRateStats = getRateStats(gauges, bucketsConfig, percentiles);
                }

                if (types.contains(COUNTER) && types.contains(COUNTER_RATE)) {
                    counters = metricsService.findMetricIdentifiersWithFilters(getTenant(), COUNTER, query.getTags())
                            .cache();
                    counterStats = counters.flatMap(counter ->
                            metricsService.findCounterStats(counter, bucketsConfig, percentiles)
                                    .map(bucketPoints -> new NamedBucketPoints<>(counter.getName(),
                                            bucketPoints)))
                            .collect(HashMap::new, (statsMap, namedBucketPoints) ->
                                    statsMap.put(namedBucketPoints.id, namedBucketPoints.bucketPoints));
                    counterRateStats = getRateStats(counters, bucketsConfig, percentiles);
                } else if (types.contains(COUNTER)) {
                    counterStats = getCounterStatsFromTags(bucketsConfig, percentiles, query.getTags());
                } else {
                    counters = metricsService.findMetricIdentifiersWithFilters(getTenant(), COUNTER, query.getTags());
                    counterRateStats = getRateStats(counters, bucketsConfig, percentiles);
                }

                if (types.contains(AVAILABILITY)) {
                    availabilityStats = getAvailabilityStatsFromTags(bucketsConfig, query.getTags());
                }
            }
        }

        return Observable.zip(gaugeStats, counterStats, availabilityStats, gaugeRateStats, counterRateStats,
                (gaugeMap, counterMap, availabiltyMap, gaugeRateMap, counterRateMap) -> {
                    Map<String, Map<String, List<? extends BucketPoint>>> stats = new HashMap<>();
                    if (!gaugeMap.isEmpty()) {
                        stats.put(GAUGE.getText(), gaugeMap);
                    }
                    if (!counterMap.isEmpty()) {
                        stats.put(COUNTER.getText(), counterMap);
                    }
                    if (!availabiltyMap.isEmpty()) {
                        stats.put(AVAILABILITY.getText(), availabiltyMap);
                    }
                    if (!gaugeRateMap.isEmpty()) {
                        stats.put(GAUGE_RATE.getText(), gaugeRateMap);
                    }
                    if (!counterRateMap.isEmpty()) {
                        stats.put(COUNTER_RATE.getText(), counterRateMap);
                    }
                    return stats;
                })
                .first();
    }

    private void checkRequiredParams(StatsQueryRequest query) {
        if (isMetricIdsEmpty(query) && query.getTags() == null) {
            throw new IllegalArgumentException("Either the metrics or the tags property must be set");
        }
        if (query.getBuckets() == null && query.getBucketDuration() == null) {
            throw new IllegalArgumentException("Either the buckets or bucketDuration property must be set");
        }
    }

    private Observable<Map<String, List<? extends BucketPoint>>> getCounterStatsFromTags(BucketConfig bucketsConfig,
            List<Percentile> percentiles, String tags) {
        Observable<MetricId<Long>> counters = metricsService.findMetricIdentifiersWithFilters(getTenant(), COUNTER,
                tags);
        return counters.flatMap(counter ->
                metricsService.findCounterStats(counter, bucketsConfig, percentiles)
                        .map(bucketPoints -> new NamedBucketPoints<>(counter.getName(),
                                bucketPoints)))
                .collect(HashMap::new, (statsMap, namedBucketPoints) ->
                        statsMap.put(namedBucketPoints.id, namedBucketPoints.bucketPoints));
    }

    private Observable<Map<String, List<? extends BucketPoint>>> getGaugeStatsFromTags(BucketConfig bucketsConfig,
            List<Percentile> percentiles, String tags) {
        Observable<MetricId<Double>> gauges = metricsService.findMetricIdentifiersWithFilters(getTenant(), GAUGE, tags);
        return gauges.flatMap(gauge ->
                metricsService.findGaugeStats(gauge, bucketsConfig, percentiles)
                .map(bucketPoints -> new NamedBucketPoints<>(gauge.getName(),
                                bucketPoints)))
                .collect(HashMap::new, (statsMap, namedBucketPoints) ->
                        statsMap.put(namedBucketPoints.id, namedBucketPoints.bucketPoints));
    }

    private Observable<Map<String, List<? extends BucketPoint>>> getAvailabilityStatsFromTags(
            BucketConfig bucketsConfig, String tags) {
        Observable<MetricId<AvailabilityType>> availabilities = metricsService.findMetricIdentifiersWithFilters
                (getTenant(), AVAILABILITY, tags);
        Observable<Map<String, List<? extends BucketPoint>>> availabilityStats;
        availabilityStats = availabilities.flatMap(availability -> metricsService.findAvailabilityStats(
                availability, bucketsConfig.getTimeRange().getStart(),
                bucketsConfig.getTimeRange().getEnd(), bucketsConfig.getBuckets())
                .map(bucketPoints -> new NamedBucketPoints<>(availability.getName(),
                        bucketPoints)))
                .collect(HashMap::new, (statsMap, namedBucketPoints) ->
                        statsMap.put(namedBucketPoints.id, namedBucketPoints.bucketPoints));
        return availabilityStats;
    }

    private Observable<Map<String, List<? extends BucketPoint>>> getGaugeStats(StatsQueryRequest query,
            BucketConfig bucketsConfig, List<Percentile> percentiles) {
        Observable<Map<String, List<? extends BucketPoint>>> gaugeStats;
        gaugeStats = Observable.from(query.getMetrics().get("gauge"))
                .flatMap(id -> metricsService.findGaugeStats(new MetricId<>(getTenant(), GAUGE, id),
                        bucketsConfig, percentiles)
                        .map(bucketPoints -> new NamedBucketPoints<>(id, bucketPoints)))
                .collect(HashMap::new, (statsMap, namedBucketPoints) ->
                        statsMap.put(namedBucketPoints.id, namedBucketPoints.bucketPoints));
        return gaugeStats;
    }

    private Observable<Map<String, List<? extends BucketPoint>>> getGaugeStats(Observable<MetricId<Double>> ids,
            BucketConfig bucketConfig, List<Percentile> percentiles) {
        return ids.flatMap(id -> metricsService.findGaugeStats(id, bucketConfig, percentiles)
                .map(bucketPoints -> new NamedBucketPoints<>(id.getName(), bucketPoints)))
                .collect(HashMap::new, (statsMap, namedBucketPoints) ->
                        statsMap.put(namedBucketPoints.id, namedBucketPoints.bucketPoints));
    }

    private Observable<Map<String, List<? extends BucketPoint>>> getCounterStats(Observable<MetricId<Long>> ids,
            BucketConfig bucketConfig, List<Percentile> percentiles) {
        return ids.flatMap(id -> metricsService.findCounterStats(id, bucketConfig, percentiles)
                .map(bucketPoints -> new NamedBucketPoints<>(id.getName(), bucketPoints)))
                .collect(HashMap::new, (statsMap, namedBucketPoints) ->
                        statsMap.put(namedBucketPoints.id, namedBucketPoints.bucketPoints));
    }

    private Observable<Map<String, List<? extends BucketPoint>>> getCounterStats(StatsQueryRequest query,
            BucketConfig bucketsConfig, List<Percentile> percentiles) {
        Observable<Map<String, List<? extends BucketPoint>>> counterStats;
        counterStats = Observable.from(query.getMetrics().get("counter"))
                .flatMap(id -> metricsService.findCounterStats(new MetricId<>(getTenant(), COUNTER, id),
                        bucketsConfig, percentiles)
                        .map(bucketPoints -> new NamedBucketPoints<>(id, bucketPoints)))
                .collect(HashMap::new, (statsMap, namedBucketPoints) -> statsMap.put(namedBucketPoints.id,
                        namedBucketPoints.bucketPoints));
        return counterStats;
    }

    private <T> Observable<MetricId<T>> getMetricIds(StatsQueryRequest query, MetricType<T> type) {
        return Observable.from(query.getMetrics().get(type.getText())).map(id -> new MetricId<>(getTenant(), type, id));
    }

    private <T extends Number> Observable<Map<String, List<? extends BucketPoint>>> getRateStats(
            Observable<MetricId<T>> ids, BucketConfig bucketConfig, List<Percentile> percentiles) {
        return ids.flatMap(id -> metricsService.findRateStats(id, bucketConfig, percentiles)
                .map(bucketPoints -> new NamedBucketPoints<>(id.getName(), bucketPoints)))
                .collect(HashMap::new, (statsMap, namedBucketPoints) -> statsMap.put(namedBucketPoints.id,
                        namedBucketPoints.bucketPoints));
    }

    private <T> boolean isMetricsEmpty(StatsQueryRequest query, MetricType<T> type) {
        return query.getMetrics().get(type.getText()) == null || query.getMetrics().get(type.getText()).isEmpty();
    }

    private boolean isMetricIdsEmpty(StatsQueryRequest query) {
        if (query.getMetrics().isEmpty()) {
            return true;
        }
        return query.getMetrics().getOrDefault("gauge", emptyList()).isEmpty() &&
                query.getMetrics().getOrDefault("counter", emptyList()).isEmpty() &&
                query.getMetrics().getOrDefault("availability", emptyList()).isEmpty();
    }

    private class NamedBucketPoints<T extends BucketPoint> {
        public String id;
        public List<T> bucketPoints;

        public NamedBucketPoints(String id, List<T> bucketPoints) {
            this.id = id;
            this.bucketPoints = bucketPoints;
        }
    }

}
