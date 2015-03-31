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

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import static org.hawkular.metrics.api.jaxrs.util.ResponseUtils.MAP_COLLECTION;
import static org.hawkular.metrics.api.jaxrs.util.ResponseUtils.MAP_VALUE;
import static org.hawkular.metrics.api.jaxrs.util.ResponseUtils.MAP_VOID;
import static org.hawkular.metrics.api.jaxrs.util.ResponseUtils.alreadyExistsFallback;
import static org.hawkular.metrics.api.jaxrs.util.ResponseUtils.badRequest;
import static org.hawkular.metrics.api.jaxrs.util.ResponseUtils.created;
import static org.hawkular.metrics.api.jaxrs.util.ResponseUtils.emptyPayload;
import static org.hawkular.metrics.core.api.MetricsService.DEFAULT_TENANT_ID;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.Futures.withFallback;

import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.hawkular.metrics.api.jaxrs.param.Duration;
import org.hawkular.metrics.api.jaxrs.param.Tags;
import org.hawkular.metrics.api.jaxrs.service.MetricServiceBase;
import org.hawkular.metrics.api.jaxrs.util.ResponseUtils;
import org.hawkular.metrics.core.api.Availability;
import org.hawkular.metrics.core.api.AvailabilityBucketDataPoint;
import org.hawkular.metrics.core.api.AvailabilityMetric;
import org.hawkular.metrics.core.api.BucketedOutput;
import org.hawkular.metrics.core.api.Buckets;
import org.hawkular.metrics.core.api.Counter;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricAlreadyExistsException;
import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.MetricType;
import org.hawkular.metrics.core.api.MetricsService;
import org.hawkular.metrics.core.api.NumericBucketDataPoint;
import org.hawkular.metrics.core.api.NumericData;
import org.hawkular.metrics.core.api.NumericMetric;
import org.hawkular.metrics.core.impl.request.TagRequest;

import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureFallback;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * @author Heiko W. Rupp
 * @author John Sanda
 * @author Stefan Negrea
 * @author Thomas Segismont
 */
@ApplicationScoped
public class MetricHandler extends MetricServiceBase {
    private static final long EIGHT_HOURS = MILLISECONDS.convert(8, HOURS);

    @Inject
    private MetricsService metricsService;

    @Override
    protected ListenableFuture<Response> _createNumericMetric(String tenantId, NumericMetric metric, UriInfo uriInfo) {
        if (metric == null) {
            return emptyPayload();
        }
        metric.setTenantId(tenantId);
        ListenableFuture<Void> insertFuture = metricsService.createMetric(metric);
        URI location = uriInfo.getBaseUriBuilder()
                              .path("/{tenantId}/metrics/numeric/{id}")
                              .build(tenantId, metric.getId().getName());
        ListenableFuture<Response> responseFuture = transform(insertFuture, created(location));
        FutureFallback<Response> fallback = alreadyExistsFallback(
                MetricAlreadyExistsException.class,
                MetricHandler::alreadyExistsError
        );
        return withFallback(responseFuture, fallback);
    }

    private static ApiError alreadyExistsError(MetricAlreadyExistsException e) {
        return new ApiError("A metric with id " + e.getMetric().getId() + " already exists");
    }

    @Override
    protected ListenableFuture<Response> _createAvailabilityMetric(
            String tenantId,
            AvailabilityMetric metric,
            UriInfo uriInfo
    ) {
        if (metric == null) {
            return emptyPayload();
        }
        metric.setTenantId(tenantId);
        ListenableFuture<Void> insertFuture = metricsService.createMetric(metric);
        URI location = uriInfo.getBaseUriBuilder()
                              .path("/{tenantId}/metrics/availability/{id}")
                              .build(tenantId, metric.getId().getName());
        ListenableFuture<Response> responseFuture = transform(insertFuture, created(location));
        FutureFallback<Response> fallback = alreadyExistsFallback(
                MetricAlreadyExistsException.class,
                MetricHandler::alreadyExistsError
        );
        return withFallback(responseFuture, fallback);
    }

    @Override
    protected ListenableFuture<Response> _getNumericMetricTags(String tenantId, String id) {
        ListenableFuture<Optional<Metric<?>>> future;
        future = metricsService.findMetric(tenantId, MetricType.NUMERIC, new MetricId(id));
        return transform(future, ResponseUtils.MAP_VALUE);
    }

    @Override
    protected ListenableFuture<Response> _updateNumericMetricTags(
            String tenantId, String id, Map<String, String> tags
    ) {
        NumericMetric metric = new NumericMetric(tenantId, new MetricId(id));
        ListenableFuture<Void> future = metricsService.addTags(metric, tags);
        return transform(future, ResponseUtils.MAP_VOID);
    }

    @Override
    protected ListenableFuture<Response> _deleteNumericMetricTags(String tenantId, String id, Tags tags) {
        NumericMetric metric = new NumericMetric(tenantId, new MetricId(id));
        ListenableFuture<Void> future = metricsService.deleteTags(metric, tags.getTags());
        return transform(future, ResponseUtils.MAP_VOID);
    }

    @Override
    protected ListenableFuture<Response> _getAvailabilityMetricTags(String tenantId, String id) {
        ListenableFuture<Optional<Metric<?>>> future;
        future = metricsService.findMetric(tenantId, MetricType.AVAILABILITY, new MetricId(id));
        return transform(future, ResponseUtils.MAP_VALUE);
    }

    @Override
    protected ListenableFuture<Response> _updateAvailabilityMetricTags(
            String tenantId, String id, Map<String, String> tags
    ) {
        AvailabilityMetric metric = new AvailabilityMetric(tenantId, new MetricId(id));
        ListenableFuture<Void> future = metricsService.addTags(metric, tags);
        return transform(future, ResponseUtils.MAP_VOID);
    }

    @Override
    protected ListenableFuture<Response> _deleteAvailabilityMetricTags(String tenantId, String id, Tags tags) {
        AvailabilityMetric metric = new AvailabilityMetric(tenantId, new MetricId(id));
        ListenableFuture<Void> future = metricsService.deleteTags(metric, tags.getTags());
        return transform(future, ResponseUtils.MAP_VOID);
    }

    @Override
    protected ListenableFuture<Response> _addDataForMetric(String tenantId, String id, List<NumericData> data) {
        if (data == null) {
            return emptyPayload();
        }
        NumericMetric metric = new NumericMetric(tenantId, new MetricId(id));
        metric.getData().addAll(data);
        ListenableFuture<Void> future = metricsService.addNumericData(Collections.singletonList(metric));
        return transform(future, ResponseUtils.MAP_VOID);
    }

    @Override
    protected ListenableFuture<Response> _addAvailabilityForMetric(
            String tenantId, String id, List<Availability> data
    ) {
        if (data == null) {
            return emptyPayload();
        }
        AvailabilityMetric metric = new AvailabilityMetric(tenantId, new MetricId(id));
        metric.getData().addAll(data);
        ListenableFuture<Void> future = metricsService.addAvailabilityData(Collections.singletonList(metric));
        return transform(future, ResponseUtils.MAP_VOID);
    }

    @Override
    protected ListenableFuture<Response> _addNumericData(String tenantId, List<NumericMetric> metrics) {
        if (metrics.isEmpty()) {
            return immediateFuture(Response.ok().build());
        }
        metrics.forEach(m -> m.setTenantId(tenantId));
        ListenableFuture<Void> future = metricsService.addNumericData(metrics);
        return transform(future, ResponseUtils.MAP_VOID);
    }

    @Override
    protected ListenableFuture<Response> _addAvailabilityData(String tenantId, List<AvailabilityMetric> metrics) {
        if (metrics.isEmpty()) {
            return immediateFuture(Response.ok().build());
        }
        metrics.forEach(m -> m.setTenantId(tenantId));
        ListenableFuture<Void> future = metricsService.addAvailabilityData(metrics);
        return transform(future, ResponseUtils.MAP_VOID);
    }

    @Override
    protected ListenableFuture<Response> _findNumericDataByTags(String tenantId, Tags tags) {
        if (tags == null) {
            return badRequest(new ApiError("Missing tags query"));
        }
        ListenableFuture<Map<MetricId, Set<NumericData>>> future;
        future = metricsService.findNumericDataByTags(tenantId, tags.getTags());
        return transform(future, ResponseUtils.MAP_MAP);
    }

    @Override
    protected ListenableFuture<Response> _findAvailabilityDataByTags(String tenantId, Tags tags) {
        if (tags == null) {
            return badRequest(new ApiError("Missing tags query"));
        }
        ListenableFuture<Map<MetricId, Set<Availability>>> future;
        future = metricsService.findAvailabilityByTags(tenantId, tags.getTags());
        return transform(future, ResponseUtils.MAP_MAP);
    }

    @Override
    protected ListenableFuture<Response> _findNumericData(
            String tenantId, String id, Long start, Long end, Integer bucketsCount, Duration bucketDuration
    ) {
        long now = System.currentTimeMillis();
        long startTime = start == null ? now - EIGHT_HOURS : start;
        long endTime = end == null ? now : end;

        NumericMetric metric = new NumericMetric(tenantId, new MetricId(id));

        if (bucketsCount == null && bucketDuration == null) {
            ListenableFuture<List<NumericData>> dataFuture = metricsService.findNumericData(
                    tenantId,
                    new MetricId(id), startTime, endTime
            );
            return transform(dataFuture, ResponseUtils.MAP_COLLECTION);
        }

        if (bucketsCount != null && bucketDuration != null) {
            return badRequest(new ApiError("Both buckets and bucketDuration parameters are used"));
        }

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

        ListenableFuture<BucketedOutput<NumericBucketDataPoint>> dataFuture;
        dataFuture = metricsService.findNumericStats(metric, startTime, endTime, buckets);

        ListenableFuture<List<NumericBucketDataPoint>> outputFuture;
        outputFuture = transform(dataFuture, BucketedOutput<NumericBucketDataPoint>::getData);

        return transform(outputFuture, ResponseUtils.MAP_COLLECTION);
    }

    @Override
    protected ListenableFuture<Response> _findPeriods(
            String tenantId, String id, Long start, Long end, double threshold, String operator
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
        switch (operator) {
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
                    new ApiError(
                            "Invalid value for op parameter. Supported values are lt, " +
                            "lte, eq, gt, gte."
                    )
            );
        } else {
            ListenableFuture<List<long[]>> future = metricsService.getPeriods(
                    tenantId, new MetricId(id), predicate,
                    startTime, endTime
            );
            return transform(future, ResponseUtils.MAP_COLLECTION);
        }
    }

    @Override
    protected ListenableFuture<Response> _findAvailabilityData(
            String tenantId, String id, Long start, Long end, Integer bucketsCount, Duration bucketDuration
    ) {
        long now = System.currentTimeMillis();
        Long startTime = start == null ? now - EIGHT_HOURS : start;
        Long endTime = end == null ? now : end;

        AvailabilityMetric metric = new AvailabilityMetric(tenantId, new MetricId(id));

        if (bucketsCount == null && bucketDuration == null) {
            ListenableFuture<List<Availability>> dataFuture = metricsService.findAvailabilityData(
                    tenantId,
                    metric.getId(), startTime, endTime
            );
            return transform(dataFuture, ResponseUtils.MAP_COLLECTION);
        }

        if (bucketsCount != null && bucketDuration != null) {
            return badRequest(new ApiError("Both buckets and bucketDuration parameters are used"));
        }

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
        ListenableFuture<BucketedOutput<AvailabilityBucketDataPoint>> dataFuture;
        dataFuture = metricsService.findAvailabilityStats(metric, startTime, endTime, buckets);

        ListenableFuture<List<AvailabilityBucketDataPoint>> outputFuture;
        outputFuture = transform(dataFuture, BucketedOutput<AvailabilityBucketDataPoint>::getData);
        return transform(outputFuture, ResponseUtils.MAP_COLLECTION);
    }

    @Override
    protected ListenableFuture<Response> _tagNumericData(String tenantId, String id, TagRequest params) {
        ListenableFuture<List<NumericData>> future;
        NumericMetric metric = new NumericMetric(tenantId, new MetricId(id));
        if (params.getTimestamp() != null) {
            future = metricsService.tagNumericData(metric, params.getTags(), params.getTimestamp());
        } else {
            future = metricsService.tagNumericData(
                    metric,
                    params.getTags(),
                    params.getStart(),
                    params.getEnd()
            );
        }
        return transform(future, (List<NumericData> data) -> Response.ok().build());
    }

    @Override
    protected ListenableFuture<Response> _tagAvailabilityData(String tenantId, String id, TagRequest params) {
        ListenableFuture<List<Availability>> future;
        AvailabilityMetric metric = new AvailabilityMetric(tenantId, new MetricId(id));
        if (params.getTimestamp() != null) {
            future = metricsService.tagAvailabilityData(metric, params.getTags(), params.getTimestamp());
        } else {
            future = metricsService.tagAvailabilityData(
                    metric, params.getTags(), params.getStart(),
                    params.getEnd()
            );
        }
        return transform(future, (List<Availability> data) -> Response.ok().build());
    }

    @Override
    protected ListenableFuture<Response> _findTaggedNumericData(String tenantId, Tags tags) {
        ListenableFuture<Map<MetricId, Set<NumericData>>> queryFuture;
        queryFuture = metricsService.findNumericDataByTags(tenantId, tags.getTags());
        ListenableFuture<Map<String, Set<NumericData>>> resultFuture = transform(
                queryFuture,
                new Function<Map<MetricId, Set<NumericData>>, Map<String, Set<NumericData>>>() {
                    @Override
                    public Map<String, Set<NumericData>> apply(Map<MetricId, Set<NumericData>> input) {
                        Map<String, Set<NumericData>> result = new HashMap<>(input.size());
                        for (Map.Entry<MetricId, Set<NumericData>> entry : input.entrySet()) {
                            result.put(entry.getKey().getName(), entry.getValue());
                        }
                        return result;
                    }
                }
        );
        return transform(resultFuture, ResponseUtils.MAP_MAP);
    }

    @Override
    protected ListenableFuture<Response> _findTaggedAvailabilityData(String tenantId, Tags tags) {
        ListenableFuture<Map<MetricId, Set<Availability>>> future;
        future = metricsService.findAvailabilityByTags(tenantId, tags.getTags());
        return transform(future, ResponseUtils.MAP_MAP);
    }

    @Override
    protected ListenableFuture<Response> _updateCountersForGroups(Collection<Counter> counters) {
        ListenableFuture<Void> future = metricsService.updateCounters(counters);
        return transform(future, MAP_VOID);
    }

    @Override
    protected ListenableFuture<Response> _updateCounterForGroup(String group, Collection<Counter> counters) {
        for (Counter counter : counters) {
            counter.setGroup(group);
        }
        ListenableFuture<Void> future = metricsService.updateCounters(counters);
        return transform(future, MAP_VOID);
    }

    @Override
    protected ListenableFuture<Response> _updateCounter(String group, String counter) {
        return _updateCounter(group, counter, 1L);
    }

    @Override
    protected ListenableFuture<Response> _updateCounter(String group, String counter, Long value) {
        Counter c = new Counter(DEFAULT_TENANT_ID, group, counter, value);
        ListenableFuture<Void> future = metricsService.updateCounter(c);
        return transform(future, MAP_VOID);
    }

    @Override
    protected ListenableFuture<Response> _getCountersForGroup(String group) {
        ListenableFuture<List<Counter>> future = metricsService.findCounters(group);
        return transform(future, MAP_COLLECTION);
    }

    @Override
    protected ListenableFuture<Response> _getCounter(String group, String counter) {
        ListenableFuture<List<Counter>> future = metricsService.findCounters(group, Collections.singletonList(counter));
        ListenableFuture<Optional<Counter>> optionalFuture;
        optionalFuture = transform(future, (List<Counter> l) -> l.isEmpty() ? Optional.empty() : Optional.of(l.get(0)));
        return transform(optionalFuture, MAP_VALUE);
    }

    @Override
    protected ListenableFuture<Response> _findMetrics(String tenantId, String type) {
        MetricType metricType;
        try {
            metricType = MetricType.fromTextCode(type);
        } catch (IllegalArgumentException e) {
            return badRequest(
                    new ApiError("[" + type + "] is not a valid type. Accepted values are num|avail|log")
            );
        }
        ListenableFuture<List<Metric<?>>> future = metricsService.findMetrics(tenantId, metricType);
        return transform(future, ResponseUtils.MAP_COLLECTION);
    }
}
