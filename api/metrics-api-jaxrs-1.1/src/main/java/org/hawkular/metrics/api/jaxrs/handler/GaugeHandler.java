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
import static org.hawkular.metrics.api.jaxrs.util.ApiUtils.noContent;
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
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.hawkular.metrics.api.jaxrs.util.ApiUtils;
import org.hawkular.metrics.core.service.Functions;
import org.hawkular.metrics.core.service.MetricsService;
import org.hawkular.metrics.model.ApiError;
import org.hawkular.metrics.model.Buckets;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.MetricType;
import org.hawkular.metrics.model.exception.MetricAlreadyExistsException;
import org.hawkular.metrics.model.param.BucketConfig;
import org.hawkular.metrics.model.param.Duration;
import org.hawkular.metrics.model.param.Percentiles;
import org.hawkular.metrics.model.param.Tags;
import org.hawkular.metrics.model.param.TimeRange;

import rx.Observable;

/**
 * @author Stefan Negrea
 *
 */
@Path("/gauges")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
public class GaugeHandler {

    @Inject
    private MetricsService metricsService;

    @HeaderParam(TENANT_HEADER_NAME)
    private String tenantId;

    @POST
    @Path("/")
    public Response createGaugeMetric(
            Metric<Double> metric,
            @Context UriInfo uriInfo
    ) {
        if (metric.getType() != null
                && MetricType.UNDEFINED != metric.getType()
                && MetricType.GAUGE != metric.getType()) {
            return badRequest(new ApiError("Metric type does not match " + MetricType
                    .GAUGE.getText()));
        }
        metric = new Metric<>(new MetricId<>(tenantId, GAUGE, metric.getId()),
                metric.getTags(), metric.getDataRetention());
        URI location = uriInfo.getBaseUriBuilder().path("/gauges/{id}").build(metric.getMetricId().getName());
        try {
            Observable<Void> observable = metricsService.createMetric(metric);
            observable.toBlocking().lastOrDefault(null);
            return Response.created(location).build();
        } catch (MetricAlreadyExistsException e) {
            String message = "A metric with name [" + e.getMetric().getMetricId().getName() + "] already exists";
            return Response.status(Response.Status.CONFLICT).entity(new ApiError(message)).build();
        } catch (Exception e) {
            return serverError(e);
        }
    }

    @GET
    @Path("/")
    public Response findGaugeMetrics(
            @QueryParam("tags") Tags tags) {

        Observable<Metric<Double>> metricObservable = (tags == null)
                ? metricsService.findMetrics(tenantId, GAUGE)
                : metricsService.findMetricsWithFilters(tenantId, tags.getTags(), GAUGE);

        try {
            return metricObservable
                    .toList()
                    .map(ApiUtils::collectionToResponse)
                    .toBlocking()
                    .lastOrDefault(null);
        } catch (PatternSyntaxException e) {
            return badRequest(e);
        } catch (Exception e) {
            return serverError(e);
        }
    }

    @GET
    @Path("/{id}")
    public Response getGaugeMetric(@PathParam("id") String id) {
        try {
            return metricsService.findMetric(new MetricId<>(tenantId, GAUGE, id))
                .map(metricDef -> Response.ok(metricDef).build())
                    .switchIfEmpty(Observable.just(noContent())).toBlocking().lastOrDefault(null);
        } catch (Exception e) {
            return serverError(e);
        }
    }

    @GET
    @Path("/{id}/tags")
    public Response getMetricTags(@PathParam("id") String id) {
        try {
            return metricsService.getMetricTags(new MetricId<>(tenantId, GAUGE, id))
                    .map(ApiUtils::valueToResponse)
                    .toBlocking().lastOrDefault(null);
        } catch (Exception e) {
            return serverError(e);
        }
    }

    @PUT
    @Path("/{id}/tags")
    public Response updateMetricTags(
            @PathParam("id") String id,
            Map<String, String> tags) {
        Metric<Double> metric = new Metric<>(new MetricId<>(tenantId, GAUGE, id));
        try {
            metricsService.addTags(metric, tags).toBlocking().lastOrDefault(null);
            return Response.ok().build();
        } catch (IllegalArgumentException e1) {
            return badRequest(new ApiError(e1.getMessage()));
        } catch (Exception e) {
            return serverError(e);
        }
    }

    @DELETE
    @Path("/{id}/tags/{tags}")
    public Response deleteMetricTags(
            @PathParam("id") String id,
            @PathParam("tags") Tags tags
    ) {
        try {
            Metric<Double> metric = new Metric<>(new MetricId<>(tenantId, GAUGE, id));
            metricsService.deleteTags(metric, tags.getTags()).toBlocking().lastOrDefault(null);
            return Response.ok().build();
        } catch (Exception e) {
            return serverError(e);
        }
    }

    @POST
    @Path("/{id}/data")
    public Response addDataForMetric(
            @PathParam("id") String id,
            List<DataPoint<Double>> data
    ) {
        Observable<Metric<Double>> metrics = Functions.dataPointToObservable(tenantId, id, data, GAUGE);
        try {
            metricsService.addDataPoints(GAUGE, metrics).toBlocking().lastOrDefault(null);
            return Response.ok().build();
        } catch (Exception e) {
            return serverError(e);
        }
    }

    @POST
    @Path("/data")
    public Response addGaugeData(
            List<Metric<Double>> gauges
    ) {
        Observable<Metric<Double>> metrics = Functions.metricToObservable(tenantId, gauges, GAUGE);
        try {
            metricsService.addDataPoints(GAUGE, metrics).toBlocking().lastOrDefault(null);
            return Response.ok().build();
        } catch (Exception e) {
            return serverError(e);
        }
    }

    @GET
    @Path("/{id}/data")
    public Response findGaugeData(
            @PathParam("id") String id,
            @QueryParam("start") final Long start,
            @QueryParam("end") final Long end,
            @QueryParam("buckets") Integer bucketsCount,
            @QueryParam("bucketDuration") Duration bucketDuration,
            @QueryParam("percentiles") Percentiles percentiles
    ) {
        TimeRange timeRange = new TimeRange(start, end);
        if (!timeRange.isValid()) {
            return badRequest(new ApiError(timeRange.getProblem()));
        }
        BucketConfig bucketConfig = new BucketConfig(bucketsCount, bucketDuration, timeRange);
        if (!bucketConfig.isValid()) {
            return badRequest(new ApiError(bucketConfig.getProblem()));
        }

        MetricId<Double> metricId = new MetricId<>(tenantId, GAUGE, id);
        Buckets buckets = bucketConfig.getBuckets();
        try {
            if (buckets == null) {
                return metricsService
                        .findDataPoints(metricId, timeRange.getStart(), timeRange.getEnd())
                        .toList()
                        .map(ApiUtils::collectionToResponse)
                        .toBlocking()
                        .lastOrDefault(null);
            } else {
                if (percentiles == null) {
                    percentiles = new Percentiles(Collections.<Double>emptyList());
                }

                return metricsService
                        .findGaugeStats(metricId, timeRange.getStart(), timeRange.getEnd(), buckets,
                                percentiles.getPercentiles())
                        .map(ApiUtils::collectionToResponse)
                        .toBlocking()
                        .lastOrDefault(null);
            }
        } catch (Exception e) {
            return serverError(e);
        }
    }

    @GET
    @Path("/data")
    public Response findGaugeData(
            @QueryParam("start") final Long start,
            @QueryParam("end") final Long end,
            @QueryParam("tags") Tags tags,
            @QueryParam("buckets") Integer bucketsCount,
            @QueryParam("bucketDuration") Duration bucketDuration,
            @QueryParam("percentiles") Percentiles percentiles,
            @QueryParam("metrics") List<String> metricNames,
            @DefaultValue("false") @QueryParam("stacked") Boolean stacked) {

        TimeRange timeRange = new TimeRange(start, end);
        if (!timeRange.isValid()) {
            return badRequest(new ApiError(timeRange.getProblem()));
        }
        BucketConfig bucketConfig = new BucketConfig(bucketsCount, bucketDuration, timeRange);
        if (bucketConfig.isEmpty()) {
            return badRequest(new ApiError("Either the buckets or bucketsDuration parameter must be used"));
        }
        if (!bucketConfig.isValid()) {
            return badRequest(new ApiError(bucketConfig.getProblem()));
        }
        if (metricNames.isEmpty() && (tags == null || tags.getTags().isEmpty())) {
            return badRequest(new ApiError("Either metrics or tags parameter must be used"));
        }
        if (!metricNames.isEmpty() && !(tags == null || tags.getTags().isEmpty())) {
            return badRequest(new ApiError("Cannot use both the metrics and tags parameters"));
        }

        if(percentiles == null) {
            percentiles = new Percentiles(Collections.<Double>emptyList());
        }

        if (metricNames.isEmpty()) {
            return metricsService
                    .findNumericStats(tenantId, MetricType.GAUGE, tags.getTags(), timeRange.getStart(),
                            timeRange.getEnd(),
                            bucketConfig.getBuckets(), percentiles.getPercentiles(), stacked)
                    .map(ApiUtils::collectionToResponse)
                    .toBlocking()
                    .lastOrDefault(null);
        } else {
            return metricsService
                    .findNumericStats(tenantId, MetricType.GAUGE, metricNames, timeRange.getStart(),
                            timeRange.getEnd(), bucketConfig.getBuckets(), percentiles.getPercentiles(), stacked)
                    .map(ApiUtils::collectionToResponse)
                    .toBlocking()
                    .lastOrDefault(null);
        }
    }

    @GET
    @Path("/{id}/periods")
    public Response findPeriods(
            @PathParam("id") String id,
            @QueryParam("start") final Long start,
            @QueryParam("end") final Long end,
            @QueryParam("threshold") double threshold,
            @QueryParam("op") String operator
    ) {
        TimeRange timeRange = new TimeRange(start, end);
        if (!timeRange.isValid()) {
            return badRequest(new ApiError(timeRange.getProblem()));
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
            return badRequest(
                    new ApiError("Invalid value for op parameter. Supported values are lt, lte, eq, gt, gte."));
        } else {
            try {
                MetricId<Double> metricId = new MetricId<>(tenantId, GAUGE, id);
                return metricsService.getPeriods(metricId, predicate, timeRange.getStart(), timeRange.getEnd())
                        .map(ApiUtils::collectionToResponse)
                        .toBlocking().lastOrDefault(null);
            } catch (Exception e) {
                return serverError(e);
            }
        }
    }
}
