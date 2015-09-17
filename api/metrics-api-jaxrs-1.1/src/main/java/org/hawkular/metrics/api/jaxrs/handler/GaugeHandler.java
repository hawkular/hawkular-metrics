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
import static org.hawkular.metrics.core.api.MetricType.GAUGE;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
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

import org.hawkular.metrics.api.jaxrs.model.ApiError;
import org.hawkular.metrics.api.jaxrs.model.Gauge;
import org.hawkular.metrics.api.jaxrs.model.GaugeDataPoint;
import org.hawkular.metrics.api.jaxrs.model.MetricDefinition;
import org.hawkular.metrics.api.jaxrs.param.Duration;
import org.hawkular.metrics.api.jaxrs.param.Tags;
import org.hawkular.metrics.api.jaxrs.util.ApiUtils;
import org.hawkular.metrics.core.api.Buckets;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricAlreadyExistsException;
import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.MetricType;
import org.hawkular.metrics.core.api.MetricsService;

import rx.Observable;

/**
 * @author Stefan Negrea
 *
 */
@Path("/gauges")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
public class GaugeHandler {
    private static final long EIGHT_HOURS = MILLISECONDS.convert(8, HOURS);

    @Inject
    private MetricsService metricsService;

    @HeaderParam(TENANT_HEADER_NAME)
    private String tenantId;

    @POST
    @Produces(APPLICATION_JSON)
    @Path("/")
    public Response createGaugeMetric(
            MetricDefinition metricDefinition,
            @Context UriInfo uriInfo
    ) {
        if(metricDefinition.getType() != null && MetricType.GAUGE != metricDefinition.getType()) {
            return ApiUtils.badRequest(new ApiError("MetricDefinition type does not match " + MetricType
                    .GAUGE.getText()));
        }
        Metric<Double> metric = new Metric<>(new MetricId<>(tenantId, GAUGE, metricDefinition.getId()),
                metricDefinition.getTags(), metricDefinition.getDataRetention());
        URI location = uriInfo.getBaseUriBuilder().path("/gauges/{id}").build(metric.getId().getName());
        try {
            Observable<Void> observable = metricsService.createMetric(metric);
            observable.toBlocking().lastOrDefault(null);
            return Response.created(location).build();
        } catch (MetricAlreadyExistsException e) {
            String message = "A metric with name [" + e.getMetric().getId().getName() + "] already exists";
            return Response.status(Response.Status.CONFLICT).entity(new ApiError(message)).build();
        } catch (Exception e) {
            return ApiUtils.serverError(e);
        }
    }

    @GET
    @Produces(APPLICATION_JSON)
    @Path("/{id}")
    public Response getGaugeMetric(@PathParam("id") String id) {
        try {
            return metricsService.findMetric(new MetricId<>(tenantId, GAUGE, id))
                .map(MetricDefinition::new)
                .map(metricDef -> Response.ok(metricDef).build())
                .switchIfEmpty(Observable.just(ApiUtils.noContent())).toBlocking().lastOrDefault(null);
        } catch (Exception e) {
            return ApiUtils.serverError(e);
        }
    }

    @GET
    @Produces(APPLICATION_JSON)
    @Path("/{id}/tags")
    public Response getGaugeMetricTags(@PathParam("id") String id) {
        try {
            return metricsService.getMetricTags(new MetricId<>(tenantId, GAUGE, id))
                    .map(ApiUtils::valueToResponse)
                    .toBlocking().lastOrDefault(null);
        } catch (Exception e) {
            return ApiUtils.serverError(e);
        }
    }

    @PUT
    @Produces(APPLICATION_JSON)
    @Path("/{id}/tags")
    public Response updateGaugeMetricTags(
            @PathParam("id") String id,
            Map<String, String> tags
    ) {
        Metric<Double> metric = new Metric<>(new MetricId<>(tenantId, GAUGE, id));
        try {
            metricsService.addTags(metric, tags).toBlocking().lastOrDefault(null);
            return Response.ok().build();
        } catch (Exception e) {
            return ApiUtils.serverError(e);
        }
    }

    @DELETE
    @Produces(APPLICATION_JSON)
    @Path("/{id}/tags/{tags}")
    public Response deleteGaugeMetricTags(
            @PathParam("id") String id,
            @PathParam("tags") Tags tags
    ) {
        try {
            Metric<Double> metric = new Metric<>(new MetricId<>(tenantId, GAUGE, id));
            metricsService.deleteTags(metric, tags.getTags()).toBlocking().lastOrDefault(null);
            return Response.ok().build();
        } catch (Exception e) {
            return ApiUtils.serverError(e);
        }
    }

    @POST
    @Produces(APPLICATION_JSON)
    @Path("/{id}/data")
    public Response addDataForMetric(
            @PathParam("id") String id,
            List<GaugeDataPoint> data
    ) {
        Observable<Metric<Double>> metrics = GaugeDataPoint.toObservable(tenantId, id, data);
        try {
            metricsService.addDataPoints(GAUGE, metrics).toBlocking().lastOrDefault(null);
            return Response.ok().build();
        } catch (Exception e) {
            return ApiUtils.serverError(e);
        }
    }

    @POST
    @Produces(APPLICATION_JSON)
    @Path("/data")
    public Response addGaugeData(
            List<Gauge> gauges
    ) {
        Observable<Metric<Double>> metrics = Gauge.toObservable(tenantId, gauges);
        try {
            metricsService.addDataPoints(GAUGE, metrics).toBlocking().lastOrDefault(null);
            return Response.ok().build();
        } catch (Exception e) {
            return ApiUtils.serverError(e);
        }
    }

    @GET
    @Produces(APPLICATION_JSON)
    @Path("/")
    public Response findGaugeMetricsByTags(@QueryParam("tags") Tags tags) {
        if (tags == null) {
            return badRequest(new ApiError("Missing tags query"));
        } else {
            try{
                return metricsService.findMetricsWithFilters(tenantId, tags.getTags(), GAUGE)
                        .map(MetricDefinition::new)
                        .toList()
                        .map(m -> {
                            if (m.isEmpty()) {
                                return ApiUtils.noContent();
                            } else {
                                return Response.ok(m).build();
                            }
                        }).toBlocking().lastOrDefault(null);
            } catch (Exception e) {
                return Response.serverError().entity(new ApiError(e.getMessage())).build();
                    }
        }
    }

    @GET
    @Path("/{id}/data")
    @Produces(APPLICATION_JSON)
    public Response findGaugeData(
            @PathParam("id") String id,
            @QueryParam("start") final Long start,
            @QueryParam("end") final Long end,
            @QueryParam("buckets") Integer bucketsCount,
            @QueryParam("bucketDuration") Duration bucketDuration
    ) {
        long now = System.currentTimeMillis();
        long startTime = start == null ? now - EIGHT_HOURS : start;
        long endTime = end == null ? now : end;

        MetricId<Double> metricId = new MetricId<>(tenantId, GAUGE, id);

        if (bucketsCount == null && bucketDuration == null) {
            try {
                return metricsService
                        .findDataPoints(metricId, startTime, endTime)
                        .map(GaugeDataPoint::new)
                        .toList()
                        .map(ApiUtils::collectionToResponse)
                        .toBlocking()
                        .lastOrDefault(null);
            } catch (Exception e) {
                return ApiUtils.serverError(e);
            }
        } else if (bucketsCount != null && bucketDuration != null) {
            return badRequest(new ApiError("Both buckets and bucketDuration parameters are used"));
        } else {
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

            try {
                return metricsService
                        .findGaugeStats(metricId, startTime, endTime, buckets)
                        .map(ApiUtils::collectionToResponse)
                        .toBlocking()
                        .lastOrDefault(null);
            } catch (Exception e) {
                return ApiUtils.serverError(e);
            }
        }
    }

    @GET
    @Produces(APPLICATION_JSON)
    @Path("/{id}/periods")
    public Response findPeriods(
            @PathParam("id") String id,
            @QueryParam("start") final Long start,
            @QueryParam("end") final Long end,
            @QueryParam("threshold") double threshold,
            @QueryParam("op") String operator
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
                return metricsService.getPeriods(new MetricId<>(tenantId, GAUGE, id), predicate, startTime, endTime)
                        .map(ApiUtils::collectionToResponse).toBlocking().lastOrDefault(null);
            } catch (Exception e) {
                return ApiUtils.serverError(e);
            }
        }
    }

    @GET
    @Produces(APPLICATION_JSON)
    @Path("/tags/{tags}")
    public Response findTaggedGaugeMetrics(@PathParam("tags") Tags tags) {
        try {
            return metricsService.findMetricsWithFilters(tenantId, tags.getTags(), GAUGE)
                    .map(MetricDefinition::new)
                    .toList()
                    .map(m -> {
                        if (m.isEmpty()) {
                        return ApiUtils.noContent();
                        } else {
                            return Response.ok(m).build();
                        }
                    }).toBlocking().lastOrDefault(null);
        } catch (Exception e) {
            return ApiUtils.serverError(e);
        }
    }
}
