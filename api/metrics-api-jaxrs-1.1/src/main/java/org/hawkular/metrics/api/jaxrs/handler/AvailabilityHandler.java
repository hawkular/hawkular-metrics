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
import static org.hawkular.metrics.core.api.MetricType.AVAILABILITY;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

import org.hawkular.metrics.api.jaxrs.model.ApiError;
import org.hawkular.metrics.api.jaxrs.model.Availability;
import org.hawkular.metrics.api.jaxrs.model.AvailabilityDataPoint;
import org.hawkular.metrics.api.jaxrs.model.MetricDefinition;
import org.hawkular.metrics.api.jaxrs.param.BucketConfig;
import org.hawkular.metrics.api.jaxrs.param.Duration;
import org.hawkular.metrics.api.jaxrs.param.Tags;
import org.hawkular.metrics.api.jaxrs.param.TimeRange;
import org.hawkular.metrics.api.jaxrs.util.ApiUtils;
import org.hawkular.metrics.core.api.AvailabilityType;
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
@Path("/availability")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
public class AvailabilityHandler {

    @Inject
    private MetricsService metricsService;

    @HeaderParam(TENANT_HEADER_NAME)
    private String tenantId;

    @POST
    @Path("/")
    public Response createAvailabilityMetric(
            MetricDefinition metricDefinition,
            @Context UriInfo uriInfo
    ) {
        if(metricDefinition.getType() != null && MetricType.AVAILABILITY != metricDefinition.getType()) {
            return ApiUtils.badRequest(new ApiError("MetricDefinition type does not match " + MetricType
                    .AVAILABILITY.getText()));
        }
        URI location = uriInfo.getBaseUriBuilder().path("/availability/{id}").build(metricDefinition.getId());
        Metric<AvailabilityType> metric = new Metric<>(new MetricId<>(tenantId, AVAILABILITY, metricDefinition.getId()),
                metricDefinition.getTags(), metricDefinition.getDataRetention());
        try {
            Observable<Void> observable = metricsService.createMetric(metric);
            observable.toBlocking().lastOrDefault(null);
            return Response.created(location).build();
        } catch (MetricAlreadyExistsException e) {
            String message = "A metric with name [" + e.getMetric().getId().getName() + "] already exists";
            return Response.status(Response.Status.CONFLICT).entity(new ApiError(message)).build();
        } catch (Exception e) {
            return serverError(e);
        }
    }

    @GET
    @Path("/")
    public Response findAvailabilityMetrics(
            @QueryParam("tags") Tags tags) {

        Observable<Metric<AvailabilityType>> metricObservable = (tags == null)
                ? metricsService.findMetrics(tenantId, AVAILABILITY)
                : metricsService.findMetricsWithFilters(tenantId, tags.getTags(), AVAILABILITY);

        try {
            return metricObservable
                    .map(MetricDefinition::new)
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
    public Response getAvailabilityMetric(@PathParam("id") String id) {
        try {
            return metricsService.findMetric(new MetricId<>(tenantId, AVAILABILITY, id))
                .map(MetricDefinition::new)
                .map(metricDef -> Response.ok(metricDef).build())
                .switchIfEmpty(Observable.just(noContent()))
                .toBlocking()
                .firstOrDefault(null);
        } catch (Exception e) {
            return serverError(e);
        }
    }

    @GET
    @Path("/{id}/tags")
    public Response getMetricTags(
            @PathParam("id") String id
    ) {
        Observable<Optional<Map<String, String>>> something = metricsService
                .getMetricTags(new MetricId<>(tenantId, AVAILABILITY, id));
        try {
            return something
                    .map(ApiUtils::valueToResponse)
                    .toBlocking()
                    .lastOrDefault(null);
        } catch (Exception e) {
            return serverError(e);
        }
    }


    @PUT
    @Path("/{id}/tags")
    public Response updateMetricTags(
            @PathParam("id") String id,
            Map<String, String> tags
    ) {
        Metric<AvailabilityType> metric = new Metric<>(new MetricId<>(tenantId, AVAILABILITY, id));
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
        Metric<AvailabilityType> metric = new Metric<>(new MetricId<>(tenantId, AVAILABILITY, id));

        try {
            metricsService.deleteTags(metric, tags.getTags()).toBlocking().lastOrDefault(null);
            return Response.ok().build();
        } catch (Exception e) {
            return serverError(e);
        }
    }

    @POST
    @Path("/{id}/data")
    public Response addAvailabilityForMetric(
            @PathParam("id") String id,
            List<AvailabilityDataPoint> data
    ) {
        Observable<Metric<AvailabilityType>> metrics = AvailabilityDataPoint.toObservable(tenantId, id, data);
        try {
            metricsService.addDataPoints(AVAILABILITY, metrics).toBlocking().lastOrDefault(null);
            return Response.ok().build();
        } catch (Exception e) {
            return serverError(e);
        }
    }

    @POST
    @Path("/data")
    public Response addAvailabilityData(
            List<Availability> availabilities
    ) {
        Observable<Metric<AvailabilityType>> metrics = Availability.toObservable(tenantId, availabilities);
        try {
            metricsService.addDataPoints(AVAILABILITY, metrics).toBlocking().lastOrDefault(null);
            return Response.ok().build();
        } catch (Exception e) {
            return serverError(e);
        }
    }

    @GET
    @Path("/{id}/data")
    public Response findAvailabilityData(
            @PathParam("id") String id,
            @QueryParam("start") final Long start,
            @QueryParam("end") final Long end,
            @QueryParam("buckets") Integer bucketsCount,
            @QueryParam("bucketDuration") Duration bucketDuration,
            @QueryParam("distinct") @DefaultValue("false") Boolean distinct
    ) {
        TimeRange timeRange = new TimeRange(start, end);
        if (!timeRange.isValid()) {
            return badRequest(new ApiError(timeRange.getProblem()));
        }
        BucketConfig bucketConfig = new BucketConfig(bucketsCount, bucketDuration, timeRange);
        if (!bucketConfig.isValid()) {
            return badRequest(new ApiError(bucketConfig.getProblem()));
        }

        MetricId<AvailabilityType> metricId = new MetricId<>(tenantId, AVAILABILITY, id);
        Buckets buckets = bucketConfig.getBuckets();
        try {
            if (buckets == null) {
                return metricsService.findAvailabilityData(metricId, timeRange.getStart(), timeRange.getEnd(), distinct)
                        .map(AvailabilityDataPoint::new)
                        .toList()
                        .map(ApiUtils::collectionToResponse)
                        .toBlocking()
                        .lastOrDefault(null);
            } else {
                return metricsService.findAvailabilityStats(metricId, timeRange.getStart(), timeRange.getEnd(), buckets)
                        .map(ApiUtils::collectionToResponse).toBlocking()
                        .lastOrDefault(null);
            }
        } catch (Exception e) {
            return serverError(e);
        }
    }
}
