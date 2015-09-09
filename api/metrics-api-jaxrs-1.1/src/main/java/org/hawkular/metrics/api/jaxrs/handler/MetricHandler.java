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
import static org.hawkular.metrics.api.jaxrs.util.ApiUtils.emptyPayload;
import static org.hawkular.metrics.api.jaxrs.util.ApiUtils.serverError;
import static org.hawkular.metrics.core.api.MetricType.AVAILABILITY;
import static org.hawkular.metrics.core.api.MetricType.COUNTER;
import static org.hawkular.metrics.core.api.MetricType.GAUGE;

import java.net.URI;
import java.util.List;
import java.util.regex.PatternSyntaxException;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.hawkular.metrics.api.jaxrs.ApiError;
import org.hawkular.metrics.api.jaxrs.model.Availability;
import org.hawkular.metrics.api.jaxrs.model.Counter;
import org.hawkular.metrics.api.jaxrs.model.Gauge;
import org.hawkular.metrics.api.jaxrs.model.MetricDefinition;
import org.hawkular.metrics.api.jaxrs.model.MixedMetricsRequest;
import org.hawkular.metrics.api.jaxrs.param.Tags;
import org.hawkular.metrics.api.jaxrs.util.ApiUtils;
import org.hawkular.metrics.api.jaxrs.util.MetricTypeTextConverter;
import org.hawkular.metrics.core.api.AvailabilityType;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricAlreadyExistsException;
import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.MetricType;
import org.hawkular.metrics.core.api.MetricsService;

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;

import rx.Observable;

/**
 * Interface to deal with metrics
 *
 * @author Heiko W. Rupp
 */
@Path("/metrics")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
@Api(value = "", description = "Metrics related REST interface")
public class MetricHandler {

    @Inject
    private MetricsService metricsService;

    @HeaderParam(TENANT_HEADER_NAME)
    private String tenantId;

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
    public Response createMetric(
            @ApiParam(required = true) MetricDefinition<?> metricDefinition,
            @Context UriInfo uriInfo
    ) {
        if (metricDefinition.getType() == null || !metricDefinition.getType().isUserType()) {
            return badRequest(new ApiError("MetricDefinition type is invalid"));
        }
        MetricId<?> id = new MetricId<>(tenantId, metricDefinition.getType(), metricDefinition.getId());
        Metric<?> metric = new Metric<>(id, metricDefinition.getTags(), metricDefinition.getDataRetention());
        URI location = uriInfo.getBaseUriBuilder().path("/{type}/{id}").build(MetricTypeTextConverter.getLongForm(id
                .getType()), id.getName());

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


    @SuppressWarnings("rawtypes")
    @GET
    @Path("/")
    @ApiOperation(value = "Find tenant's metric definitions.", notes = "Does not include any metric values. ",
            response = List.class, responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved at least one metric definition."),
            @ApiResponse(code = 204, message = "No metrics found."),
            @ApiResponse(code = 400, message = "Invalid type parameter type.", response = ApiError.class),
            @ApiResponse(code = 500, message = "Failed to retrieve metrics due to unexpected error.",
                    response = ApiError.class)
    })
    public <T> Response findMetrics(
            @ApiParam(value = "Queried metric type",
                    required = false,
                    allowableValues = "[gauge, availability, counter]")
            @QueryParam("type") MetricType<T> metricType,
            @ApiParam(value = "List of tags filters", required = false) @QueryParam("tags") Tags tags) {

        if (metricType != null && !metricType.isUserType()) {
            return badRequest(new ApiError("Incorrect type param " + metricType.toString()));
        }

        Observable<Metric<T>> metricObservable = (tags == null)
                ? metricsService.findMetrics(tenantId, metricType)
                : metricsService.findMetricsWithFilters(tenantId, tags.getTags(), metricType);

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

    @POST
    @Path("/data")
    @ApiOperation(value = "Add data for multiple metrics in a single call.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Adding data succeeded."),
            @ApiResponse(code = 400, message = "Missing or invalid payload.", response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error happened while storing the data",
                    response = ApiError.class)
    })
    public Response addMetricsData(
            @ApiParam(value = "List of metrics", required = true) MixedMetricsRequest metricsRequest
    ) {
        if (metricsRequest.isEmpty()) {
            return emptyPayload();
        }

        Observable<Metric<Double>> gauges = Gauge.toObservable(tenantId, metricsRequest.getGauges());
        Observable<Metric<AvailabilityType>> availabilities = Availability.toObservable(tenantId,
                metricsRequest.getAvailabilities());
        Observable<Metric<Long>> counters = Counter.toObservable(tenantId, metricsRequest.getCounters());

        try {
            metricsService.addDataPoints(GAUGE, gauges)
                    .mergeWith(metricsService.addDataPoints(AVAILABILITY, availabilities))
                    .mergeWith(metricsService.addDataPoints(COUNTER, counters))
                    .toBlocking().lastOrDefault(null);
            return Response.ok().build();
        } catch (Exception e) {
            return serverError(e);
        }
    }
}
