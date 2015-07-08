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
import static org.hawkular.metrics.api.jaxrs.util.ApiUtils.requestToAvailabilities;
import static org.hawkular.metrics.api.jaxrs.util.ApiUtils.requestToCounters;
import static org.hawkular.metrics.api.jaxrs.util.ApiUtils.requestToGauges;

import java.util.List;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Response;

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;
import org.hawkular.metrics.api.jaxrs.ApiError;
import org.hawkular.metrics.api.jaxrs.param.Tags;
import org.hawkular.metrics.api.jaxrs.request.MetricDefinition;
import org.hawkular.metrics.api.jaxrs.request.MixedMetricsRequest;
import org.hawkular.metrics.api.jaxrs.util.ApiUtils;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricType;
import org.hawkular.metrics.core.api.MetricsService;
import rx.Observable;
import rx.schedulers.Schedulers;


/**
 * Interface to deal with metrics
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
    public void findMetrics(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Queried metric type",
                      required = false,
                      allowableValues = "[gauge, availability, counter]")
            @QueryParam("type") MetricType metricType,
            @ApiParam(value = "List of tags", required = false) @QueryParam ("tags") Tags tags) {

        if(metricType != null && !MetricType.userTypes().contains(metricType)) {
            asyncResponse.resume(badRequest(new ApiError("Incorrect type param")));
            return;
        }

        Observable<Metric> metricObservable = (tags == null) ? metricsService.findMetrics(tenantId, metricType)
                : metricsService.findMetricsWithTags(tenantId, tags.getTags(), metricType);

        metricObservable
                    .map(MetricDefinition::new)
                    .toList()
                    .map(ApiUtils::collectionToResponse)
                    .subscribe(asyncResponse::resume, t -> asyncResponse.resume(ApiUtils.serverError(t)));
    }

    @POST
    @Path("/data")
    @ApiOperation(value = "Add data for multiple metrics in a single call.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Adding data succeeded."),
            @ApiResponse(code = 500, message = "Unexpected error happened while storing the data",
                response = ApiError.class) })
    public void addMetricsData(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "List of metrics", required = true) MixedMetricsRequest metricsRequest) {

        Observable<Void> gaugeInserts = metricsService.addGaugeData(requestToGauges(tenantId,
                metricsRequest.getGaugeMetrics()).subscribeOn(Schedulers.computation()));
        Observable<Void> counterInserts = metricsService.addCounterData(requestToCounters(tenantId,
                metricsRequest.getCounters()).subscribeOn(Schedulers.computation()));
        Observable<Void> availabilityInserts = metricsService.addAvailabilityData(requestToAvailabilities(tenantId,
                metricsRequest.getAvailabilityMetrics()).subscribeOn(Schedulers.computation()));

        Observable.merge(gaugeInserts, counterInserts, availabilityInserts).subscribe(
                aVoid -> {},
                t -> asyncResponse.resume(ApiUtils.serverError(t)),
                () -> asyncResponse.resume(Response.ok().build())
        );
    }
}
