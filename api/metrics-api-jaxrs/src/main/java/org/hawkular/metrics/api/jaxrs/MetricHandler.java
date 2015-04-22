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

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.hawkular.metrics.api.jaxrs.util.ApiUtils.badRequest;
import static org.hawkular.metrics.api.jaxrs.util.ApiUtils.executeAsync;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Response;

import org.hawkular.metrics.api.jaxrs.util.ApiUtils;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricType;
import org.hawkular.metrics.core.api.MetricsService;
import org.hawkular.metrics.core.impl.request.MixedMetricsRequest;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;


/**
 * Interface to deal with metrics
 * @author Heiko W. Rupp
 */
@Path("/")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
@Api(value = "/", description = "Metrics related REST interface")
public class MetricHandler {
    @Inject
    private MetricsService metricsService;

    @GET
    @Path("/{tenantId}")
    @ApiOperation(value = "Find tenant's metric definitions.", notes = "Does not include any metric values. ",
            response = List.class, responseContainer = "List")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Successfully retrieved at least one metric "
            + "definition."),
            @ApiResponse(code = 204, message = "No metrics found."),
            @ApiResponse(code = 400, message = "Given type is not a valid type.", response = ApiError.class),
            @ApiResponse(code = 500, message = "Failed to retrieve metrics due to unexpected error.",
                         response = ApiError.class)
    })
    public void findMetrics(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenantId") final String tenantId,
        @ApiParam(value = "Queried metric type", required = true, allowableValues = "[gauge, avail, log]")
        @QueryParam("type") String type) {

        executeAsync(
                asyncResponse, () -> {
                    MetricType metricType = null;
                    try {
                        metricType = MetricType.fromTextCode(type);
                    } catch (IllegalArgumentException e) {
                        return badRequest(
                                new ApiError("[" + type + "] is not a valid type. Accepted values are gauge|avail|log")
                        );
                    }
                    ListenableFuture<List<Metric<?>>> future = metricsService.findMetrics(tenantId, metricType);
                    return Futures.transform(future, ApiUtils.MAP_COLLECTION);
                }
        );
    }

    @POST
    @Path("/{tenantId}/data")
    @ApiOperation(value = "Add data for multiple metrics in a single call.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Adding data succeeded."),
            @ApiResponse(code = 500, message = "Unexpected error happened while storing the data",
                response = ApiError.class) })
    public void addMetricsData(@Suspended final AsyncResponse asyncResponse, @PathParam("tenantId") String tenantId,
            @ApiParam(value = "List of metrics", required = true) MixedMetricsRequest metricsRequest) {
        executeAsync(asyncResponse, () -> {
            if ((metricsRequest.getGaugeMetrics() == null || !metricsRequest.getGaugeMetrics().isEmpty())
                    && (metricsRequest.getAvailabilityMetrics() == null || metricsRequest.getAvailabilityMetrics()
                            .isEmpty())) {
                return Futures.immediateFuture(Response.ok().build());
            }

            List<ListenableFuture<Void>> simpleFuturesList = new ArrayList<>();

            if (metricsRequest.getGaugeMetrics() != null && !metricsRequest.getGaugeMetrics().isEmpty()) {
                metricsRequest.getGaugeMetrics().forEach(m -> m.setTenantId(tenantId));
                simpleFuturesList.add(metricsService.addGaugeData(metricsRequest.getGaugeMetrics()));
            }

            if (metricsRequest.getAvailabilityMetrics() != null && !metricsRequest.getAvailabilityMetrics().isEmpty()) {
                metricsRequest.getAvailabilityMetrics().forEach(m -> m.setTenantId(tenantId));
                simpleFuturesList.add(metricsService.addAvailabilityData(metricsRequest.getAvailabilityMetrics()));
            }

            return Futures.transform(Futures.successfulAsList(simpleFuturesList), ApiUtils.MAP_LIST_VOID);
        });
    }

}
