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
package org.hawkular.metrics.api.handler;

import java.net.URI;
import java.net.URISyntaxException;

import javax.inject.Inject;

import org.hawkular.handlers.RestEndpoint;
import org.hawkular.handlers.RestHandler;
import org.hawkular.metrics.api.MetricsApp;
import org.hawkular.metrics.api.filter.TenantFilter;
import org.hawkular.metrics.api.handler.observer.MetricCreatedObserver;
import org.hawkular.metrics.api.jaxrs.util.Logged;
import org.hawkular.metrics.api.util.Wrappers;
import org.hawkular.metrics.core.service.MetricsService;
import org.hawkular.metrics.model.ApiError;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;


/**
 * Mixed metrics handler
 *
 * @author Stefan Negrea
 */
@RestEndpoint(path = "/metrics")
@Api(tags = "Metric")
@Logged
public class MetricHandler implements RestHandler {

    @Inject
    private MetricsService metricsService;

    @Inject
    private ObjectMapper objectMapper;

    @Override
    public void initRoutes(String baseUrl, Router router) {
        Wrappers.setupTenantRoute(router, HttpMethod.POST, (baseUrl + "/metrics"), this::createMetric);

        metricsService = MetricsApp.msl.getMetricsService();
        objectMapper = MetricsApp.msl.objectMapper;
    }

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
    public <T> void createMetric(RoutingContext ctx) {
    /*        @Suspended final AsyncResponse asyncResponse,
            @ApiParam(required = true) Metric<T> metric,
            @ApiParam(value = "Overwrite previously created metric if it exists. Defaults to false.",
                    required = false) @DefaultValue("false") @QueryParam("overwrite") Boolean overwrite,
            @Context UriInfo uriInfo
    )*/
        Metric<T> metric = null;
        try {
            metric = objectMapper.reader(Metric.class).readValue(ctx.getBodyAsString());
        } catch (Exception e) {
            ctx.fail(e);
            return;
        }

        Boolean overwrite = Boolean.parseBoolean(ctx.request().getParam("overwrite"));

        if (metric == null || metric.getType() == null || !metric.getType().isUserType()) {
            //asyncResponse.resume(badRequest(new ApiError("Metric type is invalid")));
            ctx.fail(new Exception("Metric type is invalid"));
            return;
        }

        MetricId<T> id = new MetricId<>(TenantFilter.getTenant(ctx), metric.getMetricId().getType(), metric.getId());
        //MetricId<T> id = new MetricId<>(null, metric.getMetricId().getType(), metric.getId());
        metric = new Metric<>(id, metric.getTags(), metric.getDataRetention());
        /*URI location = uriInfo.getBaseUriBuilder().path("/{type}/{id}").build(MetricTypeTextConverter.getLongForm(id
                .getType()), id.getName());*/
        URI location = null;
        try {
            location = new URI("/hawkular/metrics/");
        } catch (URISyntaxException e) {
            ctx.fail(e);
            return;
        }

        metricsService.createMetric(metric, overwrite).subscribe(new MetricCreatedObserver(ctx, location));
    }


}
