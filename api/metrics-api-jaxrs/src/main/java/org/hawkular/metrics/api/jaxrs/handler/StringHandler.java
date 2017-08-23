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
import static org.hawkular.metrics.model.MetricType.STRING;
import static org.hawkular.metrics.model.MetricType.UNDEFINED;

import java.net.URI;
import java.util.List;
import java.util.Map;
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

import org.hawkular.metrics.api.jaxrs.QueryRequest;
import org.hawkular.metrics.api.jaxrs.handler.observer.MetricCreatedObserver;
import org.hawkular.metrics.api.jaxrs.handler.observer.ResultSetObserver;
import org.hawkular.metrics.api.jaxrs.handler.template.IMetricsHandler;
import org.hawkular.metrics.api.jaxrs.param.TimeAndSortParams;
import org.hawkular.metrics.api.jaxrs.util.ApiUtils;
import org.hawkular.metrics.api.jaxrs.util.Logged;
import org.hawkular.metrics.core.service.Functions;
import org.hawkular.metrics.core.service.Order;
import org.hawkular.metrics.core.service.transformers.MinMaxTimestampTransformer;
import org.hawkular.metrics.model.ApiError;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.param.TagNames;
import org.hawkular.metrics.model.param.Tags;
import org.jboss.resteasy.annotations.GZIP;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import rx.Observable;
import rx.schedulers.Schedulers;

/**
 * @author jsanda
 */
@Path("/strings")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
@GZIP
@Api(tags = "String", description = "This resource is experimental and changes may be made to it in subsequent " +
        "releases that are not backwards compatible.")
@ApplicationScoped
@Logged
public class StringHandler extends MetricsServiceHandler implements IMetricsHandler<String> {

    @POST
    @Path("/")
    @ApiOperation(value = "Create string metric.", notes = "Clients are not required to explicitly create "
            + "a metric before storing data. Doing so however allows clients to prevent naming collisions and to "
            + "specify tags and data retention.")
    @ApiResponses(value = {
            @ApiResponse(code = 201, message = "Metric created successfully"),
            @ApiResponse(code = 400, message = "Missing or invalid payload", response = ApiError.class),
            @ApiResponse(code = 409, message = "String metric with given id already exists",
                    response = ApiError.class),
            @ApiResponse(code = 500, message = "Metric creation failed due to an unexpected error",
                    response = ApiError.class)
    })
    public void createMetric(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(required = true) Metric<String> metric,
            @ApiParam(value = "Overwrite previously created metric configuration if it exists. "
                    + "Only data retention and tags are overwriten; existing data points are unnafected. "
                    + "Defaults to false."
            ) @DefaultValue("false") @QueryParam("overwrite") Boolean overwrite,
            @Context UriInfo uriInfo
    ) {
        if (metric.getType() != null && UNDEFINED != metric.getType() && STRING != metric.getType()) {
            asyncResponse.resume(badRequest(new ApiError("Metric type does not match " + STRING.getText())));
        }
        metric = new Metric<>(new MetricId<>(getTenant(), STRING, metric.getId()), metric.getTags(),
                metric.getDataRetention());
        URI location = uriInfo.getBaseUriBuilder().path("/strings/{id}").build(metric.getMetricId().getName());
        metricsService.createMetric(metric, overwrite).subscribe(new MetricCreatedObserver(asyncResponse, location));
    }

    @GET
    @Path("/")
    @ApiOperation(value = "Find tenant's metric definitions.", notes = "Does not include any metric values. ",
                response = Metric.class, responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved at least one metric definition."),
            @ApiResponse(code = 204, message = "No metrics found."),
            @ApiResponse(code = 400, message = "Invalid type parameter type.", response = ApiError.class),
            @ApiResponse(code = 500, message = "Failed to retrieve metrics due to unexpected error.", response = ApiError.class)
    })
    public void getMetrics(
            @Suspended AsyncResponse asyncResponse,
            @ApiParam(value = "List of tags filters") @QueryParam("tags") String tags,
            @ApiParam(value = "Fetch min and max timestamps of available datapoints") @DefaultValue("false")
            @QueryParam("timestamps") Boolean fetchTimestamps) {

        Observable<Metric<String>> metricObservable = null;
        if (tags != null) {
            metricObservable = metricsService.findMetricIdentifiersWithFilters(getTenant(), STRING, tags)
                    .flatMap(metricsService::findMetric);
        } else {
            metricObservable = metricsService.findMetrics(getTenant(), STRING);
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

    @GET
    @Path("/{id}")
    @ApiOperation(value = "Retrieve single metric definition.", response = Metric.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Metric's definition was successfully retrieved."),
            @ApiResponse(code = 204, message = "Query was successful, but no metrics definition is set."),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching metric's definition.", response = ApiError.class) })
    public void getMetric(@Suspended final AsyncResponse asyncResponse, @PathParam("id") String id) {
        metricsService.findMetric(new MetricId<>(getTenant(), STRING, id))
                .compose(new MinMaxTimestampTransformer<>(metricsService))
                .map(metric -> Response.ok(metric).build())
                .switchIfEmpty(Observable.just(ApiUtils.noContent()))
                .subscribe(asyncResponse::resume, t -> asyncResponse.resume(ApiUtils.serverError(t)));
    }

    @DELETE
    @Path("/{id}")
    @ApiOperation(value = "Deletes the metric and associated uncompressed data points, and updates internal indexes."
            + " Note: compressed data will not be deleted immediately. It is deleted as part of the normal"
            + " data expiration as defined by the data retention settings. Consequently, compressed data will"
            + " be accessible until it automatically expires.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Metric deletion was successful."),
            @ApiResponse(code = 500, message = "Unexpected error occurred trying to delete the metric.")
    })
    public void deleteMetric(@Suspended AsyncResponse asyncResponse, @PathParam("id") String id) {
        MetricId<String> metric = new MetricId<>(getTenant(), STRING, id);
        metricsService.deleteMetric(metric).subscribe(new ResultSetObserver(asyncResponse));
    }

    @GET
    @Path("/tags/{tags}")
    @ApiOperation(value = "Retrieve string type's tag values", response = Map.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Tags successfully retrieved."),
            @ApiResponse(code = 204, message = "No matching tags were found"),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching tags.", response = ApiError.class)
    })
    public void getTags(@Suspended final AsyncResponse asyncResponse,
            @ApiParam("Tag query") @PathParam("tags") Tags tags) {
        metricsService.getTagValues(getTenant(), STRING, tags.getTags())
                .map(ApiUtils::mapToResponse)
                .subscribe(asyncResponse::resume, t -> asyncResponse.resume(ApiUtils.serverError(t)));
    }

    @GET
    @Path("/{id}/tags")
    @ApiOperation(value = "Retrieve tags associated with the metric definition.", response = Map.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Metric's tags were successfully retrieved."),
            @ApiResponse(code = 204, message = "Query was successful, but no metrics were found."),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching metric's tags.", response = ApiError.class) })
    public void getMetricTags(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("id") String id) {
        metricsService.getMetricTags(new MetricId<>(getTenant(), STRING, id))
                .map(ApiUtils::mapToResponse)
                .subscribe(asyncResponse::resume, t -> asyncResponse.resume(ApiUtils.serverError(t)));
    }

    @PUT
    @Path("/{id}/tags")
    @ApiOperation(value = "Update tags associated with the metric definition.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Metric's tags were successfully updated."),
            @ApiResponse(code = 500, message = "Unexpected error occurred while updating metric's tags.", response = ApiError.class) })
    public void updateMetricTags(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("id") String id,
            @ApiParam(required = true) Map<String, String> tags) {
        Metric<String> metric = new Metric<>(new MetricId<>(getTenant(), STRING, id));
        metricsService.addTags(metric, tags).subscribe(new ResultSetObserver(asyncResponse));
    }

    @DELETE
    @Path("/{id}/tags/{tags}")
    @ApiOperation(value = "Delete tags associated with the metric definition.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Metric's tags were successfully deleted."),
            @ApiResponse(code = 400, message = "Invalid tags", response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error occurred while trying to delete metric's tags.", response = ApiError.class) })
    public void deleteMetricTags(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("id") String id,
            @ApiParam(value = "Tag names", allowableValues = "Comma-separated list of tag names") @PathParam("tags") TagNames tags) {
        Metric<String> metric = new Metric<>(new MetricId<>(getTenant(), STRING, id));
        metricsService.deleteTags(metric, tags.getNames()).subscribe(new ResultSetObserver(asyncResponse));
    }

    @POST
    @Path("/{id}/raw")
    @ApiOperation(value = "Add data for a single string metric.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Adding data succeeded."),
            @ApiResponse(code = 400, message = "Missing or invalid payload", response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error happened while storing the data",
                    response = ApiError.class)
    })
    public void addMetricData(
            @Suspended final AsyncResponse asyncResponse, @PathParam("id") String id,
            @ApiParam(value = "List of string datapoints", required = true) List<DataPoint<String>> data
    ) {
        Observable<Metric<String>> metrics = Functions.dataPointToObservable(getTenant(), id, data,
                STRING);
        Observable<Void> observable = metricsService.addDataPoints(STRING, metrics);
        observable.subscribe(new ResultSetObserver(asyncResponse));
    }

    @POST
    @Path("/raw")
    @ApiOperation(value = "Add metric data for multiple string metrics in a single call.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Adding data succeeded."),
            @ApiResponse(code = 400, message = "Missing or invalid payload", response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error happened while storing the data",
                    response = ApiError.class)
    })
    public void addData(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "List of string metrics", required = true)
            @JsonDeserialize()
                    List<Metric<String>> availabilities
    ) {
        Observable<Metric<String>> metrics = Functions.metricToObservable(getTenant(), availabilities,
                STRING);
        Observable<Void> observable = metricsService.addDataPoints(STRING, metrics);
        observable.subscribe(new ResultSetObserver(asyncResponse));
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
        findMetricsByNameOrTag(query.getIds(), query.getTags(), STRING)
                .toList()
                .flatMap(metricIds -> TimeAndSortParams.<String>deferredBuilder(query.getStart(), query.getEnd())
                        .fromEarliest(query.getFromEarliest(), metricIds, this::findTimeRange)
                        .sortOptions(query.getLimit(), query.getOrder())
                        .forString()
                        .toObservable()
                        .flatMap(p -> metricsService.findDataPoints(metricIds, p.getTimeRange().getStart(),
                                p.getTimeRange().getEnd(), p.getLimit(), p.getOrder())
                                .observeOn(Schedulers.io())))
                .subscribe(createNamedDataPointObserver(asyncResponse, STRING));
    }

    @GET
    @Path("/{id}/raw")
    @ApiOperation(value = "Retrieve string data.", response = DataPoint.class, responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully fetched string data."),
            @ApiResponse(code = 204, message = "No string data was found."),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching string data.",
                    response = ApiError.class)
    })
    public void getMetricData(
            @Suspended AsyncResponse asyncResponse,
            @PathParam("id") String id,
            @ApiParam(value = "Defaults to now - 8 hours") @QueryParam("start") String start,
            @ApiParam(value = "Defaults to now") @QueryParam("end") String end,
            @ApiParam(value = "Use data from earliest received, subject to retention period")
                @QueryParam("fromEarliest") Boolean fromEarliest,
            @ApiParam(value = "Set to true to return only distinct, contiguous values")
            @QueryParam("distinct") @DefaultValue("false") Boolean distinct,
            @ApiParam(value = "Limit the number of data points returned") @QueryParam("limit") Integer limit,
            @ApiParam(value = "Data point sort order, based on timestamp") @QueryParam("order") Order order
    ) {
        MetricId<String> metricId = new MetricId<>(getTenant(), STRING, id);
        TimeAndSortParams.<String>deferredBuilder(start, end)
                .fromEarliest(fromEarliest, metricId, this::findTimeRange)
                .sortOptions(limit, order)
                .forString()
                .toObservable()
                .flatMap(p -> metricsService.findStringData(metricId, p.getTimeRange().getStart(), p
                        .getTimeRange().getEnd(), distinct, p.getLimit(), p.getOrder()))
                .toList()
                .map(ApiUtils::collectionToResponse)
                .subscribe(asyncResponse::resume, t -> asyncResponse.resume(ApiUtils.error(t)));
    }

    @GET
    @Path("/tags/{tags}/raw")
    @ApiOperation(value = "Retrieve string data on multiple metrics by tags.", response = DataPoint.class,
            responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully fetched metric data points."),
            @ApiResponse(code = 204, message = "Query was successful, but no data was found."),
            @ApiResponse(code = 400, message = "No metric ids are specified", response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching metric data.",
                    response = ApiError.class)
    })
    public void getRawDataByTag(
            @Suspended AsyncResponse asyncResponse,
            @PathParam("tags") String tags,
            @ApiParam(value = "Defaults to now - 8 hours") @QueryParam("start") String start,
            @ApiParam(value = "Defaults to now") @QueryParam("end") String end,
            @ApiParam(value = "Use data from earliest received, subject to retention period")
            @QueryParam("fromEarliest") Boolean fromEarliest,
            @ApiParam(value = "Limit the number of data points returned") @QueryParam("limit") Integer limit,
            @ApiParam(value = "Data point sort order, based on timestamp") @QueryParam("order") Order order
    ) {
        metricsService.findMetricIdentifiersWithFilters(getTenant(), STRING, tags)
                .toList()
                .flatMap(metricIds -> TimeAndSortParams.<String>deferredBuilder(start, end)
                        .fromEarliest(fromEarliest, metricIds, this::findTimeRange)
                        .sortOptions(limit, order)
                        .forString()
                        .toObservable()
                        .flatMap(p -> metricsService.findDataPoints(metricIds, p.getTimeRange().getStart(),
                                p.getTimeRange().getEnd(), p.getLimit(), p.getOrder())
                                .observeOn(Schedulers.io())))
                .subscribe(createNamedDataPointObserver(asyncResponse, STRING));
    }
}
