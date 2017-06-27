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

import static org.hawkular.metrics.model.MetricType.AVAILABILITY;
import static org.hawkular.metrics.model.MetricType.COUNTER;
import static org.hawkular.metrics.model.MetricType.GAUGE;
import static org.hawkular.metrics.model.MetricType.STRING;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

import org.hawkular.handlers.RestEndpoint;
import org.hawkular.handlers.RestHandler;
import org.hawkular.metrics.api.MetricsApp;
import org.hawkular.metrics.api.filter.TenantFilter;
import org.hawkular.metrics.api.handler.observer.MetricCreatedObserver;
import org.hawkular.metrics.api.jaxrs.StatsQueryRequest;
import org.hawkular.metrics.api.jaxrs.param.MetricTypeConverter;
import org.hawkular.metrics.api.jaxrs.param.TagsConverter;
import org.hawkular.metrics.api.jaxrs.util.Logged;
import org.hawkular.metrics.api.util.JsonUtil;
import org.hawkular.metrics.api.util.Wrappers;
import org.hawkular.metrics.core.service.Functions;
import org.hawkular.metrics.core.service.MetricsService;
import org.hawkular.metrics.core.service.transformers.MinMaxTimestampTransformer;
import org.hawkular.metrics.model.AvailabilityType;
import org.hawkular.metrics.model.BucketPoint;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.MetricType;
import org.hawkular.metrics.model.MixedMetricsRequest;
import org.hawkular.metrics.model.param.Tags;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.swagger.annotations.Api;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import rx.Observable;
import rx.schedulers.Schedulers;

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

    private MetricTypeConverter mtc;
    private TagsConverter tc;

    @Override
    public void initRoutes(String baseUrl, Router router) {
        Wrappers.setupTenantRoute(router, HttpMethod.POST, (baseUrl + "/metrics"), this::createMetric);
        Wrappers.setupTenantRoute(router, HttpMethod.POST, (baseUrl + "/metrics/raw"), this::addMetricsData);
        Wrappers.setupTenantRoute(router, HttpMethod.POST, (baseUrl + "/metrics/stats/query"), this::findStats);
        Wrappers.setupTenantRoute(router, HttpMethod.POST, (baseUrl + "/metrics/stats/batch/query"),
                this::findStatsBatched);

        Wrappers.setupTenantRoute(router, HttpMethod.GET, (baseUrl + "/metrics/tags"), this::getTagNames);
        Wrappers.setupTenantRoute(router, HttpMethod.GET, (baseUrl + "/metrics/tags/:tags"), this::getTags);
        Wrappers.setupTenantRoute(router, HttpMethod.GET, (baseUrl + "/metrics"), this::findMetrics);

        metricsService = MetricsApp.msl.getMetricsService();
        objectMapper = MetricsApp.msl.objectMapper;

        mtc = new MetricTypeConverter();
        tc = new TagsConverter();
    }

    public <T> void createMetric(RoutingContext ctx) {
        Metric<T> metric = null;
        try {
            metric = objectMapper.reader(Metric.class).readValue(ctx.getBodyAsString());
        } catch (Exception e) {
            ctx.fail(e);
            return;
        }

        Boolean overwrite = Boolean.parseBoolean(ctx.request().getParam("overwrite"));

        if (metric == null || metric.getType() == null || !metric.getType().isUserType()) {
            ctx.fail(new Exception("Metric type is invalid"));
            return;
        }

        MetricId<T> id = new MetricId<>(TenantFilter.getTenant(ctx), metric.getMetricId().getType(), metric.getId());
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

    public void getTagNames(RoutingContext ctx) {
        String tagNameFilter = ctx.request().getParam("filter");

        MetricType<?> metricType;
        try {
            metricType = mtc.fromString(ctx.request().getParam("type"));
        } catch (Exception e) {
            ctx.fail(e);
            return;
        }

        ctx.response().setChunked(true);
        metricsService.getTagNames(TenantFilter.getTenant(ctx), metricType, tagNameFilter)
                .toList()
                .map(JsonUtil::toJson)
                .doOnNext(ctx.response()::write)
                .doOnError(error -> {
                    ctx.response().setStatusCode(HttpResponseStatus.BAD_REQUEST.code()).end(error.getMessage());
                })
                .doOnCompleted(ctx.response()::end)
                .subscribeOn(Schedulers.io())
                .subscribe();
    }

    public <T> void getTags(RoutingContext ctx) {
        MetricType<T> metricType;
        try {
            metricType = (MetricType<T>) mtc.fromString(ctx.request().getParam("type"));
        } catch (Exception e) {
            ctx.fail(e);
            return;
        }

        if (metricType != null & !metricType.isUserType()) {
            ctx.fail(new Exception("Metric type is invalid"));
            return;
        }

        Tags tags = tc.fromString(ctx.request().getParam("tags"));

        ctx.response().setChunked(true);
        metricsService.getTagValues(TenantFilter.getTenant(ctx), metricType, tags.getTags())
                .map(JsonUtil::toJson)
                .doOnNext(ctx.response()::write)
                .doOnError(error -> {
                    ctx.response().setStatusCode(HttpResponseStatus.BAD_REQUEST.code()).end(error.getMessage());
                })
                .doOnCompleted(ctx.response()::end)
                .subscribeOn(Schedulers.io())
                .subscribe();
    }

    public <T> void findMetrics(RoutingContext ctx) {
        MetricType<T> metricType;
        try {
            metricType = (MetricType<T>) mtc.fromString(ctx.request().getParam("type"));
        } catch (Exception e) {
            ctx.fail(e);
            return;
        }

        if (metricType != null & !metricType.isUserType()) {
            ctx.fail(new Exception("Metric type is invalid"));
            return;
        }

        Boolean fetchTimestamps = Boolean.parseBoolean(ctx.request().getParam("fetchTimestamps"));
        String tags = ctx.request().getParam("tags");
        String id = ctx.request().getParam("id");

        Observable<Metric<T>> metricObservable;

        if (tags != null) {
            metricObservable = metricsService.findMetricsWithFilters(TenantFilter.getTenant(ctx), metricType, tags);
            if (!Strings.isNullOrEmpty(id)) {
                metricObservable = metricObservable.filter(metricsService.idFilter(id));
            }
        } else {
            if (!Strings.isNullOrEmpty(id)) {
                // HWKMETRICS-461
                if (metricType == null) {
                    ctx.fail(new Exception("Exact id search requires type to be set"));
                    return;
                }
                String[] ids = id.split("\\|");
                metricObservable = Observable.from(ids)
                        .map(idPart -> new MetricId<>(TenantFilter.getTenant(ctx), metricType, idPart))
                        .flatMap(mId -> metricsService.findMetric(mId));
            } else {
                metricObservable = metricsService.findMetrics(TenantFilter.getTenant(ctx), metricType);
            }
        }

        if (fetchTimestamps) {
            metricObservable = metricObservable
                    .compose(new MinMaxTimestampTransformer<>(metricsService));
        }

        ctx.response().setChunked(true);
        metricObservable
                .toList()
                .map(JsonUtil::toJson)
                .doOnNext(ctx.response()::write)
                .doOnError(error -> {
                    ctx.response().setStatusCode(HttpResponseStatus.BAD_REQUEST.code()).end(error.getMessage());
                })
                .doOnCompleted(ctx.response()::end)
                .subscribeOn(Schedulers.io())
                .subscribe();
    }

    public void addMetricsData(RoutingContext ctx) {
        String bodyAsString = ctx.getBodyAsString();
        if (bodyAsString == null || bodyAsString.isEmpty()) {
            ctx.fail(new Exception("Payload is empty"));
            return;
        }

        MixedMetricsRequest metricsRequest = null;
        try {
            metricsRequest = objectMapper.reader(MixedMetricsRequest.class).readValue(bodyAsString);
        } catch (Exception e) {
            ctx.fail(e);
            return;
        }

        Observable<Metric<Double>> gauges = Functions.metricToObservable(TenantFilter.getTenant(ctx),
                metricsRequest.getGauges(),
                GAUGE);
        Observable<Metric<AvailabilityType>> availabilities = Functions.metricToObservable(TenantFilter.getTenant(ctx),
                metricsRequest.getAvailabilities(), AVAILABILITY);
        Observable<Metric<Long>> counters = Functions.metricToObservable(TenantFilter.getTenant(ctx),
                metricsRequest.getCounters(),
                COUNTER);
        Observable<Metric<String>> strings = Functions.metricToObservable(TenantFilter.getTenant(ctx),
                metricsRequest.getStrings(),
                STRING);

        ctx.response().setChunked(true);
        metricsService.addDataPoints(GAUGE, gauges)
                .mergeWith(metricsService.addDataPoints(AVAILABILITY, availabilities))
                .mergeWith(metricsService.addDataPoints(COUNTER, counters))
                .mergeWith(metricsService.addDataPoints(STRING, strings))
                .map(JsonUtil::toJson)
                .doOnNext(ctx.response()::write)
                .doOnError(error -> {
                    ctx.response().setStatusCode(HttpResponseStatus.BAD_REQUEST.code()).end(error.getMessage());
                })
                .doOnCompleted(ctx.response()::end)
                .subscribeOn(Schedulers.io())
                .subscribe();
    }

    public void findStats(RoutingContext ctx) {
        StatsQueryRequest query = null;
        try {
            query = objectMapper.reader(StatsQueryRequest.class).readValue(ctx.getBodyAsString());
        } catch (Exception e) {
            ctx.fail(e);
            return;
        }

        try {
            org.hawkular.metrics.api.jaxrs.handler.MetricHandler.checkRequiredParams(query);

            ctx.response().setChunked(true);
            org.hawkular.metrics.api.jaxrs.handler.MetricHandler
                    .doStatsQuery(query, this.metricsService, TenantFilter.getTenant(ctx))
                    .map(JsonUtil::toJson)
                    .doOnNext(ctx.response()::write)
                    .doOnError(error -> {
                        ctx.response().setStatusCode(HttpResponseStatus.BAD_REQUEST.code()).end(error.getMessage());
                    })
                    .doOnCompleted(ctx.response()::end)
                    .subscribeOn(Schedulers.io())
                    .subscribe();
        } catch (IllegalArgumentException e) {
            ctx.fail(new Exception(e.getMessage()));
        }
    }

    @POST
    @Path("/stats/batch/query")
    public void findStatsBatched(RoutingContext ctx) {
        Map<String, StatsQueryRequest> queries = null;
        try {
            TypeReference<Map<String, StatsQueryRequest>> typeRef = new TypeReference<Map<String, StatsQueryRequest>>() {
            };
            queries = objectMapper.reader(typeRef).readValue(ctx.getBodyAsString());
        } catch (Exception e) {
            ctx.fail(e);
            return;
        }

        try {
            queries.values().forEach(org.hawkular.metrics.api.jaxrs.handler.MetricHandler::checkRequiredParams);
            Map<String, Observable<Map<String, Map<String, List<? extends BucketPoint>>>>> results = new HashMap<>();
            queries.entrySet().forEach(entry -> results.put(entry.getKey(),
                    org.hawkular.metrics.api.jaxrs.handler.MetricHandler.doStatsQuery(entry.getValue(),
                            this.metricsService, TenantFilter.getTenant(ctx))));

            ctx.response().setChunked(true);
            Observable.from(results.entrySet())
                    .flatMap(entry -> entry.getValue()
                            .map(map -> ImmutableMap.of(entry.getKey(), map)))
                    .collect(HashMap::new, HashMap::putAll)
                    .map(JsonUtil::toJson)
                    .doOnNext(ctx.response()::write)
                    .doOnError(error -> {
                        ctx.response().setStatusCode(HttpResponseStatus.BAD_REQUEST.code()).end(error.getMessage());
                    })
                    .doOnCompleted(ctx.response()::end)
                    .subscribeOn(Schedulers.io())
                    .subscribe();
        } catch (IllegalArgumentException e) {
            ctx.fail(new Exception(e.getMessage()));
        }
    }
}
