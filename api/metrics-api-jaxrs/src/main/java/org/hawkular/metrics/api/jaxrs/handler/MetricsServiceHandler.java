/*
 * Copyright 2014-2016 Red Hat, Inc. and/or its affiliates
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

import static java.util.stream.Collectors.toList;

import static org.hawkular.metrics.api.jaxrs.filter.TenantFilter.TENANT_HEADER_NAME;
import static org.hawkular.metrics.api.jaxrs.util.ApiUtils.badRequest;
import static org.hawkular.metrics.model.MetricType.COUNTER;
import static org.hawkular.metrics.model.MetricType.COUNTER_RATE;
import static org.hawkular.metrics.model.MetricType.GAUGE;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;

import org.hawkular.metrics.api.jaxrs.QueryRequest;
import org.hawkular.metrics.api.jaxrs.handler.observer.NamedDataPointObserver;
import org.hawkular.metrics.api.jaxrs.param.TagsConverter;
import org.hawkular.metrics.core.service.MetricsService;
import org.hawkular.metrics.core.service.Order;
import org.hawkular.metrics.model.ApiError;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.MetricType;
import org.hawkular.metrics.model.NamedDataPoint;
import org.hawkular.metrics.model.param.TimeRange;
import org.jboss.resteasy.spi.ResteasyProviderFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import rx.Observable;
import rx.schedulers.Schedulers;

/**
 * @author jsanda
 */
public abstract class MetricsServiceHandler {

    @Inject
    protected MetricsService metricsService;

    @Inject
    protected ObjectMapper mapper;

    @Context
    protected HttpHeaders httpHeaders;

    protected String getTenant() {
        return httpHeaders.getRequestHeaders().getFirst(TENANT_HEADER_NAME);
    }

    protected <T> void findRawDataPointsForMetrics(AsyncResponse asyncResponse, QueryRequest query,
                                                   MetricType<T> type) {
        TimeRange timeRange = new TimeRange(query.getStart(), query.getEnd());
        if (!timeRange.isValid()) {
            asyncResponse.resume(badRequest(new ApiError(timeRange.getProblem())));
            return;
        }

        int limit;
        if (query.getLimit() == null) {
            limit = 0;
        } else {
            limit = query.getLimit();
        }
        Order order;
        if (query.getOrder() == null) {
            order = Order.defaultValue(limit, timeRange.getStart(), timeRange.getEnd());
        } else {
            order = Order.fromText(query.getOrder());
        }

        Map<String, String> tags = TagsConverter.fromNullable(query.getTags()).getTags();
        List<String> ids = query.getIds() == null ? Collections.emptyList() : query.getIds();

        if (ids.isEmpty() && tags.isEmpty()) {
            asyncResponse.resume(badRequest(new ApiError("Either metrics or tags parameter must be used")));
            return;
        }
        if (!ids.isEmpty() && !tags.isEmpty()) {
            asyncResponse.resume(badRequest(new ApiError("Cannot use both the metrics and tags parameters")));
            return;
        }

        final Observable<NamedDataPoint<T>> dataPoints;
        if (tags.isEmpty()) {
            List<MetricId<T>> metricIds = ids.stream().map(id -> new MetricId<>(getTenant(), type, id))
                    .collect(toList());
            dataPoints = metricsService.findDataPoints(metricIds, timeRange.getStart(),
                    timeRange.getEnd(), limit, order).observeOn(Schedulers.io());
        } else {
            dataPoints = metricsService.findDataPoints(getTenant(), type, tags, timeRange.getStart(),
                    timeRange.getEnd(), limit, order).observeOn(Schedulers.io());
        }

        HttpServletRequest request = ResteasyProviderFactory.getContextData(HttpServletRequest.class);
        HttpServletResponse response = ResteasyProviderFactory.getContextData(HttpServletResponse.class);

        dataPoints.subscribe(new NamedDataPointObserver<>(request, response, mapper, type));
    }

    protected void findRateDataPointsForMetrics(AsyncResponse asyncResponse, QueryRequest query,
                                                MetricType<? extends Number> type) {
        TimeRange timeRange = new TimeRange(query.getStart(), query.getEnd());
        if (!timeRange.isValid()) {
            asyncResponse.resume(badRequest(new ApiError(timeRange.getProblem())));
            return;
        }

        int limit;
        if (query.getLimit() == null) {
            limit = 0;
        } else {
            limit = query.getLimit();
        }
        Order order;
        if (query.getOrder() == null) {
            order = Order.defaultValue(limit, timeRange.getStart(), timeRange.getEnd());
        } else {
            order = Order.fromText(query.getOrder());
        }

        Map<String, String> tags = TagsConverter.fromNullable(query.getTags()).getTags();
        List<String> ids = query.getIds() == null ? Collections.emptyList() : query.getIds();

        if (ids.isEmpty() && tags.isEmpty()) {
            asyncResponse.resume(badRequest(new ApiError("Either metrics or tags parameter must be used")));
            return;
        }
        if (!ids.isEmpty() && !tags.isEmpty()) {
            asyncResponse.resume(badRequest(new ApiError("Cannot use both the metrics and tags parameters")));
            return;
        }

        final Observable<NamedDataPoint<Double>> dataPoints;
        if (tags.isEmpty()) {
            List<MetricId<? extends Number>> metricIds = ids.stream().map(id -> new MetricId<>(getTenant(), type, id))
                    .collect(toList());
            dataPoints = metricsService.findRateData(metricIds, timeRange.getStart(),
                    timeRange.getEnd(), limit, order).observeOn(Schedulers.io());
        } else {
            dataPoints = metricsService.findRateData(getTenant(), type, tags, timeRange.getStart(),
                    timeRange.getEnd(), limit, order).observeOn(Schedulers.io());
        }

        HttpServletRequest request = ResteasyProviderFactory.getContextData(HttpServletRequest.class);
        HttpServletResponse response = ResteasyProviderFactory.getContextData(HttpServletResponse.class);

        if (type == GAUGE) {
            dataPoints.subscribe(new NamedDataPointObserver<>(request, response, mapper, GAUGE));
        } else if (type == COUNTER) {
            dataPoints.subscribe(new NamedDataPointObserver<>(request, response, mapper, COUNTER_RATE));
        } else {
            throw new IllegalArgumentException(type + " is not a supported metric type for rate data points");
        }
    }
}
