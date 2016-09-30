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

import static org.hawkular.metrics.api.jaxrs.filter.TenantFilter.TENANT_HEADER_NAME;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.validation.constraints.NotNull;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;

import org.hawkular.metrics.api.jaxrs.handler.observer.NamedDataPointObserver;
import org.hawkular.metrics.api.jaxrs.param.TagsConverter;
import org.hawkular.metrics.core.service.MetricsService;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.MetricType;
import org.hawkular.metrics.model.exception.RuntimeApiError;
import org.hawkular.metrics.model.param.Tags;
import org.hawkular.metrics.model.param.TimeRange;
import org.jboss.resteasy.spi.ResteasyProviderFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import rx.Observable;

/**
 * @author jsanda
 */
abstract class MetricsServiceHandler {

    @Inject
    protected MetricsService metricsService;

    @Inject
    protected ObjectMapper mapper;

    @Context
    protected HttpHeaders httpHeaders;

    protected String getTenant() {
        return httpHeaders.getRequestHeaders().getFirst(TENANT_HEADER_NAME);
    }

    <T> NamedDataPointObserver<T>  createNamedDataPointObserver(MetricType<T> type) {
        HttpServletRequest request = ResteasyProviderFactory.getContextData(HttpServletRequest.class);
        HttpServletResponse response = ResteasyProviderFactory.getContextData(HttpServletResponse.class);
        return new NamedDataPointObserver<>(request, response, mapper, type);
    }

    <T> Observable<MetricId<T>> findMetricsByNameOrTag(List<String> metricNames, String tags, MetricType<T> type) {
        List<String> nonNullNames = metricNames == null ? Collections.emptyList() : metricNames;
        Map<String, String> mapTags = TagsConverter.fromNullable(tags).getTags();
        return findMetricsByNameOrTag(nonNullNames, mapTags, type);
    }

    <T> Observable<MetricId<T>> findMetricsByNameOrTag(List<String> metricNames, Tags tags, MetricType<T> type) {
        List<String> nonNullNames = metricNames == null ? Collections.emptyList() : metricNames;
        Map<String, String> mapTags = tags == null ? Collections.emptyMap() : tags.getTags();
        return findMetricsByNameOrTag(nonNullNames, mapTags, type);
    }

    private <T> Observable<MetricId<T>> findMetricsByNameOrTag(@NotNull List<String> metricNames, @NotNull Map<String,
            String> tags, MetricType<T> type) {

        if (metricNames.isEmpty() && tags.isEmpty()) {
            return Observable.error(new RuntimeApiError("Either metrics or tags parameter must be used"));
        }
        if (!metricNames.isEmpty() && !tags.isEmpty()) {
            return Observable.error(new RuntimeApiError("Cannot use both the metrics and tags parameters"));
        }

        if (!metricNames.isEmpty()) {
            return Observable.from(metricNames)
                    .map(id -> new MetricId<>(getTenant(), type, id));
        }
        // Tags case
        return metricsService.findMetricsWithFilters(getTenant(), type, tags)
                .map(Metric::getMetricId);
    }

    <T> Observable<TimeRange> findTimeRange(String start, String end, Boolean fromEarliest,
                                                    Collection<MetricId<T>> metricIds) {
        if (Boolean.TRUE.equals(fromEarliest)) {
            if (start != null || end != null) {
                return Observable.error(new RuntimeApiError("fromEarliest can only be used without start & end"));
            }
            return Observable.from(metricIds)
                    .flatMap(metricsService::findMetric)
                    .toList()
                    .map(metrics -> metrics.stream()
                            .map(Metric::getDataRetention)
                            .filter(r -> r != null)
                            .reduce(Math::max)
                            .map(maxRetention -> {
                                long dataRetention = maxRetention * 24 * 60 * 60 * 1000L;
                                long now = System.currentTimeMillis();
                                long earliest = now - dataRetention;
                                return new TimeRange(earliest, now);
                            }))
                    .filter(Optional::isPresent)
                    .map(Optional::get);
        }

        TimeRange timeRange = new TimeRange(start, end);
        if (!timeRange.isValid()) {
            return Observable.error(new RuntimeApiError(timeRange.getProblem()));
        }
        return Observable.just(timeRange);
    }
}
