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

import static org.hawkular.metrics.api.jaxrs.filter.TenantFilter.TENANT_HEADER_NAME;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import javax.inject.Inject;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;

import org.hawkular.metrics.api.jaxrs.handler.observer.NamedDataPointObserver;
import org.hawkular.metrics.core.service.MetricsService;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.MetricType;
import org.hawkular.metrics.model.exception.RuntimeApiError;
import org.hawkular.metrics.model.param.TimeRange;

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

    <T> NamedDataPointObserver<T> createNamedDataPointObserver(AsyncResponse response, MetricType<T> type) {
        return new NamedDataPointObserver<>(response, mapper, type);
    }

    <T> Observable<MetricId<T>> findMetricsByNameOrTag(List<String> metricNames, String tags, MetricType<T> type) {
        if ((metricNames == null || metricNames.isEmpty()) && tags == null) {
            return Observable.error(new RuntimeApiError("Either metrics or tags query parameters must be used"));
        }
        if (metricNames != null && !metricNames.isEmpty() && tags != null) {
            return Observable.error(new RuntimeApiError("Cannot use both the metrics and tags query parameters"));
        }
        if (metricNames != null && !metricNames.isEmpty()) {
            return Observable.from(metricNames)
                    .map(id -> new MetricId<>(getTenant(), type, id));
        }

        return metricsService.findMetricIdentifiersWithFilters(getTenant(), type, tags);
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
