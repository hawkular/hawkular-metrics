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

import java.util.List;

import org.hawkular.handlers.RestEndpoint;
import org.hawkular.handlers.RestHandler;
import org.hawkular.metrics.api.MetricsApp;
import org.hawkular.metrics.api.filter.TenantFilter;
import org.hawkular.metrics.api.handler.observer.ResultSetObserver;
import org.hawkular.metrics.api.util.Wrappers;
import org.hawkular.metrics.core.service.Functions;
import org.hawkular.metrics.core.service.MetricsService;
import org.hawkular.metrics.model.AvailabilityType;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricType;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import rx.Observable;
import rx.schedulers.Schedulers;

@RestEndpoint(path = "/")
public class GenericInserter implements RestHandler {

    private MetricsService metricsService;

    private ObjectMapper objectMapper;

    @Override
    public void initRoutes(String baseUrl, Router router) {
        Wrappers.setupTenantRoute(router, HttpMethod.POST, (baseUrl + "/counters/raw"), ctx -> {
            this.addData(ctx, COUNTER);
        });
        Wrappers.setupTenantRoute(router, HttpMethod.POST, (baseUrl + "/gauges/raw"), ctx -> {
            this.addData(ctx, GAUGE);
        });
        Wrappers.setupTenantRoute(router, HttpMethod.POST, (baseUrl + "/availability/raw"), ctx -> {
            this.addData(ctx, AVAILABILITY);
        });
        Wrappers.setupTenantRoute(router, HttpMethod.POST, (baseUrl + "/strings/raw"), ctx -> {
            this.addData(ctx, STRING);
        });

        metricsService = MetricsApp.msl.getMetricsService();
        objectMapper = MetricsApp.msl.objectMapper;
    }

    public <T> void addData(RoutingContext ctx, MetricType<T> type) {
        List<Metric<T>> metrics = null;
        try {

            TypeReference<?> typeRef = new TypeReference<List<Metric<String>>>() {
            };
            if (type.equals(GAUGE)) {
                typeRef = new TypeReference<List<Metric<Double>>>() {
                };
            } else if (type.equals(COUNTER)) {
                typeRef = new TypeReference<List<Metric<Long>>>() {
                };
            } else if (type.equals(AVAILABILITY)) {
                typeRef = new TypeReference<List<Metric<AvailabilityType>>>() {
                };
            }

            metrics = objectMapper.reader(typeRef).readValue(ctx.getBodyAsString());
            System.out.println(metrics);
        } catch (Exception e) {
            ctx.fail(e);
            return;
        }

        Observable<Metric<T>> metricsObservable = Functions.metricToObservable(TenantFilter.getTenant(ctx),
                metrics, type);
        Observable<Void> observable = metricsService.addDataPoints(type, metricsObservable);
        observable.subscribeOn(Schedulers.io()).subscribe(new ResultSetObserver(ctx));
    }
}
