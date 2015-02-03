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
package org.rhq.metrics.api.undertow;

import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.RoutingHandler;
import io.undertow.util.Methods;

import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;

import org.rhq.metrics.RHQMetrics;
import org.rhq.metrics.core.MetricId;
import org.rhq.metrics.core.MetricsService;
import org.rhq.metrics.core.NumericMetric;
import org.rhq.metrics.restServlet.NumericDataParams;
import org.rhq.metrics.restServlet.NumericDataPoint;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public class MetricsHandlers {
    private final MetricsService metricsService;
    private final ObjectMapper mapper;

    public MetricsHandlers() {
        metricsService = getMetricsService("mem");
        mapper = new ObjectMapper();
        mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    }

    public void setup(RoutingHandler commonHandler) {

        commonHandler.add(Methods.POST, "/{tenantId}/metrics/numeric", new HttpHandler() {
            @Override
            public void handleRequest(HttpServerExchange exchange) throws Exception {
                @SuppressWarnings("unchecked")
                Map<String, Object> body = mapper.readValue(Channels.newInputStream(exchange.getRequestChannel()),
                        Map.class);
                Map<String, Deque<String>> queryParams = exchange.getQueryParameters();

                String tenantId = queryParams.get("tenantId").getFirst();
                String name = body.get("name").toString();
                Integer dataRetention = Integer.parseInt(body.get("dataRetention").toString());

                NumericMetric metric = new NumericMetric(tenantId, new MetricId(name), null, dataRetention);
                ListenableFuture<Void> future = metricsService.createMetric(metric);
                Futures.addCallback(future, new FutureCallback<Void>() {
                    @Override
                    public void onSuccess(Void result) {
                        exchange.setResponseCode(200);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        exchange.setResponseCode(501);
                    }
                });
            }
        });

        commonHandler.add(Methods.POST, "/{tenantId}/metrics/numeric/data", new HttpHandler() {

            @Override
            public void handleRequest(HttpServerExchange exchange) throws Exception {
                List<NumericDataParams> body = mapper.readValue(Channels.newInputStream(exchange.getRequestChannel()),
                        new TypeReference<List<NumericDataParams>>() {
                        });
                Map<String, Deque<String>> queryParams = exchange.getQueryParameters();
                String tenantId = queryParams.get("tenantId").getFirst();


                if (body.size() == 0) {
                    exchange.setResponseCode(200);
                }

                List<NumericMetric> metrics = new ArrayList<>(body.size());
                for(NumericDataParams params : body) {
                    NumericMetric metric = new NumericMetric(tenantId, new MetricId(params.getName()), params
                            .getMetadata());

                    for (NumericDataPoint p : params.getData()) {
                        metric.addData(p.getTimestamp(), p.getValue());
                    }

                    metrics.add(metric);
                }

                ListenableFuture<Void> future = metricsService.addNumericData(metrics);
                Futures.addCallback(future, new FutureCallback<Void>() {
                    @Override
                    public void onSuccess(Void result) {
                        exchange.setResponseCode(200);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        exchange.setResponseCode(501);
                    }
                });
            }
        });
    }

    public MetricsService getMetricsService(String backend) {
        RHQMetrics.Builder metricsServiceBuilder = new RHQMetrics.Builder();
        if (backend != null) {
            switch (backend) {
            case "cass":
                metricsServiceBuilder.withCassandraDataStore();
                break;
            case "mem":
                metricsServiceBuilder.withInMemoryDataStore();
                break;
            case "embedded_cass":
            default:
                metricsServiceBuilder.withCassandraDataStore();
            }
        } else {
            metricsServiceBuilder.withCassandraDataStore();
        }

        return metricsServiceBuilder.build();
    }

}