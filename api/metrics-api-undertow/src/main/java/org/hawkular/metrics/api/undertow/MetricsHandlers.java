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
package org.hawkular.metrics.api.undertow;

import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.RoutingHandler;
import io.undertow.util.Methods;

import java.nio.channels.Channels;
import java.util.Deque;
import java.util.List;
import java.util.Map;

import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.MetricsService;
import org.hawkular.metrics.core.api.NumericMetric;
import org.hawkular.metrics.core.impl.HawkularMetrics;

import com.fasterxml.jackson.annotation.JsonInclude;
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
        metricsService = getMetricsService();
        mapper = new ObjectMapper();
        mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
    }

    public void setup(RoutingHandler commonHandler) {

        commonHandler.add(Methods.POST, "/{tenantId}/metrics/numeric", new AsyncHttpHandler() {

            public void handleRequestAsync(HttpServerExchange exchange) throws Exception {
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

        commonHandler.add(Methods.POST, "/{tenantId}/metrics/numeric/data", new AsyncHttpHandler() {
            public void handleRequestAsync(HttpServerExchange exchange) throws Exception {
                List<NumericMetric> metrics = mapper.readValue(Channels.newInputStream(exchange.getRequestChannel()),
                        new TypeReference<List<NumericMetric>>() {
                        });
                Map<String, Deque<String>> queryParams = exchange.getQueryParameters();
                String tenantId = queryParams.get("tenantId").getFirst();

                if (metrics == null || metrics.size() == 0) {
                    exchange.setResponseCode(200);
                    return;
                }

                for (NumericMetric metric : metrics) {
                    metric.setTenantId(tenantId);
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

    public MetricsService getMetricsService() {
        HawkularMetrics.Builder metricsServiceBuilder = new HawkularMetrics.Builder();
        return metricsServiceBuilder.build();
    }

    public abstract static class AsyncHttpHandler implements HttpHandler {

        abstract void handleRequestAsync(HttpServerExchange exchange) throws Exception;

        @Override
        public void handleRequest(HttpServerExchange exchange) throws Exception {
            exchange.dispatch(new Runnable() {
                @Override
                public void run() {
                    try {
                        handleRequestAsync(exchange);
                    } catch (Exception e) {
                        System.out.println(e);
                        e.printStackTrace();
                        exchange.setResponseCode(501);

                    }

                    exchange.endExchange();
                }
            });
        }
    }
}