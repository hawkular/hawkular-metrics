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
package org.hawkular.metrics.clients.ptrans.backend;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.hawkular.metrics.clients.ptrans.backend.Constants.METRIC_ADDRESS;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.hawkular.metrics.client.common.Batcher;
import org.hawkular.metrics.client.common.SingleMetric;
import org.hawkular.metrics.clients.ptrans.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpHeaders;

/**
 * @author Thomas Segismont
 */
public class MetricsSender extends AbstractVerticle {
    private static final Logger LOG = LoggerFactory.getLogger(MetricsSender.class);

    private final String host;
    private final int port;
    private final String postUri;
    private final CharSequence hostHeader;

    private final CharSequence tenant;

    private final int batchSize;
    private final long batchDelay;
    private final List<SingleMetric> queue;

    private HttpClient httpClient;
    private long timerId;
    private long sendTime;

    public MetricsSender(Configuration configuration) {
        URI restUrl = configuration.getRestUrl();
        URI httpProxy = configuration.getHttpProxy();
        if (httpProxy == null) {
            host = restUrl.getHost();
            port = restUrl.getPort();
            postUri = restUrl.getPath();
        } else {
            host = httpProxy.getHost();
            port = httpProxy.getPort();
            postUri = restUrl.toString();
        }
        hostHeader = HttpHeaders.createOptimized(restUrl.getHost());

        tenant = HttpHeaders.createOptimized(configuration.getTenant());

        batchSize = configuration.getMinimumBatchSize();
        batchDelay = configuration.getMaximumBatchDelay();
        queue = new ArrayList<>(batchSize);
    }

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        HttpClientOptions httpClientOptions = new HttpClientOptions().setDefaultHost(host)
                                                                     .setDefaultPort(port)
                                                                     .setKeepAlive(true)
                                                                     .setTryUseCompression(true);
        httpClient = vertx.createHttpClient(httpClientOptions);
        timerId = vertx.setPeriodic(MILLISECONDS.convert(batchDelay, SECONDS), this::flushIfIdle);
        sendTime = System.nanoTime();
        vertx.eventBus().registerDefaultCodec(SingleMetric.class, new SingleMetricCodec());
        vertx.eventBus().localConsumer(METRIC_ADDRESS, this::handleMetric)
             .completionHandler(v -> startFuture.complete());
    }

    private void handleMetric(Message<SingleMetric> metricMessage) {
        queue.add(metricMessage.body());
        if (queue.size() < batchSize) {
            return;
        }
        List<SingleMetric> metrics = new ArrayList<>(queue);
        queue.clear();
        do {
            List<SingleMetric> subList = metrics.subList(0, batchSize);
            send(subList);
            subList.clear();
        } while (metrics.size() >= batchSize);
        queue.addAll(metrics);
    }

    private void send(List<SingleMetric> metrics) {
        String json = Batcher.metricListToJson(metrics);
        Buffer buffer = Buffer.buffer(json);
        HttpClientRequest req = httpClient.post(
                postUri,
                response -> {
                    if (response.statusCode() != 200 && LOG.isTraceEnabled()) {
                        response.bodyHandler(
                                msg -> LOG.trace(
                                        "Could not send metrics: " + response.statusCode() + " : "
                                        + msg.toString()
                                )
                        );
                    }
                }
        );
        req.putHeader(HttpHeaders.HOST, hostHeader);
        req.putHeader(HttpHeaders.CONTENT_LENGTH, String.valueOf(buffer.length()));
        req.putHeader(HttpHeaders.CONTENT_TYPE, Constants.APPLICATION_JSON);
        req.putHeader(Constants.TENANT_HEADER_NAME, tenant);
        req.exceptionHandler(err -> LOG.trace("Could not send metrics", err));
        req.write(buffer);
        req.end();
        sendTime = System.nanoTime();
    }

    private void flushIfIdle(Long timerId) {
        if (System.nanoTime() - sendTime > NANOSECONDS.convert(batchDelay, SECONDS)
            && queue.size() > 0) {
            List<SingleMetric> metrics = new ArrayList<>(queue);
            queue.clear();
            send(metrics);
        }
    }

    @Override
    public void stop() throws Exception {
        vertx.cancelTimer(timerId);
        httpClient.close();
    }

    public static class SingleMetricCodec implements MessageCodec<SingleMetric, SingleMetric> {
        @Override
        public void encodeToWire(Buffer buffer, SingleMetric singleMetric) {
        }

        @Override
        public SingleMetric decodeFromWire(int pos, Buffer buffer) {
            return null;
        }

        @Override
        public SingleMetric transform(SingleMetric singleMetric) {
            return singleMetric;
        }

        @Override
        public String name() {
            return SingleMetricCodec.class.getCanonicalName();
        }

        @Override
        public byte systemCodecID() {
            return -1;
        }
    }
}
