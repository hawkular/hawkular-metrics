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

import static org.hawkular.metrics.clients.ptrans.backend.Constants.METRIC_ADDRESS;

import java.net.URI;
import java.util.List;

import org.hawkular.metrics.client.common.Batcher;
import org.hawkular.metrics.client.common.MetricBuffer;
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
 * Forwards metrics to the REST backend. This verticle consumes metrics published on the bus and inserts them in a
 * buffer. Metrics are sent as soon as the buffer is larger than the batch size. If servers are idle then the buffer is
 * flushed, regardless of its size.
 * <p>
 * When batches fail, the corresponding metrics are re-inserted in the buffer.
 *
 * @author Thomas Segismont
 */
public class MetricsSender extends AbstractVerticle {
    private static final Logger LOG = LoggerFactory.getLogger(MetricsSender.class);

    private final String host;
    private final int port;
    private final String postUri;
    private final CharSequence hostHeader;

    private final CharSequence tenant;

    private final MetricBuffer buffer;
    private final int batchSize;
    private final int maxConnections;

    private HttpClient httpClient;

    private int connectionsUsed;

    private boolean flushScheduled;
    private long flushScheduleId;

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

        buffer = new MetricBuffer(configuration.getBufferCapacity());
        batchSize = configuration.getBatchSize();
        maxConnections = configuration.getRestMaxConnections();
    }

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        HttpClientOptions httpClientOptions = new HttpClientOptions()
                .setDefaultHost(host)
                .setDefaultPort(port)
                .setKeepAlive(true)
                .setTryUseCompression(true)
                .setMaxPoolSize(maxConnections);
        httpClient = vertx.createHttpClient(httpClientOptions);

        connectionsUsed = 0;

        flushScheduled = false;

        vertx.eventBus().registerDefaultCodec(SingleMetric.class, new SingleMetricCodec());
        vertx.eventBus().localConsumer(METRIC_ADDRESS, this::handleMetric)
                .completionHandler(v -> startFuture.complete());
    }

    private void handleMetric(Message<SingleMetric> metricMessage) {
        buffer.insert(metricMessage.body());
        metricInserted();
    }

    private void metricInserted() {
        sendBatches(false);
        scheduleFlush();
    }

    private void sendBatches(boolean force) {
        for (int bufferSize = buffer.size(); ; bufferSize = buffer.size()) {
            if ((!force && bufferSize < batchSize) || bufferSize < 1) {
                break;
            }
            if (connectionsUsed >= maxConnections) {
                break;
            }
            List<SingleMetric> metrics = buffer.remove(Math.min(bufferSize, batchSize));
            send(metrics);
        }
    }

    private void scheduleFlush() {
        if (flushScheduled) {
            vertx.cancelTimer(flushScheduleId);
        } else {
            flushScheduled = true;
        }
        flushScheduleId = vertx.setTimer(10, h -> {
            flushScheduled = false;
            sendBatches(true);
        });
    }

    private void send(List<SingleMetric> metrics) {
        connectionsUsed++;
        String json = Batcher.metricListToJson(metrics);
        Buffer data = Buffer.buffer(json);
        HttpClientRequest req = httpClient.post(postUri, response -> {
            connectionsUsed--;
            if (response.statusCode() != 200) {
                if (LOG.isTraceEnabled()) {
                    response.bodyHandler(msg -> {
                        LOG.trace("Could not send metrics: " + response.statusCode() + " : " + msg.toString());
                    });
                }
                buffer.reInsert(metrics);
                metricInserted();
            } else {
                scheduleFlush();
            }
        });
        req.putHeader(HttpHeaders.HOST, hostHeader);
        req.putHeader(HttpHeaders.CONTENT_LENGTH, String.valueOf(data.length()));
        req.putHeader(HttpHeaders.CONTENT_TYPE, Constants.APPLICATION_JSON);
        req.putHeader(Constants.TENANT_HEADER_NAME, tenant);
        req.exceptionHandler(err -> {
            connectionsUsed--;
            LOG.trace("Could not send metrics", err);
            buffer.reInsert(metrics);
            metricInserted();
        });
        req.write(data);
        req.end();
    }

    @Override
    public void stop() throws Exception {
        if (flushScheduled) {
            vertx.cancelTimer(flushScheduleId);
        }
    }

    private static class SingleMetricCodec implements MessageCodec<SingleMetric, SingleMetric> {
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
