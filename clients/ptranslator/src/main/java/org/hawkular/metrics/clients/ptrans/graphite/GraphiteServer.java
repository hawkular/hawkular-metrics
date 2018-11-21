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
package org.hawkular.metrics.clients.ptrans.graphite;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.hawkular.metrics.clients.ptrans.backend.Constants.METRIC_ADDRESS;

import org.hawkular.metrics.clients.ptrans.Configuration;
import org.jboss.logging.Logger;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.NetServer;
import io.vertx.core.parsetools.RecordParser;

/**
 * A TCP server for the Graphite plaintext protocol.
 *
 * @author Thomas Segismont
 */
public class GraphiteServer extends AbstractVerticle {
    private static final Logger log = Logger.getLogger(GraphiteServer.class);

    private final int port;
    private final RecordParser recordParser;

    public GraphiteServer(Configuration configuration) {
        port = configuration.getGraphitePort();
        recordParser = RecordParser.newDelimited("\n", this::handleRecord);
    }

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        NetServer tcpServer = vertx.createNetServer();
        tcpServer.connectHandler(socket -> {
            socket.handler(recordParser);
        });
        tcpServer.listen(port, result -> {
            if (result.succeeded()) {
                startFuture.complete();
            } else {
                startFuture.fail(result.cause());
            }
        });
    }

    private void handleRecord(Buffer buf) {
        String msg = buf.toString("UTF-8");

        String[] items = msg.split(" ");
        if (items.length != 3) {
            log.tracef("Unknown data format for '%s', skipping", msg);
            return;
        }

        String name = items[0];
        double value = Double.parseDouble(items[1]);
        long timestamp = MILLISECONDS.convert(Long.parseLong(items[2]), SECONDS);

        JsonObject metric = new JsonObject()
                .put("id", name)
                .put("timestamp", timestamp)
                .put("value", value);
        vertx.eventBus().publish(METRIC_ADDRESS, metric);
    }
}
