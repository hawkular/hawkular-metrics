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
package org.hawkular.metrics.clients.ptrans.collectd;

import static org.hawkular.metrics.clients.ptrans.backend.Constants.METRIC_ADDRESS;

import java.util.List;
import java.util.ListIterator;

import org.hawkular.metrics.clients.ptrans.Configuration;
import org.hawkular.metrics.clients.ptrans.collectd.event.CollectdEventsDecoder;
import org.hawkular.metrics.clients.ptrans.collectd.event.Event;
import org.hawkular.metrics.clients.ptrans.collectd.event.TimeResolution;
import org.hawkular.metrics.clients.ptrans.collectd.event.TimeSpan;
import org.hawkular.metrics.clients.ptrans.collectd.event.ValueListEvent;
import org.hawkular.metrics.clients.ptrans.collectd.packet.CollectdPacket;
import org.hawkular.metrics.clients.ptrans.collectd.packet.CollectdPacketDecoder;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.datagram.DatagramPacket;
import io.vertx.core.datagram.DatagramSocket;
import io.vertx.core.json.JsonObject;

/**
 * @author Thomas Segismont
 */
public class CollectdServer extends AbstractVerticle {
    private final int port;
    private final CollectdPacketDecoder packetDecoder;
    private final CollectdEventsDecoder eventsDecoder;

    public CollectdServer(Configuration configuration) {
        port = configuration.getCollectdPort();
        packetDecoder = new CollectdPacketDecoder();
        eventsDecoder = new CollectdEventsDecoder();
    }

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        DatagramSocket socket = vertx.createDatagramSocket();
        socket.handler(this::handlePacket);
        socket.listen(port, "0.0.0.0", result -> {
            if (result.succeeded()) {
                startFuture.complete();
            } else {
                startFuture.fail(result.cause());
            }
        });
    }

    private void handlePacket(DatagramPacket packet) {
        CollectdPacket collectdPacket = packetDecoder.decode(packet);
        if (collectdPacket == null) {
            return;
        }
        List<Event> events = eventsDecoder.decode(collectdPacket);
        events.forEach(event -> {
            if (event instanceof ValueListEvent) {
                handleValueListEvent((ValueListEvent) event);
            }
        });
    }

    private void handleValueListEvent(ValueListEvent event) {
        StringBuilder prefixBuilder = new StringBuilder().append(event.getHost()).append(".")
                .append(event.getPluginName());
        String pluginInstance = event.getPluginInstance();
        if (pluginInstance != null && pluginInstance.length() > 0) {
            prefixBuilder.append(".").append(pluginInstance);
        }
        prefixBuilder.append(".").append(event.getTypeName());
        String typeInstance = event.getTypeInstance();
        if (typeInstance != null && typeInstance.length() > 0) {
            prefixBuilder.append(".").append(typeInstance);
        }
        String prefix = prefixBuilder.toString();
        TimeSpan timeSpan = event.getTimestamp();
        List<Number> values = event.getValues();
        for (ListIterator<Number> iterator = values.listIterator(); iterator.hasNext(); ) {
            Number value = iterator.next();
            long timestamp = TimeResolution.toMillis(timeSpan);
            StringBuilder sourceBuilder = new StringBuilder(prefix);
            if (values.size() > 1) {
                sourceBuilder.append(".").append(iterator.previousIndex());
            }
            JsonObject metric = new JsonObject()
                    .put("id", sourceBuilder.toString())
                    .put("timestamp", timestamp)
                    .put("value", value.doubleValue());
            vertx.eventBus().publish(METRIC_ADDRESS, metric);
        }
    }
}
