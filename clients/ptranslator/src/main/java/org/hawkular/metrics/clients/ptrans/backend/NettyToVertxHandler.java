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
package org.hawkular.metrics.clients.ptrans.backend;

import static org.hawkular.metrics.clients.ptrans.backend.Constants.METRIC_ADDRESS;

import java.util.List;

import org.hawkular.metrics.client.common.SingleMetric;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.vertx.core.eventbus.EventBus;

/**
 * A channel used during transition from vanilla Netty to vertx 3. At the time of writing only collectd and graphite
 * servers have been refactored. This class can be deleted as soon as all servers are implemented on top of vertx.
 *
 * @author Thomas Segismont
 */
@Sharable
public class NettyToVertxHandler extends SimpleChannelInboundHandler<Object> {
    private final EventBus eventBus;

    public NettyToVertxHandler(EventBus eventBus) {
        this.eventBus = eventBus;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object object) throws Exception {
        if (object instanceof SingleMetric) {
            SingleMetric singleMetric = (SingleMetric) object;
            eventBus.publish(METRIC_ADDRESS, SingleMetricConverter.toJsonObject(singleMetric));
        } else if (object instanceof List) {
            List<?> objects = (List<?>) object;
            objects.stream()
                   .filter(o -> o instanceof SingleMetric)
                    .map(o -> (SingleMetric) o)
                    .map(SingleMetricConverter::toJsonObject)
                    .forEach(jsonObject -> eventBus.publish(METRIC_ADDRESS, jsonObject));
        }
    }
}
