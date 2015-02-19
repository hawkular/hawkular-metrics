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
package org.hawkular.metrics.clients.ptrans.collectd;

import org.hawkular.metrics.clients.ptrans.Configuration;
import org.hawkular.metrics.clients.ptrans.MetricBatcher;
import org.hawkular.metrics.clients.ptrans.backend.RestForwardingHandler;
import org.hawkular.metrics.clients.ptrans.collectd.event.CollectdEventsDecoder;
import org.hawkular.metrics.clients.ptrans.collectd.packet.CollectdPacketDecoder;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.timeout.IdleStateHandler;

/**
 * Channel initializer to be used when bootstrapping a collectd server.
 *
 * @author Thomas Segismont
 */
public class CollectdChannelInitializer extends ChannelInitializer<Channel> {
    private final Configuration configuration;
    private final RestForwardingHandler forwardingHandler;

    public CollectdChannelInitializer(Configuration configuration, RestForwardingHandler forwardingHandler) {
        this.configuration = configuration;
        this.forwardingHandler = forwardingHandler;
    }

    @Override
    public void initChannel(Channel socketChannel) throws Exception {
        ChannelPipeline pipeline = socketChannel.pipeline();
        pipeline.addLast(new CollectdPacketDecoder());
        pipeline.addLast(new CollectdEventsDecoder());
        pipeline.addLast(new CollectdEventHandler());
        pipeline.addLast(new IdleStateHandler(configuration.getMaximumBatchDelay(), 0, 0));
        pipeline.addLast(new MetricBatcher("collectd", configuration.getMinimumBatchSize()));
        pipeline.addLast(forwardingHandler);
    }
}
