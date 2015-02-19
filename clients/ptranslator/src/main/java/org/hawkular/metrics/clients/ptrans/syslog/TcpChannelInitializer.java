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
package org.hawkular.metrics.clients.ptrans.syslog;

import org.hawkular.metrics.clients.ptrans.Configuration;
import org.hawkular.metrics.clients.ptrans.DemuxHandler;
import org.hawkular.metrics.clients.ptrans.backend.RestForwardingHandler;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;

/**
 * Channel initializer to be used when bootstrapping a TCP server.
 *
 * @author Thomas Segismont
 */
public class TcpChannelInitializer extends ChannelInitializer<SocketChannel> {
    private final Configuration configuration;
    private final RestForwardingHandler forwardingHandler;

    public TcpChannelInitializer(Configuration configuration, RestForwardingHandler forwardingHandler) {
        this.configuration = configuration;
        this.forwardingHandler = forwardingHandler;
    }

    @Override
    public void initChannel(SocketChannel socketChannel) throws Exception {
        ChannelPipeline pipeline = socketChannel.pipeline();
        pipeline.addLast(new DemuxHandler(configuration, forwardingHandler));
    }
}
