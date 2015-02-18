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

import java.net.InetSocketAddress;

import org.hawkular.metrics.clients.ptrans.collectd.event.CollectdEventsDecoder;
import org.hawkular.metrics.clients.ptrans.collectd.packet.CollectdPacketDecoder;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;

/**
 * Simple test program which dumps decoded {@link org.hawkular.metrics.clients.ptrans.collectd.event.Event} instances to
 * a logger. Start it with Maven:<br> <br> <code>mvn exec:java -Dexec.mainClass=org.hawkular.metrics.clients.ptrans
 * .collectd.DumpToLog</code>
 * <br> <br> Optionnaly the program accepts a single integer argument as UDP port (defaults to 25826).
 *
 * @author Thomas Segismont
 */
public class DumpToLog {
    private final EventLoopGroup group;
    private final Bootstrap bootstrap;

    public DumpToLog(InetSocketAddress address) {
        group = new NioEventLoopGroup();
        bootstrap = new Bootstrap();
        bootstrap.group(group).channel(NioDatagramChannel.class).handler(
                new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel channel) throws Exception {
                        ChannelPipeline pipeline = channel.pipeline();
                        pipeline.addLast(new CollectdPacketDecoder());
                        pipeline.addLast(new CollectdEventsDecoder());
                        pipeline.addLast(new DumpToLogEventHandler());
                    }
                }
        ).localAddress(address);

    }

    public Channel bind() {
        return bootstrap.bind().syncUninterruptibly().channel();
    }

    public void stop() {
        group.shutdownGracefully();
    }

    public static void main(String[] args) throws Exception {
        int port;
        if (args.length == 0) {
            port = 25826;
        } else {
            port = Integer.parseInt(args[0]);
        }
        DumpToLog dumpToLog = new DumpToLog(new InetSocketAddress(port));
        try {
            Channel channel = dumpToLog.bind();
            channel.closeFuture().await();
        } finally {
            dumpToLog.stop();
        }
    }
}
