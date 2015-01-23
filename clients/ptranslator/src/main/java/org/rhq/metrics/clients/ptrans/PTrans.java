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
package org.rhq.metrics.clients.ptrans;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.rhq.metrics.clients.ptrans.backend.RestForwardingHandler;
import org.rhq.metrics.clients.ptrans.collectd.CollectdEventHandler;
import org.rhq.metrics.clients.ptrans.ganglia.UdpGangliaDecoder;
import org.rhq.metrics.clients.ptrans.statsd.StatsdDecoder;
import org.rhq.metrics.clients.ptrans.syslog.UdpSyslogEventDecoder;
import org.rhq.metrics.netty.collectd.event.CollectdEventsDecoder;
import org.rhq.metrics.netty.collectd.packet.CollectdPacketDecoder;

/**
 * PTrans core. After an instance is created for a specific {@link org.rhq.metrics.clients.ptrans.Configuration}, it
 * must be started and eventually stopped.
 *
 * @author Heiko W. Rupp
 * @author Thomas Segismont
 */
public class PTrans {
    private static final Logger LOG = LoggerFactory.getLogger(PTrans.class);

    private final Configuration configuration;
    private final EventLoopGroup group;
    private final EventLoopGroup workerGroup;

    public PTrans(Configuration configuration) {
        this.configuration = configuration;
        group = new NioEventLoopGroup();
        workerGroup = new NioEventLoopGroup();
    }

    /**
     * Starts this PTrans instance. the calling thread will be blocked until another thread calls {@link #stop()}.
     *
     * @throws Exception if a problem occurs, like binding a port
     */
    public void start() throws Exception {
        final RestForwardingHandler forwardingHandler = new RestForwardingHandler(configuration);

        // The generic TCP socket server
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(group, workerGroup).channel(NioServerSocketChannel.class)
            .localAddress(configuration.getTcpPort()).childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel socketChannel) throws Exception {
                    ChannelPipeline pipeline = socketChannel.pipeline();
                    pipeline.addLast(new DemuxHandler(configuration, forwardingHandler));
                }
            });
        ChannelFuture graphiteFuture = serverBootstrap.bind().sync();
        LOG.info("Server listening on TCP " + graphiteFuture.channel().localAddress());
        graphiteFuture.channel().closeFuture();

        // The syslog UPD socket server
        Bootstrap udpBootstrap = new Bootstrap();
        udpBootstrap.group(group).channel(NioDatagramChannel.class).localAddress(configuration.getUdpPort())
            .handler(new ChannelInitializer<Channel>() {
                @Override
                public void initChannel(Channel socketChannel) throws Exception {
                    ChannelPipeline pipeline = socketChannel.pipeline();
                    pipeline.addLast(new UdpSyslogEventDecoder());

                    pipeline.addLast(forwardingHandler);
                }
            });
        ChannelFuture udpFuture = udpBootstrap.bind().sync();
        LOG.info("Syslogd listening on udp " + udpFuture.channel().localAddress());

        // Try to set up an upd listener for Ganglia Messages
        setupGangliaUdp(group, forwardingHandler);

        // Setup statsd listener
        setupStatsdUdp(group, forwardingHandler);

        // Setup collectd listener
        setupCollectdUdp(group, forwardingHandler);

        udpFuture.channel().closeFuture().sync();
    }

    private void setupCollectdUdp(EventLoopGroup group, final ChannelInboundHandlerAdapter forwardingHandler) {
        Bootstrap collectdBootstrap = new Bootstrap();
        collectdBootstrap.group(group).channel(NioDatagramChannel.class).localAddress(configuration.getCollectdPort())
            .handler(new ChannelInitializer<Channel>() {
                @Override
                public void initChannel(Channel socketChannel) throws Exception {
                    ChannelPipeline pipeline = socketChannel.pipeline();
                    pipeline.addLast(new CollectdPacketDecoder());
                    pipeline.addLast(new CollectdEventsDecoder());
                    pipeline.addLast(new CollectdEventHandler());
                    pipeline.addLast(new MetricBatcher("collectd", configuration.getMinimumBatchSize()));
                    pipeline.addLast(forwardingHandler);
                }
            });
        try {
            ChannelFuture collectdFuture = collectdBootstrap.bind().sync();
            LOG.info("Collectd listening on udp " + collectdFuture.channel().localAddress());
        } catch (InterruptedException e) {
            e.printStackTrace(); // TODO: Customise this generated block
        }
    }

    private void setupStatsdUdp(EventLoopGroup group, final ChannelInboundHandlerAdapter forwardingHandler) {
        Bootstrap statsdBootstrap = new Bootstrap();
        statsdBootstrap.group(group).channel(NioDatagramChannel.class).localAddress(configuration.getStatsDport())
            .handler(new ChannelInitializer<Channel>() {
                @Override
                public void initChannel(Channel socketChannel) throws Exception {
                    ChannelPipeline pipeline = socketChannel.pipeline();
                    pipeline.addLast(new StatsdDecoder());
                    pipeline.addLast(new MetricBatcher("statsd", configuration.getMinimumBatchSize()));
                    pipeline.addLast(forwardingHandler);
                }
            });
        try {
            ChannelFuture statsdFuture = statsdBootstrap.bind().sync();
            LOG.info("Statsd listening on udp " + statsdFuture.channel().localAddress());
        } catch (InterruptedException e) {
            e.printStackTrace(); // TODO: Customise this generated block
        }

    }

    private void setupGangliaUdp(EventLoopGroup group, final ChannelInboundHandlerAdapter fowardingHandler) {
        // The ganglia UPD socket server

        try {

            NetworkInterface mcIf;
            String multicastIfOverride = configuration.getMulticastIfOverride();
            if (multicastIfOverride == null) {
                Inet4Address hostAddr = (Inet4Address) InetAddress.getLocalHost();
                mcIf = NetworkInterface.getByInetAddress(hostAddr);
            } else {
                mcIf = NetworkInterface.getByName(multicastIfOverride);
            }

            InetSocketAddress gangliaSocket = new InetSocketAddress(configuration.getGangliaGroup(),
                configuration.getGangliaPort());

            Bootstrap gangliaBootstrap = new Bootstrap();
            gangliaBootstrap.group(group).channel(NioDatagramChannel.class).option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.IP_MULTICAST_IF, mcIf).localAddress(gangliaSocket)
                .handler(new ChannelInitializer<Channel>() {
                    @Override
                    public void initChannel(Channel socketChannel) throws Exception {
                        ChannelPipeline pipeline = socketChannel.pipeline();
                        pipeline.addLast(new UdpGangliaDecoder());
                        pipeline.addLast(new MetricBatcher("ganglia", configuration.getMinimumBatchSize()));
                        pipeline.addLast(fowardingHandler);
                    }
                });

            LOG.info("Bootstrap is " + gangliaBootstrap);
            ChannelFuture gangliaFuture = gangliaBootstrap.bind().sync();
            LOG.info("Ganglia listening on udp " + gangliaFuture.channel().localAddress());
            DatagramChannel channel = (DatagramChannel) gangliaFuture.channel();
            channel.joinGroup(gangliaSocket, mcIf).sync();
            LOG.info("Joined the group");
            channel.closeFuture();
        } catch (InterruptedException | SocketException | UnknownHostException e) {
            LOG.warn("Setup of udp multicast for Ganglia failed");
            e.printStackTrace();
        }
    }

    /**
     * Stops this PTrans instance.
     */
    public void stop() {
        LOG.info("Stopping ptrans...");
        Future<?> groupShutdownFuture = group.shutdownGracefully();
        Future<?> workerGroupShutdownFuture = workerGroup.shutdownGracefully();
        try {
            groupShutdownFuture.sync();
        } catch (InterruptedException ignored) {
        }
        try {
            workerGroupShutdownFuture.sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        LOG.info("Stopped");
    }
}
