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
package org.hawkular.metrics.clients.ptrans;

import static java.util.stream.Collectors.joining;
import static org.hawkular.metrics.clients.ptrans.util.Arguments.checkArgument;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.hawkular.metrics.clients.ptrans.backend.RestForwardingHandler;
import org.hawkular.metrics.clients.ptrans.collectd.CollectdChannelInitializer;
import org.hawkular.metrics.clients.ptrans.ganglia.GangliaChannelInitializer;
import org.hawkular.metrics.clients.ptrans.statsd.StatsdChannelInitializer;
import org.hawkular.metrics.clients.ptrans.syslog.TcpChannelInitializer;
import org.hawkular.metrics.clients.ptrans.syslog.UdpChannelInitializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * PTrans core. After an instance is created for a specific {@link org.hawkular.metrics.clients.ptrans.Configuration},
 * it must be started and eventually stopped.
 *
 * @author Heiko W. Rupp
 * @author Thomas Segismont
 */
public class PTrans {
    private static final Logger LOG = LoggerFactory.getLogger(PTrans.class);

    private final Configuration configuration;
    private final EventLoopGroup group;
    private final EventLoopGroup workerGroup;
    private final RestForwardingHandler forwardingHandler;

    /**
     * Creates a new PTrans instance for the given {@code configuration}.
     *
     * @param configuration a PTrans configuration object, must not be null
     *
     * @throws java.lang.IllegalArgumentException if the configuration parameter is null or invalid
     *
     * @see Configuration#isValid()
     */
    public PTrans(Configuration configuration) {
        checkArgument(configuration != null, "Configuration is null");
        checkArgument(configuration.isValid(), configuration.getValidationMessages().stream().collect(joining(", ")));
        this.configuration = configuration;
        group = new NioEventLoopGroup();
        workerGroup = new NioEventLoopGroup();
        forwardingHandler = new RestForwardingHandler(configuration);
    }

    /**
     * Starts this PTrans instance. the calling thread will be blocked until another thread calls {@link #stop()}.
     */
    public void start() {
        LOG.info("Starting ptrans...");

        Set<Service> services = configuration.getServices();
        List<ChannelFuture> closeFutures = new ArrayList<>(services.size());

        if (services.contains(Service.TCP)) {
            ServerBootstrap serverBootstrap = new ServerBootstrap()
                    .group(group, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .localAddress(configuration.getTcpPort())
                    .childHandler(new TcpChannelInitializer(configuration, forwardingHandler));
            ChannelFuture tcpBindFuture = serverBootstrap.bind().syncUninterruptibly();
            LOG.info("Server listening on TCP {}", tcpBindFuture.channel().localAddress());
            closeFutures.add(tcpBindFuture.channel().closeFuture());
        }

        if (services.contains(Service.UDP)) {
            Bootstrap udpBootstrap = new Bootstrap()
                    .group(group)
                    .channel(NioDatagramChannel.class)
                    .localAddress(configuration.getUdpPort())
                    .handler(new UdpChannelInitializer(forwardingHandler));
            ChannelFuture udpBindFuture = udpBootstrap.bind().syncUninterruptibly();
            LOG.info("Syslogd listening on UDP {}", udpBindFuture.channel().localAddress());
            closeFutures.add(udpBindFuture.channel().closeFuture());
        }

        if (services.contains(Service.GANGLIA)) {
            NetworkInterface mcIf;
            try {
                String multicastIfOverride = configuration.getMulticastIfOverride();
                if (multicastIfOverride == null) {
                    Inet4Address hostAddr = (Inet4Address) InetAddress.getLocalHost();
                    mcIf = NetworkInterface.getByInetAddress(hostAddr);
                } else {
                    mcIf = NetworkInterface.getByName(multicastIfOverride);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            InetSocketAddress gangliaSocket = new InetSocketAddress(
                    configuration.getGangliaGroup(), configuration.getGangliaPort()
            );
            Bootstrap gangliaBootstrap = new Bootstrap()
                    .group(group)
                    .channel(NioDatagramChannel.class)
                    .option(ChannelOption.SO_REUSEADDR, true)
                    .option(ChannelOption.IP_MULTICAST_IF, mcIf)
                    .localAddress(gangliaSocket)
                    .handler(new GangliaChannelInitializer(configuration, forwardingHandler));
            LOG.trace("Ganglia bootstrap is {}", gangliaBootstrap);
            ChannelFuture gangliaBindFuture = gangliaBootstrap.bind().syncUninterruptibly();
            LOG.info("Ganglia listening on UDP {}", gangliaBindFuture.channel().localAddress());
            DatagramChannel gangliaChannel = (DatagramChannel) gangliaBindFuture.channel();
            gangliaChannel.joinGroup(gangliaSocket, mcIf);
            LOG.trace("Joined the Ganglia group");
            closeFutures.add(gangliaChannel.closeFuture());
        }

        if (services.contains(Service.STATSD)) {
            Bootstrap statsdBootstrap = new Bootstrap()
                    .group(group)
                    .channel(NioDatagramChannel.class)
                    .localAddress(configuration.getStatsDport())
                    .handler(new StatsdChannelInitializer(configuration, forwardingHandler));
            ChannelFuture statsdBindFuture = statsdBootstrap.bind().syncUninterruptibly();
            LOG.info("Statsd listening on UDP {}", statsdBindFuture.channel().localAddress());
            closeFutures.add(statsdBindFuture.channel().closeFuture());
        }

        if (services.contains(Service.COLLECTD)) {
            Bootstrap collectdBootstrap = new Bootstrap()
                    .group(group)
                    .channel(NioDatagramChannel.class)
                    .localAddress(configuration.getCollectdPort())
                    .handler(new CollectdChannelInitializer(configuration, forwardingHandler));
            ChannelFuture collectdBindFuture = collectdBootstrap.bind().syncUninterruptibly();
            LOG.info("Collectd listening on UDP {}", collectdBindFuture.channel().localAddress());
            closeFutures.add(collectdBindFuture.channel().closeFuture());
        }

        LOG.info("ptrans started");

        closeFutures.forEach(ChannelFuture::syncUninterruptibly);
    }

    /**
     * Stops this PTrans instance.
     */
    public void stop() {
        LOG.info("Stopping ptrans...");
        group.shutdownGracefully().syncUninterruptibly();
        workerGroup.shutdownGracefully().syncUninterruptibly();
        LOG.info("ptrans stopped");
    }
}
