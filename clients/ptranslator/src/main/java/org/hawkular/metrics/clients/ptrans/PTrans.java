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
package org.hawkular.metrics.clients.ptrans;

import static java.util.stream.Collectors.joining;

import static org.hawkular.metrics.clients.ptrans.util.Arguments.checkArgument;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.hawkular.metrics.clients.ptrans.backend.Constants;
import org.hawkular.metrics.clients.ptrans.backend.NettyToVertxHandler;
import org.hawkular.metrics.clients.ptrans.collectd.CollectdServer;
import org.hawkular.metrics.clients.ptrans.ganglia.GangliaChannelInitializer;
import org.hawkular.metrics.clients.ptrans.graphite.GraphiteServer;
import org.hawkular.metrics.clients.ptrans.log.PTransLogger;
import org.hawkular.metrics.clients.ptrans.log.PTransLogging;
import org.hawkular.metrics.clients.ptrans.statsd.StatsdChannelInitializer;
import org.hawkular.metrics.clients.ptrans.syslog.TcpChannelInitializer;
import org.hawkular.metrics.clients.ptrans.syslog.UdpChannelInitializer;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.ext.hawkular.AuthenticationOptions;
import io.vertx.ext.hawkular.VertxHawkularOptions;

/**
 * PTrans core. After an instance is created for a specific {@link org.hawkular.metrics.clients.ptrans.Configuration},
 * it must be started and eventually stopped.
 *
 * @author Heiko W. Rupp
 * @author Thomas Segismont
 */
public class PTrans {
    private static final PTransLogger log = PTransLogging.getPTransLogger(PTrans.class);

    private final Configuration configuration;

    private EventLoopGroup group;
    private EventLoopGroup workerGroup;
    private Vertx vertx;
    // Needed as long as some servers are still Netty-based
    // Can be removed as soon as all servers are implemented on top of vertx
    private NettyToVertxHandler nettyToVertxHandler;

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
    }

    /**
     * Starts this PTrans instance. the calling thread will be blocked until another thread calls {@link #stop()}.
     */
    public void start() throws InterruptedException {
        log.infoStarting();

        group = new NioEventLoopGroup();
        workerGroup = new NioEventLoopGroup();

        URI metricsUrl = configuration.getMetricsUrl();

        VertxHawkularOptions metricsOptions = new VertxHawkularOptions()
                .setEnabled(true)
                .setHost(metricsUrl.getHost())
                .setPort(metricsUrl.getPort())
                .setMetricsServiceUri(metricsUrl.getPath())
                .setHttpOptions(new HttpClientOptions()
                        .setMaxPoolSize(configuration.getMaxConnections())
                        .setSsl("https".equalsIgnoreCase(metricsUrl.getScheme())))
                .setTenant(configuration.getTenant())
                .setSendTenantHeader(configuration.isSendTenant())
                .setAuthenticationOptions(new AuthenticationOptions()
                        .setEnabled(configuration.isAuthEnabled())
                        .setId(configuration.getAuthId())
                        .setSecret(configuration.getAuthSecret()))
                .setHttpHeaders(configuration.getHttpHeaders())
                .setBatchSize(configuration.getBatchSize())
                .setMetricsBridgeEnabled(true)
                .setMetricsBridgeAddress(Constants.METRIC_ADDRESS);

        vertx = Vertx.vertx(new VertxOptions().setMetricsOptions(metricsOptions));
        nettyToVertxHandler = new NettyToVertxHandler(vertx.eventBus());

        Set<Service> services = configuration.getServices();
        List<ChannelFuture> closeFutures = new ArrayList<>(services.size());

        if (services.contains(Service.TCP)) {
            ServerBootstrap serverBootstrap = new ServerBootstrap()
                    .group(group, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .localAddress(configuration.getTcpPort())
                    .childHandler(new TcpChannelInitializer(nettyToVertxHandler));
            ChannelFuture tcpBindFuture = serverBootstrap.bind().syncUninterruptibly();
            log.infoServerListening("Server", "TCP", tcpBindFuture.channel().localAddress());
            closeFutures.add(tcpBindFuture.channel().closeFuture());
        }

        if (services.contains(Service.UDP)) {
            Bootstrap udpBootstrap = new Bootstrap()
                    .group(group)
                    .channel(NioDatagramChannel.class)
                    .localAddress(configuration.getUdpPort())
                    .handler(new UdpChannelInitializer(nettyToVertxHandler));
            ChannelFuture udpBindFuture = udpBootstrap.bind().syncUninterruptibly();
            log.infoServerListening("Syslogd", "UDP", udpBindFuture.channel().localAddress());
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
                    .handler(new GangliaChannelInitializer(nettyToVertxHandler));
            log.tracef("Ganglia bootstrap is %s", gangliaBootstrap);
            ChannelFuture gangliaBindFuture = gangliaBootstrap.bind().syncUninterruptibly();
            log.infoServerListening("Ganglia", "UDP", gangliaBindFuture.channel().localAddress());
            DatagramChannel gangliaChannel = (DatagramChannel) gangliaBindFuture.channel();
            gangliaChannel.joinGroup(gangliaSocket, mcIf);
            log.trace("Joined the Ganglia group");
            closeFutures.add(gangliaChannel.closeFuture());
        }

        if (services.contains(Service.STATSD)) {
            Bootstrap statsdBootstrap = new Bootstrap()
                    .group(group)
                    .channel(NioDatagramChannel.class)
                    .localAddress(configuration.getStatsDport())
                    .handler(new StatsdChannelInitializer(nettyToVertxHandler));
            ChannelFuture statsdBindFuture = statsdBootstrap.bind().syncUninterruptibly();
            log.infoServerListening("Statsd", "UDP", statsdBindFuture.channel().localAddress());
            closeFutures.add(statsdBindFuture.channel().closeFuture());
        }

        if (services.contains(Service.GRAPHITE)) {
            CountDownLatch latch = new CountDownLatch(1);
            vertx.deployVerticle(new GraphiteServer(configuration), handler -> {
                log.infoServerListening("Graphite", "TCP", configuration.getGraphitePort());
                latch.countDown();
            });
            latch.await();
        }

        if (services.contains(Service.COLLECTD)) {
            vertx.deployVerticle(new CollectdServer(configuration), handler -> {
                log.infoServerListening("Collectd", "UDP", configuration.getCollectdPort());
            });
        }

        log.infoStarted();

        closeFutures.forEach(ChannelFuture::syncUninterruptibly);
    }

    /**
     * Stops this PTrans instance.
     */
    public void stop() {
        log.infoStopping();
        group.shutdownGracefully().syncUninterruptibly();
        workerGroup.shutdownGracefully().syncUninterruptibly();
        Set<String> deploymentIDs = vertx.deploymentIDs();
        CountDownLatch deploymentsLatch = new CountDownLatch(deploymentIDs.size());
        deploymentIDs.forEach(id -> vertx.undeploy(id, handler -> deploymentsLatch.countDown()));
        try {
            deploymentsLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        CountDownLatch vertxLatch = new CountDownLatch(1);
        vertx.close(handler -> vertxLatch.countDown());
        try {
            vertxLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        log.infoStopped();
    }
}
