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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Properties;

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
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
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
 * Simple client (proxy) that receives messages from various protocols
 * and forwards the data to the rest server.
 * Multiple protocols are supported.
 * @author Heiko W. Rupp
 */
public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    private static final String HELP_OPT = "h";
    private static final String HELP_LONGOPT = "help";
    private static final String CONFIG_FILE_OPT = "c";
    private static final String CONFIG_FILE_LONGOPT = "config-file";
    private static final int DEFAULT_PORT = 5140;
    private static final int GANGLIA_DEFAULT_PORT = 8649;
    private static final String GANGLIA_DEFAULT_GROUP = "239.2.11.71";
    private static final int STATSD_DEFAULT_PORT = 8125;
    private static final int COLLETCD_DEFAULT_PORT = 25826;

    private String gangliaGroup = GANGLIA_DEFAULT_GROUP;
    private int gangliaPort = GANGLIA_DEFAULT_PORT;
    private String multicastIfOverride;
    private int tcpPort = DEFAULT_PORT;
    private int udpPort = DEFAULT_PORT;
    private int statsDport = STATSD_DEFAULT_PORT;
    private int collectdPort = COLLETCD_DEFAULT_PORT;
    private final int minimumBatchSize;


    private final Properties configuration;
    private final EventLoopGroup group;
    private final EventLoopGroup workerGroup;

    public static void main(String[] args) throws Exception {
        Options options = getCommandOptions();
        CommandLineParser parser = new PosixParser();
        boolean commandLineError = false;
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args, true);
        } catch (MissingOptionException e) {
            commandLineError = true;
            System.err.println(e.getMessage());
        }
        if (commandLineError || cmd.hasOption(HELP_OPT)) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(Integer.MAX_VALUE); // Do not wrap
            formatter.printHelp("java -jar ptrans-all.jar", options, true);
            System.exit(commandLineError ? 1 : 0);
        }
        File configFile = new File(cmd.getOptionValue(CONFIG_FILE_OPT));
        if (!configFile.isFile()) {
            System.err.println("Configuration file " + configFile.getAbsolutePath()
                + " does not exist or is not readable.");
            System.exit(1);
        }
        Main main = new Main(configFile);
        main.run();
    }

    private static Options getCommandOptions() {
        Options options = new Options();
        Option helpOption = new Option(HELP_OPT, HELP_LONGOPT, false, "Print usage and exit.");
        options.addOption(helpOption);
        Option configOption = new Option(CONFIG_FILE_OPT, CONFIG_FILE_LONGOPT, true,
            "Set the path to the configuration file.");
        configOption.setRequired(true);
        options.addOption(configOption);
        return options;
    }

    private Main(File configFile) {
        group = new NioEventLoopGroup();
        workerGroup = new NioEventLoopGroup();
        configuration = loadConfigurationFromProperties(configFile);
        loadPortsFromProperties(configuration);
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                stop();
            }
        }));
        minimumBatchSize = Integer.parseInt(configuration.getProperty("batch.size","5"));
    }

    private void run() throws Exception {
        final RestForwardingHandler forwardingHandler = new RestForwardingHandler(configuration);

        // The generic TCP socket server
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(group, workerGroup).channel(NioServerSocketChannel.class).localAddress(tcpPort)
            .childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel socketChannel) throws Exception {
                    ChannelPipeline pipeline = socketChannel.pipeline();
                    pipeline.addLast(new DemuxHandler(configuration, forwardingHandler));
                }
            });
        ChannelFuture graphiteFuture = serverBootstrap.bind().sync();
        logger.info("Server listening on TCP " + graphiteFuture.channel().localAddress());
        graphiteFuture.channel().closeFuture();

        // The syslog UPD socket server
        Bootstrap udpBootstrap = new Bootstrap();
        udpBootstrap.group(group).channel(NioDatagramChannel.class).localAddress(udpPort)
            .handler(new ChannelInitializer<Channel>() {
                @Override
                public void initChannel(Channel socketChannel) throws Exception {
                    ChannelPipeline pipeline = socketChannel.pipeline();
                    pipeline.addLast(new UdpSyslogEventDecoder());

                    pipeline.addLast(forwardingHandler);
                }
            });
        ChannelFuture udpFuture = udpBootstrap.bind().sync();
        logger.info("Syslogd listening on udp " + udpFuture.channel().localAddress());

        // Try to set up an upd listener for Ganglia Messages
        setupGangliaUdp(group, forwardingHandler);

        // Setup statsd listener
        setupStatsdUdp(group, forwardingHandler);

        // Setup collectd listener
        setupCollectdUdp(group, forwardingHandler);

        udpFuture.channel().closeFuture().sync();
    }

    private void stop() {
        logger.info("Stopping ptrans...");
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
        logger.info("Stopped");
    }

    private void setupCollectdUdp(EventLoopGroup group, final ChannelInboundHandlerAdapter forwardingHandler) {
        Bootstrap collectdBootstrap = new Bootstrap();
        collectdBootstrap.group(group).channel(NioDatagramChannel.class).localAddress(collectdPort)
            .handler(new ChannelInitializer<Channel>() {
                @Override
                public void initChannel(Channel socketChannel) throws Exception {
                    ChannelPipeline pipeline = socketChannel.pipeline();
                    pipeline.addLast(new CollectdPacketDecoder());
                    pipeline.addLast(new CollectdEventsDecoder());
                    pipeline.addLast(new CollectdEventHandler());
                    pipeline.addLast(new MetricBatcher("collectd", minimumBatchSize));
                    pipeline.addLast(forwardingHandler);
                }
            });
        try {
            ChannelFuture collectdFuture = collectdBootstrap.bind().sync();
            logger.info("Collectd listening on udp " + collectdFuture.channel().localAddress());
        } catch (InterruptedException e) {
            e.printStackTrace(); // TODO: Customise this generated block
        }
    }

    private void setupStatsdUdp(EventLoopGroup group, final ChannelInboundHandlerAdapter forwardingHandler) {
        Bootstrap statsdBootstrap = new Bootstrap();
        statsdBootstrap.group(group).channel(NioDatagramChannel.class).localAddress(statsDport)
            .handler(new ChannelInitializer<Channel>() {
                @Override
                public void initChannel(Channel socketChannel) throws Exception {
                    ChannelPipeline pipeline = socketChannel.pipeline();
                    pipeline.addLast(new StatsdDecoder());
                    pipeline.addLast(new MetricBatcher("statsd", minimumBatchSize));
                    pipeline.addLast(forwardingHandler);
                }
            });
        try {
            ChannelFuture statsdFuture = statsdBootstrap.bind().sync();
            logger.info("Statsd listening on udp " + statsdFuture.channel().localAddress());
        } catch (InterruptedException e) {
            e.printStackTrace(); // TODO: Customise this generated block
        }

    }

    private void setupGangliaUdp(EventLoopGroup group, final ChannelInboundHandlerAdapter fowardingHandler) {
        // The ganglia UPD socket server

        try {

            NetworkInterface mcIf;
            if (multicastIfOverride ==null) {
                Inet4Address hostAddr = (Inet4Address) InetAddress.getLocalHost();
                mcIf = NetworkInterface.getByInetAddress(hostAddr);
            }
            else {
                mcIf = NetworkInterface.getByName(multicastIfOverride);
            }

            InetSocketAddress gangliaSocket = new InetSocketAddress(gangliaGroup, gangliaPort);

            Bootstrap gangliaBootstrap = new Bootstrap();
            gangliaBootstrap
                .group(group)
                .channel(NioDatagramChannel.class)
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.IP_MULTICAST_IF,mcIf)
                .localAddress(gangliaSocket)
                .handler(new ChannelInitializer<Channel>() {
                    @Override
                    public void initChannel(Channel socketChannel) throws Exception {
                        ChannelPipeline pipeline = socketChannel.pipeline();
                        pipeline.addLast(new UdpGangliaDecoder());
                        pipeline.addLast(new MetricBatcher("ganglia", minimumBatchSize));
                        pipeline.addLast(fowardingHandler);
                    }
                })
            ;

            logger.info("Bootstrap is " + gangliaBootstrap);
            ChannelFuture gangliaFuture = gangliaBootstrap.bind().sync();
            logger.info("Ganglia listening on udp " + gangliaFuture.channel().localAddress());
            DatagramChannel channel = (DatagramChannel) gangliaFuture.channel();
            channel.joinGroup(gangliaSocket,mcIf).sync();
            logger.info("Joined the group");
            channel.closeFuture();
        } catch (InterruptedException|SocketException | UnknownHostException e) {
            logger.warn("Setup of udp multicast for Ganglia failed");
            e.printStackTrace();
        }
    }

    private Properties loadConfigurationFromProperties(File configFile) {
        Properties properties = new Properties();
        try (InputStream inputStream = new FileInputStream(configFile)) {
            properties.load(inputStream);
        } catch (IOException e) {
            logger.warn("Can not load properties from '" + configFile.getAbsolutePath() + "'");
        }
        return properties;
    }

    private void loadPortsFromProperties(Properties configuration) {
        udpPort = Integer.parseInt(configuration.getProperty("port.udp", String.valueOf(DEFAULT_PORT)));
        tcpPort = Integer.parseInt(configuration.getProperty("port.tcp", String.valueOf(DEFAULT_PORT)));
        gangliaGroup = configuration.getProperty("ganglia.group", GANGLIA_DEFAULT_GROUP);
        gangliaPort = Integer.parseInt(configuration.getProperty("ganglia.port", String.valueOf(GANGLIA_DEFAULT_PORT)));
        multicastIfOverride = configuration.getProperty("multicast.interface");
        statsDport = Integer.parseInt(configuration.getProperty("statsd.port", String.valueOf(STATSD_DEFAULT_PORT)));
        collectdPort = Integer.parseInt(configuration.getProperty("collectd.port",
            String.valueOf(COLLETCD_DEFAULT_PORT)));
    }
}
