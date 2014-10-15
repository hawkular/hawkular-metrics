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
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.rhq.metrics.clients.ptrans.backend.RestForwardingHandler;
import org.rhq.metrics.clients.ptrans.ganglia.UdpGangliaDecoder;
import org.rhq.metrics.clients.ptrans.statsd.StatsdDecoder;
import org.rhq.metrics.clients.ptrans.syslog.UdpSyslogEventDecoder;

/**
 * Simple client (proxy) that receives messages from other syslogs and
 * forwards the (matching) messages to the rest server
 * @author Heiko W. Rupp
 */
public class Main {

    private static final int DEFAULT_PORT = 5140;
    public static final String CONFIG_PROPERTIES_FILE_NAME = "ptrans.properties";
    private static final int GANGLIA_DEFAULT_PORT = 8649;
    int tcpPort = DEFAULT_PORT;
    int udpPort = DEFAULT_PORT;

    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    private static final String GANGLIA_DEFAULT_GROUP = "239.2.11.71";
    private String gangliaGroup = GANGLIA_DEFAULT_GROUP;
    private int gangliaPort = GANGLIA_DEFAULT_PORT;
    private String multicastIfOverride;

    private static final int STATSD_DEFAULT_PORT = 8125;
    private int statsDport = STATSD_DEFAULT_PORT;

    Properties configuration;

    public static void main(String[] args) throws Exception {

        Main main;
        if (args.length>0) {
            main = new Main(args[0]);
        } else {
            main = new Main(CONFIG_PROPERTIES_FILE_NAME);
        }
        main.run();
    }

    public Main(String propertiesPath) {

        configuration = loadConfigurationFromProperties(propertiesPath);
        loadPortsFromProperties(configuration);
    }

    private void run() throws Exception {
        EventLoopGroup group = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {

            // The generic TCP socket server
            ServerBootstrap serverBootstrap = new ServerBootstrap();
                serverBootstrap.group(group, workerGroup)
                .channel(NioServerSocketChannel.class)
                .localAddress(tcpPort)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel socketChannel) throws Exception {
                        ChannelPipeline pipeline = socketChannel.pipeline();
                        pipeline.addLast(new DemuxHandler(configuration));
                    }
                });
            ChannelFuture graphiteFuture = serverBootstrap.bind().sync();
            logger.info("Server listening on TCP " + graphiteFuture.channel().localAddress());
            graphiteFuture.channel().closeFuture();


            // The syslog UPD socket server
            Bootstrap udpBootstrap = new Bootstrap();
            udpBootstrap
                .group(group)
                .channel(NioDatagramChannel.class)
                .localAddress(udpPort)
                .handler(new ChannelInitializer<Channel>() {
                    @Override
                    public void initChannel(Channel socketChannel) throws Exception {
                        ChannelPipeline pipeline = socketChannel.pipeline();
                        pipeline.addLast(new UdpSyslogEventDecoder());
                        pipeline.addLast(new RestForwardingHandler(configuration));
                    }
                })
            ;
            ChannelFuture udpFuture = udpBootstrap.bind().sync();
            logger.info("Syslogd listening on udp " + udpFuture.channel().localAddress());

            // Try to set up an upd listener for Ganglia Messages
            setupGangliaUdp(group);

            // Setup statsd listener
            setupStatsdUdp(group);

            udpFuture.channel().closeFuture().sync();
        } finally {
            group.shutdownGracefully().sync();
            group.shutdownGracefully().sync();
        }
    }

    private void setupStatsdUdp(EventLoopGroup group) {
        Bootstrap statsdBootstrap = new Bootstrap();
        statsdBootstrap
            .group(group)
            .channel(NioDatagramChannel.class)
            .localAddress(statsDport)
            .handler(new ChannelInitializer<Channel>() {
                @Override
                public void initChannel(Channel socketChannel) throws Exception {
                    ChannelPipeline pipeline = socketChannel.pipeline();
                    pipeline.addLast(new StatsdDecoder());
                    pipeline.addLast(new MetricBatcher("statsd"));
                    pipeline.addLast(new RestForwardingHandler(configuration));
                }
            })
        ;
        try {
            ChannelFuture statsdFuture = statsdBootstrap.bind().sync();
            logger.info("Statsd listening on udp " + statsdFuture.channel().localAddress());
        } catch (InterruptedException e) {
            e.printStackTrace();  // TODO: Customise this generated block
        }

    }

    private void setupGangliaUdp(EventLoopGroup group) {
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
                        pipeline.addLast(new MetricBatcher("ganglia"));
                        pipeline.addLast(new RestForwardingHandler(configuration));
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

    private Properties loadConfigurationFromProperties(String path) {


        File file = new File(path);
        logger.info("Using configuration properties from ==> " + file.getAbsolutePath());
        if (!file.exists() || !file.canRead()) {
            throw new IllegalArgumentException("Properties at " + path + " do not exist or are not readable");
        }

        Properties properties = new Properties();
        try (InputStream inputStream = new FileInputStream(file)) {
            properties.load(inputStream);
        } catch (IOException e) {
            logger.warn("Can not load properties from '" + file.getAbsolutePath() + "'");
        }

        return properties;
    }

    private void loadPortsFromProperties(Properties configuration) {

            udpPort = Integer.parseInt(configuration.getProperty("port.udp", String.valueOf(DEFAULT_PORT)));
            tcpPort = Integer.parseInt(configuration.getProperty("port.tcp", String.valueOf(DEFAULT_PORT)));
            gangliaGroup = configuration.getProperty("ganglia.group",GANGLIA_DEFAULT_GROUP);
            gangliaPort = Integer.parseInt(configuration.getProperty("ganglia.port", String.valueOf(GANGLIA_DEFAULT_PORT)));
            multicastIfOverride = configuration.getProperty("multicast.interface");
            statsDport = Integer.parseInt(configuration.getProperty("statsd.port", String.valueOf(STATSD_DEFAULT_PORT)));

    }
}
