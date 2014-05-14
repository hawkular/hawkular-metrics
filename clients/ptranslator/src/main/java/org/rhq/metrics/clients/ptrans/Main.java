package org.rhq.metrics.clients.ptrans;


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

    public static void main(String[] args) throws Exception {
        Main main = new Main(args);
        main.run();
    }

    public Main(String[] args) {
        loadPortsFromProperties();
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
                        pipeline.addLast(new DemuxHandler());
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
                        pipeline.addLast(new RestForwardingHandler());
                    }
                })
            ;
            ChannelFuture udpFuture = udpBootstrap.bind().sync();
            logger.info("Syslogd listening on udp " + udpFuture.channel().localAddress());

            // Try to set up an upd listener for Ganglia Messages
            setupGangliaUdp(group);

            udpFuture.channel().closeFuture().sync();
        } finally {
            group.shutdownGracefully().sync();
            group.shutdownGracefully().sync();
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
                        pipeline.addLast(new RestForwardingHandler());
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

    private void loadPortsFromProperties() {
        Properties properties;
        try (InputStream inputStream = ClassLoader.getSystemResourceAsStream(CONFIG_PROPERTIES_FILE_NAME)) {
            if (inputStream==null) {
                logger.warn("Can not load properties from '"+ CONFIG_PROPERTIES_FILE_NAME +"', using defaults");
                return;
            }
            properties = new Properties();
            properties.load(inputStream);

            udpPort = Integer.parseInt(properties.getProperty("port.udp", String.valueOf(DEFAULT_PORT)));
            tcpPort = Integer.parseInt(properties.getProperty("port.tcp", String.valueOf(DEFAULT_PORT)));
            gangliaGroup = properties.getProperty("ganglia.group",GANGLIA_DEFAULT_GROUP);
            gangliaPort = Integer.parseInt(properties.getProperty("ganglia.port", String.valueOf(GANGLIA_DEFAULT_PORT)));
            multicastIfOverride = properties.getProperty("multicast.interface");

        } catch (IOException e) {
            logger.warn("Can not load properties from '" + CONFIG_PROPERTIES_FILE_NAME + "'");
        }
    }
}
