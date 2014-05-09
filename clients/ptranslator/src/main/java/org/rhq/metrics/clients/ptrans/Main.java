package org.rhq.metrics.clients.ptrans;


import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.rhq.metrics.clients.ptrans.backend.RestForwardingHandler;
import org.rhq.metrics.clients.ptrans.syslog.UdpSyslogEventDecoder;

/**
 * Simple client (proxy) that receives messages from other syslogs and
 * forwards the (matching) messages to the rest server
 * @author Heiko W. Rupp
 */
public class Main {

    int tcpPort = 5140; // Default UDP & TCP port to listen on for syslog messages
    int udpPort = 5140; // Default UDP & TCP port to listen on for syslog messages

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        Main main = new Main(args);
        main.run();
    }

    public Main(String[] args) {
        InputStream inputStream = ClassLoader.getSystemResourceAsStream("ptrans.properties");
        loadPortsFromProperties(inputStream);
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
            logger.info("Server listening on TCP" + graphiteFuture.channel().localAddress());
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
            logger.info("Syslogd listening on udp" + udpFuture.channel().localAddress());
            udpFuture.channel().closeFuture().sync();

        } finally {
            group.shutdownGracefully().sync();
        }
    }

    private void loadPortsFromProperties(InputStream inputStream) {
        Properties properties;
        try {
            properties = new Properties();
            properties.load(inputStream);
        } catch (IOException e) {
            logger.warn("Can not load properties from 'ptrans.properties'");
            return;
        }

        if (properties.containsKey("port.udp")) {
            udpPort = Integer.parseInt(properties.getProperty("port.udp"));
        }
        if (properties.containsKey("port.tcp")) {
            tcpPort = Integer.parseInt(properties.getProperty("port.tcp"));
        }
    }
}
