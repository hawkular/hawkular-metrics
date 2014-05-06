package org.rhq.metrics.clients.syslogRest;


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
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple client (proxy) that receives messages from other syslogs and
 * forwards the (matching) messages to the rest server
 * @author Heiko W. Rupp
 */
public class Main {

    int syslogPort = 5140; // UDP port to listen on

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        Main main = new Main(args);
        main.run();
    }

    public Main(String[] args) {
    }

    private void run() throws Exception {
        EventLoopGroup group = new NioEventLoopGroup();
        final EventExecutorGroup executorGroup = new DefaultEventExecutorGroup(5);
        try {

            // The syslog TCP socket server

            ServerBootstrap tcpBootstrap = new ServerBootstrap();
            tcpBootstrap
                .group(group)
                .channel(NioServerSocketChannel.class)
                .localAddress(syslogPort)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel socketChannel) throws Exception {
                        ChannelPipeline pipeline = socketChannel.pipeline();
                        pipeline.addLast(new SyslogEventDecoder());
                        pipeline.addLast(new RsyslogHandler());
                    }
                })
            ;
            ChannelFuture tcpFuture = tcpBootstrap.bind().sync();
            logger.info("Syslogd listening on tcp" + tcpFuture.channel().localAddress());
            tcpFuture.channel().closeFuture();


            // The syslog UPD socket server
            Bootstrap udpBootstrap = new Bootstrap();
            udpBootstrap
                .group(group)
                .channel(NioDatagramChannel.class)
                .localAddress(syslogPort)
                .handler(new ChannelInitializer<Channel>() {
                    @Override
                    public void initChannel(Channel socketChannel) throws Exception {
                        ChannelPipeline pipeline = socketChannel.pipeline();
                        pipeline.addLast(new UdpSyslogEventDecoder());
                        pipeline.addLast(new RsyslogHandler());
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
}
