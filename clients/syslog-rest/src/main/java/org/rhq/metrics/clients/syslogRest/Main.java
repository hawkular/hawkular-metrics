package org.rhq.metrics.clients.syslogRest;


import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;

/**
 * Simple client (proxy) that receives messages from other syslogs and
 * forwards the (matching) messages to the rest server
 * @author Heiko W. Rupp
 */
public class Main {

    int syslogPort = 5140; // UDP port to listen on

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

            // The rest client
            Bootstrap clientBootstrap = new Bootstrap();
            clientBootstrap
                .group(group)
                .channel(NioSocketChannel.class)
                .remoteAddress("localhost", 8080)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
//                        pipeline.addLast(executorGroup, new RestEncoder());
//                        pipeline.addLast(executorGroup, new RestClientHandler());
                    }
                })
            ;
            final ChannelFuture clientFuture = clientBootstrap.connect().sync();
            System.out.println("Connected to the rest server at " + clientFuture.channel().remoteAddress());



            // The syslog UPD socket server
            Bootstrap serverBootstrap = new Bootstrap();
            serverBootstrap
                .group(group)
                .channel(NioDatagramChannel.class)
                .localAddress(syslogPort)
                .handler(new ChannelInitializer<Channel>() {
                    @Override
                    public void initChannel(Channel socketChannel) throws Exception {
                        ChannelPipeline pipeline = socketChannel.pipeline();
                        pipeline.addLast(new SyslogEventDecoder());
                        pipeline.addLast(new RsyslogHandler(clientFuture.channel()));
                    }
                })
            ;
            ChannelFuture serverFuture = serverBootstrap.bind().sync();
            System.out.println("Syslogd listening on " + serverFuture.channel().localAddress());
            serverFuture.channel().closeFuture().sync();

        } finally {
            group.shutdownGracefully().sync();
        }
    }
}
