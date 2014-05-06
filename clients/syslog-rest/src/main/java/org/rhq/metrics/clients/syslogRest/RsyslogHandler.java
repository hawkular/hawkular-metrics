package org.rhq.metrics.clients.syslogRest;


import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestEncoder;
import io.netty.handler.codec.http.HttpResponseDecoder;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.CharsetUtil;

import static io.netty.channel.ChannelHandler.Sharable;

/**
 * // TODO: Document this
 * @author Heiko W. Rupp
 */
@Sharable
public class RsyslogHandler extends ChannelInboundHandlerAdapter {


    public RsyslogHandler() {
        System.out.println("RsyslogHandler init");
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) throws Exception {

        final SyslogMetricEvent in = (SyslogMetricEvent) msg;
        System.out.println("Received a metric :[" + in +"]");

        ChannelFuture cf = connectRestServer(ctx.channel().eventLoop().parent());

        cf.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (!future.isSuccess()) {
                    // something went wrong
//                    packet.release(); TODO what do we need to do?
                    System.err.println("something went wrong");
                } else {
                    ByteBuf content = Unpooled.copiedBuffer(in.toJson(), CharsetUtil.UTF_8);
                    final Channel ch = future.channel();
                    FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/rest/metric", content);
                    HttpHeaders.setContentLength(request, content.readableBytes());
                    HttpHeaders.setKeepAlive(request, true);
                    HttpHeaders.setHeader(request, HttpHeaders.Names.CONTENT_TYPE, "application/json;charset=utf-8");
                    ch.writeAndFlush(request).addListener(new ChannelFutureListener() {
                        @Override
                        public void operationComplete(ChannelFuture future) throws Exception {
                            if (!future.isSuccess()) {
                                // and remove from connection pool if you have one etc...
                                ch.close();
                                System.err.println("op not complete: " + future.cause());
                            }
                        }
                    });
                }
            }
        });
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }

    ChannelFuture connectRestServer(EventLoopGroup group) throws Exception{

        Bootstrap clientBootstrap = new Bootstrap();
        clientBootstrap
            .group(group)
            .channel(NioSocketChannel.class)
            .remoteAddress("localhost", 8080)
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) throws Exception {

                    ChannelPipeline pipeline = ch.pipeline();
                    pipeline.addLast(new HttpRequestEncoder());
                    pipeline.addLast(new HttpResponseDecoder());
                    pipeline.addLast(new HttpObjectAggregator(1024));
                    pipeline.addLast(new ChannelInboundHandlerAdapter() {
                        @Override
                        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                            if (msg instanceof FullHttpResponse) {
                                FullHttpResponse response = (FullHttpResponse) msg;
                                if (!response.getStatus().equals(HttpResponseStatus.NO_CONTENT) &&
                                    !response.getStatus().equals(HttpResponseStatus.OK)){
                                    System.err.println("Send failed:" + response.toString());
                                }
                            }
                        }
                    });
                }
            })
        ;
        ChannelFuture clientFuture = clientBootstrap.connect();

        return clientFuture;
                        }
}
