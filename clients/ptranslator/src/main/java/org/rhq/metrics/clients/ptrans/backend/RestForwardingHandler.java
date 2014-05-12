package org.rhq.metrics.clients.ptrans.backend;


import java.util.Iterator;
import java.util.List;

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
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestEncoder;
import io.netty.handler.codec.http.HttpResponseDecoder;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.CharsetUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.rhq.metrics.clients.ptrans.SingleMetric;

import static io.netty.channel.ChannelHandler.Sharable;

/**
 * Handler that takes incoming syslog metric messages (which are already parsed)
 * and forwards them to rhq-metrics rest servlet.
 * @author Heiko W. Rupp
 */
@Sharable
public class RestForwardingHandler extends ChannelInboundHandlerAdapter {


    private static final Logger logger = LoggerFactory.getLogger(RestForwardingHandler.class);

    public RestForwardingHandler() {
        logger.info("RsyslogHandler init");
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) throws Exception {

        @SuppressWarnings("unchecked")
		final List<SingleMetric> in = (List<SingleMetric>) msg;
        logger.debug("Received some metrics :[" + in + "]");

        ChannelFuture cf = connectRestServer(ctx.channel().eventLoop().parent());

        cf.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (!future.isSuccess()) {
                    // something went wrong
//                    packet.release(); TODO what do we need to do?
                    logger.warn("something went wrong");
                } else {
                    String payload = eventsToJson(in);
                    ByteBuf content = Unpooled.copiedBuffer(payload, CharsetUtil.UTF_8);
                    final Channel ch = future.channel();
                    FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/rest/metric/s", content);
                    HttpHeaders.setContentLength(request, content.readableBytes());
                    HttpHeaders.setKeepAlive(request, true);
                    HttpHeaders.setHeader(request, HttpHeaders.Names.CONTENT_TYPE, "application/json;charset=utf-8");
                    ch.writeAndFlush(request).addListener(new ChannelFutureListener() {
                        @Override
                        public void operationComplete(ChannelFuture future) throws Exception {
                            if (!future.isSuccess()) {
                                // and remove from connection pool if you have one etc...
                                ch.close();
                                logger.error("op not complete: " + future.cause());
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
                    // data is sent here and the http response obtained
                    pipeline.addLast(new HttpResponseDecoder());
                    pipeline.addLast(new HttpObjectAggregator(1024));
                    pipeline.addLast(new HttpErrorLogger());
                }
            })
        ;
        ChannelFuture clientFuture = clientBootstrap.connect();

        return clientFuture;
    }

    private String eventsToJson(List<SingleMetric> events) {
        StringBuilder builder = new StringBuilder("[");
        Iterator<SingleMetric> iter = events.iterator();
        while (iter.hasNext()) {
            SingleMetric event = iter.next();
            builder.append(event.toJson());
            if (iter.hasNext()) {
                builder.append(',');
            }
        }
        builder.append(']');

        return builder.toString();
    }
}
