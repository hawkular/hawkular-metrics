package org.rhq.metrics.clients.ptrans.backend;


import java.net.ConnectException;
import java.util.List;
import java.util.Properties;

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

import org.rhq.metrics.client.common.Batcher;
import org.rhq.metrics.client.common.SingleMetric;

import static io.netty.channel.ChannelHandler.Sharable;

/**
 * Handler that takes incoming syslog metric messages (which are already parsed)
 * and forwards them to rhq-metrics rest servlet.
 * @author Heiko W. Rupp
 */
@Sharable
public class RestForwardingHandler extends ChannelInboundHandlerAdapter {

    private static final String RHQ_METRICS_PREFIX = "/rhq-metrics";
    private static final String METRICS_PREFIX = "/metrics";
    private static final String DEFAULT_REST_PORT = "8080";
    private String restHost = "localhost";
    private int restPort = 8080;
    private String restPrefix = RHQ_METRICS_PREFIX + METRICS_PREFIX;

    private static final int CLOSE_AFTER_REQUESTS = 200;


    private Channel senderChannel;
    private int sendCounter = 0;

    private static final Logger logger = LoggerFactory.getLogger(RestForwardingHandler.class);
    private int closeAfterRequests = CLOSE_AFTER_REQUESTS;

    public RestForwardingHandler(Properties configuration) {
        logger.debug("RsyslogHandler init");
        loadRestEndpointInfoFromProperties(configuration);
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) throws Exception {

        @SuppressWarnings("unchecked")
		final List<SingleMetric> in = (List<SingleMetric>) msg;
        logger.debug("Received some metrics :[" + in + "]");

        if (senderChannel!=null ) {
            sendToChannel(senderChannel,in);
            return;
        }

        ChannelFuture cf = connectRestServer(ctx.channel().eventLoop().parent());

        cf.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (!future.isSuccess()) {
                    Throwable cause = future.cause();
                    if (cause instanceof ConnectException ) {
                        logger.warn("Sending failed: " + cause.getLocalizedMessage());
                    }
                    else {
                        logger.warn("Something went wrong: " + cause);
                    }
                    // TODO at this point we may consider buffering the data until
                    //   the remote is back up again.
                } else {
                    senderChannel = future.channel();
                    sendToChannel(senderChannel, in);
                }
            }
        });
    }

    private void sendToChannel(final Channel ch, List<SingleMetric> in) {
        if (logger.isDebugEnabled()) {
            logger.debug("Sending to channel " +ch );
        }
        String payload = Batcher.metricListToJson(in);
        ByteBuf content = Unpooled.copiedBuffer(payload, CharsetUtil.UTF_8);
        FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
            restPrefix, content);
        HttpHeaders.setContentLength(request, content.readableBytes());
        HttpHeaders.setKeepAlive(request, true);
        HttpHeaders.setHeader(request, HttpHeaders.Names.CONTENT_TYPE, "application/json;charset=utf-8");
        ch.writeAndFlush(request).addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (!future.isSuccess()) {
                    // and remove from connection pool if you have one etc...
                    ch.close();
                    senderChannel=null;
                    logger.error("Sending to the rhq-metrics server failed: " + future.cause());
                }
                else {
                    sendCounter++;
                    if (sendCounter >= closeAfterRequests) {
                        logger.info("Doing a periodic close after "+ closeAfterRequests+" requests");
                        ch.close();
                        senderChannel=null;
                        sendCounter=0;
                    }
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
            .remoteAddress(restHost, restPort)
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

    private void loadRestEndpointInfoFromProperties(Properties configuration) {

        restHost = configuration.getProperty("rest.host", "localhost");
        restPort = Integer.parseInt(configuration.getProperty("rest.port", DEFAULT_REST_PORT));
        restPrefix = configuration.getProperty("rest.prefix", RHQ_METRICS_PREFIX);
        restPrefix += METRICS_PREFIX;
        closeAfterRequests = Integer.parseInt(
            configuration.getProperty("rest.close-after", String.valueOf(CLOSE_AFTER_REQUESTS)));

    }

}
