package org.rhq.metrics.clients.ptrans.backend;


import java.net.ConnectException;
import java.net.InetAddress;
import java.net.UnknownHostException;
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
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestEncoder;
import io.netty.handler.codec.http.HttpResponseDecoder;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.AttributeKey;
import io.netty.util.CharsetUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.rhq.metrics.client.common.Batcher;
import org.rhq.metrics.client.common.BoundMetricFifo;
import org.rhq.metrics.client.common.SingleMetric;

import static io.netty.channel.ChannelHandler.Sharable;

/**
 * Handler that takes incoming syslog metric messages (which are already parsed)
 * and forwards them to rhq-metrics rest servlet.
 * @author Heiko W. Rupp
 */
@Sharable
@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
public class RestForwardingHandler extends ChannelInboundHandlerAdapter {

    private static final String RHQ_METRICS_PREFIX = "/rhq-metrics";
    private static final String METRICS_PREFIX = "/metrics";
    private static final String DEFAULT_REST_PORT = "8080";
    private String restHost = "localhost";
    private int restPort = 8080;
    private String restPrefix = RHQ_METRICS_PREFIX + METRICS_PREFIX;

    private static final int CLOSE_AFTER_REQUESTS = 200;
    private long numberOfMetrics = 0;

    BoundMetricFifo fifo;
    AttributeKey<List<SingleMetric>> listKey = AttributeKey.valueOf("listToSend");

    private Channel senderChannel;
    private int sendCounter = 0;

    private String localHostName;

    private static final Logger logger = LoggerFactory.getLogger(RestForwardingHandler.class);
    private int closeAfterRequests = CLOSE_AFTER_REQUESTS;
    private int spoolSize;
    boolean isConnecting = false;

    final Object connectingMutex;

    public RestForwardingHandler(Properties configuration) {
        connectingMutex = this;
        logger.debug("RestForwardingHandler init");
        loadRestEndpointInfoFromProperties(configuration);

        fifo = new BoundMetricFifo(10, spoolSize);
        try {
            localHostName  = InetAddress.getLocalHost().getCanonicalHostName();
        } catch (UnknownHostException e) {
            logger.error(e.getLocalizedMessage());
            localHostName="- unknown host -";
        }
    }



    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) throws Exception {

        @SuppressWarnings("unchecked")
        final List<SingleMetric> in = (List<SingleMetric>) msg;
        if (logger.isTraceEnabled()) {
            logger.trace("Received some metrics :[" + in + "]");
        }

        fifo.addAll(in);

        synchronized (connectingMutex) {
            // make sure to only open one connection at a time
            if (isConnecting)
                return;
        }

        if (senderChannel!=null ) {
            sendToChannel(senderChannel);
            return;
        }


        ChannelFuture cf = connectRestServer(ctx.channel().eventLoop().parent());

        cf.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                synchronized (connectingMutex) {
                    isConnecting=false;
                }
                if (!future.isSuccess()) {
                    Throwable cause = future.cause();
                    if (cause instanceof ConnectException ) {
                        logger.warn("Sending failed: " + cause.getLocalizedMessage());
                    }
                    else {
                        logger.warn("Something went wrong: " + cause);
                    }
                } else {
                    //   the remote is up.
                    senderChannel = future.channel();
                    sendToChannel(senderChannel);
                }
            }
        });
    }

    private void sendToChannel(final Channel ch) {
        if (logger.isTraceEnabled()) {
            logger.trace("Sending to channel " +ch );
        }
        final List<SingleMetric> metricsToSend = fifo.getList();

        String payload = Batcher.metricListToJson(metricsToSend);
        ByteBuf content = Unpooled.copiedBuffer(payload, CharsetUtil.UTF_8);
        FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
            restPrefix, content);
        HttpHeaders.setContentLength(request, content.readableBytes());
        HttpHeaders.setKeepAlive(request, true);
        HttpHeaders.setHeader(request, HttpHeaders.Names.CONTENT_TYPE, "application/json;charset=utf-8");
        // We need to send the list of metrics we are sending down the pipeline, so the status watcher
        // can later clean them out of the fifo
        ch.attr(listKey).set(metricsToSend);
        ch.writeAndFlush(request).addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (!future.isSuccess()) {
                    // and remove from connection pool if you have one etc...
                    ch.close();
                    senderChannel = null;
                    logger.error("Sending to the rhq-metrics server failed: " + future.cause());

                } else {
                    sendCounter++;
                    if (sendCounter >= closeAfterRequests) {
                        SingleMetric perfMetric = new SingleMetric(localHostName + ".ptrans.counter",
                                System.currentTimeMillis(), (double) (numberOfMetrics + metricsToSend.size()));
                        fifo.offer(perfMetric);
                        logger.info("Doing a periodic close after " + closeAfterRequests + " requests (" +
                            numberOfMetrics + ") items");
                        numberOfMetrics = 0;
                        ch.close();
                        senderChannel = null;
                        sendCounter = 0;
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

        synchronized (connectingMutex) {
            isConnecting = true;
        }

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
                    pipeline.addLast(new HttpStatusWatcher());
                }
            })
        ;

        return clientBootstrap.connect();
    }

    private void loadRestEndpointInfoFromProperties(Properties configuration) {

        restHost = configuration.getProperty("rest.host", "localhost");
        restPort = Integer.parseInt(configuration.getProperty("rest.port", DEFAULT_REST_PORT));
        restPrefix = configuration.getProperty("rest.prefix", RHQ_METRICS_PREFIX);
        restPrefix += METRICS_PREFIX;
        closeAfterRequests = Integer.parseInt(
            configuration.getProperty("rest.close-after", String.valueOf(CLOSE_AFTER_REQUESTS)));

        spoolSize = Integer.parseInt(configuration.getProperty("spool.size", "1000"));
    }

    /**
     * Adapter that checks the http response and only flushes the fifo if the
     * return code is 200 or 204 (ok, empty doc). Otherwise we keep the data
     * for the next try.
     * @author Heiko W. Rupp
    */
    class HttpStatusWatcher extends ChannelInboundHandlerAdapter {

        private final Logger logger = LoggerFactory.getLogger(HttpStatusWatcher.class);

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof FullHttpResponse) {
                FullHttpResponse response = (FullHttpResponse) msg;
                HttpResponseStatus status = response.getStatus();

                if (status.equals(HttpResponseStatus.NO_CONTENT) ||
                    status.equals(HttpResponseStatus.OK)) {

                    List<SingleMetric> metricsSent = ctx.channel().attr(listKey).getAndRemove();

                    if (metricsSent!=null) {
                        // only clear the ones we sent - new ones may have arrived between batching and now
                        fifo.cleanout(metricsSent);
                        numberOfMetrics += metricsSent.size();
                        if (logger.isDebugEnabled()) {
                            logger.debug("sent " + metricsSent.size() + " items");
                        }
                    }
                }
                else {
                    logger.warn("Send to rest-server failed:" + status);
                }
            }
            else {
                System.err.println("msg" + msg);
            }
        }
    }
}
