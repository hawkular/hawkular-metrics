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
package org.hawkular.metrics.clients.ptrans.backend;

import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
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
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestEncoder;
import io.netty.handler.codec.http.HttpResponseDecoder;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.AttributeKey;
import io.netty.util.CharsetUtil;

import java.net.ConnectException;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.List;

import org.hawkular.metrics.client.common.Batcher;
import org.hawkular.metrics.client.common.BoundMetricFifo;
import org.hawkular.metrics.client.common.SingleMetric;
import org.hawkular.metrics.clients.ptrans.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler that takes incoming syslog metric messages (which are already parsed)
 * and forwards them to hawkular-metrics rest servlet.
 *
 * @author Heiko W. Rupp
 */
@Sharable
public class RestForwardingHandler extends ChannelInboundHandlerAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(RestForwardingHandler.class);

    private final String restHost;
    private final int restPort;
    private final String restPrefix;
    private final int restCloseAfterRequests;

    BoundMetricFifo fifo;
    AttributeKey<List<SingleMetric>> listKey = AttributeKey.valueOf("listToSend");

    private Channel senderChannel;
    private int sendCounter = 0;

    private String localHostName;

    boolean isConnecting = false;

    final Object connectingMutex;
    private long numberOfMetrics = 0;

    public RestForwardingHandler(Configuration configuration) {
        LOG.debug("RestForwardingHandler init");

        URI restUrl = configuration.getRestUrl();
        restHost = restUrl.getHost();
        restPort = restUrl.getPort();
        restPrefix = restUrl.getPath();

        restCloseAfterRequests = configuration.getRestCloseAfterRequests();

        connectingMutex = this;
        fifo = new BoundMetricFifo(10, configuration.getSpoolSize());
        try {
            localHostName = InetAddress.getLocalHost().getCanonicalHostName();
        } catch (UnknownHostException e) {
            LOG.error(e.getLocalizedMessage());
            localHostName = "- unknown host -";
        }
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) throws Exception {

        @SuppressWarnings("unchecked")
        final List<SingleMetric> in = (List<SingleMetric>) msg;
        LOG.trace("Received some metrics: {}", in);

        fifo.addAll(in);

        synchronized (connectingMutex) {
            // make sure to only open one connection at a time
            if (isConnecting)
                return;
        }

        if (senderChannel != null) {
            sendToChannel(senderChannel);
            return;
        }

        ChannelFuture cf = connectRestServer(ctx.channel().eventLoop().parent());

        cf.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                synchronized (connectingMutex) {
                    isConnecting = false;
                }
                if (!future.isSuccess()) {
                    Throwable cause = future.cause();
                    if (cause instanceof ConnectException) {
                        LOG.warn("Sending failed: " + cause.getLocalizedMessage());
                    } else {
                        LOG.warn("Something went wrong: " + cause);
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
        LOG.trace("Sending to channel {}", ch);

        final List<SingleMetric> metricsToSend = fifo.getList();

        String payload = Batcher.metricListToJson(metricsToSend);
        ByteBuf content = Unpooled.copiedBuffer(payload, CharsetUtil.UTF_8);
        FullHttpRequest request = new DefaultFullHttpRequest(HTTP_1_1, POST, restPrefix, content);
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
                    LOG.error("Sending to the hawkular-metrics server failed: " + future.cause());

                } else {
                    sendCounter++;
                    if (sendCounter >= restCloseAfterRequests) {
                        SingleMetric perfMetric = new SingleMetric(localHostName + ".ptrans.counter", System
                            .currentTimeMillis(), (double) (numberOfMetrics + metricsToSend.size()));
                        fifo.offer(perfMetric);
                        LOG.trace("Doing a periodic close after {} requests and {} items", restCloseAfterRequests,
                            numberOfMetrics);
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

    ChannelFuture connectRestServer(EventLoopGroup group) throws Exception {

        synchronized (connectingMutex) {
            isConnecting = true;
        }

        Bootstrap clientBootstrap = new Bootstrap();
        clientBootstrap.group(group).channel(NioSocketChannel.class).remoteAddress(restHost, restPort)
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
            });

        return clientBootstrap.connect();
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

                if (status.equals(HttpResponseStatus.NO_CONTENT) || status.equals(HttpResponseStatus.OK)) {

                    List<SingleMetric> metricsSent = ctx.channel().attr(listKey).getAndRemove();

                    if (metricsSent != null) {
                        // only clear the ones we sent - new ones may have arrived between batching and now
                        fifo.cleanout(metricsSent);
                        numberOfMetrics += metricsSent.size();
                        logger.debug("sent {} items", metricsSent.size());
                    }
                } else {
                    logger.warn("Send to rest-server failed:" + status);
                }
            } else {
                logger.error("msg " + msg);
            }
        }
    }
}
