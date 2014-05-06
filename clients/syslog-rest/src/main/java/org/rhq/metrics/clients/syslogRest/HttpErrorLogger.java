package org.rhq.metrics.clients.syslogRest;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that checks for http response code and logs an error
 * if this is other than 200 or 204
 * @author Heiko W. Rupp
*/
class HttpErrorLogger extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(HttpErrorLogger.class);

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof FullHttpResponse) {
            FullHttpResponse response = (FullHttpResponse) msg;
            if (!response.getStatus().equals(HttpResponseStatus.NO_CONTENT) &&
                !response.getStatus().equals(HttpResponseStatus.OK)){
                logger.warn("Send failed to rest-server failed:" + response.toString());
            }
        }
    }
}
