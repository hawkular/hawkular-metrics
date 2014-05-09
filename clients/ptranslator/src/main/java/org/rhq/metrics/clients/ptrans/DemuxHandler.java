package org.rhq.metrics.clients.ptrans;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.util.CharsetUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.rhq.metrics.clients.ptrans.backend.RestForwardingHandler;
import org.rhq.metrics.clients.ptrans.graphite.GraphiteEventDecoder;
import org.rhq.metrics.clients.ptrans.syslog.SyslogEventDecoder;

/**
 * Demultiplex incoming connection data into their own pipelines
 * @author Heiko W. Rupp
 */
public class DemuxHandler extends ByteToMessageDecoder {

    private static final Logger logger = LoggerFactory.getLogger(DemuxHandler.class);

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List out) throws Exception {

        String data = msg.toString(CharsetUtil.UTF_8);
        if (logger.isDebugEnabled()) {
            logger.debug("Incoming: [" + data + "]");
        }

        boolean done = false;

        ChannelPipeline pipeline = ctx.pipeline();
        if (data.contains("type=metric")) {
            pipeline.addLast(new SyslogEventDecoder());
            pipeline.addLast("forwarder", new RestForwardingHandler());
            pipeline.remove(this);
            done = true;
        } else if (!data.contains("=")){
            String[] items = data.split(" |\\n");
            if (items.length % 3 == 0) {
                pipeline.addLast("encoder", new GraphiteEventDecoder());
                pipeline.addLast("forwarder", new RestForwardingHandler());
                pipeline.remove(this);
                done = true;
            }
        }
        if (!done) {
            logger.warn("Unknown input [" + data + "], ignoring");
            msg.clear();
            ctx.close();
        }
    }
}
