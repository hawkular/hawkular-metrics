package org.rhq.metrics.clients.syslogRest;

import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.util.CharsetUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decoder for plaintext metric data sent from Graphite
 * See {@see http://graphite.readthedocs.org/en/latest/feeding-carbon.html}
 *
 * Format is source value path[\nsource value path]?
 *
 * @author Heiko W. Rupp
 */
public class GraphiteEventDecoder extends MessageToMessageDecoder<ByteBuf> {

    private static final Logger logger = LoggerFactory.getLogger(GraphiteEventDecoder.class);

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {

        if (msg.readableBytes()<1) {
            return; // Nothing to do
        }

        String data = msg.toString(CharsetUtil.UTF_8);

        if (logger.isDebugEnabled()) {
            logger.debug("Incoming: >>" + data + "<<");
        }
        data = data.trim();

        String[] lines = data.split("\\n");
        if (lines.length==0) {
            return;
        }
        List<SingleMetric> metricList = new ArrayList<>(lines.length);

        for (String line : lines) {
            String[] items = line.split(" ");
            if (items.length != 3) {
                logger.debug("Unknown data format for [" + data + "], skipping");
                return;
            }

            long secondsSinceEpoch = Long.parseLong(items[2]);
            long timestamp = secondsSinceEpoch * 1000L;
            SingleMetric metric = new SingleMetric(items[0], timestamp, Double.parseDouble(items[1]));
            metricList.add(metric);
        }
        out.add(metricList);
    }
}
