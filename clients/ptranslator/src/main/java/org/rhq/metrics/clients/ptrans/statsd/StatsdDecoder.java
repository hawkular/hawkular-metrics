package org.rhq.metrics.clients.ptrans.statsd;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.util.CharsetUtil;

import org.rhq.metrics.client.common.MetricType;
import org.rhq.metrics.client.common.SingleMetric;

/**
 * Decoder for Stats packets that comes in the form of
 * name:value[|type]
 * Type is a letter:
 * <ul>
 *     <li>g: Gauge</li>
 *     <li>c: Counter</li>
 *     <li>ms: Timing</li>
 *     <li>s: Set</li>
 * </ul>
 * @author Heiko W. Rupp
 */
public class StatsdDecoder extends MessageToMessageDecoder<DatagramPacket> {

    @Override
    protected void decode(ChannelHandlerContext ctx, DatagramPacket msg, List<Object> out) throws Exception {
        ByteBuf buf = msg.content();

        if (buf.readableBytes()<3) {
            // Not enough data - nothing to do.
            buf.clear();
            ctx.close();
            return;
        }

        String packet = buf.toString(CharsetUtil.UTF_8).trim();
        if (!packet.contains(":")) {
            buf.clear();
            ctx.close();
            return;
        }


        String name = packet.substring(0,packet.indexOf(":"));
        String remainder = packet.substring(packet.indexOf(":")+1);
        String valString;
        String type;
        if (remainder.contains("|")) {
            valString = remainder.substring(0,remainder.indexOf("|"));
            type = remainder.substring(remainder.indexOf("|")+1);
        }
        else {
            valString = remainder;
            type = "";
        }
        Double value = Double.valueOf(valString);
        SingleMetric singleMetric = new SingleMetric(name, System.currentTimeMillis(),value, MetricType.from(type));
        out.add(singleMetric);
    }
}
