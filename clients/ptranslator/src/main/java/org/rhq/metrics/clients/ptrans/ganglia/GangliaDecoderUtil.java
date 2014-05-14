package org.rhq.metrics.clients.ptrans.ganglia;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;

import org.acplt.oncrpc.OncRpcException;
import org.acplt.oncrpc.XdrBufferDecodingStream;
import org.rhq.metrics.clients.ptrans.SingleMetric;

/**
 * // TODO: Document this
 * @author Heiko W. Rupp
 */
public class GangliaDecoderUtil {

    @SuppressWarnings("unused")
	static void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws OncRpcException, IOException {
        if (msg.readableBytes()<5) {
            msg.clear();
            ctx.close();
            return;
        }

        short magic = msg.getUnsignedByte(3);
        System.out.println("Magic: " + magic);
        if (msg.getByte(0)==0 && msg.getByte(1)==0 && msg.getByte(2)==0&& magic ==134) {

            // We have an UnsafeSuperDuperBuffer, so we need to "manually" pull the bytes from it.
            byte[] bytes = new byte[msg.readableBytes()];
            msg.readBytes(bytes);

            XdrBufferDecodingStream stream = new XdrBufferDecodingStream(bytes);
            stream.beginDecoding();
            int id = stream.xdrDecodeInt(); // Packet id , should be 134 as in above magic => type of value
            String host = stream.xdrDecodeString();
            String metricName = stream.xdrDecodeString();
            int spoof = stream.xdrDecodeInt();
            String format = stream.xdrDecodeString(); // e.g. .0f for a number
            String value;
            if (format.endsWith("f")) {
                value = String.valueOf(stream.xdrDecodeFloat());
            } else {
                value = stream.xdrDecodeString();
            }
            stream.endDecoding();

            try {
                String path = host + "." + metricName;
                Double val = Double.parseDouble(value);

                SingleMetric metric = new SingleMetric(path,System.currentTimeMillis(),val);
                List<SingleMetric> metrics = new ArrayList<>(1);
                metrics.add(metric);
                out.add(metrics);
            }
            catch (Exception e) {
                e.printStackTrace();
                msg.clear();
                ctx.close();

            }
        }
    }
}
