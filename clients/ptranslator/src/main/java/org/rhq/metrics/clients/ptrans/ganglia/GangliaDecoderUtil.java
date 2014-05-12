package org.rhq.metrics.clients.ptrans.ganglia;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

import org.acplt.oncrpc.OncRpcException;
import org.acplt.oncrpc.XdrBufferDecodingStream;

import org.rhq.metrics.clients.ptrans.SingleMetric;

/**
 * // TODO: Document this
 * @author Heiko W. Rupp
 */
public class GangliaDecoderUtil {

    static void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws OncRpcException, IOException {
        if (msg.readableBytes()<5) {
            msg.clear();
            ctx.close();
            return;
        }
        if (msg.getByte(0)==0 && msg.getByte(1)==0 && msg.getByte(2)==0&&msg.getByte(3)==133) {
            System.out.println("Ganglia");

            XdrBufferDecodingStream stream = new XdrBufferDecodingStream(msg.array());
            stream.beginDecoding();
            int id = stream.xdrDecodeInt();
            String host = stream.xdrDecodeString();
            String metricName = stream.xdrDecodeString();
            int spoof = stream.xdrDecodeInt();
            String format = stream.xdrDecodeString();
            String value = stream.xdrDecodeString();
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
