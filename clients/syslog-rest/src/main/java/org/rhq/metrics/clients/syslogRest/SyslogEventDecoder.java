package org.rhq.metrics.clients.syslogRest;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.util.CharsetUtil;

/**
 * Decoder that splits up syslog
 * @author Heiko W. Rupp
 */
public class SyslogEventDecoder extends MessageToMessageDecoder<DatagramPacket> {

//    private DateTimeFormatter timeFormatter = DateTimeFormat.forPattern("MMM dd HH:mm:ss").withLocale(Locale.US);

    @Override
    protected void decode(ChannelHandlerContext ctx, DatagramPacket msg, List<Object> out) throws Exception {

        ByteBuf data = msg.content();

        String s = data.toString(CharsetUtil.UTF_8);
        System.out.println("Incoming: " + s);
        if (!s.contains("type=metric")) {
            return;
        }

        int i = data.indexOf(0, data.readableBytes(),
            (byte) '>');
        ByteBuf buf = data.slice(i + 1, data.readableBytes());
        ByteBuf dateBuf = data.slice(i+1,i+13);

        i = data.indexOf(i+16,data.readableBytes(),(byte)':');
        buf = data.slice(i+2,data.readableBytes());
        String text = buf.toString(CharsetUtil.UTF_8);

        if (text.contains("type=metric")) {

            long now = System.currentTimeMillis();

            String[] entries = text.split(" ");
            for (String entry: entries) {
                if (entry.equals("type=metric")) {
                    continue;
                }
                String[] keyVal = entry.split("=");
                SyslogMetricEvent event = new SyslogMetricEvent(keyVal[0],now, Double.parseDouble(keyVal[1]));
                out.add(event);
            }
        }
    }
}
