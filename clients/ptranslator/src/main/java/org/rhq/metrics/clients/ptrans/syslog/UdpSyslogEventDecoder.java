package org.rhq.metrics.clients.ptrans.syslog;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.MessageToMessageDecoder;

/**
 * Decode Udp syslog packets by extracting the
 * content of the datagram and passing it to the decoder.
 * @author Heiko W. Rupp
 */
public class UdpSyslogEventDecoder extends MessageToMessageDecoder<DatagramPacket>{

    @Override
    protected void decode(ChannelHandlerContext ctx, DatagramPacket msg, List<Object> out) throws Exception {

        ByteBuf buf = msg.content();

        DecoderUtil.decodeTheBuffer(buf, out);
    }
}
