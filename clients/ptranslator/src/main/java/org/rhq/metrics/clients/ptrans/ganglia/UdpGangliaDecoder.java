package org.rhq.metrics.clients.ptrans.ganglia;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.MessageToMessageDecoder;

/**
 * A Decoder for Ganglia metrics packets
 * @author Heiko W. Rupp
 */
public class UdpGangliaDecoder extends MessageToMessageDecoder<DatagramPacket> {

    @Override
    protected void decode(ChannelHandlerContext ctx, DatagramPacket in, List<Object> out) throws Exception {
        ByteBuf msg = in.content();
        GangliaDecoderUtil.decode(ctx, msg, out);

    }
}
