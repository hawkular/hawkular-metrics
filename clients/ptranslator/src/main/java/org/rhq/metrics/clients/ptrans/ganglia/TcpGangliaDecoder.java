package org.rhq.metrics.clients.ptrans.ganglia;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

/**
 * // TODO: Document this
 * @author Heiko W. Rupp
 */
public class TcpGangliaDecoder extends MessageToMessageDecoder<ByteBuf> {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {
        GangliaDecoderUtil.decode(ctx, msg, out);
    }
}
