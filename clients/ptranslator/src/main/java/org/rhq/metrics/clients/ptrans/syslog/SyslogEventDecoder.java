package org.rhq.metrics.clients.ptrans.syslog;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

/**
 * Decoder that splits up syslog
 * @author Heiko W. Rupp
 */
public class SyslogEventDecoder extends MessageToMessageDecoder<ByteBuf> {


    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf data, List<Object> out) throws Exception {

        DecoderUtil.decodeTheBuffer(data, out);
    }

}
