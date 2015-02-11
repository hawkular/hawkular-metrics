/*
 * Copyright 2014-2015 Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hawkular.metrics.clients.ptrans.ganglia;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.util.List;

import org.acplt.oncrpc.XdrBufferDecodingStream;
import org.hawkular.metrics.client.common.SingleMetric;

/**
 * A Decoder for Ganglia metrics packets.
 * Ganglia sends each metric value in its own packet.
 * Data is XDR encoded.
 * @author Heiko W. Rupp
 */
public class UdpGangliaDecoder extends MessageToMessageDecoder<DatagramPacket> {

    @Override
    protected void decode(ChannelHandlerContext ctx, DatagramPacket in, List<Object> out) throws Exception {
        ByteBuf msg = in.content();
        if (msg.readableBytes()<5) {
            msg.clear();
            ctx.close();
            return;
        }

        short magic = msg.getUnsignedByte(3);
        if (msg.getByte(0)==0 && msg.getByte(1)==0 && msg.getByte(2)==0&& magic ==134) {

            // We have an UnsafeSuperDuperBuffer, so we need to "manually" pull the bytes from it.
            byte[] bytes = new byte[msg.readableBytes()];
            msg.readBytes(bytes);

            XdrBufferDecodingStream stream = new XdrBufferDecodingStream(bytes);
            stream.beginDecoding();
            stream.xdrDecodeInt(); // Packet id , should be 134 as in above magic => type of value
            String host = stream.xdrDecodeString();
            String metricName = stream.xdrDecodeString();
            stream.xdrDecodeInt();
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
                out.add(metric);
            }
            catch (Exception e) {
                e.printStackTrace();
                msg.clear();
                ctx.close();

            }
        }

    }
}
