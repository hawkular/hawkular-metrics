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
package org.hawkular.metrics.clients.ptrans.statsd;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.util.CharsetUtil;

import org.hawkular.metrics.client.common.MetricType;
import org.hawkular.metrics.client.common.SingleMetric;

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
