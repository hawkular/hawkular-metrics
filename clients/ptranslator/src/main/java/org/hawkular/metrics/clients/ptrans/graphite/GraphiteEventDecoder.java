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
package org.hawkular.metrics.clients.ptrans.graphite;

import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.util.CharsetUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.hawkular.metrics.client.common.SingleMetric;

/**
 * Decoder for plaintext metric data sent from Graphite
 * @see <a href="http://graphite.readthedocs.org/en/latest/feeding-carbon.html">Graphite - Feeding Carbon</a>
 *
 * Format is source value path[\nsource value path]?
 *
 * @author Heiko W. Rupp
 */
public class GraphiteEventDecoder extends MessageToMessageDecoder<ByteBuf> {

    private static final Logger logger = LoggerFactory.getLogger(GraphiteEventDecoder.class);

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {

        if (msg.readableBytes()<1) {
            return; // Nothing to do
        }

        String data = msg.toString(CharsetUtil.UTF_8);

        data = data.trim();

        String[] lines = data.split("\\n");
        if (lines.length==0) {
            return;
        }
        List<SingleMetric> metricList = new ArrayList<>(lines.length);

        for (String line : lines) {
            String[] items = line.split(" ");
            if (items.length != 3) {
                logger.debug("Unknown data format for [" + data + "], skipping");
                return;
            }

            long secondsSinceEpoch = Long.parseLong(items[2]);
            long timestamp = secondsSinceEpoch * 1000L;
            SingleMetric metric = new SingleMetric(items[0], timestamp, Double.parseDouble(items[1]));
            metricList.add(metric);
        }
        out.add(metricList);
    }
}
