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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.Collections;
import java.util.List;

import org.hawkular.metrics.client.common.SingleMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

/**
 * Decoder for plaintext metric data sent from Graphite.
 *
 * @author Heiko W. Rupp
 * @author Thomas Segismont
 */
public class GraphiteEventDecoder extends MessageToMessageDecoder<String> {
    private static final Logger LOG = LoggerFactory.getLogger(GraphiteEventDecoder.class);

    @Override
    protected void decode(ChannelHandlerContext ctx, String msg, List<Object> out) throws Exception {
        String[] items = msg.split(" ");
        if (items.length != 3) {
            LOG.debug("Unknown data format for '{}', skipping", msg);
            return;
        }

        String name = items[0];
        double value = Double.parseDouble(items[1]);
        long timestamp = MILLISECONDS.convert(Long.parseLong(items[2]), SECONDS);

        SingleMetric metric = new SingleMetric(name, timestamp, value);

        out.add(Collections.singletonList(metric));
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        LOG.debug("Could not decode message", cause);
        ctx.channel().close();
    }
}
