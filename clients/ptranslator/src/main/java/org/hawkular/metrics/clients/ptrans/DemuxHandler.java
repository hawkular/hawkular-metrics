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
package org.hawkular.metrics.clients.ptrans;

import java.util.List;

import org.hawkular.metrics.clients.ptrans.syslog.SyslogEventDecoder;
import org.jboss.logging.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.util.CharsetUtil;

/**
 * Demultiplex incoming connection data into their own pipelines
 * @author Heiko W. Rupp
 */
public class DemuxHandler extends ByteToMessageDecoder {
    private static final Logger log = Logger.getLogger(DemuxHandler.class);

    private ChannelInboundHandlerAdapter forwardingHandler;

    public DemuxHandler(ChannelInboundHandlerAdapter forwardingHandler) {
        this.forwardingHandler = forwardingHandler;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf msg, @SuppressWarnings("rawtypes") List out)
        throws Exception {

        if (msg.readableBytes() < 5) {
            msg.clear();
            ctx.close();
            return;
        }
        ChannelPipeline pipeline = ctx.pipeline();

        String data = msg.toString(CharsetUtil.UTF_8);
        if (log.isTraceEnabled()) {
            log.trace("Incoming: [" + data + "]");
        }

        boolean done = false;

        if (data.contains("type=metric")) {
            pipeline.addLast(new SyslogEventDecoder());
            pipeline.addLast("forwarder", forwardingHandler);
            pipeline.remove(this);
            done = true;
        }

        if (!done) {
            if (log.isTraceEnabled()) {
                log.trace("Unknown input [" + data + "], ignoring");
            }
            msg.clear();
            ctx.close();
        }
    }
}
