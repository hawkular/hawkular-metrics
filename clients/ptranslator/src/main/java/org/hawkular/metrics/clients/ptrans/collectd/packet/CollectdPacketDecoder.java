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
package org.hawkular.metrics.clients.ptrans.collectd.packet;

import static io.netty.channel.ChannelHandler.Sharable;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import org.hawkular.metrics.clients.ptrans.collectd.event.DataType;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.util.CharsetUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

/**
 * A Netty decoding handler: from {@link DatagramPacket} to {@link CollectdPacket}.
 *
 * @author Thomas Segismont
 */
@Sharable
public final class CollectdPacketDecoder extends MessageToMessageDecoder<DatagramPacket> {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(CollectdPacketDecoder.class);

    @Override
    protected void decode(ChannelHandlerContext context, DatagramPacket packet, List<Object> out) throws Exception {
        long start = System.currentTimeMillis();
        ByteBuf content = packet.content();
        List<Part> parts = new ArrayList<>(100);
        for (; ; ) {
            if (!hasReadableBytes(content, 4)) {
                break;
            }
            short partTypeId = content.readShort();
            PartType partType = PartType.findById(partTypeId);
            int partLength = content.readUnsignedShort();
            int valueLength = partLength - 4;
            if (!hasReadableBytes(content, valueLength)) {
                break;
            }
            if (partType == null) {
                content.skipBytes(valueLength);
                continue;
            }
            Part part;
            switch (partType) {
                case HOST:
                case PLUGIN:
                case PLUGIN_INSTANCE:
                case TYPE:
                case INSTANCE:
                    part = new StringPart(partType, readStringPartContent(content, valueLength));
                    break;
                case TIME:
                case TIME_HIGH_RESOLUTION:
                case INTERVAL:
                case INTERVAL_HIGH_RESOLUTION:
                    part = new NumericPart(partType, readNumericPartContent(content));
                    break;
                case VALUES:
                    part = new ValuePart(partType, readValuePartContent(content, valueLength));
                    break;
                default:
                    part = null;
                    content.skipBytes(valueLength);
            }
            //noinspection ConstantConditions
            if (part != null) {
                logger.trace("Decoded part: {}", part);
                parts.add(part);
            }
        }

        if (logger.isTraceEnabled()) {
            long stop = System.currentTimeMillis();
            logger.trace("Decoded datagram in {} ms", stop - start);
        }

        if (parts.size() > 0) {
            CollectdPacket collectdPacket = new CollectdPacket(parts);
            out.add(collectdPacket);
        } else {
            logger.debug("No parts decoded, no CollectdPacket output");
        }
    }

    private boolean hasReadableBytes(ByteBuf content, int count) {
        return content.readableBytes() >= count;
    }

    private String readStringPartContent(ByteBuf content, int length) {
        String string = content.toString(
                content.readerIndex(), length - 1 /* collectd strings are \0 terminated */,
                CharsetUtil.US_ASCII
        );
        content.skipBytes(length); // the previous call does not move the readerIndex
        return string;
    }

    private long readNumericPartContent(ByteBuf content) {
        return content.readLong();
    }

    private Values readValuePartContent(ByteBuf content, int length) {
        int beginIndex = content.readerIndex();
        int total = content.readUnsignedShort();
        List<DataType> dataTypes = new ArrayList<>(total);
        for (int i = 0; i < total; i++) {
            byte sampleTypeId = content.readByte();
            dataTypes.add(DataType.findById(sampleTypeId));
        }
        List<Number> data = new ArrayList<>(total);
        for (DataType dataType : dataTypes) {
            switch (dataType) {
                case COUNTER:
                case ABSOLUTE:
                    byte[] valueBytes = new byte[8];
                    content.readBytes(valueBytes);
                    data.add(new BigInteger(1, valueBytes));
                    break;
                case DERIVE:
                    data.add(content.readLong());
                    break;
                case GAUGE:
                    data.add(Double.longBitsToDouble(ByteBufUtil.swapLong(content.readLong())));
                    break;
                default:
                    logger.debug("Skipping unknown data type: {}", dataType);
            }
        }
        // Skip any additionnal bytes
        int readCount = content.readerIndex() - beginIndex;
        if (length > readCount) {
            content.skipBytes(readCount - length);
        }
        return new Values(dataTypes, data);
    }
}
