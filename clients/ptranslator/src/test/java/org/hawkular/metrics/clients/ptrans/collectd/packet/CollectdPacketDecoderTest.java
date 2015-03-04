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

import static java.util.stream.Collectors.toList;
import static org.hawkular.metrics.clients.ptrans.collectd.packet.PacketDecodingTest.createNumericPartBuffer;
import static org.hawkular.metrics.clients.ptrans.collectd.packet.PacketDecodingTest.createStringPartBuffer;
import static org.hawkular.metrics.clients.ptrans.collectd.packet.PacketDecodingTest.createValuesPartBuffer;
import static org.hawkular.metrics.clients.ptrans.collectd.packet.PacketDecodingTest.newValuesInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.socket.DatagramPacket;

public class CollectdPacketDecoderTest {
    private static final InetSocketAddress DUMMY_ADDRESS = InetSocketAddress.createUnresolved("dummy", 9999);

    @Test
    public void handlerShouldNotOutputCollectdPacketWhenNoPartIsDecoded() {
        DatagramPacket datagramPacket = new DatagramPacket(Unpooled.buffer(), DUMMY_ADDRESS);
        EmbeddedChannel channel = new EmbeddedChannel(new CollectdPacketDecoder());
        assertFalse("Expected no CollectdPacket", channel.writeInbound(datagramPacket));
    }

    @Test
    public void handlerShouldDecodePacketsInOrder() {
        int numberOfPartTypes = PartType.values().length;
        int numberOfParts = numberOfPartTypes * 50;

        List<Part> parts = new ArrayList<>(numberOfParts);
        for (int i = 0; i < numberOfParts; i++) {
            PartType partType = PartType.values()[i % numberOfPartTypes];
            switch (partType) {
                case HOST:
                case PLUGIN:
                case PLUGIN_INSTANCE:
                case TYPE:
                case INSTANCE:
                    parts.add(new StringPart(partType, "marseille"));
                    break;
                case TIME:
                case TIME_HIGH_RESOLUTION:
                case INTERVAL:
                case INTERVAL_HIGH_RESOLUTION:
                    parts.add(new NumericPart(partType, 13L));
                    break;
                case VALUES:
                    parts.add(new ValuePart(partType, newValuesInstance()));
                    break;
                default:
                    fail("Unknown part type: " + partType);
            }
        }
        Collections.shuffle(parts);

        ByteBuf buffer = Unpooled.buffer();
        for (Part part : parts) {
            PartType partType = part.getPartType();
            switch (partType) {
                case HOST:
                case PLUGIN:
                case PLUGIN_INSTANCE:
                case TYPE:
                case INSTANCE:
                    buffer.writeBytes(createStringPartBuffer((String) part.getValue(), partType));
                    break;
                case TIME:
                case TIME_HIGH_RESOLUTION:
                case INTERVAL:
                case INTERVAL_HIGH_RESOLUTION:
                    buffer.writeBytes(createNumericPartBuffer((Long) part.getValue(), partType));
                    break;
                case VALUES:
                    buffer.writeBytes(createValuesPartBuffer((Values) part.getValue()));
                    break;
                default:
                    fail("Unknown part type: " + partType);
            }
        }

        DatagramPacket datagramPacket = new DatagramPacket(buffer, DUMMY_ADDRESS);
        EmbeddedChannel channel = new EmbeddedChannel(new CollectdPacketDecoder());
        assertTrue("Expected CollectdPacket", channel.writeInbound(datagramPacket));

        Object output = channel.readInbound();
        assertEquals(CollectdPacket.class, output.getClass());

        CollectdPacket collectdPacket = (CollectdPacket) output;
        List<Part> partsResult = collectdPacket.getParts();
        assertEquals("Wrong number of parts in the packet", numberOfParts, partsResult.size());
        assertEquals("Wrong packet order", toPartTypeList(parts), toPartTypeList(partsResult));

        assertNull("Expected just one CollectdPacket", channel.readInbound());
    }

    private List<PartType> toPartTypeList(List<Part> parts) {
        return parts.stream()
                    .map(Part::getPartType)
                    .collect(toList());
    }
}