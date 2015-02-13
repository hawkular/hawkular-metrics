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

import static org.hawkular.metrics.clients.ptrans.collectd.packet.PartType.HOST;
import static org.hawkular.metrics.clients.ptrans.collectd.packet.PartType.INSTANCE;
import static org.hawkular.metrics.clients.ptrans.collectd.packet.PartType.INTERVAL;
import static org.hawkular.metrics.clients.ptrans.collectd.packet.PartType.INTERVAL_HIGH_RESOLUTION;
import static org.hawkular.metrics.clients.ptrans.collectd.packet.PartType.PLUGIN;
import static org.hawkular.metrics.clients.ptrans.collectd.packet.PartType.PLUGIN_INSTANCE;
import static org.hawkular.metrics.clients.ptrans.collectd.packet.PartType.TIME;
import static org.hawkular.metrics.clients.ptrans.collectd.packet.PartType.TIME_HIGH_RESOLUTION;
import static org.hawkular.metrics.clients.ptrans.collectd.packet.PartType.TYPE;
import static org.hawkular.metrics.clients.ptrans.collectd.packet.PartType.VALUES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.util.Arrays;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.core.IsEqual;
import org.hawkular.metrics.clients.ptrans.collectd.event.DataType;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.socket.DatagramPacket;
import io.netty.util.CharsetUtil;

public class PacketDecodingTest {
    private static final InetSocketAddress DUMMY_ADDRESS = InetSocketAddress.createUnresolved("dummy", 9999);

    @Test
    public void shouldDecodeHostPart() {
        shouldDecodeStringPart("marseille", HOST);
    }

    @Test
    public void shouldDecodePluginPart() {
        shouldDecodeStringPart("marseille", PLUGIN);
    }

    @Test
    public void shouldDecodePluginInstancePart() {
        shouldDecodeStringPart("marseille", PLUGIN_INSTANCE);
    }

    @Test
    public void shouldDecodeTypePart() {
        shouldDecodeStringPart("marseille", TYPE);
    }

    @Test
    public void shouldDecodeTypeInstancePart() {
        shouldDecodeStringPart("marseille", INSTANCE);
    }

    private void shouldDecodeStringPart(String value, PartType partType) {
        shouldDecodePart(value, partType, createStringPartBuffer(value, partType), StringPart.class);
    }

    static ByteBuf createStringPartBuffer(String value, PartType partType) {
        ByteBuf buffer = Unpooled.buffer();
        buffer.writeShort(partType.getId());
        ByteBuf src = Unpooled.copiedBuffer(value, CharsetUtil.US_ASCII);
        buffer.writeShort(4 + src.readableBytes() + 1);
        buffer.writeBytes(src);
        buffer.writeByte(0);
        return buffer;
    }

    @Test
    public void shouldDecodeTimePart() {
        shouldDecodeNumericPart(4505408L, TIME);
    }

    @Test
    public void shouldDecodeTimeHighResolutionPart() {
        shouldDecodeNumericPart(4505408L, TIME_HIGH_RESOLUTION);
    }

    @Test
    public void shouldDecodeIntervalPart() {
        shouldDecodeNumericPart(4505408L, INTERVAL);
    }

    @Test
    public void shouldDecodeIntervalHighResolutionPart() {
        shouldDecodeNumericPart(4505408L, INTERVAL_HIGH_RESOLUTION);
    }

    private void shouldDecodeNumericPart(Long value, PartType partType) {
        shouldDecodePart(value, partType, createNumericPartBuffer(value, partType), NumericPart.class);
    }

    static ByteBuf createNumericPartBuffer(Long value, PartType partType) {
        ByteBuf buffer = Unpooled.buffer();
        buffer.writeShort(partType.getId());
        buffer.writeShort(12);
        buffer.writeLong(value);
        return buffer;
    }

    @Test
    public void shouldDecodeValuesPart() {
        Values values = newValuesInstance();
        shouldDecodePart(VALUES, createValuesPartBuffer(values), ValuePart.class, new ValuesMatcher(values));
    }

    static Values newValuesInstance() {
        DataType[] dataTypes = DataType.values();
        Number[] data = new Number[dataTypes.length];
        for (int i = 0; i < data.length; i++) {
            DataType dataType = dataTypes[i];
            switch (dataType) {
                case COUNTER:
                case ABSOLUTE:
                    byte[] valueBytes = new byte[8];
                    Arrays.fill(valueBytes, (byte) 4);
                    data[i] = new BigInteger(1, valueBytes);
                    break;
                case DERIVE:
                    data[i] = 981254L;
                    break;
                case GAUGE:
                    data[i] = 15784.02564d;
                    break;
                default:
                    fail("Unknown data type: " + dataType);
            }
        }
        return new Values(dataTypes, data);
    }

    static ByteBuf createValuesPartBuffer(Values values) {
        Number[] data = values.getData();
        DataType[] dataTypes = values.getDataTypes();
        ByteBuf payloadBuffer = Unpooled.buffer();
        for (int i = 0; i < data.length; i++) {
            payloadBuffer.writeByte(dataTypes[i].getId());
        }
        for (int i = 0; i < data.length; i++) {
            DataType dataType = dataTypes[i];
            switch (dataType) {
                case COUNTER:
                case ABSOLUTE:
                    BigInteger bigInteger = (BigInteger) data[i];
                    payloadBuffer.writeBytes(bigInteger.toByteArray());
                    break;
                case DERIVE:
                    payloadBuffer.writeLong((Long) data[i]);
                    break;
                case GAUGE:
                    payloadBuffer.writeLong(ByteBufUtil.swapLong(Double.doubleToLongBits((Double) data[i])));
                    break;
                default:
                    fail("Unknown data type: " + dataType);
            }
        }

        ByteBuf headerBuffer = Unpooled.buffer();
        headerBuffer.writeShort(VALUES.getId());
        headerBuffer.writeShort(6 + payloadBuffer.writerIndex());
        headerBuffer.writeShort(data.length);

        ByteBuf buffer = Unpooled.buffer();
        buffer.writeBytes(headerBuffer.duplicate()).writeBytes(payloadBuffer.duplicate());
        return buffer;
    }

    private void shouldDecodePart(Object value, PartType partType, ByteBuf buffer, Class<? extends Part> partClass) {
        shouldDecodePart(partType, buffer, partClass, new IsEqual<>(value));
    }

    private void shouldDecodePart(
            PartType partType, ByteBuf buffer, Class<? extends Part> partClass,
            Matcher<Object> matcher
    ) {

        DatagramPacket datagramPacket = new DatagramPacket(buffer.duplicate(), DUMMY_ADDRESS);

        EmbeddedChannel channel = new EmbeddedChannel(new CollectdPacketDecoder());
        assertTrue("Expected a CollectdPacket", channel.writeInbound(datagramPacket));

        Object output = channel.readInbound();
        assertEquals(CollectdPacket.class, output.getClass());

        CollectdPacket collectdPacket = (CollectdPacket) output;
        Part[] parts = collectdPacket.getParts();
        assertEquals("Expected only one part in the packet", 1, parts.length);

        Part part = parts[0];
        assertEquals(partClass, part.getClass());
        assertEquals(partType, part.getPartType());
        assertThat(part.getValue(), matcher);

        assertNull("Expected just one CollectdPacket", channel.readInbound());
    }

    private static class ValuesMatcher extends BaseMatcher<Object> {
        private final Values expected;

        public ValuesMatcher(Values expected) {
            this.expected = expected;
        }

        @Override
        public boolean matches(Object item) {
            if (!(item instanceof Values)) {
                return false;
            }
            Values actual = (Values) item;
            return Arrays.equals(expected.getData(), actual.getData())
                   && Arrays.equals(expected.getDataTypes(), actual.getDataTypes());
        }

        @Override
        public void describeTo(Description description) {
            description.appendValue(expected);
        }
    }
}