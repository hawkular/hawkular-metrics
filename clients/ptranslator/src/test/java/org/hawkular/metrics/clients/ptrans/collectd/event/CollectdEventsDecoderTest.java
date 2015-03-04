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
package org.hawkular.metrics.clients.ptrans.collectd.event;

import static org.hawkular.metrics.clients.ptrans.collectd.event.DataType.ABSOLUTE;
import static org.hawkular.metrics.clients.ptrans.collectd.event.DataType.GAUGE;
import static org.hawkular.metrics.clients.ptrans.collectd.event.TimeResolution.HIGH_RES;
import static org.hawkular.metrics.clients.ptrans.collectd.packet.PartType.HOST;
import static org.hawkular.metrics.clients.ptrans.collectd.packet.PartType.INSTANCE;
import static org.hawkular.metrics.clients.ptrans.collectd.packet.PartType.INTERVAL_HIGH_RESOLUTION;
import static org.hawkular.metrics.clients.ptrans.collectd.packet.PartType.PLUGIN;
import static org.hawkular.metrics.clients.ptrans.collectd.packet.PartType.PLUGIN_INSTANCE;
import static org.hawkular.metrics.clients.ptrans.collectd.packet.PartType.TIME_HIGH_RESOLUTION;
import static org.hawkular.metrics.clients.ptrans.collectd.packet.PartType.TYPE;
import static org.hawkular.metrics.clients.ptrans.collectd.packet.PartType.VALUES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.hawkular.metrics.clients.ptrans.collectd.packet.CollectdPacket;
import org.hawkular.metrics.clients.ptrans.collectd.packet.NumericPart;
import org.hawkular.metrics.clients.ptrans.collectd.packet.Part;
import org.hawkular.metrics.clients.ptrans.collectd.packet.PartType;
import org.hawkular.metrics.clients.ptrans.collectd.packet.StringPart;
import org.hawkular.metrics.clients.ptrans.collectd.packet.ValuePart;
import org.hawkular.metrics.clients.ptrans.collectd.packet.Values;
import org.junit.Test;

import io.netty.channel.embedded.EmbeddedChannel;

public class CollectdEventsDecoderTest {

    @Test
    public void handlerShouldNotOutputEventsWhenNoValuePartIsInCollectdPacket() throws Exception {
        List<Part> parts = new ArrayList<>();
        for (PartType partType : PartType.values()) {
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
                    // Don't add such part;
                    break;
                default:
                    fail("Unknown part type: " + partType);
            }
        }
        CollectdPacket packet = new CollectdPacket(parts);

        EmbeddedChannel channel = new EmbeddedChannel(new CollectdEventsDecoder());
        assertFalse("Expected no event", channel.writeInbound(packet));
    }

    @Test
    public void handlerShouldDecodeSimplePacket() throws Exception {
        List<Part> parts = new ArrayList<>();
        parts.add(new StringPart(HOST, HOST.name()));
        parts.add(new StringPart(PLUGIN, PLUGIN.name()));
        parts.add(new StringPart(PLUGIN_INSTANCE, PLUGIN_INSTANCE.name()));
        parts.add(new StringPart(TYPE, TYPE.name()));
        parts.add(new StringPart(INSTANCE, INSTANCE.name()));
        parts.add(new NumericPart(TIME_HIGH_RESOLUTION, (long) TIME_HIGH_RESOLUTION.ordinal()));
        parts.add(new NumericPart(INTERVAL_HIGH_RESOLUTION, (long) INTERVAL_HIGH_RESOLUTION.ordinal()));
        Values values = new Values(Arrays.asList(GAUGE, ABSOLUTE), Arrays.asList(13.13d, BigInteger.valueOf(13)));
        ValuePart valuePart = new ValuePart(VALUES, values);
        parts.add(valuePart);
        parts.add(valuePart); // add it twice
        CollectdPacket packet = new CollectdPacket(parts);

        EmbeddedChannel channel = new EmbeddedChannel(new CollectdEventsDecoder());
        assertTrue("Expected an event", channel.writeInbound(packet));

        Object output = channel.readInbound();
        assertEquals(ValueListEvent.class, output.getClass());
        ValueListEvent event = (ValueListEvent) output;
        checkValueListEvent(event);

        // A second event with same values should be emitted
        output = channel.readInbound();
        assertEquals(ValueListEvent.class, output.getClass());
        event = (ValueListEvent) output;
        checkValueListEvent(event);

        assertNull("Expected no more than two instances of Event", channel.readInbound());
    }

    private void checkValueListEvent(ValueListEvent event) {
        assertEquals(HOST.name(), event.getHost());
        assertEquals(PLUGIN.name(), event.getPluginName());
        assertEquals(PLUGIN_INSTANCE.name(), event.getPluginInstance());
        assertEquals(TYPE.name(), event.getTypeName());
        assertEquals(INSTANCE.name(), event.getTypeInstance());
        TimeSpan timestamp = event.getTimestamp();
        assertEquals(TIME_HIGH_RESOLUTION.ordinal(), timestamp.getValue());
        assertEquals(HIGH_RES, timestamp.getResolution());
        TimeSpan interval = event.getInterval();
        assertEquals(INTERVAL_HIGH_RESOLUTION.ordinal(), interval.getValue());
        assertEquals(HIGH_RES, interval.getResolution());
        List<Number> values = event.getValues();
        assertEquals("Expected two values", 2, values.size());
        assertEquals(Double.class, values.get(0).getClass());
        assertEquals(13.13d, values.get(0));
        assertEquals(BigInteger.class, values.get(1).getClass());
        assertEquals(BigInteger.valueOf(13), values.get(1));
    }

    @Test
    public void handlerShouldNotUseNullWhenNoTypeInstancePartHasBeenSent() throws Exception {
        List<Part> parts = new ArrayList<>();
        parts.add(new StringPart(HOST, HOST.name()));
        parts.add(new StringPart(PLUGIN, PLUGIN.name()));
        parts.add(new StringPart(PLUGIN_INSTANCE, PLUGIN_INSTANCE.name()));
        parts.add(new StringPart(TYPE, TYPE.name()));
        parts.add(new NumericPart(TIME_HIGH_RESOLUTION, (long) TIME_HIGH_RESOLUTION.ordinal()));
        parts.add(new NumericPart(INTERVAL_HIGH_RESOLUTION, (long) INTERVAL_HIGH_RESOLUTION.ordinal()));
        Values values = new Values(Arrays.asList(GAUGE, ABSOLUTE), Arrays.asList(13.13d, BigInteger.valueOf(13)));
        ValuePart valuePart = new ValuePart(VALUES, values);
        parts.add(valuePart);
        CollectdPacket packet = new CollectdPacket(parts);

        EmbeddedChannel channel = new EmbeddedChannel(new CollectdEventsDecoder());
        assertTrue("Expected an event", channel.writeInbound(packet));

        Object output = channel.readInbound();
        assertEquals(ValueListEvent.class, output.getClass());
        ValueListEvent event = (ValueListEvent) output;
        assertNotNull(event.getTypeInstance());

        assertNull("Expected exactly one instance of Event", channel.readInbound());
    }
}
