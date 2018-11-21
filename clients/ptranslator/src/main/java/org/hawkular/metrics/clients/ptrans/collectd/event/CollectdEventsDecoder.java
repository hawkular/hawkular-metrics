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

import static org.hawkular.metrics.clients.ptrans.collectd.event.TimeResolution.HIGH_RES;
import static org.hawkular.metrics.clients.ptrans.collectd.event.TimeResolution.SECONDS;

import java.util.ArrayList;
import java.util.List;

import org.hawkular.metrics.clients.ptrans.collectd.packet.CollectdPacket;
import org.hawkular.metrics.clients.ptrans.collectd.packet.NumericPart;
import org.hawkular.metrics.clients.ptrans.collectd.packet.Part;
import org.hawkular.metrics.clients.ptrans.collectd.packet.PartType;
import org.hawkular.metrics.clients.ptrans.collectd.packet.StringPart;
import org.hawkular.metrics.clients.ptrans.collectd.packet.ValuePart;
import org.hawkular.metrics.clients.ptrans.collectd.packet.Values;
import org.jboss.logging.Logger;

/**
 * @author Thomas Segismont
 */
public final class CollectdEventsDecoder {
    private static final Logger log = Logger.getLogger(CollectdEventsDecoder.class);

    private static final String EMPTY_STRING_VALUE = "";

    public List<Event> decode(CollectdPacket packet) {
        long start = System.currentTimeMillis();
        String host = EMPTY_STRING_VALUE, pluginName = EMPTY_STRING_VALUE, pluginInstance = EMPTY_STRING_VALUE;
        String typeName = EMPTY_STRING_VALUE, typeInstance = EMPTY_STRING_VALUE;
        TimeSpan timestamp = null, interval = null;
        List<Event> events = new ArrayList<>(50);
        for (Part<?> part : packet.getParts()) {
            PartType partType = part.getPartType();
            switch (partType) {
                case HOST:
                    host = getString(part);
                    break;
                case PLUGIN:
                    pluginName = getString(part);
                    break;
                case PLUGIN_INSTANCE:
                    pluginInstance = getString(part);
                    break;
                case TYPE:
                    typeName = getString(part);
                    break;
                case INSTANCE:
                    typeInstance = getString(part);
                    break;
                case TIME:
                    timestamp = new TimeSpan(getLong(part), SECONDS);
                    break;
                case TIME_HIGH_RESOLUTION:
                    timestamp = new TimeSpan(getLong(part), HIGH_RES);
                    break;
                case INTERVAL:
                    interval = new TimeSpan(getLong(part), SECONDS);
                    break;
                case INTERVAL_HIGH_RESOLUTION:
                    interval = new TimeSpan(getLong(part), HIGH_RES);
                    break;
                case VALUES:
                    ValueListEvent event = new ValueListEvent(
                            host, timestamp, pluginName, pluginInstance, typeName,
                            typeInstance, getValues(part).getData(), interval
                    );
                    log.tracef("Decoded ValueListEvent: %s", event);
                    events.add(event);
                    break;
                default:
                    log.tracef("Skipping unknown part type: %s", partType);
            }
        }

        if (log.isTraceEnabled()) {
            long stop = System.currentTimeMillis();
            log.tracef("Decoded events in %d ms", stop - start);
        }

        return events;
    }

    private String getString(Part<?> part) {
        return ((StringPart) part).getValue();
    }

    private Long getLong(Part<?> part) {
        return ((NumericPart) part).getValue();
    }

    private Values getValues(Part<?> part) {
        return ((ValuePart) part).getValue();
    }
}
