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

import java.util.HashMap;
import java.util.Map;

/**
 * Enumerates <a href="https://collectd.org/wiki/index.php/Binary_protocol#Part_types">Part Types</a>.
 *
 * @author Thomas Segismont
 */
public enum PartType {
    /**
     * Hostname of the machine where the values were collected.
     */
    HOST(PartTypeId.HOST),
    /**
     * When the values were collected, in second resolution since epoch. *
     */
    TIME(PartTypeId.TIME),
    /**
     * When the values were collected, in high resolution since epoch.
     *
     * @see org.hawkular.metrics.clients.ptrans.collectd.event.TimeResolution#HIGH_RES
     */
    TIME_HIGH_RESOLUTION(PartTypeId.TIME_HIGH_RESOLUTION),
    /**
     * The collectd plugin corresponding to the values collected, e.g. "cpu". *
     */
    PLUGIN(PartTypeId.PLUGIN),
    /**
     * The collectd plugin instance corresponding to the values collected, e.g. "1". *
     */
    PLUGIN_INSTANCE(PartTypeId.PLUGIN_INSTANCE),
    /**
     * The collectd type corresponding to the values collected, e.g. "cpu". *
     */
    TYPE(PartTypeId.TYPE),
    /**
     * The collectd type instance corresponding to the values collected, e.g. "idle". *
     */
    INSTANCE(PartTypeId.INSTANCE),
    /**
     * Samples with data value type. *
     */
    VALUES(PartTypeId.VALUES),
    /**
     * How often values are collected, in second resolution since epoch. *
     */
    INTERVAL(PartTypeId.INTERVAL),
    /**
     * How often values are collected, in high resolution since epoch.
     *
     * @see org.hawkular.metrics.clients.ptrans.collectd.event.TimeResolution#HIGH_RES
     */
    INTERVAL_HIGH_RESOLUTION(PartTypeId.INTERVAL_HIGH_RESOLUTION);

    private short id;

    PartType(short id) {
        this.id = id;
    }

    /**
     * @return the part type id
     */
    public short getId() {
        return id;
    }

    private static final Map<Short, PartType> TYPE_BY_ID = new HashMap<>();

    static {
        for (PartType partType : PartType.values()) {
            TYPE_BY_ID.put(partType.id, partType);
        }
    }

    /**
     * @param id part type id
     *
     * @return the {@link PartType} which id is <code>id</code>, null otherwise
     */
    public static PartType findById(short id) {
        return TYPE_BY_ID.get(id);
    }

    private static class PartTypeId {
        private static final short HOST = 0x0000;
        private static final short TIME = 0x0001;
        private static final short TIME_HIGH_RESOLUTION = 0x0008;
        private static final short PLUGIN = 0x0002;
        private static final short PLUGIN_INSTANCE = 0x0003;
        private static final short TYPE = 0x0004;
        private static final short INSTANCE = 0x0005;
        private static final short VALUES = 0x0006;
        private static final short INTERVAL = 0x0007;
        private static final short INTERVAL_HIGH_RESOLUTION = 0x0009;
    }
}
