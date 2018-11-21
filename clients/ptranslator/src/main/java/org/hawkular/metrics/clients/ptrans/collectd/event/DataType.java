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

import java.util.HashMap;
import java.util.Map;

/**
 * Enumerates <a href="https://collectd.org/wiki/index.php/Data_source">Data source types</a>.
 *
 * @author Thomas Segismont
 */
public enum DataType {
    /**
     * Gauges are for measurements which may go up or down, like free disk space.
     */
    GAUGE(DataTypeId.GAUGE),
    /**
     * Derives are for measurements which rate is interesting, like network packets transmitted.
     */
    DERIVE(DataTypeId.DERIVE),
    /**
     * Counters are similiar to derives, but vary circularly between the min and max value; like closed web sessions
     * since application server start.
     */
    COUNTER(DataTypeId.COUNTER),
    /**
     * Absolute is similar to counter, but is reset to min any time it is read.
     */
    ABSOLUTE(DataTypeId.ABSOLUTE);

    private byte id;

    DataType(byte id) {
        this.id = id;
    }

    /**
     * @return data type id
     */
    public byte getId() {
        return id;
    }

    private static final Map<Byte, DataType> TYPE_BY_ID = new HashMap<>();

    static {
        for (DataType dataType : DataType.values()) {
            TYPE_BY_ID.put(dataType.id, dataType);
        }
    }

    /**
     * @param id data type id
     *
     * @return the {@link DataType} which id is <code>id</code>, null otherwise
     */
    public static DataType findById(byte id) {
        return TYPE_BY_ID.get(id);
    }

    private static class DataTypeId {
        private static final byte COUNTER = 0x0;
        private static final byte GAUGE = 0x1;
        private static final byte DERIVE = 0x2;
        private static final byte ABSOLUTE = 0x3;
    }
}
