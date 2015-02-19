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

import static org.hawkular.metrics.clients.ptrans.collectd.util.Assert.assertEquals;
import static org.hawkular.metrics.clients.ptrans.collectd.util.Assert.assertNotNull;

import java.util.Arrays;

import org.hawkular.metrics.clients.ptrans.collectd.event.DataType;

/**
 * Holds the content of a <a href="https://collectd.org/wiki/index.php/Binary_protocol#Value_parts">Value Part</a>.
 *
 * @author Thomas Segismont
 */
public final class Values {
    private final DataType[] dataTypes;
    private final Number[] data;

    /**
     * Creates a new Value Part.
     *
     * @param dataTypes the {@link DataType}s for each data sample in <code>data</code>
     * @param data      the data samples
     *
     * @throws java.lang.IllegalArgumentException if <code>dataTypes</code> is null, <code>data</code> is null, or they
     *                                            don't have the same size
     */
    public Values(DataType[] dataTypes, Number[] data) {
        assertNotNull(dataTypes, "dataTypes is null");
        assertNotNull(data, "data is null");
        assertEquals(
                dataTypes.length, data.length,
                "dataTypes and data arrays have different sizes: %d, %d", dataTypes.length, data.length
        );
        this.dataTypes = dataTypes;
        this.data = data;
    }

    /**
     * @return the number of data samples in this part
     */
    public int getCount() {
        return data.length;
    }

    /**
     * @return the {@link DataType}s for each data sample in {@link #getData()}
     */
    public DataType[] getDataTypes() {
        return dataTypes;
    }

    /**
     * @return the data samples
     */
    public Number[] getData() {
        return data;
    }

    @Override
    public String toString() {
        return "Values[" + "dataTypes=" + Arrays.asList(dataTypes) + ", data=" + Arrays.asList(data) + ']';
    }
}
