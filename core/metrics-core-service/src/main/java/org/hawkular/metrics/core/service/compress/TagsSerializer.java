/*
 * Copyright 2014-2016 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.core.service.compress;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Serializes multiple rows of tags to a single blob. For compression, we rely on the Cassandra's block compression.
 *
 * @author Michael Burman
 */
public class TagsSerializer {

    private ByteBuffer bb;
    private int MAX_SIZE = 255;
    private long blockStart;
    public static byte SIMPLE_SERIALIZER = 0x10;

    public TagsSerializer(long blockStart) {
        // Max byteAmount is 510 bytes per key/value + 4 bytes for delta + 1 byte for

        bb = ByteBuffer.allocate(33*4096);
        bb.put(SIMPLE_SERIALIZER);
        this.blockStart = blockStart; // No need to encode blockStart as we have that stored on the row already
    }

    public void addDataPointTags(long timestamp, Map<String, String> tags) {
        if(tags.entrySet().size() > MAX_SIZE) {
            throw new RuntimeException("Single datapoint can only store max of " + MAX_SIZE + " tags");
        }

        // Store delta of block timestamp.. 23 bits is enough to represent any delta value
        long delta = timestamp - blockStart;

        bb.putInt((int) delta);
        bb.put((byte) tags.entrySet().size()); // MAX of 255 tags per datapoint?

        // Now write the values
        for (Map.Entry<String, String> tagEntry : tags.entrySet()) {
            byte[] key = tagEntry.getKey().getBytes(StandardCharsets.UTF_8);
            byte[] value = tagEntry.getValue().getBytes(StandardCharsets.UTF_8);

            if(key.length > MAX_SIZE || value.length > MAX_SIZE) {
                throw new RuntimeException("Could not store pair key->" + tagEntry.getKey() + ", value->" + tagEntry
                        .getValue() + ", they exceed max length of " + MAX_SIZE + " bytes");
            }

            bb.put((byte) key.length);
            bb.put((byte) value.length);
            bb.put(key);
            bb.put(value);
        }
    }

    public ByteBuffer getByteBuffer() {
        return bb;
    }
}
