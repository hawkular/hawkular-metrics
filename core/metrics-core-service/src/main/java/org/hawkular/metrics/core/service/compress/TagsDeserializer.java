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
import java.util.HashMap;
import java.util.Map;

/**
 * Deserializes the data_compressed table tags map
 *
 * @author Michael Burman
 */
public class TagsDeserializer {
    private long blockStart;

    public TagsDeserializer(long blockStart) {
        this.blockStart = blockStart;
    }

    public Map<Long, Map<String, String>> deserialize(ByteBuffer bb) {
        Map<Long, Map<String, String>> tagsMap = new HashMap<>();

        if(bb.hasRemaining()) {
            if(TagsSerializer.SIMPLE_SERIALIZER == bb.get()) {
                while(bb.hasRemaining()) {
                    int delta = bb.getInt();
                    long timestamp = blockStart + delta;

                    int tagsSize = bb.get() & 0xFF;

                    Map<String, String> entryMap = new HashMap<>();

                    for(byte i = 0; i < tagsSize; i++) {
                        int keyLength = bb.get() & 0xFF;
                        int valueLength = bb.get() & 0xFF;

                        byte[] key = new byte[keyLength];
                        byte[] value = new byte[valueLength];

                        bb.get(key);
                        bb.get(value);

                        String mapKey = new String(key, StandardCharsets.UTF_8);
                        String mapValue = new String(value, StandardCharsets.UTF_8);

                        entryMap.put(mapKey, mapValue);
                    }

                    tagsMap.put(Long.valueOf(timestamp), entryMap);
                }
            } else {
                // Return back to start position, let something else handle it
                bb.rewind();
            }
        }

        return tagsMap;
    }
}
