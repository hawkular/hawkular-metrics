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
package org.hawkular.metrics.core.compress;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.Map;

import org.hawkular.metrics.core.service.compress.TagsDeserializer;
import org.hawkular.metrics.core.service.compress.TagsSerializer;
import org.hawkular.metrics.datetime.DateTimeService;
import org.joda.time.Duration;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

/**
 * Test that data_compressed table's tags serialization works
 *
 * @author Michael Burman
 */
public class TagsMapSerializingTest {

    @Test
    public void singleValueMapTest() {
        long timeSlice = DateTimeService.getTimeSlice(DateTimeService.now.get().getMillis(), Duration.standardHours(2));

        TagsSerializer serializer = new TagsSerializer(timeSlice);

        serializer.addDataPointTags(timeSlice + 20, ImmutableMap.of("a", "b"));

        ByteBuffer serializedTags = serializer.getByteBuffer();

        serializedTags.flip();

        TagsDeserializer deserializer = new TagsDeserializer(timeSlice);
        Map<Long, Map<String, String>> deTags = deserializer.deserialize(serializedTags);

        Long key = Long.valueOf(timeSlice + 20);

        assertEquals(1, deTags.size());
        assertEquals(1, deTags.get(key).size());

        Map<String, String> abMap = deTags.get(key);
        assertEquals("b", abMap.get("a"));
    }

    @Test
    public void multiValueMapTest() {
        long timeSlice = DateTimeService.getTimeSlice(DateTimeService.now.get().getMillis(), Duration.standardHours(2));

        TagsSerializer serializer = new TagsSerializer(timeSlice);

        serializer.addDataPointTags(timeSlice + 20, ImmutableMap.of("a", "b", "c", "d"));
        serializer.addDataPointTags(timeSlice + 32, ImmutableMap.of("a2", "b2", "c2", "d2", "e2", "f2"));

        ByteBuffer serializedTags = serializer.getByteBuffer();

        serializedTags.flip();

        TagsDeserializer deserializer = new TagsDeserializer(timeSlice);
        Map<Long, Map<String, String>> deTags = deserializer.deserialize(serializedTags);

        Long key = Long.valueOf(timeSlice + 20);
        Long key2 = Long.valueOf(timeSlice + 32);

        assertEquals(2, deTags.size());
        assertEquals(3, deTags.get(key2).size());

        Map<String, String> abMap = deTags.get(key);
        assertEquals("b", abMap.get("a"));
        assertEquals("d", abMap.get("c"));
    }
}
