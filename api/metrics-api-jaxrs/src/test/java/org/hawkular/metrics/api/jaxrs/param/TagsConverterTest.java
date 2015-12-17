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

package org.hawkular.metrics.api.jaxrs.param;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;

import org.hawkular.metrics.model.param.Tags;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.ImmutableMap;

/**
 * @author Thomas Segismont
 */
@RunWith(Parameterized.class)
public class TagsConverterTest {

    @Parameters(name = "{0}")
    public static Iterable<Object[]> params() {
        return Arrays.asList(
                new Object[][]{
                        {"", new Tags(Collections.emptyMap())},
                        {"1:2", new Tags(ImmutableMap.of("1", "2"))},
                        {"a:b,c:defg", new Tags(ImmutableMap.of("a", "b", "c", "defg"))},
                        {"3:b,c:d4 efg ,   7 : 2", new Tags(ImmutableMap.of("3", "b", "c", "d4 efg ", "   7 ", " 2"))},
                }
        );
    }

    private String value;
    private Tags tags;

    public TagsConverterTest(String value, Tags tags) {
        this.value = value;
        this.tags = tags;
    }

    @Test
    public void testFromString() throws Exception {
        assertEquals(tags, new TagsConverter().fromString(value));
    }

    @Test
    public void testToString() throws Exception {
        assertEquals(value, new TagsConverter().toString(tags));
    }
}