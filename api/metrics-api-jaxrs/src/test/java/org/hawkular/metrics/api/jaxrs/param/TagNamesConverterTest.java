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
package org.hawkular.metrics.api.jaxrs.param;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Set;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.ImmutableSet;

/**
 * @author Thomas Segismont
 */
@RunWith(Parameterized.class)
public class TagNamesConverterTest {

    @Parameters(name = "{0}")
    public static Iterable<Object[]> params() {
        return Arrays.asList(
                new Object[][]{
                        {"1:2", ImmutableSet.of("1")},
                        {"a:b,c:defg", ImmutableSet.of("a", "c")},
                        {"3:b,c:d4 efg ,   7 : 2", ImmutableSet.of("3", "c", "   7 ")},
                        {"1", ImmutableSet.of("1")},
                        {"a,c", ImmutableSet.of("a", "c")},
                        {"3,c,   7 ", ImmutableSet.of("3", "c", "   7 ")},
                }
        );
    }

    private String value;
    private Set<String> tags;

    public TagNamesConverterTest(String value, Set<String> tags) {
        this.value = value;
        this.tags = tags;
    }

    @Test
    public void fromString() throws Exception {
        assertEquals(tags, new TagNamesConverter().fromString(value).getNames());
    }

}