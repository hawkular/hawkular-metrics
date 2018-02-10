/*
 * Copyright 2014-2018 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.core.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.google.common.collect.Maps;

/**
 * Test some TagQueryParser optimization cases
 *
 * @author Michael Burman
 */
public class TagQueryParserTest {

    @Test
    public void testSomeCommonOpenshiftQueries() {
        String podQuery1 = "test-container/test-podid/test/metric/A/XYZ";
        String podQuery2 = "/system.slice/dbus.service//cpu/usage";

        String allMatchCustomQuery = "*";
        String allMatchProperQuery = ".*";
        String orQuery = "a|b";
        String orComplexQuery = "a|!b";
        String startsWithQuery = "^abc+";
        String wordMatchQuery = "\\w";
        String notQuery = "!b";

        assertFalse(TagQueryParser.QueryOptimizer.isRegExp(podQuery1));
        assertFalse(TagQueryParser.QueryOptimizer.isRegExp(podQuery2));
        assertTrue(TagQueryParser.QueryOptimizer.isRegExp(orQuery));
        assertTrue(TagQueryParser.QueryOptimizer.isRegExp(orComplexQuery));
        assertTrue(TagQueryParser.QueryOptimizer.isRegExp(notQuery));
        assertTrue(TagQueryParser.QueryOptimizer.isRegExp(allMatchCustomQuery));
        assertTrue(TagQueryParser.QueryOptimizer.isRegExp(allMatchProperQuery));
        assertTrue(TagQueryParser.QueryOptimizer.isRegExp(startsWithQuery));
        assertTrue(TagQueryParser.QueryOptimizer.isRegExp(wordMatchQuery));
    }

    @Test
    public void testMultiSingleQueriesMatcher() {
        String orQuery = "a|b";
        String orComplexQuery = "a|!b";

        assertEquals(TagQueryParser.QueryOptimizer.RegExpOptimizer.OR_SINGLE_SEEK, TagQueryParser
                .QueryOptimizer.optimalStrategy(orQuery));
        assertEquals(TagQueryParser.QueryOptimizer.RegExpOptimizer.NONE, TagQueryParser
                .QueryOptimizer.optimalStrategy(orComplexQuery));
    }

    @Test
    public void testReOrder() {
        Map<String, String> tagsQuery = Maps.newHashMap();
        tagsQuery.put("pod_hit", "norate"); // A
        tagsQuery.put("pod_id", "pod1|pod2|pod3"); // AOR
        tagsQuery.put("time", "*"); // B
        tagsQuery.put("!seek", ""); // C
        tagsQuery.put("pod_name", "pod1|!pod2"); // B


        Map<Long, List<TagQueryParser.Query>> entries =
                TagQueryParser.QueryOptimizer.reOrderTagsQuery(tagsQuery, true);

        assertEquals(1, entries.get(TagQueryParser.QueryOptimizer.GROUP_A_COST).size());
        assertEquals(1, entries.get(TagQueryParser.QueryOptimizer.GROUP_A_OR_COST).size());
        assertEquals(3, entries.get(TagQueryParser.QueryOptimizer.GROUP_A_OR_COST).get(0).getTagValues().length);
        assertEquals(2, entries.get(TagQueryParser.QueryOptimizer.GROUP_B_COST).size());
        assertEquals(1, entries.get(TagQueryParser.QueryOptimizer.GROUP_C_COST).size());
    }
}
