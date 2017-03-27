/*
 * Copyright 2014-2017 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.alerter.groups;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
//import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hawkular.alerts.api.json.GroupMemberInfo;
import org.hawkular.alerts.api.model.trigger.Trigger;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.MetricType;
import org.junit.Test;

public class GroupTriggerManagerTest {

    @Test
    public void generateSourceMapSingleSourceTest() {
        String sourceBy = "m";

        Map<String, String> tags = new HashMap<>();
        tags.put("m", "m0");
        tags.put("name", "HeapUsed");
        Metric<?> m0HU = new Metric<>(new MetricId<>("tenant", MetricType.GAUGE, "/m0/HeapUsed"), tags);

        tags = new HashMap<>();
        tags.put("m", "m0");
        tags.put("name", "HeapMax");
        Metric<?> m0HM = new Metric<>(new MetricId<>("tenant", MetricType.GAUGE, "/m0/HeapMax"), tags);

        tags = new HashMap<>();
        tags.put("m", "m1");
        tags.put("name", "HeapUsed");
        Metric<?> m1HU = new Metric<>(new MetricId<>("tenant", MetricType.GAUGE, "/m1/HeapUsed"), tags);

        tags = new HashMap<>();
        tags.put("m", "m1");
        tags.put("name", "HeapMax");
        Metric<?> m1HM = new Metric<>(new MetricId<>("tenant", MetricType.GAUGE, "/m1/HeapMax"), tags);

        Map<String, Set<Metric<?>>> dataIdMap = new HashMap<>();
        dataIdMap.put("HeapUsed", new HashSet<Metric<?>>(Arrays.asList(m0HU, m1HU)));
        dataIdMap.put("HeapMax", new HashSet<Metric<?>>(Arrays.asList(m0HM, m1HM)));

        Map<List<String>, Map<String, List<String>>> sourceMap = GroupTriggerManager.MGTRunner.generateSourceMap(
                sourceBy,
                dataIdMap);
        assertEquals(2, sourceMap.keySet().size());
        List<String> sourceM0 = Arrays.asList("m0");
        List<String> sourceM1 = Arrays.asList("m1");
        assertTrue(sourceMap.keySet().contains(sourceM0));
        assertTrue(sourceMap.keySet().contains(sourceM1));
        assertEquals(2, sourceMap.get(sourceM0).size());
        assertTrue(sourceMap.get(sourceM0).keySet().contains("HeapUsed"));
        assertTrue(sourceMap.get(sourceM0).keySet().contains("HeapMax"));
        assertTrue(sourceMap.get(sourceM0).get("HeapUsed").contains(m0HU.getId()));
        assertTrue(sourceMap.get(sourceM0).get("HeapMax").contains(m0HM.getId()));
        assertEquals(2, sourceMap.get(sourceM1).size());
        assertTrue(sourceMap.get(sourceM1).keySet().contains("HeapUsed"));
        assertTrue(sourceMap.get(sourceM1).keySet().contains("HeapMax"));
        assertTrue(sourceMap.get(sourceM1).get("HeapUsed").contains(m1HU.getId()));
        assertTrue(sourceMap.get(sourceM1).get("HeapMax").contains(m1HM.getId()));
    }

    @Test
    public void generateSourceMapStarSourceTest() {
        String sourceBy = "*";

        Map<String, String> tags = new HashMap<>();
        tags.put("m", "m0");
        tags.put("name", "HeapUsed");
        Metric<?> m0HU = new Metric<>(new MetricId<>("tenant", MetricType.GAUGE, "/m0/HeapUsed"), tags);

        tags = new HashMap<>();
        tags.put("m", "m0");
        tags.put("name", "HeapMax");
        Metric<?> m0HM = new Metric<>(new MetricId<>("tenant", MetricType.GAUGE, "/m0/HeapMax"), tags);

        tags = new HashMap<>();
        tags.put("m", "m1");
        tags.put("name", "HeapUsed");
        Metric<?> m1HU = new Metric<>(new MetricId<>("tenant", MetricType.GAUGE, "/m1/HeapUsed"), tags);

        tags = new HashMap<>();
        tags.put("m", "m1");
        tags.put("name", "HeapMax");
        Metric<?> m1HM = new Metric<>(new MetricId<>("tenant", MetricType.GAUGE, "/m1/HeapMax"), tags);

        Map<String, Set<Metric<?>>> dataIdMap = new HashMap<>();
        dataIdMap.put("HeapUsed", new HashSet<Metric<?>>(Arrays.asList(m0HU, m1HU)));
        dataIdMap.put("HeapMax", new HashSet<Metric<?>>(Arrays.asList(m0HM, m1HM)));

        Map<List<String>, Map<String, List<String>>> sourceMap = GroupTriggerManager.MGTRunner.generateSourceMap(
                sourceBy,
                dataIdMap);
        assertEquals(1, sourceMap.keySet().size());
        List<String> sourceStar = Arrays.asList("*");
        assertTrue(sourceMap.keySet().contains(sourceStar));
        assertEquals(2, sourceMap.get(sourceStar).size());
        assertTrue(sourceMap.get(sourceStar).keySet().contains("HeapUsed"));
        assertTrue(sourceMap.get(sourceStar).keySet().contains("HeapMax"));
        assertTrue(sourceMap.get(sourceStar).get("HeapUsed").contains(m0HU.getId()));
        assertTrue(sourceMap.get(sourceStar).get("HeapMax").contains(m0HM.getId()));
        assertTrue(sourceMap.get(sourceStar).get("HeapUsed").contains(m1HU.getId()));
        assertTrue(sourceMap.get(sourceStar).get("HeapMax").contains(m1HM.getId()));
    }

    @Test
    public void generateSourceMapMissingSourceTest() {
        String sourceBy = "m";

        Map<String, String> tags = new HashMap<>();
        tags.put("m", "m0");
        tags.put("name", "HeapUsed");
        Metric<?> m0HU = new Metric<>(new MetricId<>("tenant", MetricType.GAUGE, "/m0/HeapUsed"), tags);

        tags = new HashMap<>();
        tags.put("m", "m0");
        tags.put("name", "HeapMax");
        Metric<?> m0HM = new Metric<>(new MetricId<>("tenant", MetricType.GAUGE, "/m0/HeapMax"), tags);

        tags = new HashMap<>();
        tags.put("name", "HeapUsed");
        Metric<?> m1HU = new Metric<>(new MetricId<>("tenant", MetricType.GAUGE, "/m1/HeapUsed"), tags);

        tags = new HashMap<>();
        tags.put("name", "HeapMax");
        Metric<?> m1HM = new Metric<>(new MetricId<>("tenant", MetricType.GAUGE, "/m1/HeapMax"), tags);

        Map<String, Set<Metric<?>>> dataIdMap = new HashMap<>();
        dataIdMap.put("HeapUsed", new HashSet<Metric<?>>(Arrays.asList(m0HU, m1HU)));
        dataIdMap.put("HeapMax", new HashSet<Metric<?>>(Arrays.asList(m0HM, m1HM)));

        Map<List<String>, Map<String, List<String>>> sourceMap = GroupTriggerManager.MGTRunner.generateSourceMap(
                sourceBy,
                dataIdMap);
        assertEquals(1, sourceMap.keySet().size());
        List<String> sourceM0 = Arrays.asList("m0");
        assertTrue(sourceMap.keySet().contains(sourceM0));
        assertEquals(2, sourceMap.get(sourceM0).size());
        assertTrue(sourceMap.get(sourceM0).keySet().contains("HeapUsed"));
        assertTrue(sourceMap.get(sourceM0).keySet().contains("HeapMax"));
        assertTrue(sourceMap.get(sourceM0).get("HeapUsed").contains(m0HU.getId()));
        assertTrue(sourceMap.get(sourceM0).get("HeapMax").contains(m0HM.getId()));
    }

    @Test
    public void generateSourceMapDoubleSourceTest() {
        String sourceBy = " dc , m "; // for fun, add some extraneous spaces

        Map<String, String> tags = new HashMap<>();
        tags.put("dc", "dc0");
        tags.put("m", "m0");
        tags.put("name", "HeapUsed");
        Metric<?> dc0m0HU = new Metric<>(new MetricId<>("tenant", MetricType.GAUGE, "/dc0/m0/HeapUsed"), tags);

        tags = new HashMap<>();
        tags.put("dc", "dc0");
        tags.put("m", "m0");
        tags.put("name", "HeapMax");
        Metric<?> dc0m0HM = new Metric<>(new MetricId<>("tenant", MetricType.GAUGE, "/dc0/m0/HeapMax"), tags);

        tags = new HashMap<>();
        tags.put("dc", "dc0");
        tags.put("m", "m1");
        tags.put("name", "HeapUsed");
        Metric<?> dc0m1HU = new Metric<>(new MetricId<>("tenant", MetricType.GAUGE, "/dc0/m1/HeapUsed"), tags);

        tags = new HashMap<>();
        tags.put("dc", "dc0");
        tags.put("m", "m1");
        tags.put("name", "HeapMax");
        Metric<?> dc0m1HM = new Metric<>(new MetricId<>("tenant", MetricType.GAUGE, "/dc0/m1/HeapMax"), tags);

        tags = new HashMap<>();
        tags.put("dc", "dc1");
        tags.put("m", "m0");
        tags.put("name", "HeapUsed");
        Metric<?> dc1m0HU = new Metric<>(new MetricId<>("tenant", MetricType.GAUGE, "/dc1/m0/HeapUsed"), tags);

        tags = new HashMap<>();
        tags.put("dc", "dc1");
        tags.put("m", "m0");
        tags.put("name", "HeapMax");
        Metric<?> dc1m0HM = new Metric<>(new MetricId<>("tenant", MetricType.GAUGE, "/dc1/m0/HeapMax"), tags);

        tags = new HashMap<>();
        tags.put("dc", "dc1");
        tags.put("m", "m1");
        tags.put("name", "HeapUsed");
        Metric<?> dc1m1HU = new Metric<>(new MetricId<>("tenant", MetricType.GAUGE, "/dc1/m1/HeapUsed"), tags);

        tags = new HashMap<>();
        tags.put("dc", "dc1");
        tags.put("m", "m1");
        tags.put("name", "HeapMax");
        Metric<?> dc1m1HM = new Metric<>(new MetricId<>("tenant", MetricType.GAUGE, "/dc1/m1/HeapMax"), tags);

        Map<String, Set<Metric<?>>> dataIdMap = new HashMap<>();
        dataIdMap.put("HeapUsed", new HashSet<Metric<?>>(Arrays.asList(dc0m0HU, dc0m1HU, dc1m0HU, dc1m1HU)));
        dataIdMap.put("HeapMax", new HashSet<Metric<?>>(Arrays.asList(dc0m0HM, dc0m1HM, dc1m0HM, dc1m1HM)));

        Map<List<String>, Map<String, List<String>>> sourceMap = GroupTriggerManager.MGTRunner.generateSourceMap(
                sourceBy,
                dataIdMap);
        assertEquals(4, sourceMap.keySet().size());
        List<String> sourceDC0M0 = Arrays.asList("dc0", "m0");
        List<String> sourceDC0M1 = Arrays.asList("dc0", "m1");
        List<String> sourceDC1M0 = Arrays.asList("dc1", "m0");
        List<String> sourceDC1M1 = Arrays.asList("dc1", "m1");
        assertTrue(sourceMap.keySet().contains(sourceDC0M0));
        assertTrue(sourceMap.keySet().contains(sourceDC0M1));
        assertTrue(sourceMap.keySet().contains(sourceDC1M0));
        assertTrue(sourceMap.keySet().contains(sourceDC1M1));
        assertEquals(2, sourceMap.get(sourceDC0M0).size());
        assertTrue(sourceMap.get(sourceDC0M0).keySet().contains("HeapUsed"));
        assertTrue(sourceMap.get(sourceDC0M0).keySet().contains("HeapMax"));
        assertTrue(sourceMap.get(sourceDC0M0).get("HeapUsed").contains(dc0m0HU.getId()));
        assertTrue(sourceMap.get(sourceDC0M0).get("HeapMax").contains(dc0m0HM.getId()));
        assertEquals(2, sourceMap.get(sourceDC0M1).size());
        assertTrue(sourceMap.get(sourceDC0M1).keySet().contains("HeapUsed"));
        assertTrue(sourceMap.get(sourceDC0M1).keySet().contains("HeapMax"));
        assertTrue(sourceMap.get(sourceDC0M1).get("HeapUsed").contains(dc0m1HU.getId()));
        assertTrue(sourceMap.get(sourceDC0M1).get("HeapMax").contains(dc0m1HM.getId()));
        assertEquals(2, sourceMap.get(sourceDC1M0).size());
        assertTrue(sourceMap.get(sourceDC1M0).keySet().contains("HeapUsed"));
        assertTrue(sourceMap.get(sourceDC1M0).keySet().contains("HeapMax"));
        assertTrue(sourceMap.get(sourceDC1M0).get("HeapUsed").contains(dc1m0HU.getId()));
        assertTrue(sourceMap.get(sourceDC1M0).get("HeapMax").contains(dc1m0HM.getId()));
        assertEquals(2, sourceMap.get(sourceDC1M1).size());
        assertTrue(sourceMap.get(sourceDC1M1).keySet().contains("HeapUsed"));
        assertTrue(sourceMap.get(sourceDC1M1).keySet().contains("HeapMax"));
        assertTrue(sourceMap.get(sourceDC1M1).get("HeapUsed").contains(dc1m1HU.getId()));
        assertTrue(sourceMap.get(sourceDC1M1).get("HeapMax").contains(dc1m1HM.getId()));
    }

    @Test
    public void generateMembersDoubleSourceTest() {
        String sourceBy = " dc,m";

        // Same sourceMap as in "generateSourceMapDoubleSourceTest" but remove dc1m1HM metric to test
        // that we don't get a member for source dc1m1.
        Map<String, String> tags = new HashMap<>();
        tags.put("dc", "dc0");
        tags.put("m", "m0");
        tags.put("name", "HeapUsed");
        Metric<?> dc0m0HU = new Metric<>(new MetricId<>("tenant", MetricType.GAUGE, "/dc0/m0/HeapUsed"), tags);

        tags = new HashMap<>();
        tags.put("dc", "dc0");
        tags.put("m", "m0");
        tags.put("name", "HeapMax");
        Metric<?> dc0m0HM = new Metric<>(new MetricId<>("tenant", MetricType.GAUGE, "/dc0/m0/HeapMax"), tags);

        tags = new HashMap<>();
        tags.put("dc", "dc0");
        tags.put("m", "m1");
        tags.put("name", "HeapUsed");
        Metric<?> dc0m1HU = new Metric<>(new MetricId<>("tenant", MetricType.GAUGE, "/dc0/m1/HeapUsed"), tags);

        tags = new HashMap<>();
        tags.put("dc", "dc0");
        tags.put("m", "m1");
        tags.put("name", "HeapMax");
        Metric<?> dc0m1HM = new Metric<>(new MetricId<>("tenant", MetricType.GAUGE, "/dc0/m1/HeapMax"), tags);

        tags = new HashMap<>();
        tags.put("dc", "dc1");
        tags.put("m", "m0");
        tags.put("name", "HeapUsed");
        Metric<?> dc1m0HU = new Metric<>(new MetricId<>("tenant", MetricType.GAUGE, "/dc1/m0/HeapUsed"), tags);

        tags = new HashMap<>();
        tags.put("dc", "dc1");
        tags.put("m", "m0");
        tags.put("name", "HeapMax");
        Metric<?> dc1m0HM = new Metric<>(new MetricId<>("tenant", MetricType.GAUGE, "/dc1/m0/HeapMax"), tags);

        tags = new HashMap<>();
        tags.put("dc", "dc1");
        tags.put("m", "m1");
        tags.put("name", "HeapUsed");
        Metric<?> dc1m1HU = new Metric<>(new MetricId<>("tenant", MetricType.GAUGE, "/dc1/m1/HeapUsed"), tags);

        // deliberately missing dc1m1HM to ensure we don't get a dc1,m1 member

        Map<String, Set<Metric<?>>> dataIdMap = new HashMap<>();
        dataIdMap.put("HeapUsed", new HashSet<Metric<?>>(Arrays.asList(dc0m0HU, dc0m1HU, dc1m0HU, dc1m1HU)));
        dataIdMap.put("HeapMax", new HashSet<Metric<?>>(Arrays.asList(dc0m0HM, dc0m1HM, dc1m0HM)));

        Map<List<String>, Map<String, List<String>>> sourceMap = GroupTriggerManager.MGTRunner.generateSourceMap(
                sourceBy,
                dataIdMap);

        Trigger mgt = new Trigger("tenantId", "mgtId");
        Set<String> dataIds = new HashSet<>(Arrays.asList("HeapUsed", "HeapMax"));
        Set<GroupMemberInfo> members = GroupTriggerManager.MGTRunner.generateMembers(mgt, dataIds, sourceMap);
        assertEquals(4, sourceMap.keySet().size());
        assertEquals(3, members.size());
        assertFalse(members.stream()
                .anyMatch(m -> null == m.getMemberContext() || null == m.getMemberContext().get("source")));
        List<GroupMemberInfo> membersList = new ArrayList<>(members);
        Collections.sort(membersList, new Comparator<GroupMemberInfo>() {
            @Override
            public int compare(GroupMemberInfo o1, GroupMemberInfo o2) {
                return o1.getMemberContext().get("source").compareTo(o2.getMemberContext().get("source"));
            }
        });
        System.out.println("Members=" + membersList);
        assertEquals("[dc0, m0]", membersList.get(0).getMemberContext().get("source"));
        assertEquals("[dc0, m1]", membersList.get(1).getMemberContext().get("source"));
        assertEquals("[dc1, m0]", membersList.get(2).getMemberContext().get("source"));
    }

}
