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
package org.hawkular.metrics.alerting

import groovy.json.JsonOutput
import org.hawkular.alerts.api.json.GroupConditionsInfo
import org.hawkular.alerts.api.model.condition.Condition
import org.hawkular.alerts.api.model.condition.AvailabilityCondition
import org.hawkular.alerts.api.model.condition.ThresholdCondition
import org.hawkular.alerts.api.model.trigger.Mode
import org.hawkular.alerts.api.model.trigger.Trigger
import org.hawkular.alerts.api.model.trigger.TriggerType
import org.hawkular.metrics.alerter.ConditionExpression
import org.hawkular.metrics.alerter.ConditionExpression.EvalType
import org.hawkular.metrics.alerter.ConditionExpression.Function
import org.hawkular.metrics.alerter.ConditionExpression.Query
import org.hawkular.metrics.model.MetricType
import org.junit.FixMethodOrder
import org.junit.Test
import org.junit.Ignore

import static org.hawkular.alerts.api.model.condition.AvailabilityCondition.Operator
import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertFalse
import static org.junit.Assert.assertTrue
import static org.junit.runners.MethodSorters.NAME_ASCENDING

import java.util.List;
import java.util.Map;

/**
 * Tests Metrics Group Trigger support in the Alerter.
 *
 * @author Jay Shaughnessy
 */
@FixMethodOrder(NAME_ASCENDING)
class GroupTriggerAlerterITest extends AlertingITestBase {

    static start = String.valueOf(System.currentTimeMillis())

    @Test
    void t01_mgtTest() {
        // Create an MGT for HeapUsed > 80.  Start with two metrics, one tagged to monitored by a member trigger,
        // and the other not.  Then update the tagging to swap the relevant metric and validate that the
        // job updates the member trigger set appropriately.

        // send some gauge data to automatically create metrics |<ts>|machine0|HeapUsed and |<ts>|machine1|HeapUsed
        // The <ts> in the name just prevents conflicts from older test runs
        long now = System.currentTimeMillis();
        String metric0 = "|" + now + "|machine0|HeapUsed"
        String metric1 = "|" + now + "|machine1|HeapUsed"
        DataPoint dp1 = new DataPoint( metric0, 50.0, now - 30000 );  // 30 seconds ago
        DataPoint dp2 = new DataPoint( metric1, 50.0, now - 30000 );  // 30 seconds ago
        sendGaugeDataViaRest( tenantId, dp1, dp2);

        // update metric0 to have the necessary tags
        Map<String, String> tags = new HashMap<>();
        tags.put("name", "HeapUsed");
        tags.put("machine", "machine0");
        addGaugeTagsViaRest(metric0, tags);

        // update metric1 to have the name tag but not the machine tag
        tags = new HashMap<>();
        tags.put("name", "HeapUsed");
        addGaugeTagsViaRest(metric1, tags)

        // remove test MGT if it exists
        def resp = client.delete(path: "triggers/groups/mgt")
        assert(200 == resp.status || 404 == resp.status)

        // CREATE the MGT
        Trigger mgt = new Trigger("mgt", "mgt");
        mgt.addTag("HawkularMetrics","GroupTrigger");
        mgt.addTag("DataIds","HeapUsed");
        mgt.addTag("HeapUsed","name = HeapUsed");
        mgt.addTag("SourceBy","machine");
        mgt.setType(TriggerType.GROUP);
        mgt.setEnabled(true);
        mgt.setAutoDisable(true);

        resp = client.post(path: "triggers/groups", body: mgt)
        assertEquals(200, resp.status)

        ThresholdCondition condition = new ThresholdCondition("mgt", Mode.FIRING, "HeapUsed",
            ThresholdCondition.Operator.GT, 80.0)
        Collection<Condition> conditions = new ArrayList<>(1);
        conditions.add( condition );
        GroupConditionsInfo groupConditionsInfo = new GroupConditionsInfo( conditions, null );
        resp = client.put(path: "triggers/groups/mgt/conditions/firing", body: groupConditionsInfo)
        assertEquals(200, resp.status)
        assertEquals(1, resp.data.size())

        // FETCH group trigger to validate
        resp = client.get(path: "triggers/mgt");
        assertEquals(200, resp.status)
        mgt = (Trigger)resp.data;
        assertEquals("mgt", mgt.getName())
        assertTrue(mgt.isGroup());
        assertTrue(mgt.isEnabled());
        assertTrue(mgt.isAutoDisable());

        // It should not take long to generate the member but give it a few seconds
        for ( int i=0; i < 10; ++i ) {
            Thread.sleep(500);

            resp = client.get(path: "triggers/groups/mgt/members" )
            if ( resp.status == 200 && resp.data.size() >= 1 ) {
                break;
            }
            assertEquals(200, resp.status)
        }
        assertEquals(200, resp.status)
        assertEquals(1, resp.data.size())

        Trigger member = (Trigger)resp.data;
        assertEquals("mgt", member.getName())
        assertFalse(member.isGroup());
        assertTrue(member.isEnabled());
        assertTrue(member.isAutoDisable());
        assertEquals("[machine0]", member.getContext().get("source").toString())
        def memberId = member.getId();

        resp = client.get(path: "triggers/" + memberId + "/conditions")
        assertEquals(200, resp.status);
        assertEquals(1, resp.data.size());
        ThresholdCondition mtc = (ThresholdCondition)resp.data[0];
        assertEquals(metric0, mtc.getDataId());

        // remove tag from metric0 to remove it as a member
        deleteGaugeTagsViaRest(metric0, "machine");

        // add tag to metric1 to add it as a member
        tags = new HashMap<>();
        tags.put("name", "HeapUsed");
        tags.put("machine", "machine1");
        addGaugeTagsViaRest(metric1, tags)

        // It should not take more than about 5s to reprocess, let's wait 10
        Thread.sleep(10000);
        resp = client.get(path: "triggers/groups/mgt/members" )
        assertEquals(200, resp.status)
        assertEquals(1, resp.data.size())

        member = (Trigger)resp.data;
        assertEquals("mgt", member.getName())
        assertFalse(member.isGroup());
        assertTrue(member.isEnabled());
        assertTrue(member.isAutoDisable());
        assertEquals("[machine1]", member.getContext().get("source").toString())
        memberId = member.getId();

        resp = client.get(path: "triggers/" + memberId + "/conditions")
        assertEquals(200, resp.status);
        assertEquals(1, resp.data.size());
        mtc = (ThresholdCondition)resp.data[0];
        assertEquals(metric1, mtc.getDataId());
    }

    @Test
    void t02_mgtAutoResolveTest() {
        // Create an MGT for Avail NOT UP with AutoResolve of AVAIL UP.  Use universal '*' SourceBy.  This tests using
        // the same DataId in multiple places, as well as both condition firing types.

        // send avail data to automatically create metrics |<ts>|machine0|Avail and |<ts>|machine1|Avail
        // The <ts> in the name just prevents conflicts from older test runs
        long now = System.currentTimeMillis();
        String avail0 = "|" + now + "|machine0|Avail"
        String avail1 = "|" + now + "|machine1|Avail"
        AvailPoint ap1 = new AvailPoint( avail0, "UP", now - 30000 );  // 30 seconds ago
        AvailPoint ap2 = new AvailPoint( avail1, "UP", now - 30000 );  // 30 seconds ago
        sendAvailDataViaRest( tenantId, ap1, ap2);

        // update avail0 to have the necessary tags
        Map<String, String> tags = new HashMap<>();
        tags.put("name", "Avail");
        addAvailTagsViaRest(avail0, tags);

        // update avail1 to have the name tag but not the machine tag
        tags = new HashMap<>();
        tags.put("name", "Avail");
        addAvailTagsViaRest(avail1, tags)

        // remove test MGT if it exists
        def resp = client.delete(path: "triggers/groups/mgt2")
        assert(200 == resp.status || 404 == resp.status)

        // CREATE the MGT
        Trigger mgt = new Trigger("mgt2", "mgt2");
        mgt.addTag("HawkularMetrics","GroupTrigger");
        mgt.addTag("DataIds","Avail");
        mgt.addTag("Avail","name = Avail");
        mgt.addTag("SourceBy","*");
        mgt.setType(TriggerType.GROUP);
        mgt.setEnabled(true);
        mgt.setAutoDisable(true);

        resp = client.post(path: "triggers/groups", body: mgt)
        assertEquals(200, resp.status)

        AvailabilityCondition fCondition = new AvailabilityCondition("mgt2", Mode.FIRING, "Avail", Operator.NOT_UP)
        AvailabilityCondition arCondition = new AvailabilityCondition("mgt2", Mode.AUTORESOLVE, "Avail", Operator.UP)
        Collection<Condition> conditions = new ArrayList<>(2);
        conditions.add( fCondition );
        conditions.add( arCondition );
        GroupConditionsInfo groupConditionsInfo = new GroupConditionsInfo( conditions, null );
        resp = client.put(path: "triggers/groups/mgt2/conditions", body: groupConditionsInfo)
        assertEquals(200, resp.status)
        assertEquals(2, resp.data.size())

        // FETCH group trigger to validate
        resp = client.get(path: "triggers/mgt2");
        assertEquals(200, resp.status)
        mgt = (Trigger)resp.data;
        assertEquals("mgt2", mgt.getName())
        assertTrue(mgt.isGroup());
        assertTrue(mgt.isEnabled());
        assertTrue(mgt.isAutoDisable());

        // It should not take long to generate the members but give it a few seconds
        for ( int i=0; i < 10; ++i ) {
            Thread.sleep(2500);

            resp = client.get(path: "triggers/groups/mgt2/members" )
            if ( resp.status == 200 && resp.data.size() >= 2 ) {
                break;
            }
            assertEquals(200, resp.status)
        }
        assertEquals(200, resp.status)
        assertEquals(2, resp.data.size())
        def members = resp.data

        Trigger member = (Trigger)members[0];
        println(member)
        assertEquals("mgt2", member.getName())
        assertFalse(member.isGroup());
        assertTrue(member.isEnabled());
        assertTrue(member.isAutoDisable());
        assertEquals("[*]", member.getContext().get("source").toString());
        def dataId = avail0.equals(member.getDataIdMap().get("Avail")) ? avail0 : avail1;
        def memberId = member.getId();

        resp = client.get(path: "triggers/" + memberId + "/conditions")
        assertEquals(200, resp.status);
        assertEquals(2, resp.data.size());
        AvailabilityCondition mac = (AvailabilityCondition)resp.data[0];
        assertEquals(dataId, mac.getDataId());
        mac = (AvailabilityCondition)resp.data[1];
        assertEquals(dataId, mac.getDataId());

        member = (Trigger)members[1];
        println(member)
        assertEquals("mgt2", member.getName())
        assertFalse(member.isGroup());
        assertTrue(member.isEnabled());
        assertTrue(member.isAutoDisable());
        assertTrue(null != member.getContext().get("source"))
        assertEquals("[*]", member.getContext().get("source").toString());
        dataId = avail0.equals(dataId) ? avail1 : avail0;
        memberId = member.getId();

        resp = client.get(path: "triggers/" + memberId + "/conditions")
        assertEquals(200, resp.status);
        assertEquals(2, resp.data.size());
        mac = (AvailabilityCondition)resp.data[0];
        assertEquals(dataId, mac.getDataId());
        mac = (AvailabilityCondition)resp.data[1];
        assertEquals(dataId, mac.getDataId());
    }

    @Test
    void t09_cleanupTriggers() {
        // stop the external alerter from processing these test triggers...
        def resp = client.delete(path: "triggers/groups/mgt")
        resp = client.delete(path: "triggers/groups/mgt2")
    }

    private void sendGaugeDataViaRest(String tenantId, DataPoint... dataPoints) {

                List<Map<String, Object>> mMetrics = new ArrayList<>();

                for( DataPoint dp : dataPoints ) {
                    addDataItem(mMetrics, dp.metricId, dp.time, dp.value);
                }

                // Send it to metrics via rest
                def resp = metricsClient.post(path:"gauges/raw", body:mMetrics, headers:['Hawkular-Tenant':tenantId]);
                assertEquals(200, resp.status)
            }

    private void sendCounterDataViaRest(String tenantId, DataPoint... dataPoints) {
       List<Map<String, Object>> mMetrics = new ArrayList<>();

       for( DataPoint dp : dataPoints ) {
           addDataItem(mMetrics, dp.metricId, dp.time, dp.value);
       }

       // Send it to metrics via rest
       def resp = metricsClient.post(path:"counters/raw", body:mMetrics, headers:['Hawkular-Tenant':tenantId]);
       assertEquals(200, resp.status)
   }

    private void addGaugeTagsViaRest(String metricName, HashMap<String,String> tags) {
        def resp = metricsClient.put(path:"gauges/" + metricName + "/tags", body:tags);
        assertEquals(200, resp.status)
    }

    private void deleteGaugeTagsViaRest(String metricName, String tags) {
        def resp = metricsClient.delete(path:"gauges/" + metricName + "/tags/" + tags);
        assertEquals(200, resp.status)
    }

    private void addAvailTagsViaRest(String metricName, HashMap<String,String> tags) {
        def resp = metricsClient.put(path:"availability/" + metricName + "/tags", body:tags);
        assertEquals(200, resp.status)
    }

    private void deleteAvailTagsViaRest(String metricName, String tags) {
        def resp = metricsClient.delete(path:"availability/" + metricName + "/tags/" + tags);
        assertEquals(200, resp.status)
    }

    private static void addDataItem(List<Map<String, Object>> mMetrics, String metricId, Long timestamp, Number value) {
        Map<String, Number> dataMap = new HashMap<>(2);
        dataMap.put("timestamp", timestamp);
        dataMap.put("value", value);
        List<Map<String, Number>> data = new ArrayList<>(1);
        data.add(dataMap);
        Map<String, Object> outer = new HashMap<>(2);
        outer.put("id", metricId);
        outer.put("data", data);
        mMetrics.add(outer);
    }

    private static class DataPoint {
        String metricId;
        Double value;
        long time;

        public DataPoint( String metricId, Double value, long time ) {
            this.metricId = metricId;
            this.value = value;
            this.time = time;
        }
    }

    private void sendAvailDataViaRest(String tenantId, AvailPoint... availPoints) {

        List<Map<String, Object>> mMetrics = new ArrayList<>();

        for( AvailPoint ap : availPoints ) {
            addAvailItem(mMetrics, ap.metricId, ap.time, ap.value);
        }

        // Send it to metrics via rest
        def resp = metricsClient.post(path:"availability/raw", body:mMetrics, headers:['Hawkular-Tenant':tenantId]);
        assertEquals(200, resp.status)
    }

    private static void addAvailItem(List<Map<String, Object>> mMetrics, String metricId, Long timestamp, String value) {
        Map<String, Object> dataMap = new HashMap<>(2);
        dataMap.put("timestamp", timestamp);
        dataMap.put("value", value);
        List<Map<String, Object>> data = new ArrayList<>(1);
        data.add(dataMap);
        Map<String, Object> outer = new HashMap<>(2);
        outer.put("id", metricId);
        outer.put("data", data);
        mMetrics.add(outer);
    }

    private static class AvailPoint {
        String metricId;
        String value;
        long time;

        public AvailPoint( String metricId, String value, long time ) {
            this.metricId = metricId;
            this.value = value;
            this.time = time;
        }
    }

}
