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

import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertFalse
import static org.junit.Assert.assertTrue
import static org.junit.runners.MethodSorters.NAME_ASCENDING

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

        // send some gauge data to automatically create metrics /<ts>/machine0/HeapUsed and /<ts>/machine1/HeapUsed
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
        assertEquals("mgt", mgt.getName())
        assertFalse(member.isGroup());
        assertTrue(member.isEnabled());
        assertTrue(member.isAutoDisable());
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
        assertEquals("mgt", mgt.getName())
        assertFalse(member.isGroup());
        assertTrue(member.isEnabled());
        assertTrue(member.isAutoDisable());
        memberId = member.getId();

        resp = client.get(path: "triggers/" + memberId + "/conditions")
        assertEquals(200, resp.status);
        assertEquals(1, resp.data.size());
        mtc = (ThresholdCondition)resp.data[0];
        assertEquals(metric1, mtc.getDataId());
    }

    @Test
    void t09_cleanupTriggers() {
        // stop the external alerter from processing these test triggers...
        def resp = client.delete(path: "triggers/groups/mgt")
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
}
