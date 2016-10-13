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
package org.hawkular.metrics.alerting

import org.hawkular.alerts.api.model.condition.Condition
import org.hawkular.alerts.api.model.condition.ThresholdCondition
import org.hawkular.alerts.api.model.trigger.Mode
import org.hawkular.alerts.api.model.trigger.Trigger
import org.junit.FixMethodOrder
import org.junit.Test
import org.junit.Ignore

import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertTrue
import static org.junit.runners.MethodSorters.NAME_ASCENDING

/**
 * Tests Metrics Alerting via publishing.
 *
 * @author Jay Shaughnessy
 */
@FixMethodOrder(NAME_ASCENDING)
class AlertingITest extends AlertingITestBase {

    static start = String.valueOf(System.currentTimeMillis())

    @Test
    void t01_alertingTest() {
        // use a unique metricId for the test run so that we don't conflict with prior test runs
        String metricId = "gauge1-" + start;
        String dataId = "hm_g_" + metricId;   // dataId = type-prefix + metricId

        // CREATE trigger using threshold condition
        def resp = client.get(path: "")
        assertTrue(resp.status == 200 || resp.status == 204)

        def triggerId = "AlertingITest-t01";
        Trigger trigger = new Trigger(triggerId, triggerId);

        // remove if it exists
        resp = client.delete(path: "triggers/" + triggerId)
        assertTrue(200 == resp.status || 404 == resp.status)

        trigger.setAutoDisable(true);

        resp = client.post(path: "triggers", body: trigger)
        assertEquals(200, resp.status)

        // ADD threshold condition. dataId > 100
        ThresholdCondition firingCond = new ThresholdCondition(triggerId, Mode.FIRING, dataId,
            ThresholdCondition.Operator.GT, 100.0);

        Collection<Condition> conditions = new ArrayList<>(1);
        conditions.add( firingCond );
        resp = client.put(path: "triggers/" + triggerId + "/conditions/firing", body: conditions)
        assertEquals(200, resp.status)
        assertEquals(1, resp.data.size())

        // FETCH trigger to validate and get the tenantId
        resp = client.get(path: "triggers/" + triggerId);
        assertEquals(200, resp.status)
        assertEquals(triggerId, resp.data.name)
        assertEquals(false, resp.data.enabled)
        assertEquals(true, resp.data.autoDisable);
        def tenantId = resp.data.tenantId;
        assert( null != tenantId )

        // FETCH recent alerts for trigger, should not be any
        resp = client.get(path: "", query: [startTime:start,triggerIds:triggerId] )
        assertEquals(200, resp.status)
        assertTrue(resp.data.isEmpty())

        // Send in METRICS data before enabling the trigger, these should not fire
        long now = System.currentTimeMillis();
        DataPoint dp1 = new DataPoint( metricId, 150.0, now - 30000 );  // 30 seconds ago

        sendMetricDataViaRest( tenantId, dp1 );

        // sleep to allow data to store, publish, and be processed by an alerting engine run.  If publishing is
        // too slow it could cause a test failure here and be something to look into...  Note, to keep the
        // sleep short we set the following in the pom:
        //   hawkular-alerts.engine-period  =50 ms to minimize lag in sending data to alerting
        //   hawkular.metrics.publish-period=50 ms to minimize lag in alerting running the engine
        Thread.sleep(500);

        // ENABLE Trigger, this should get picked up by the listener and the dataId should become active
        trigger.setEnabled(true);

        resp = client.put(path: "triggers/" + triggerId, body: trigger)
        assertEquals(200, resp.status)

        // Send in METRICS data after enabling the trigger, these should be processed
        dp1 = new DataPoint( metricId, 100.0, now - 20000 );  // 20 seconds ago
        DataPoint dp2 = new DataPoint( metricId, 350.0, now - 15000 );  // 15 seconds ago ALERT!
        sendMetricDataViaRest( tenantId, dp1, dp2);

        // The alert processing happens async, so give it a little time before failing...
        for ( int i=0; i < 10; ++i ) {
            Thread.sleep(500);

            // FETCH recent alerts for trigger, there should be 1
            resp = client.get(path: "", query: [startTime:start,triggerIds:triggerId] )
            if ( resp.status == 200 && resp.data.size() == 1 ) {
                break;
            }
            assertEquals(200, resp.status)
        }
        assertEquals(200, resp.status)
        assertEquals(1, resp.data.size())

        assertEquals(triggerId, resp.data[0].trigger.id)
        assertEquals(350.0, resp.data[0].evalSets[0].iterator().next().value)
    }

    @Test
    void t09_cleanupTriggers() {
        // stop the external alerter from processing these test triggers...
        def resp = client.delete(path: "triggers/AlertingITest-t01")
    }

    private void sendMetricDataViaRest(String tenantId, DataPoint... dataPoints) {

        List<Map<String, Object>> mMetrics = new ArrayList<>();

        for( DataPoint dp : dataPoints ) {
            addDataItem(mMetrics, dp.metricId, dp.time, dp.value);
        }

        // Send it to metrics via rest
        def resp = metricsClient.post(path:"gauges/raw", body:mMetrics, headers:['Hawkular-Tenant':tenantId]);
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
