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
import org.hawkular.alerts.api.model.condition.ExternalCondition
import org.hawkular.alerts.api.model.trigger.Mode
import org.hawkular.alerts.api.model.trigger.Trigger
import org.junit.FixMethodOrder
import org.junit.Test
import org.junit.Ignore

import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertTrue
import static org.junit.runners.MethodSorters.NAME_ASCENDING

/**
 * Tests External Metrics Alerting.
 *
 * @author Jay Shaughnessy
 */
@FixMethodOrder(NAME_ASCENDING)
class ExternalAlerterITest extends AlertingITestBase {

    static start = String.valueOf(System.currentTimeMillis())

    @Test
    void t01_avgTest() {
        // use a unique metricId for the test run so that we don't conflict with prior test runs
        String metricId = "alerts-test-avg-" + start;

        // CREATE trigger using external metrics expression
        def resp = client.get(path: "")
        assertTrue(resp.status == 200 || resp.status == 204)

        Trigger triggerTestAvg = new Trigger("trigger-test-avg", "trigger-test-avg");

        // remove if it exists
        resp = client.delete(path: "triggers/trigger-test-avg")
        assertTrue(200 == resp.status || 404 == resp.status)

        triggerTestAvg.setAutoDisable(true);

        // Tag the trigger as a HawkularMetrics:MetricsCondition so it gets picked up for processing
        triggerTestAvg.addTag("HawkularMetrics", "MetricsCondition" );

        resp = client.post(path: "triggers", body: triggerTestAvg)
        assertEquals(200, resp.status)

        // ADD external metrics avg condition. Note: alerterId must be "HawkularMetrics"
        // Average over the last 1 minute > 50, check every minute
        ExternalCondition firingCond = new ExternalCondition("trigger-test-avg", Mode.FIRING, "external-dataId-avg",
            "HawkularMetrics", "metric:1:avg(" + metricId + " > 50),1");

        Collection<Condition> conditions = new ArrayList<>(1);
        conditions.add( firingCond );
        resp = client.put(path: "triggers/trigger-test-avg/conditions/firing", body: conditions)
        assertEquals(200, resp.status)
        assertEquals(1, resp.data.size())

        // FETCH trigger to validate and get the tenantId
        resp = client.get(path: "triggers/trigger-test-avg");
        assertEquals(200, resp.status)
        assertEquals("trigger-test-avg", resp.data.name)
        assertEquals(false, resp.data.enabled)
        assertEquals(true, resp.data.autoDisable);
        def tenantId = resp.data.tenantId;
        assert( null != tenantId )

        // FETCH recent alerts for trigger, should not be any
        resp = client.get(path: "", query: [startTime:start,triggerIds:"trigger-test-avg"] )
        assertEquals(200, resp.status)
        assertTrue(resp.data.isEmpty())

        // Send in METRICS data before enabling the trigger because the external evaluations start as soon
        // as the enabled Trigger is processed.
        long now = System.currentTimeMillis();
        DataPoint dp1 = new DataPoint( metricId, 50.0, now - 30000 );  // 30 seconds ago
        DataPoint dp2 = new DataPoint( metricId, 60.0, now - 25000 );  // 25 seconds ago
        DataPoint dp3 = new DataPoint( metricId, 73.0, now - 20000 );  // 20 seconds ago
        sendMetricDataViaRest( tenantId, dp1, dp2, dp3 );

        // ENABLE Trigger, this should get picked up by the listener and the expression should be submitted
        // to a runner for processing...
        triggerTestAvg.setEnabled(true);

        resp = client.put(path: "triggers/trigger-test-avg/", body: triggerTestAvg)
        assertEquals(200, resp.status)

        // The alert processing happens async, so give it a little time before failing...
        for ( int i=0; i < 10; ++i ) {
            Thread.sleep(500);

            // FETCH recent alerts for trigger, there should be 1
            resp = client.get(path: "", query: [startTime:start,triggerIds:"trigger-test-avg"] )
            if ( resp.status == 200 && resp.data.size() == 1 ) {
                break;
            }
            assertEquals(200, resp.status)
        }
        assertEquals(200, resp.status)
        assertEquals(1, resp.data.size())

        assertEquals("trigger-test-avg", resp.data[0].trigger.id)
        assertEquals("61.0", resp.data[0].evalSets[0].iterator().next().value)
    }

    @Test
    void t02_avgdTest() {
        // use a unique metricId for the test run so that we don't conflict with prior test runs
        String metricId = "alerts-test-avgd-" + start;

        // CREATE trigger using external metrics expression
        def resp = client.get(path: "")
        assertTrue(resp.status == 200 || resp.status == 204)

        Trigger triggerTestAvgD = new Trigger("trigger-test-avgd", "trigger-test-avgd");

        // remove if it exists
        resp = client.delete(path: "triggers/trigger-test-avgd")
        assertTrue(200 == resp.status || 404 == resp.status)

        triggerTestAvgD.setAutoDisable(true);

        // Tag the trigger as a HawkularMetrics:MetricsCondition so it gets picked up for processing
        triggerTestAvgD.addTag("HawkularMetrics", "MetricsCondition" );

        resp = client.post(path: "triggers", body: triggerTestAvgD)
        assertEquals(200, resp.status)

        // ADD external metrics avgd condition. Note: alerterId must be "HawkularMetrics"
        // Average over the last 1 minute > 50, check every minute
        ExternalCondition firingCond = new ExternalCondition("trigger-test-avgd", Mode.FIRING, "external-dataId-avgd",
            "HawkularMetrics", "metric:1:avgd(" + metricId + " > 25),1");

        Collection<Condition> conditions = new ArrayList<>(1);
        conditions.add( firingCond );
        resp = client.put(path: "triggers/trigger-test-avgd/conditions/firing", body: conditions)
        assertEquals(200, resp.status)
        assertEquals(1, resp.data.size())

        // FETCH trigger to validate and get the tenantId
        resp = client.get(path: "triggers/trigger-test-avgd");
        assertEquals(200, resp.status)
        assertEquals("trigger-test-avgd", resp.data.name)
        assertEquals(false, resp.data.enabled)
        assertEquals(true, resp.data.autoDisable);
        def tenantId = resp.data.tenantId;
        assert( null != tenantId )

        // FETCH recent alerts for trigger, should not be any
        resp = client.get(path: "", query: [startTime:start,triggerIds:"trigger-test-avgd"] )
        assertEquals(200, resp.status)
        assertTrue(resp.data.isEmpty())

        // Send in METRICS data before enabling the trigger because the external evaluations start as soon
        // as the enabled Trigger is processed. Current avg = 15. Yesterday avg = 10. % diff = 50%
        long now = System.currentTimeMillis();
        long then = now - ( 1000 * 60 * 24 );
        DataPoint dp1 = new DataPoint( metricId, 10.0, now - 30000 );  // 30 seconds ago
        DataPoint dp2 = new DataPoint( metricId, 15.0, now - 25000 );  // 25 seconds ago
        DataPoint dp3 = new DataPoint( metricId, 20.0, now - 20000 );  // 20 seconds ago
        DataPoint dp4 = new DataPoint( metricId, 10.0, then - 30000 );  // d+30 seconds ago
        DataPoint dp5 = new DataPoint( metricId, 10.0, then - 25000 );  // d+25 seconds ago
        DataPoint dp6 = new DataPoint( metricId, 10.0, then - 20000 );  // d+20 seconds ago

        sendMetricDataViaRest( tenantId, dp1, dp2, dp3, dp4, dp5, dp6 );

        // ENABLE Trigger, this should get picked up by the listener and the expression should be submitted
        // to a runner for processing...
        triggerTestAvgD.setEnabled(true);

        resp = client.put(path: "triggers/trigger-test-avgd/", body: triggerTestAvgD)
        assertEquals(200, resp.status)

        // The alert processing happens async, so give it a little time before failing...
        for ( int i=0; i < 10; ++i ) {
            Thread.sleep(500);

            // FETCH recent alerts for trigger, there should be 1
            resp = client.get(path: "", query: [startTime:start,triggerIds:"trigger-test-avgd"] )
            if ( resp.status == 200 && resp.data.size() == 1 ) {
                break;
            }
            assertEquals(200, resp.status)
        }
        assertEquals(200, resp.status)
        assertEquals(1, resp.data.size())

        assertEquals("trigger-test-avgd", resp.data[0].trigger.id)
        assertEquals("50.0", resp.data[0].evalSets[0].iterator().next().value)
    }

    @Test
    void t03_avgwTest() {
        // use a unique metricId for the test run so that we don't conflict with prior test runs
        String metricId = "alerts-test-avgw-" + start;

        // CREATE trigger using external metrics expression
        def resp = client.get(path: "")
        assertTrue(resp.status == 200 || resp.status == 204)

        Trigger triggerTestAvgW = new Trigger("trigger-test-avgw", "trigger-test-avgw");

        // remove if it exists
        resp = client.delete(path: "triggers/trigger-test-avgw")
        assertTrue(200 == resp.status || 404 == resp.status)

        triggerTestAvgW.setAutoDisable(true);

        // Tag the trigger as a HawkularMetrics:MetricsCondition so it gets picked up for processing
        triggerTestAvgW.addTag("HawkularMetrics", "MetricsCondition" );

        resp = client.post(path: "triggers", body:triggerTestAvgW)
        assertEquals(200, resp.status)

        // ADD external metrics min condition. Note: alerterId must be "HawkularMetrics"
        // Average over the last 1 minute > 75, check every minute
        ExternalCondition firingCond = new ExternalCondition("trigger-test-avgw", Mode.FIRING, "external-dataId-avgw",
            "HawkularMetrics", "metric:1:avgw(" + metricId + " > 75),1");

        Collection<Condition> conditions = new ArrayList<>(1);
        conditions.add( firingCond );
        resp = client.put(path: "triggers/trigger-test-avgw/conditions/firing", body: conditions)
        assertEquals(200, resp.status)
        assertEquals(1, resp.data.size())

        // FETCH trigger to validate and get the tenantId
        resp = client.get(path: "triggers/trigger-test-avgw");
        assertEquals(200, resp.status)
        assertEquals("trigger-test-avgw", resp.data.name)
        assertEquals(false, resp.data.enabled)
        assertEquals(true, resp.data.autoDisable);
        def tenantId = resp.data.tenantId;
        assert( null != tenantId )

        // FETCH recent alerts for trigger, should not be any
        resp = client.get(path: "", query: [startTime:start,triggerIds:"trigger-test-avgw"] )
        assertEquals(200, resp.status)
        assertTrue(resp.data.isEmpty())

        // Send in METRICS data before enabling the trigger because the external evaluations start as soon
        // as the enabled Trigger is processed. Current avg = 20. LastWeek avg = 10. % diff = 100%
        long now = System.currentTimeMillis();
        long then = now - ( 1000 * 60 * 24 * 7 );
        DataPoint dp1 = new DataPoint( metricId, 10.0, now - 30000 );  // 30 seconds ago
        DataPoint dp2 = new DataPoint( metricId, 20.0, now - 25000 );  // 25 seconds ago
        DataPoint dp3 = new DataPoint( metricId, 30.0, now - 20000 );  // 20 seconds ago
        DataPoint dp4 = new DataPoint( metricId, 10.0, then - 30000 );  // w+30 seconds ago
        DataPoint dp5 = new DataPoint( metricId, 10.0, then - 25000 );  // w+25 seconds ago
        DataPoint dp6 = new DataPoint( metricId, 10.0, then - 20000 );  // w+20 seconds ago

        sendMetricDataViaRest( tenantId, dp1, dp2, dp3, dp4, dp5, dp6 );

        // ENABLE Trigger, this should get picked up by the listener and the expression should be submitted
        // to a runner for processing...
       triggerTestAvgW.setEnabled(true);

        resp = client.put(path: "triggers/trigger-test-avgw/", body:triggerTestAvgW)
        assertEquals(200, resp.status)

        // The alert processing happens async, so give it a little time before failing...
        for ( int i=0; i < 10; ++i ) {
            Thread.sleep(500);

            // FETCH recent alerts for trigger, there should be 1
            resp = client.get(path: "", query: [startTime:start,triggerIds:"trigger-test-avgw"] )
            if ( resp.status == 200 && resp.data.size() == 1 ) {
                break;
            }
            assertEquals(200, resp.status)
        }
        assertEquals(200, resp.status)
        assertEquals(1, resp.data.size())

        assertEquals("trigger-test-avgw", resp.data[0].trigger.id)
        assertEquals("100.0", resp.data[0].evalSets[0].iterator().next().value)
    }

    @Test
    void t04_minTest() {
        // use a unique metricId for the test run so that we don't conflict with prior test runs
        String metricId = "alerts-test-min-" + start;

        // CREATE trigger using external metrics expression
        def resp = client.get(path: "")
        assertTrue(resp.status == 200 || resp.status == 204)

        Trigger triggerTestMin = new Trigger("trigger-test-min", "trigger-test-min");

        // remove if it exists
        resp = client.delete(path: "triggers/trigger-test-min")
        assertTrue(200 == resp.status || 404 == resp.status)

        triggerTestMin.setAutoDisable(true);

        // Tag the trigger as a HawkularMetrics:MetricsCondition so it gets picked up for processing
        triggerTestMin.addTag("HawkularMetrics", "MetricsCondition" );

        resp = client.post(path: "triggers", body:triggerTestMin)
        assertEquals(200, resp.status)

        // ADD external metrics min condition. Note: alerterId must be "HawkularMetrics"
        // Average over the last 1 minute > 50, check every minute
        ExternalCondition firingCond = new ExternalCondition("trigger-test-min", Mode.FIRING, "external-dataId-min",
            "HawkularMetrics", "metric:1:min(" + metricId + " < 10),1");

        Collection<Condition> conditions = new ArrayList<>(1);
        conditions.add( firingCond );
        resp = client.put(path: "triggers/trigger-test-min/conditions/firing", body: conditions)
        assertEquals(200, resp.status)
        assertEquals(1, resp.data.size())

        // FETCH trigger to validate and get the tenantId
        resp = client.get(path: "triggers/trigger-test-min");
        assertEquals(200, resp.status)
        assertEquals("trigger-test-min", resp.data.name)
        assertEquals(false, resp.data.enabled)
        assertEquals(true, resp.data.autoDisable);
        def tenantId = resp.data.tenantId;
        assert( null != tenantId )

        // FETCH recent alerts for trigger, should not be any
        resp = client.get(path: "", query: [startTime:start,triggerIds:"trigger-test-min"] )
        assertEquals(200, resp.status)
        assertTrue(resp.data.isEmpty())

        // Send in METRICS data before enabling the trigger because the external evaluations start as soon
        // as the enabled Trigger is processed. min = 9
        long now = System.currentTimeMillis();
        DataPoint dp1 = new DataPoint( metricId, 10.0, now - 30000 );  // 30 seconds ago
        DataPoint dp2 = new DataPoint( metricId,  9.0, now - 25000 );  // 25 seconds ago
        DataPoint dp3 = new DataPoint( metricId, 20.0, now - 20000 );  // 20 seconds ago

        sendMetricDataViaRest( tenantId, dp1, dp2, dp3);

        // ENABLE Trigger, this should get picked up by the listener and the expression should be submitted
        // to a runner for processing...
       triggerTestMin.setEnabled(true);

        resp = client.put(path: "triggers/trigger-test-min/", body:triggerTestMin)
        assertEquals(200, resp.status)

        // The alert processing happens async, so give it a little time before failing...
        for ( int i=0; i < 10; ++i ) {
            Thread.sleep(500);

            // FETCH recent alerts for trigger, there should be 1
            resp = client.get(path: "", query: [startTime:start,triggerIds:"trigger-test-min"] )
            if ( resp.status == 200 && resp.data.size() == 1 ) {
                break;
            }
            assertEquals(200, resp.status)
        }
        assertEquals(200, resp.status)
        assertEquals(1, resp.data.size())

        assertEquals("trigger-test-min", resp.data[0].trigger.id)
        assertEquals("9.0", resp.data[0].evalSets[0].iterator().next().value)
    }

    @Test
    void t05_maxTest() {
        // use a unique metricId for the test run so that we don't conflict with prior test runs
        String metricId = "alerts-test-max-" + start;

        // CREATE trigger using external metrics expression
        def resp = client.get(path: "")
        assertTrue(resp.status == 200 || resp.status == 204)

        Trigger triggerTestMax = new Trigger("trigger-test-max", "trigger-test-max");

        // remove if it exists
        resp = client.delete(path: "triggers/trigger-test-max")
        assertTrue(200 == resp.status || 404 == resp.status)

        triggerTestMax.setAutoDisable(true);

        // Tag the trigger as a HawkularMetrics:MetricsCondition so it gets picked up for processing
        triggerTestMax.addTag("HawkularMetrics", "MetricsCondition" );

        resp = client.post(path: "triggers", body: triggerTestMax)
        assertEquals(200, resp.status)

        // ADD external metrics max condition. Note: alerterId must be "HawkularMetrics"
        // Average over the last 1 minute > 50, check every minute
        ExternalCondition firingCond = new ExternalCondition("trigger-test-max", Mode.FIRING, "external-dataId-max",
            "HawkularMetrics", "metric:1:max(" + metricId + " > 19),1");

        Collection<Condition> conditions = new ArrayList<>(1);
        conditions.add( firingCond );
        resp = client.put(path: "triggers/trigger-test-max/conditions/firing", body: conditions)
        assertEquals(200, resp.status)
        assertEquals(1, resp.data.size())

        // FETCH trigger to validate and get the tenantId
        resp = client.get(path: "triggers/trigger-test-max");
        assertEquals(200, resp.status)
        assertEquals("trigger-test-max", resp.data.name)
        assertEquals(false, resp.data.enabled)
        assertEquals(true, resp.data.autoDisable);
        def tenantId = resp.data.tenantId;
        assert( null != tenantId )

        // FETCH recent alerts for trigger, should not be any
        resp = client.get(path: "", query: [startTime:start,triggerIds:"trigger-test-max"] )
        assertEquals(200, resp.status)
        assertTrue(resp.data.isEmpty())

        // Send in METRICS data before enabling the trigger because the external evaluations start as soon
        // as the enabled Trigger is processed. Max = 20
        long now = System.currentTimeMillis();
        DataPoint dp1 = new DataPoint( metricId, 10.0, now - 30000 );  // 30 seconds ago
        DataPoint dp2 = new DataPoint( metricId,  9.0, now - 25000 );  // 25 seconds ago
        DataPoint dp3 = new DataPoint( metricId, 20.0, now - 20000 );  // 20 seconds ago

        sendMetricDataViaRest( tenantId, dp1, dp2, dp3);

        // ENABLE Trigger, this should get picked up by the listener and the expression should be submitted
        // to a runner for processing...
        triggerTestMax.setEnabled(true);

        resp = client.put(path: "triggers/trigger-test-max/", body: triggerTestMax)
        assertEquals(200, resp.status)

        // The alert processing happens async, so give it a little time before failing...
        for ( int i=0; i < 10; ++i ) {
            Thread.sleep(500);

            // FETCH recent alerts for trigger, there should be 1
            resp = client.get(path: "", query: [startTime:start,triggerIds:"trigger-test-max"] )
            if ( resp.status == 200 && resp.data.size() == 1 ) {
                break;
            }
            assertEquals(200, resp.status)
        }
        assertEquals(200, resp.status)
        assertEquals(1, resp.data.size())

        assertEquals("trigger-test-max", resp.data[0].trigger.id)
        assertEquals("20.0", resp.data[0].evalSets[0].iterator().next().value)
    }

    @Test
    void t06_rangeTest() {
        // use a unique metricId for the test run so that we don't conflict with prior test runs
        String metricId = "alerts-test-range-" + start;

        // CREATE trigger using external metrics expression
        def resp = client.get(path: "")
        assertTrue(resp.status == 200 || resp.status == 204)

        Trigger triggerTestRange = new Trigger("trigger-test-range", "trigger-test-range");

        // remove if it exists
        resp = client.delete(path: "triggers/trigger-test-range")
        assertTrue(200 == resp.status || 404 == resp.status)

        triggerTestRange.setAutoDisable(true);

        // Tag the trigger as a HawkularMetrics:MetricsCondition so it gets picked up for processing
        triggerTestRange.addTag("HawkularMetrics", "MetricsCondition" );

        resp = client.post(path: "triggers", body: triggerTestRange)
        assertEquals(200, resp.status)

        // ADD external metrics range condition. Note: alerterId must be "HawkularMetrics"
        // Average over the last 1 minute > 50, check every minute
        ExternalCondition firingCond = new ExternalCondition("trigger-test-range", Mode.FIRING, "external-dataId-range",
            "HawkularMetrics", "metric:1:range(" + metricId + " >= 10),1");

        Collection<Condition> conditions = new ArrayList<>(1);
        conditions.add( firingCond );
        resp = client.put(path: "triggers/trigger-test-range/conditions/firing", body: conditions)
        assertEquals(200, resp.status)
        assertEquals(1, resp.data.size())

        // FETCH trigger to validate and get the tenantId
        resp = client.get(path: "triggers/trigger-test-range");
        assertEquals(200, resp.status)
        assertEquals("trigger-test-range", resp.data.name)
        assertEquals(false, resp.data.enabled)
        assertEquals(true, resp.data.autoDisable);
        def tenantId = resp.data.tenantId;
        assert( null != tenantId )

        // FETCH recent alerts for trigger, should not be any
        resp = client.get(path: "", query: [startTime:start,triggerIds:"trigger-test-range"] )
        assertEquals(200, resp.status)
        assertTrue(resp.data.isEmpty())

        // Send in METRICS data before enabling the trigger because the external evaluations start as soon
        // as the enabled Trigger is processed. Range=10
        long now = System.currentTimeMillis();
        DataPoint dp1 = new DataPoint( metricId, 100.0, now - 30000 );  // 30 seconds ago
        DataPoint dp2 = new DataPoint( metricId, 105.0, now - 25000 );  // 25 seconds ago
        DataPoint dp3 = new DataPoint( metricId, 110.0, now - 20000 );  // 20 seconds ago

        sendMetricDataViaRest( tenantId, dp1, dp2, dp3);

        // ENABLE Trigger, this should get picked up by the listener and the expression should be submitted
        // to a runner for processing...
        triggerTestRange.setEnabled(true);

        resp = client.put(path: "triggers/trigger-test-range/", body: triggerTestRange)
        assertEquals(200, resp.status)

        // The alert processing happens async, so give it a little time before failing...
        for ( int i=0; i < 10; ++i ) {
            Thread.sleep(500);

            // FETCH recent alerts for trigger, there should be 1
            resp = client.get(path: "", query: [startTime:start,triggerIds:"trigger-test-range"] )
            if ( resp.status == 200 && resp.data.size() == 1 ) {
                break;
            }
            assertEquals(200, resp.status)
        }
        assertEquals(200, resp.status)
        assertEquals(1, resp.data.size())

        assertEquals("trigger-test-range", resp.data[0].trigger.id)
        assertEquals("10.0", resp.data[0].evalSets[0].iterator().next().value)
    }

    @Test
    public void t07_rangepTest() {
        // use a unique metricId for the test run so that we don't conflict with prior test runs
        String metricId = "alerts-test-rangep-" + start;

        // CREATE trigger using external metrics expression
        def resp = client.get(path: "")
        assertTrue(resp.status == 200 || resp.status == 204)

        Trigger triggerTestRangep = new Trigger("trigger-test-rangep", "trigger-test-rangep");

        // remove if it exists
        resp = client.delete(path: "triggers/trigger-test-rangep")
        assertTrue(200 == resp.status || 404 == resp.status)

        triggerTestRangep.setAutoDisable(true);

        // Tag the trigger as a HawkularMetrics:MetricsCondition so it gets picked up for processing
        triggerTestRangep.addTag("HawkularMetrics", "MetricsCondition" );

        resp = client.post(path: "triggers", body: triggerTestRangep)
        assertEquals(200, resp.status)

        // ADD external metrics rangep condition. Note: alerterId must be "HawkularMetrics"
        // Average over the last 1 minute > 50, check every minute
        ExternalCondition firingCond = new ExternalCondition("trigger-test-rangep", Mode.FIRING, "external-dataId-rangep",
            "HawkularMetrics", "metric:1:rangep(" + metricId + " >= 0.75),1");

        Collection<Condition> conditions = new ArrayList<>(1);
        conditions.add( firingCond );
        resp = client.put(path: "triggers/trigger-test-rangep/conditions/firing", body: conditions)
        assertEquals(200, resp.status)
        assertEquals(1, resp.data.size())

        // FETCH trigger to validate and get the tenantId
        resp = client.get(path: "triggers/trigger-test-rangep");
        assertEquals(200, resp.status)
        assertEquals("trigger-test-rangep", resp.data.name)
        assertEquals(false, resp.data.enabled)
        assertEquals(true, resp.data.autoDisable);
        def tenantId = resp.data.tenantId;
        assert( null != tenantId )

        // FETCH recent alerts for trigger, should not be any
        resp = client.get(path: "", query: [startTime:start,triggerIds:"trigger-test-rangep"] )
        assertEquals(200, resp.status)
        assertTrue(resp.data.isEmpty())

        // Send in METRICS data before enabling the trigger because the external evaluations start as soon
        // as the enabled Trigger is processed. Rangep=0.8
        long now = System.currentTimeMillis();
        DataPoint dp1 = new DataPoint( metricId,  60.0, now - 30000 );  // 30 seconds ago
        DataPoint dp2 = new DataPoint( metricId, 100.0, now - 25000 );  // 25 seconds ago
        DataPoint dp3 = new DataPoint( metricId, 140.0, now - 20000 );  // 20 seconds ago

        sendMetricDataViaRest( tenantId, dp1, dp2, dp3);

        // ENABLE Trigger, this should get picked up by the listener and the expression should be submitted
        // to a runner for processing...
        triggerTestRangep.setEnabled(true);

        resp = client.put(path: "triggers/trigger-test-rangep/", body: triggerTestRangep)
        assertEquals(200, resp.status)

        // The alert processing happens async, so give it a little time before failing...
        for ( int i=0; i < 10; ++i ) {
            Thread.sleep(500);

            // FETCH recent alerts for trigger, there should be 1
            resp = client.get(path: "", query: [startTime:start,triggerIds:"trigger-test-rangep"] )
            if ( resp.status == 200 && resp.data.size() == 1 ) {
                break;
            }
            assertEquals(200, resp.status)
        }
        assertEquals(200, resp.status)
        assertEquals(1, resp.data.size())

        assertEquals("trigger-test-rangep", resp.data[0].trigger.id)
        assertEquals("0.8", resp.data[0].evalSets[0].iterator().next().value)
    }

    @Test
    void t08_heartbeatTest() {
        // use a unique metricId for the test run so that we don't conflict with prior test runs
        String availId = "alerts-test-heartbeat-" + start;

        // CREATE trigger using external metrics expression
        def resp = client.get(path: "")
        assertTrue(resp.status == 200 || resp.status == 204)

        Trigger triggerTestHeartbeat = new Trigger("trigger-test-heartbeat", "trigger-test-heartbeat");

        // remove if it exists
        resp = client.delete(path: "triggers/trigger-test-heartbeat")
        assertTrue(200 == resp.status || 404 == resp.status)

        triggerTestHeartbeat.setAutoDisable(true);

        // Tag the trigger as a HawkularMetrics:MetricsCondition so it gets picked up for processing
        triggerTestHeartbeat.addTag("HawkularMetrics", "MetricsCondition" );

        resp = client.post(path: "triggers", body: triggerTestHeartbeat)
        assertEquals(200, resp.status)

        // ADD external metrics heartbeat condition. Note: alerterId must be "HawkularMetrics"
        // # If # UP Avail <=2 in the past minute, alert, check every minute
        ExternalCondition firingCond = new ExternalCondition("trigger-test-heartbeat", Mode.FIRING, "external-dataId-heartbeat",
            "HawkularMetrics", "metric:1:heartbeat(" + availId + " <= 2),1");

        Collection<Condition> conditions = new ArrayList<>(1);
        conditions.add( firingCond );
        resp = client.put(path: "triggers/trigger-test-heartbeat/conditions/firing", body: conditions)
        assertEquals(200, resp.status)
        assertEquals(1, resp.data.size())

        // FETCH trigger to validate and get the tenantId
        resp = client.get(path: "triggers/trigger-test-heartbeat");
        assertEquals(200, resp.status)
        assertEquals("trigger-test-heartbeat", resp.data.name)
        assertEquals(false, resp.data.enabled)
        assertEquals(true, resp.data.autoDisable);
        def tenantId = resp.data.tenantId;
        assert( null != tenantId )

        // FETCH recent alerts for trigger, should not be any
        resp = client.get(path: "", query: [startTime:start,triggerIds:"trigger-test-heartbeat"] )
        assertEquals(200, resp.status)
        assertTrue(resp.data.isEmpty())

        // Send in AVAIL data before enabling the trigger because the external evaluations start as soon
        // as the enabled Trigger is processed.
        long now = System.currentTimeMillis();
        AvailPoint ap1 = new AvailPoint( availId, "UP", now - 30000 );  // 30 seconds ago
        AvailPoint ap2 = new AvailPoint( availId, "UP", now - 20000 );  // 20 seconds ago

        sendAvailViaRest( tenantId, ap1, ap2);

        // ENABLE Trigger, this should get picked up by the listener and the expression should be submitted
        // to a runner for processing...
        triggerTestHeartbeat.setEnabled(true);

        resp = client.put(path: "triggers/trigger-test-heartbeat/", body: triggerTestHeartbeat)
        assertEquals(200, resp.status)

        // The alert processing happens async, so give it a little time before failing...
        for ( int i=0; i < 10; ++i ) {
            Thread.sleep(500);

            // FETCH recent alerts for trigger, there should be 1
            resp = client.get(path: "", query: [startTime:start,triggerIds:"trigger-test-heartbeat"] )
            if ( resp.status == 200 && resp.data.size() == 1 ) {
                break;
            }
            assertEquals(200, resp.status)
        }
        assertEquals(200, resp.status)
        assertEquals(1, resp.data.size())

        assertEquals("trigger-test-heartbeat", resp.data[0].trigger.id)
        assertEquals("2.0", resp.data[0].evalSets[0].iterator().next().value)
    }

    @Test
    void t09_cleanupTriggers() {
        // stop the external alerter from processing these test triggers...
        def resp = client.delete(path: "triggers/trigger-test-avg")
        resp = client.delete(path: "triggers/trigger-test-avgd")
        resp = client.delete(path: "triggers/trigger-test-avgw")
        resp = client.delete(path: "triggers/trigger-test-heartbeat")
        resp = client.delete(path: "triggers/trigger-test-min")
        resp = client.delete(path: "triggers/trigger-test-max")
        resp = client.delete(path: "triggers/trigger-test-range")
        resp = client.delete(path: "triggers/trigger-test-rangep")
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

    private void sendAvailViaRest(String tenantId, AvailPoint... availPoints) {

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
