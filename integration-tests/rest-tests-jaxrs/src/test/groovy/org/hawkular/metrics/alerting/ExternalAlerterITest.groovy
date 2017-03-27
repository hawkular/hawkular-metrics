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
import org.hawkular.alerts.api.model.condition.Condition
import org.hawkular.alerts.api.model.condition.ExternalCondition
import org.hawkular.alerts.api.model.trigger.Mode
import org.hawkular.alerts.api.model.trigger.Trigger
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

        // Note: alerterId must be "HawkularMetrics"
        // Average over the last 1 minute > 50, check every minute
        Query qNow = Query.builder("qNow").metric(metricId).duration("1mn").build();
        ConditionExpression ce = new ConditionExpression(qNow, "1mn", "q(qNow,avg) > 50" )
        // to see the JSON uncomment
        //println( JsonOutput.prettyPrint( ce.toJson()) )
        ExternalCondition firingCond = new ExternalCondition("trigger-test-avg", Mode.FIRING, "external-dataId-avg",
            "HawkularMetrics", ce.toJson());

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
        sendGaugeDataViaRest( tenantId, dp1, dp2, dp3 );

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
        assertEquals("{q(qNow,avg)=61.0}", resp.data[0].evalSets[0].iterator().next().event.text)
    }

    // Same test as above using tag search and explicit JSON
    @Test
    void t01_tagTest() {
        // use a unique metricId for the test run so that we don't conflict with prior test runs
        String metricId = "alerts-test-tag-" + start;

        // CREATE trigger using external metrics expression
        def resp = client.get(path: "")
        assertTrue(resp.status == 200 || resp.status == 204)

        Trigger triggerTestAvg = new Trigger("trigger-test-tag", "trigger-test-tag");

        // remove if it exists
        resp = client.delete(path: "triggers/trigger-test-tag")
        assertTrue(200 == resp.status || 404 == resp.status)

        triggerTestAvg.setAutoDisable(true);

        // Tag the trigger as a HawkularMetrics:MetricsCondition so it gets picked up for processing
        triggerTestAvg.addTag("HawkularMetrics", "MetricsCondition" );

        resp = client.post(path: "triggers", body: triggerTestAvg)
        assertEquals(200, resp.status)

        // Note: alerterId must be "HawkularMetrics"
        // Average over the last 1 minute > 50, check every minute
        def conditionExpression =
        "{" +
        "    \"queries\": [" +
        "        {" +
        "            \"name\": \"qNow\"," +
        "            \"type\": \"gauge\"," +
        "            \"metrics\": null," +
        "            \"tags\": \"test-metric-name:" + metricId + "\"," +
        "            \"percentiles\": [" +
        "                " +
        "            ]," +
        "            \"duration\": \"1mn\"," +
        "            \"offset\": null" +
        "        }" +
        "    ]," +
        "    \"frequency\": \"1mn\"," +
        "    \"evalType\": \"ALL\"," +
        "    \"eval\": \"q(qNow,avg) > 50\"" +
        "}"
        ExternalCondition firingCond = new ExternalCondition("trigger-test-tag", Mode.FIRING, "external-dataId-avg",
            "HawkularMetrics", conditionExpression);

        Collection<Condition> conditions = new ArrayList<>(1);
        conditions.add( firingCond );
        resp = client.put(path: "triggers/trigger-test-tag/conditions/firing", body: conditions)
        assertEquals(200, resp.status)
        assertEquals(1, resp.data.size())

        // FETCH trigger to validate and get the tenantId
        resp = client.get(path: "triggers/trigger-test-tag");
        assertEquals(200, resp.status)
        assertEquals("trigger-test-tag", resp.data.name)
        assertEquals(false, resp.data.enabled)
        assertEquals(true, resp.data.autoDisable);
        def tenantId = resp.data.tenantId;
        assert( null != tenantId )

        // FETCH recent alerts for trigger, should not be any
        resp = client.get(path: "", query: [startTime:start,triggerIds:"trigger-test-tag"] )
        assertEquals(200, resp.status)
        assertTrue(resp.data.isEmpty())

        // Send in METRICS data before enabling the trigger because the external evaluations start as soon
        // as the enabled Trigger is processed.
        long now = System.currentTimeMillis();
        DataPoint dp1 = new DataPoint( metricId, 50.0, now - 30000 );  // 30 seconds ago
        DataPoint dp2 = new DataPoint( metricId, 60.0, now - 25000 );  // 25 seconds ago
        DataPoint dp3 = new DataPoint( metricId, 73.0, now - 20000 );  // 20 seconds ago
        sendGaugeDataViaRest( tenantId, dp1, dp2, dp3 );

        Map<String, String> tagMap = new HashMap<>(1);
        tagMap.put("test-metric-name", metricId);
        resp = metricsClient.put(path: "gauges/" + metricId + "/tags/", body: tagMap)
        assertEquals(200, resp.status)

        // ENABLE Trigger, this should get picked up by the listener and the expression should be submitted
        // to a runner for processing...
        triggerTestAvg.setEnabled(true);

        resp = client.put(path: "triggers/trigger-test-tag/", body: triggerTestAvg)
        assertEquals(200, resp.status)

        // The alert processing happens async, so give it a little time before failing...
        for ( int i=0; i < 10; ++i ) {
            Thread.sleep(500);

            // FETCH recent alerts for trigger, there should be 1
            resp = client.get(path: "", query: [startTime:start,triggerIds:"trigger-test-tag"] )
            if ( resp.status == 200 && resp.data.size() == 1 ) {
                break;
            }
            assertEquals(200, resp.status)
        }
        assertEquals(200, resp.status)
        assertEquals(1, resp.data.size())
        assertEquals("trigger-test-tag", resp.data[0].trigger.id)
        assertEquals("{q(qNow,avg)=61.0}", resp.data[0].evalSets[0].iterator().next().event.text)
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

        // Note: alerterId must be "HawkularMetrics"
        // Average over the last 1 minute > 25% more than yesterday for same minute, check every minute
        Query qNow = Query.builder("qNow").metric(metricId).duration("1mn").build();
        Query q1d = Query.builder("q1d", "1d", qNow).build();
        ConditionExpression ce = new ConditionExpression( Arrays.asList(qNow, q1d), "1mn",
            "((q(qNow,avg) - q(q1d,avg)) / q(q1d,avg)) > 0.25" )
        //println( JsonOutput.prettyPrint( ce.toJson()) )
        ExternalCondition firingCond = new ExternalCondition("trigger-test-avgd", Mode.FIRING, "external-dataId-avgd",
            "HawkularMetrics", ce.toJson());

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
        long then = now - ( 1000 * 60 * 60 * 24 );
        DataPoint dp1 = new DataPoint( metricId, 10.0, now - 30000 );  // 30 seconds ago
        DataPoint dp2 = new DataPoint( metricId, 15.0, now - 25000 );  // 25 seconds ago
        DataPoint dp3 = new DataPoint( metricId, 20.0, now - 20000 );  // 20 seconds ago
        DataPoint dp4 = new DataPoint( metricId, 10.0, then - 30000 );  // d+30 seconds ago
        DataPoint dp5 = new DataPoint( metricId, 10.0, then - 25000 );  // d+25 seconds ago
        DataPoint dp6 = new DataPoint( metricId, 10.0, then - 20000 );  // d+20 seconds ago

        sendGaugeDataViaRest( tenantId, dp1, dp2, dp3, dp4, dp5, dp6 );

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
        assertEquals("{q(q1d,avg)=10.0, q(qNow,avg)=15.0}", resp.data[0].evalSets[0].iterator().next().event.text)
    }

    @Test
    void t03_otherFuncsTest() {
        // use a unique metricId for the test run so that we don't conflict with prior test runs
        String metricId = "alerts-test-funcs-" + start;

        // CREATE trigger using external metrics expression
        def resp = client.get(path: "")
        assertTrue(resp.status == 200 || resp.status == 204)

        Trigger triggerTest = new Trigger("trigger-test-funcs", "trigger-test-funcs");

        // remove if it exists
        resp = client.delete(path: "triggers/trigger-test-funcs")
        assertTrue(200 == resp.status || 404 == resp.status)

        triggerTest.setAutoDisable(true);

        // Tag the trigger as a HawkularMetrics:MetricsCondition so it gets picked up for processing
        triggerTest.addTag("HawkularMetrics", "MetricsCondition" );

        resp = client.post(path: "triggers", body:triggerTest)
        assertEquals(200, resp.status)

        // Note: alerterId must be "HawkularMetrics"
        Query qNow = Query.builder("qNow").metric(metricId).duration("1mn").build();
        ConditionExpression ce = new ConditionExpression(qNow, "1mn",
            "q(qNow,min) + q(qNow,max) + q(qNow,sum) + q(qNow,median) + q(qNow,samples) == 123" )
        ExternalCondition firingCond = new ExternalCondition("trigger-test-funcs", Mode.FIRING, "external-dataId-funcs",
           "HawkularMetrics", ce.toJson());

        Collection<Condition> conditions = new ArrayList<>(1);
        conditions.add( firingCond );
        resp = client.put(path: "triggers/trigger-test-funcs/conditions/firing", body: conditions)
        assertEquals(200, resp.status)
        assertEquals(1, resp.data.size())

        // FETCH trigger to validate and get the tenantId
        resp = client.get(path: "triggers/trigger-test-funcs");
        assertEquals(200, resp.status)
        assertEquals("trigger-test-funcs", resp.data.name)
        assertEquals(false, resp.data.enabled)
        assertEquals(true, resp.data.autoDisable);
        def tenantId = resp.data.tenantId;
        assert( null != tenantId )

        // FETCH recent alerts for trigger, should not be any
        resp = client.get(path: "", query: [startTime:start,triggerIds:"trigger-test-funcs"] )
        assertEquals(200, resp.status)
        assertTrue(resp.data.isEmpty())

        // Send in METRICS data before enabling the trigger because the external evaluations start as soon
        // as the enabled Trigger is processed.
        long now = System.currentTimeMillis();
        DataPoint dp1 = new DataPoint( metricId, 10.0, now - 30000 );  // 30 seconds ago
        DataPoint dp2 = new DataPoint( metricId, 10.0, now - 20000 );  // 20 seconds ago
        DataPoint dp3 = new DataPoint( metricId, 40.0, now - 10000 );  // 10 seconds ago

        sendGaugeDataViaRest( tenantId, dp1, dp2, dp3);

        // ENABLE Trigger, this should get picked up by the listener and the expression should be submitted
        // to a runner for processing...
       triggerTest.setEnabled(true);

        resp = client.put(path: "triggers/trigger-test-funcs/", body:triggerTest)
        assertEquals(200, resp.status)

        // The alert processing happens async, so give it a little time before failing...
        for ( int i=0; i < 10; ++i ) {
            Thread.sleep(500);

            // FETCH recent alerts for trigger, there should be 1
            resp = client.get(path: "", query: [startTime:start,triggerIds:"trigger-test-funcs"] )
            if ( resp.status == 200 && resp.data.size() == 1 ) {
                break;
            }
            assertEquals(200, resp.status)
        }
        assertEquals(200, resp.status)
        assertEquals(1, resp.data.size())

        assertEquals("trigger-test-funcs", resp.data[0].trigger.id)
        assertEquals("{q(qNow,max)=40.0, q(qNow,min)=10.0, q(qNow,samples)=3.0, q(qNow,median)=10.0, q(qNow,sum)=60.0}",
             resp.data[0].evalSets[0].iterator().next().event.text)
    }

    @Test
    void t04_percentilesTest() {
        // use a unique metricId for the test run so that we don't conflict with prior test runs
        String metricId = "alerts-test-percentiles-" + start;

        // CREATE trigger using external metrics expression
        def resp = client.get(path: "")
        assertTrue(resp.status == 200 || resp.status == 204)

        Trigger triggerTest = new Trigger("trigger-test-percentiles", "trigger-test-percentiles");

        // remove if it exists
        resp = client.delete(path: "triggers/trigger-test-percentiles")
        assertTrue(200 == resp.status || 404 == resp.status)

        triggerTest.setAutoDisable(true);

        // Tag the trigger as a HawkularMetrics:MetricsCondition so it gets picked up for processing
        triggerTest.addTag("HawkularMetrics", "MetricsCondition" );

        resp = client.post(path: "triggers", body:triggerTest)
        assertEquals(200, resp.status)

        // Note: alerterId must be "HawkularMetrics"
        Query qNow = Query.builder("qNow").metric(metricId).duration("1mn")
                          .percentiles(new HashSet(Arrays.asList("25", "75"))).build();
        ConditionExpression ce = new ConditionExpression(qNow, "1mn",
            "q(qNow,%25) + q(qNow,%75) == 40.0" )
        ExternalCondition firingCond = new ExternalCondition("trigger-test-percentiles", Mode.FIRING,
            "external-dataId-percentiles", "HawkularMetrics", ce.toJson());

        Collection<Condition> conditions = new ArrayList<>(1);
        conditions.add( firingCond );
        resp = client.put(path: "triggers/trigger-test-percentiles/conditions/firing", body: conditions)
        assertEquals(200, resp.status)
        assertEquals(1, resp.data.size())

        // FETCH trigger to validate and get the tenantId
        resp = client.get(path: "triggers/trigger-test-percentiles");
        assertEquals(200, resp.status)
        assertEquals("trigger-test-percentiles", resp.data.name)
        assertEquals(false, resp.data.enabled)
        assertEquals(true, resp.data.autoDisable);
        def tenantId = resp.data.tenantId;
        assert( null != tenantId )

        // FETCH recent alerts for trigger, should not be any
        resp = client.get(path: "", query: [startTime:start,triggerIds:"trigger-test-percentiles"] )
        assertEquals(200, resp.status)
        assertTrue(resp.data.isEmpty())

        // Send in METRICS data before enabling the trigger because the external evaluations start as soon
        // as the enabled Trigger is processed
        long now = System.currentTimeMillis();
        DataPoint dp1 = new DataPoint( metricId, 10.0, now - 30000 );  // 30 seconds ago (%25)
        DataPoint dp2 = new DataPoint( metricId, 20.0, now - 20000 );  // 20 seconds ago
        DataPoint dp3 = new DataPoint( metricId, 30.0, now - 15000 );  // 15 seconds ago (%75)
        DataPoint dp4 = new DataPoint( metricId, 40.0, now - 10000 );  //  10 seconds ago

        sendGaugeDataViaRest( tenantId, dp1, dp2, dp3, dp4);

        // ENABLE Trigger, this should get picked up by the listener and the expression should be submitted
        // to a runner for processing...
       triggerTest.setEnabled(true);

        resp = client.put(path: "triggers/trigger-test-percentiles/", body:triggerTest)
        assertEquals(200, resp.status)

        // The alert processing happens async, so give it a little time before failing...
        for ( int i=0; i < 10; ++i ) {
            Thread.sleep(500);

            // FETCH recent alerts for trigger, there should be 1
            resp = client.get(path: "", query: [startTime:start,triggerIds:"trigger-test-percentiles"] )
            if ( resp.status == 200 && resp.data.size() == 1 ) {
                break;
            }
            assertEquals(200, resp.status)
        }
        assertEquals(200, resp.status)
        assertEquals(1, resp.data.size())

        assertEquals("trigger-test-percentiles", resp.data[0].trigger.id)
        assertEquals("{q(qNow,%75)=30.0, q(qNow,%25)=10.0}", resp.data[0].evalSets[0].iterator().next().event.text)
    }

    @Test
    void t05_heartbeatTest() {
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

        // Note: alerterId must be "HawkularMetrics"
        Query qNow = Query.builder("qNow").metric(availId).type(MetricType.AVAILABILITY).duration("1mn").build();
        ConditionExpression ce = new ConditionExpression(qNow, "1mn",
            "q(qNow,upCount) <= 2" )
        ExternalCondition firingCond = new ExternalCondition("trigger-test-heartbeat", Mode.FIRING,
            "external-dataId-heartbeat", "HawkularMetrics", ce.toJson());

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

        sendAvailDataViaRest( tenantId, ap1, ap2);

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
        assertEquals("{q(qNow,upCount)=2.0}", resp.data[0].evalSets[0].iterator().next().event.text)
    }

    @Test
    void t06_counterTest() {
        // use a unique metricId for the test run so that we don't conflict with prior test runs
        String metricId = "alerts-test-counter-" + start;

        // CREATE trigger using external metrics expression
        def resp = client.get(path: "")
        assertTrue(resp.status == 200 || resp.status == 204)

        Trigger triggerTest = new Trigger("trigger-test-counter", "trigger-test-counter");

        // remove if it exists
        resp = client.delete(path: "triggers/trigger-test-counter")
        assertTrue(200 == resp.status || 404 == resp.status)

        triggerTest.setAutoDisable(true);

        // Tag the trigger as a HawkularMetrics:MetricsCondition so it gets picked up for processing
        triggerTest.addTag("HawkularMetrics", "MetricsCondition" );

        resp = client.post(path: "triggers", body:triggerTest)
        assertEquals(200, resp.status)

        // Note: alerterId must be "HawkularMetrics"
        Query qNow = Query.builder("qNow").metric(metricId).type(MetricType.COUNTER).duration("1mn").build();
        ConditionExpression ce = new ConditionExpression(qNow, "1mn",
            "q(qNow,avg) + q(qNow,sum) == 80" )
        ExternalCondition firingCond = new ExternalCondition("trigger-test-counter", Mode.FIRING,
            "external-dataId-counter", "HawkularMetrics", ce.toJson());

        Collection<Condition> conditions = new ArrayList<>(1);
        conditions.add( firingCond );
        resp = client.put(path: "triggers/trigger-test-counter/conditions/firing", body: conditions)
        assertEquals(200, resp.status)
        assertEquals(1, resp.data.size())

        // FETCH trigger to validate and get the tenantId
        resp = client.get(path: "triggers/trigger-test-counter");
        assertEquals(200, resp.status)
        assertEquals("trigger-test-counter", resp.data.name)
        assertEquals(false, resp.data.enabled)
        assertEquals(true, resp.data.autoDisable);
        def tenantId = resp.data.tenantId;
        assert( null != tenantId )

        // FETCH recent alerts for trigger, should not be any
        resp = client.get(path: "", query: [startTime:start,triggerIds:"trigger-test-counter"] )
        assertEquals(200, resp.status)
        assertTrue(resp.data.isEmpty())

        // Send in METRICS data before enabling the trigger because the external evaluations start as soon
        // as the enabled Trigger is processed
        long now = System.currentTimeMillis();
        DataPoint dp1 = new DataPoint( metricId, 10.0, now - 30000 );  // 30 seconds ago
        DataPoint dp2 = new DataPoint( metricId, 20.0, now - 20000 );  // 20 seconds ago
        DataPoint dp3 = new DataPoint( metricId, 30.0, now - 10000 );  // 10 seconds ago

        sendCounterDataViaRest( tenantId, dp1, dp2, dp3);

        // ENABLE Trigger, this should get picked up by the listener and the expression should be submitted
        // to a runner for processing...
        triggerTest.setEnabled(true);

        resp = client.put(path: "triggers/trigger-test-counter/", body:triggerTest)
        assertEquals(200, resp.status)

        // The alert processing happens async, so give it a little time before failing...
        for ( int i=0; i < 10; ++i ) {
            Thread.sleep(500);

            // FETCH recent alerts for trigger, there should be 1
            resp = client.get(path: "", query: [startTime:start,triggerIds:"trigger-test-counter"] )
            if ( resp.status == 200 && resp.data.size() == 1 ) {
                break;
            }
            assertEquals(200, resp.status)
        }
        assertEquals(200, resp.status)
        assertEquals(1, resp.data.size())

        assertEquals("trigger-test-counter", resp.data[0].trigger.id)
        assertEquals("{q(qNow,avg)=20.0, q(qNow,sum)=60.0}",
             resp.data[0].evalSets[0].iterator().next().event.text)
    }

    @Test
    void t07_rateTest() {
        // use a unique metricId for the test run so that we don't conflict with prior test runs
        String metricId = "alerts-test-rate-" + start;

        // CREATE trigger using external metrics expression
        def resp = client.get(path: "")
        assertTrue(resp.status == 200 || resp.status == 204)

        Trigger triggerTest = new Trigger("trigger-test-rate", "trigger-test-rate");

        // remove if it exists
        resp = client.delete(path: "triggers/trigger-test-rate")
        assertTrue(200 == resp.status || 404 == resp.status)

        triggerTest.setAutoDisable(true);

        // Tag the trigger as a HawkularMetrics:MetricsCondition so it gets picked up for processing
        triggerTest.addTag("HawkularMetrics", "MetricsCondition" );

        resp = client.post(path: "triggers", body:triggerTest)
        assertEquals(200, resp.status)

        // Note: alerterId must be "HawkularMetrics"
        Query qNow = Query.builder("qNow").metric(metricId).type(MetricType.COUNTER_RATE).duration("1mn").build();
        // Note that rates are calculated as per-minute. Our datapoints are only 10s apert, so the deltas are
        // going to be multiplied by 10.
        ConditionExpression ce = new ConditionExpression(qNow, "1mn",
            "q(qNow,avg) + q(qNow,sum) == 90" )
        ExternalCondition firingCond = new ExternalCondition("trigger-test-rate", Mode.FIRING, "external-dataId-rate",
           "HawkularMetrics", ce.toJson());

        Collection<Condition> conditions = new ArrayList<>(1);
        conditions.add( firingCond );
        resp = client.put(path: "triggers/trigger-test-rate/conditions/firing", body: conditions)
        assertEquals(200, resp.status)
        assertEquals(1, resp.data.size())

        // FETCH trigger to validate and get the tenantId
        resp = client.get(path: "triggers/trigger-test-rate");
        assertEquals(200, resp.status)
        assertEquals("trigger-test-rate", resp.data.name)
        assertEquals(false, resp.data.enabled)
        assertEquals(true, resp.data.autoDisable);
        def tenantId = resp.data.tenantId;
        assert( null != tenantId )

        // FETCH recent alerts for trigger, should not be any
        resp = client.get(path: "", query: [startTime:start,triggerIds:"trigger-test-rate"] )
        assertEquals(200, resp.status)
        assertTrue(resp.data.isEmpty())

        // Send in METRICS data before enabling the trigger because the external evaluations start as soon
        // as the enabled Trigger is processed. Note delta of 5 for 10s -> 30 per-minute.
        long now = System.currentTimeMillis();
        DataPoint dp1 = new DataPoint( metricId, 10.0, now - 30000 );  // 30 seconds ago
        DataPoint dp2 = new DataPoint( metricId, 15.0, now - 20000 );  // 20 seconds ago sample1
        DataPoint dp3 = new DataPoint( metricId, 20.0, now - 10000 );  // 10 seconds ago sample2

        sendCounterDataViaRest( tenantId, dp1, dp2, dp3);

        // ENABLE Trigger, this should get picked up by the listener and the expression should be submitted
        // to a runner for processing...
        triggerTest.setEnabled(true);

        resp = client.put(path: "triggers/trigger-test-rate/", body:triggerTest)
        assertEquals(200, resp.status)

        // The alert processing happens async, so give it a little time before failing...
        for ( int i=0; i < 10; ++i ) {
            Thread.sleep(500);

            // FETCH recent alerts for trigger, there should be 1
            resp = client.get(path: "", query: [startTime:start,triggerIds:"trigger-test-rate"] )
            if ( resp.status == 200 && resp.data.size() == 1 ) {
                break;
            }
            assertEquals(200, resp.status)
        }
        assertEquals(200, resp.status)
        assertEquals(1, resp.data.size())

        assertEquals("trigger-test-rate", resp.data[0].trigger.id)
        assertEquals("{q(qNow,avg)=30.0, q(qNow,sum)=60.0}",
             resp.data[0].evalSets[0].iterator().next().event.text)
    }

    @Test
    void t07_forEachTest() {
        // use two unique metricIds for the test run so that we don't conflict with prior test runs
        String metricId1 = "alerts-test-each-1-" + start;
        String metricId2 = "alerts-test-each-2-" + start;

        // CREATE trigger using external metrics expression
        def resp = client.get(path: "")
        assertTrue(resp.status == 200 || resp.status == 204)

        Trigger triggerTest = new Trigger("trigger-test-each", "trigger-test-each");

        // remove if it exists
        resp = client.delete(path: "triggers/trigger-test-each")
        assertTrue(200 == resp.status || 404 == resp.status)

        // Do not use AutoDisable because by design this trigger fires on different metrics!
        triggerTest.setAutoDisable(false);

        // Tag the trigger as a HawkularMetrics:MetricsCondition so it gets picked up for processing
        triggerTest.addTag("HawkularMetrics", "MetricsCondition" );

        resp = client.post(path: "triggers", body:triggerTest)
        assertEquals(200, resp.status)

        // Note: alerterId must be "HawkularMetrics"
        Query qNow = Query.builder("qNow").metrics(metricId1,metricId2).type(MetricType.GAUGE).duration("5mn").build();
        ConditionExpression ce = new ConditionExpression(qNow, "1mn", EvalType.EACH,
            "q(qNow,avg) + q(qNow,sum) == 80 || q(qNow,avg) == 100", 1 )
        ExternalCondition firingCond = new ExternalCondition("trigger-test-each", Mode.FIRING, "external-dataId-each",
           "HawkularMetrics", ce.toJson());

        Collection<Condition> conditions = new ArrayList<>(1);
        conditions.add( firingCond );
        resp = client.put(path: "triggers/trigger-test-each/conditions/firing", body: conditions)
        assertEquals(200, resp.status)
        assertEquals(1, resp.data.size())

        // FETCH trigger to validate and get the tenantId
        resp = client.get(path: "triggers/trigger-test-each");
        assertEquals(200, resp.status)
        assertEquals("trigger-test-each", resp.data.name)
        assertEquals(false, resp.data.enabled)
        assertEquals(false, resp.data.autoDisable);
        def tenantId = resp.data.tenantId;
        assert( null != tenantId )

        // FETCH recent alerts for trigger, should not be any
        resp = client.get(path: "", query: [startTime:start,triggerIds:"trigger-test-each"] )
        assertEquals(200, resp.status)
        assertTrue(resp.data.isEmpty())

        // Send in METRICS data before enabling the trigger because the external evaluations start as soon
        // as the enabled Trigger is processed.
        long now = System.currentTimeMillis();
        DataPoint dp11 = new DataPoint( metricId1, 10.0, now - 30000 );  // 30 seconds ago
        DataPoint dp12 = new DataPoint( metricId1, 20.0, now - 20000 );  // 20 seconds ago
        DataPoint dp13 = new DataPoint( metricId1, 30.0, now - 10000 );  // 10 seconds ago

        DataPoint dp21 = new DataPoint( metricId2, 50.0, now - 30000 );  // 30 seconds ago
        DataPoint dp22 = new DataPoint( metricId2, 150.0, now - 20000 );  // 20 seconds ago

        sendGaugeDataViaRest( tenantId, dp11, dp12, dp13, dp21, dp22 );

        // ENABLE Trigger, this should get picked up by the listener and the expression should be submitted
        // to a runner for processing...
        triggerTest.setEnabled(true);

        resp = client.put(path: "triggers/trigger-test-each/", body:triggerTest)
        assertEquals(200, resp.status)

        // The alert processing happens async, so give it a little time before failing...
        for ( int i=0; i < 10; ++i ) {
            Thread.sleep(500);

            // FETCH recent alerts for trigger, there should be 2
            resp = client.get(path: "", query: [startTime:start,triggerIds:"trigger-test-each"] )
            if ( resp.status == 200 && resp.data.size() == 2 ) {
                break;
            }
            assertEquals(200, resp.status)
        }
        assertEquals(200, resp.status)
        assertEquals(2, resp.data.size())

        assertEquals("trigger-test-each", resp.data[0].trigger.id)
        assertEquals("trigger-test-each", resp.data[1].trigger.id)
        def v0 = resp.data[0].evalSets[0].iterator().next().event.text
        def v1 = resp.data[1].evalSets[0].iterator().next().event.text
        assertTrue(v0, v0.equals("{q(qNow,avg)=20.0, q(qNow,sum)=60.0}")
            || v0.equals("{q(qNow,avg)=100.0, q(qNow,sum)=200.0}"))
        assertTrue(v1, v1.equals("{q(qNow,avg)=20.0, q(qNow,sum)=60.0}")
            || v1.equals("{q(qNow,avg)=100.0, q(qNow,sum)=200.0}"))
        assertFalse(v0 + ":" + v1, v0.equals(v1))
        v0 = resp.data[0].evalSets[0].iterator().next().event.context.get("metricName")
        v1 = resp.data[1].evalSets[0].iterator().next().event.context.get("metricName")
        assertTrue(v0, v0.equals(metricId1)
            || v0.equals(metricId2))
        assertTrue(v1, v1.equals(metricId1)
            || v1.equals(metricId2))
        assertFalse(v0 + ":" + v1, v0.equals(v1))

        // Wait another 75s (I know this is painful from the test perspective, but min eval period is 60s)
        // and make sure no more alerts fire. This tests the quietCount = 1.
        for ( int i=0; i < 75; ++i ) {
            Thread.sleep(1000);
            // println ("sleep=" + i)

            // FETCH recent alerts for trigger, there should be 2
            resp = client.get(path: "", query: [startTime:start,triggerIds:"trigger-test-each"] )
            if ( resp.status == 200 && resp.data.size() > 2 ) {
                break;
            }
            assertEquals(200, resp.status)
        }
        assertEquals(200, resp.status)
        assertEquals(2, resp.data.size())
    }

    @Test
    void t09_cleanupTriggers() {
        // stop the external alerter from processing these test triggers...
        def resp = client.delete(path: "triggers/trigger-test-avg")
        resp = client.delete(path: "triggers/trigger-test-tag")
        resp = client.delete(path: "triggers/trigger-test-avgd")
        resp = client.delete(path: "triggers/trigger-test-funcs")
        resp = client.delete(path: "triggers/trigger-test-percentiles")
        resp = client.delete(path: "triggers/trigger-test-heartbeat")
        resp = client.delete(path: "triggers/trigger-test-rate")
        resp = client.delete(path: "triggers/trigger-test-each")
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
