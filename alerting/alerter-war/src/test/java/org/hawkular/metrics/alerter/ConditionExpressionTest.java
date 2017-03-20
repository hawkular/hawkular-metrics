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
package org.hawkular.metrics.alerter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hawkular.alerts.api.json.JsonUtil;
import org.hawkular.alerts.api.model.condition.ExternalCondition;
import org.hawkular.alerts.api.model.trigger.Mode;
import org.hawkular.metrics.alerter.ConditionEvaluator.QueryFunc;
import org.hawkular.metrics.alerter.ConditionExpression.EvalType;
import org.hawkular.metrics.alerter.ConditionExpression.Query;
import org.hawkular.metrics.model.BucketPoint;
import org.hawkular.metrics.model.NumericBucketPoint;
import org.hawkular.metrics.model.NumericBucketPoint.Builder;
import org.hawkular.metrics.model.Percentile;
import org.junit.Test;

public class ConditionExpressionTest {

    @Test
    public void metricsAndTagsTest() {

        try {
            new Query("query-2", Collections.singleton("metric-1"), "name = 'value'",
                    Collections.singleton("0.90"), "10mn", "15mn");
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // expected
        } catch (Exception e) {
            fail("Expected IllegalArgumentException, got " + e);
        }
    }

    @Test
    public void json1Test() {
        Query q1 = new Query("query-1", Collections.singleton("metric-1"), null,
                Collections.singleton("0.90"), "10mn", "15mn");

        ConditionExpression e = new ConditionExpression(q1, "5mn", EvalType.ALL, "q(query-1,avg) > 50");

        String mc1Json = e.toJson();
        ConditionExpression eObject = ConditionExpression.toObject(mc1Json);
        assertEquals(e, eObject);
    }

    @Test
    public void json2Test() {
        Set<String> metrics = new HashSet<>(Arrays.asList("metrics-1", "metrics-2"));
        Set<String> percentiles = new HashSet<>(Arrays.asList("0.75", "0.90"));
        List<Query> queries = new ArrayList<>();
        queries.add(new Query("query-1", metrics, null, percentiles, "10mn", "15mn"));
        queries.add(new Query("query-2", null, "name = 'value'", percentiles, "10mn", "15mn"));

        ConditionExpression mc1 = new ConditionExpression(queries, "5mn", EvalType.EACH,
                "q(query-1,avg) > q(query-2,avg)", 5);

        String mc1Json = mc1.toJson();
        ConditionExpression mc1Object = ConditionExpression.toObject(mc1Json);
        assertEquals(mc1, mc1Object);
    }

    @Test
    public void externalConditionTest() {
        Query q1 = new Query("query-1", Collections.singleton("metric-1"), null,
                Collections.singleton("0.90"), "10mn", "15mn");

        ConditionExpression mc1 = new ConditionExpression(Collections.singletonList(q1), "5mn",
                EvalType.ALL, "q(query-1,avg) > 50");

        String mc1Json = mc1.toJson();

        ExternalCondition ec = new ExternalCondition("trigger-id", Mode.FIRING, "data-id", "alerter-id", mc1Json);

        String ecJson = JsonUtil.toJson(ec);

        ExternalCondition ecObject = JsonUtil.fromJson(ecJson, ExternalCondition.class);

        assertEquals(ec, ecObject);
    }

    @Test
    public void evalParseTest() {
        ConditionEvaluator evaluator;
        String eval = "10 * 2 > 30";
        evaluator = new ConditionEvaluator(eval);
        assertEquals(0, evaluator.getQueryVars().size());
        assertEquals(eval, evaluator.getExpression().getExpression());
        assertEquals(BigDecimal.ZERO, evaluator.getExpression().eval());

        eval = "( 1.5 * q(query1, avg) ) > ( 2 * q(query2, median) )";
        evaluator = new ConditionEvaluator(eval);
        assertEquals(2, evaluator.getQueryVars().size());
        assertTrue(evaluator.getQueryVars().keySet().toString(), evaluator.getQueryVars().keySet().contains("q0"));
        assertTrue(evaluator.getQueryVars().keySet().toString(), evaluator.getQueryVars().keySet().contains("q1"));
        assertEquals(new QueryFunc("query1", "avg"), evaluator.getQueryVars().get("q0"));
        assertEquals(new QueryFunc("query2", "median"), evaluator.getQueryVars().get("q1"));
        assertEquals(BigDecimal.ZERO, evaluator.getExpression().with("q0", "10").and("q1", "10").eval());
        assertEquals(BigDecimal.ONE, evaluator.getExpression().with("q0", "20").and("q1", "10").eval());

        // invalid expressions
        try {
            eval = "1.5 10";
            evaluator = new ConditionEvaluator(eval);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            eval = "1.5 * q(query1) > 2 * q(query2, median)";
            evaluator = new ConditionEvaluator(eval);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void evalTest() {
        ConditionEvaluator evaluator;

        String eval = "( q(query1, %90) + q(query1, avg) ) > ( 2 * q(query2, median) )";
        evaluator = new ConditionEvaluator(eval);
        assertEquals(3, evaluator.getQueryVars().size());
        assertTrue(evaluator.getQueryVars().keySet().toString(), evaluator.getQueryVars().keySet().contains("q0"));
        assertTrue(evaluator.getQueryVars().keySet().toString(), evaluator.getQueryVars().keySet().contains("q1"));
        assertTrue(evaluator.getQueryVars().keySet().toString(), evaluator.getQueryVars().keySet().contains("q2"));
        assertEquals(new QueryFunc("query1", "%90"), evaluator.getQueryVars().get("q0"));
        assertEquals(new QueryFunc("query1", "avg"), evaluator.getQueryVars().get("q1"));
        assertEquals(new QueryFunc("query2", "median"), evaluator.getQueryVars().get("q2"));
        NumericBucketPoint query1BucketPoint = new Builder(0, 1).setSamples(5).setAvg(10.0)
                .setPercentiles(Collections.singletonList(new Percentile("90", 10))).build();
        NumericBucketPoint query2BucketPoint = new Builder(0, 1).setSamples(5).setMedian(10.0).build();
        Map<String, BucketPoint> queryResults = new HashMap<>();
        queryResults.put("query1", query1BucketPoint);
        queryResults.put("query2", query2BucketPoint);
        assertFalse(evaluator.prepareAndEvaluate(queryResults));
        query2BucketPoint = new Builder(0, 1).setSamples(5).setMedian(5.0).build();
        queryResults.put("query2", query2BucketPoint);
        assertTrue(evaluator.prepareAndEvaluate(queryResults));
    }

    @Test
    public void queryConstructorTest() {
        Query q1 = new Query("qNow", Collections.singleton("metric-1"), null,
                Collections.singleton("0.90"), "10mn", null);
        Query q2 = new Query("qNow", "1d", q1);

        try {
            new ConditionExpression(Arrays.asList(q1, q2), "5mn", EvalType.ALL,
                    "q(qNow,avg) > q(qYesterday,avg)");
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // expected
        } catch (Exception e) {
            fail("Expected IllegalArgumentException");
        }

        q2 = new Query("qYesterday", "1d", q1);
        ConditionExpression mc1 = new ConditionExpression(Arrays.asList(q1, q2), "5mn", EvalType.ALL,
                "q(qNow,avg) > q(qYesterday,avg)");
        assertFalse(q1.getName().equals(q2.getName()));
        assertFalse(q1.getOffset() == null && q1.getOffset() != null);
        assertTrue(q1.getDuration().equals(q2.getDuration()));
        assertTrue(q1.getMetrics().equals(q2.getMetrics()));
        assertTrue(q1.getPercentiles().equals(q2.getPercentiles()));
        assertTrue(q1.getTags() == null && q2.getTags() == null);

    }

}
