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
package org.hawkular.metrics.alerter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.hawkular.metrics.alerter.Expression.Func;
import org.hawkular.metrics.alerter.Expression.Op;
import org.hawkular.metrics.alerter.Expression.Target;
import org.junit.Test;

public class ExpressionTest {

    @Test
    public void expressionParseTest() {
        Expression x = new Expression("metric:5:avg(foo > 100.5),5");
        assertEquals(x.getTarget(), Target.Metric);
        assertEquals(x.getFunc(), Func.avg);
        assertEquals(x.getInterval().intValue(), 5);
        assertEquals(x.getPeriod().intValue(), 5);
        assertEquals(x.getOp(), Op.GT);
        assertTrue(x.getThreshold() == 100.5);
        assertEquals(x.getMetric(), "foo");

        x = new Expression("metric:5m:avgd(bar <= 100),10");
        assertEquals(x.getTarget(), Target.Metric);
        assertEquals(x.getFunc(), Func.avgd);
        assertEquals(x.getInterval().intValue(), 5);
        assertEquals(x.getPeriod().intValue(), 10);
        assertEquals(x.getOp(), Op.LTE);
        assertTrue(x.getThreshold() == 100.0);
        assertEquals(x.getMetric(), "bar");

        x = new Expression("metric:15:avgw(foo-bar >= 40), 20m");
        assertEquals(x.getTarget(), Target.Metric);
        assertEquals(x.getFunc(), Func.avgw);
        assertEquals(x.getInterval().intValue(), 15);
        assertEquals(x.getPeriod().intValue(), 20);
        assertEquals(x.getOp(), Op.GTE);
        assertTrue(x.getThreshold() == 40.0);
        assertEquals(x.getMetric(), "foo-bar");

        x = new Expression("metric:15:range(foo-bar >= 2), 20m");
        assertEquals(x.getTarget(), Target.Metric);
        assertEquals(x.getFunc(), Func.range);
        assertEquals(x.getInterval().intValue(), 15);
        assertEquals(x.getPeriod().intValue(), 20);
        assertEquals(x.getOp(), Op.GTE);
        assertTrue(x.getThreshold() == 2.0);
        assertEquals(x.getMetric(), "foo-bar");

        x = new Expression("metric:5m:rangep(bar <= 1.5),10");
        assertEquals(x.getTarget(), Target.Metric);
        assertEquals(x.getFunc(), Func.rangep);
        assertEquals(x.getInterval().intValue(), 5);
        assertEquals(x.getPeriod().intValue(), 10);
        assertEquals(x.getOp(), Op.LTE);
        assertTrue(x.getThreshold() == 1.5);
        assertEquals(x.getMetric(), "bar");

        x = new Expression("metric:5m:min(bar < 50),10");
        assertEquals(x.getTarget(), Target.Metric);
        assertEquals(x.getFunc(), Func.min);
        assertEquals(x.getInterval().intValue(), 5);
        assertEquals(x.getPeriod().intValue(), 10);
        assertEquals(x.getOp(), Op.LT);
        assertTrue(x.getThreshold() == 50.0);
        assertEquals(x.getMetric(), "bar");

        x = new Expression("metric:5m:max(bar > 50),10");
        assertEquals(x.getTarget(), Target.Metric);
        assertEquals(x.getFunc(), Func.max);
        assertEquals(x.getInterval().intValue(), 5);
        assertEquals(x.getPeriod().intValue(), 10);
        assertEquals(x.getOp(), Op.GT);
        assertTrue(x.getThreshold() == 50.0);
        assertEquals(x.getMetric(), "bar");

        x = new Expression("metric:1m:heartbeat(bar <= 0),1");
        assertEquals(x.getTarget(), Target.Metric);
        assertEquals(x.getFunc(), Func.heartbeat);
        assertEquals(x.getInterval().intValue(), 1);
        assertEquals(x.getPeriod().intValue(), 1);
        assertEquals(x.getOp(), Op.LTE);
        assertTrue(x.getThreshold() == 0.0);
        assertEquals(x.getMetric(), "bar");

        try {
            new Expression("metric:30:avg(foo > 40)");
        } catch (IllegalArgumentException e) {
            // expected due to missingtime period
        }
        try {
            new Expression("metric:30s:avg(foo > 40), 10m");
        } catch (IllegalArgumentException e) {
            // expected due to 's' unit not supported
        }
        try {
            new Expression("metric:30:up(x > 40)");
        } catch (IllegalArgumentException e) {
            // invalid target for func
        }
        try {
            new Expression("tag:30:up(x > 40),10");
        } catch (IllegalArgumentException e) {
            // invalid period for func
        }

    }
}
