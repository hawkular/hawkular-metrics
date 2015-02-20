/*
 * Copyright 2014-2015 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.api.jaxrs.influx.query.translate;

import static org.joda.time.DateTimeZone.UTC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.AndBooleanExpression;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.DateOperand;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.GtBooleanExpression;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.LtBooleanExpression;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.NameOperand;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.joda.time.Interval;
import org.junit.Test;

/**
 * @author Thomas Segismont
 */
public class ToIntervalTranslatorTest {

    private final ToIntervalTranslator translator = new ToIntervalTranslator();

    @Test
    public void testToInterval() throws Exception {

        NameOperand timeOperand = new NameOperand(null, "time");
        DateTime sylvesterNoon2008 = new DateTime(2008, 12, 31, 12, 0, UTC);
        DateTime mariaNoon2006 = new DateTime(2006, 8, 15, 12, 0, UTC);

        GtBooleanExpression gt = new GtBooleanExpression(new DateOperand(sylvesterNoon2008.toInstant()), timeOperand);

        assertEquals(new Interval(new Instant(0), sylvesterNoon2008.toInstant()), translator.toInterval(gt));

        LtBooleanExpression lt = new LtBooleanExpression(new DateOperand(mariaNoon2006.toInstant()), timeOperand);

        Instant nowBefore = Instant.now()/* minus 1 in case the test runs too fast */.minus(1);
        Interval interval = translator.toInterval(lt);
        Instant nowAfter = Instant.now();

        assertNotNull(interval);
        assertEquals(mariaNoon2006, interval.getStart());
        assertTrue(interval.contains(nowBefore));
        assertTrue(new Interval(mariaNoon2006, nowAfter).contains(interval));

        AndBooleanExpression and = new AndBooleanExpression(gt, lt);

        assertEquals(new Interval(mariaNoon2006, sylvesterNoon2008), translator.toInterval(and));

        gt = new GtBooleanExpression(timeOperand, new DateOperand(sylvesterNoon2008.toInstant()));
        lt = new LtBooleanExpression(timeOperand, new DateOperand(mariaNoon2006.toInstant()));
        and = new AndBooleanExpression(gt, lt);

        assertNull(translator.toInterval(and));
    }
}
