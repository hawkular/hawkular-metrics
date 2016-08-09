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

package org.hawkular.metrics.model.param;

import static org.hawkular.metrics.model.param.TimeRange.EIGHT_HOURS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * @author Thomas Segismont
 */
public class TimeRangeTest {

    @Test
    public void testDefaults() {
        TimeRange timeRange = new TimeRange("", "");
        assertTrue(timeRange.getProblem(), timeRange.isValid());
        assertEquals(EIGHT_HOURS, timeRange.getEnd() - timeRange.getStart());
        // Unless run on a damn slow system, this should pass
        assertTrue("End should default to now", Math.abs(System.currentTimeMillis() - timeRange.getEnd()) < 2);
    }

    @Test
    public void testLowerBoundDefault() {
        TimeRange timeRange = new TimeRange(null, "888888888888888888");
        assertTrue(timeRange.getProblem(), timeRange.isValid());
        assertEquals(888888888888888888L, timeRange.getEnd());
        long expectedStart = System.currentTimeMillis() - EIGHT_HOURS;
        // Unless run on a damn slow system, this should pass
        assertTrue("Start should default to now-8h", Math.abs(expectedStart - timeRange.getStart()) < 2);
    }

    @Test
    public void testUpperBoundDefault() {
        TimeRange timeRange = new TimeRange("123456789", null);
        assertTrue(timeRange.getProblem(), timeRange.isValid());
        assertEquals(123456789L, timeRange.getStart());
        // Unless run on a damn slow system, this should pass
        assertTrue("End should default to now", Math.abs(System.currentTimeMillis() - timeRange.getEnd()) < 2);
    }

    @Test
    public void testRelativeTimeStamps() {
        String delta = "10000ms";
        long deltaMS = 10000L;
        TimeRange timeRange = new TimeRange("-" + delta, "");
        assertTrue(timeRange.getProblem(), timeRange.isValid());
        assertEqualsWithin(System.currentTimeMillis() - deltaMS, timeRange.getStart(), 2);
        assertEqualsWithin(System.currentTimeMillis(), timeRange.getEnd(), 2);

        long endTime = System.currentTimeMillis() + (deltaMS * 2);
        timeRange = new TimeRange("+" + delta, Long.toString(endTime));
        assertTrue(timeRange.getProblem(), timeRange.isValid());
        assertEqualsWithin(System.currentTimeMillis() + deltaMS, timeRange.getStart(), 2);
        assertEquals(endTime, timeRange.getEnd());

        delta = "50s";
        deltaMS = 50000L;
        timeRange = new TimeRange("", "- " + delta);
        assertTrue(timeRange.getProblem(), timeRange.isValid());
        assertEqualsWithin(System.currentTimeMillis() - deltaMS, timeRange.getEnd(), 2);
        assertEqualsWithin(System.currentTimeMillis() - EIGHT_HOURS, timeRange.getStart(), 2);

        timeRange = new TimeRange("", "+" + delta);
        assertTrue(timeRange.getProblem(), timeRange.isValid());
        assertEqualsWithin(System.currentTimeMillis() + deltaMS, timeRange.getEnd(), 2);
        assertEqualsWithin(System.currentTimeMillis() - EIGHT_HOURS, timeRange.getStart(), 2);

        delta = "1mn";
        deltaMS = 60000L;
        timeRange = new TimeRange ("- " + delta, "+ " + delta);
        assertTrue(timeRange.getProblem(), timeRange.isValid());
        assertEqualsWithin(System.currentTimeMillis() - deltaMS, timeRange.getStart(), 2);
        assertEqualsWithin(System.currentTimeMillis() + deltaMS, timeRange.getEnd(), 2);
    }

    @Test
    public void testInvalidTimeStamps() {
        TimeRange timeRange = new TimeRange("foo", "bar");
        assertFalse(timeRange.isValid());

        timeRange = new TimeRange("", "bar");
        assertFalse(timeRange.isValid());

        timeRange = new TimeRange("foo", "");
        assertFalse(timeRange.isValid());

        timeRange = new TimeRange("+foo", "");
        assertFalse(timeRange.isValid());

        timeRange = new TimeRange("-foo", "");
        assertFalse(timeRange.isValid());

        timeRange = new TimeRange("", "+bar");
        assertFalse(timeRange.isValid());

        timeRange = new TimeRange("", "-bar");
        assertFalse(timeRange.isValid());
    }

    // Used to assert if two numbers are within a certain range of each other
    public void assertEqualsWithin(Long expected, Long actual, int within) {
        assertTrue("Expected " + expected + " but received " + actual, Math.abs(expected - actual) < within);
    }

    @Test
    public void testEmptyRange() {
        assertFalse(new TimeRange("7", "7").isValid());
    }

    @Test
    public void testInvalidRange() {
        assertFalse(new TimeRange("7", "5").isValid());
    }
}