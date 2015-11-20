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

package org.hawkular.metrics.api.jaxrs.param;

import static org.hawkular.metrics.core.api.param.TimeRange.EIGHT_HOURS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.hawkular.metrics.core.api.param.TimeRange;
import org.junit.Test;

/**
 * @author Thomas Segismont
 */
public class TimeRangeTest {

    @Test
    public void testDefaults() {
        TimeRange timeRange = new TimeRange(null, null);
        assertTrue(timeRange.getProblem(), timeRange.isValid());
        assertEquals(EIGHT_HOURS, timeRange.getEnd() - timeRange.getStart());
        // Unless run on a damn slow system, this should pass
        assertTrue("End should default to now", Math.abs(System.currentTimeMillis() - timeRange.getEnd()) < 2);
    }

    @Test
    public void testLowerBoundDefault() {
        TimeRange timeRange = new TimeRange(null, 888888888888888888L);
        assertTrue(timeRange.getProblem(), timeRange.isValid());
        assertEquals(888888888888888888L, timeRange.getEnd());
        long expectedStart = System.currentTimeMillis() - EIGHT_HOURS;
        // Unless run on a damn slow system, this should pass
        assertTrue("Start should default to now-8h", Math.abs(expectedStart - timeRange.getStart()) < 2);
    }

    @Test
    public void testUpperBoundDefault() {
        TimeRange timeRange = new TimeRange(123456789L, null);
        assertTrue(timeRange.getProblem(), timeRange.isValid());
        assertEquals(123456789L, timeRange.getStart());
        // Unless run on a damn slow system, this should pass
        assertTrue("End should default to now", Math.abs(System.currentTimeMillis() - timeRange.getEnd()) < 2);
    }

    @Test
    public void testEmptyRange() {
        assertFalse(new TimeRange(7L, 7L).isValid());
    }

    @Test
    public void testInvalidRange() {
        assertFalse(new TimeRange(7L, 5L).isValid());
    }
}