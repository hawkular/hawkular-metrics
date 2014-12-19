/*
 * Copyright 2014 Red Hat, Inc.
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

package org.rhq.metrics.core;

import static org.rhq.metrics.core.Interval.Units.DAYS;
import static org.rhq.metrics.core.Interval.Units.HOURS;
import static org.rhq.metrics.core.Interval.Units.MINUTES;
import static org.rhq.metrics.core.Interval.parse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;


/**
 * @author John Sanda
 */
public class IntervalTest {

    @Test
    public void parseIntervals() {
        assertEquals(new Interval(15, MINUTES), parse("15min"));
        assertEquals(new Interval(1, HOURS), parse("1hr"));
        assertEquals(new Interval(1, DAYS), parse("1d"));

        assertExceptionThrown("15minutes");
        assertExceptionThrown("15 minutes");
        assertExceptionThrown("min15");
        assertExceptionThrown("12.2hr");
        assertExceptionThrown("1d3min");
        assertExceptionThrown("1d 3min");
    }

    private void assertExceptionThrown(String interval) {
        IllegalArgumentException exception = null;
        try {
            parse(interval);
        } catch (IllegalArgumentException e) {
            exception = e;
        }
        assertNotNull(exception);
    }

}
