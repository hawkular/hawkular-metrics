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
package org.rhq.metrics.core;

import static org.rhq.metrics.core.Interval.Units.DAYS;
import static org.rhq.metrics.core.Interval.Units.HOURS;
import static org.rhq.metrics.core.Interval.Units.MINUTES;
import static org.rhq.metrics.core.Interval.parse;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import org.testng.annotations.Test;

/**
 * @author John Sanda
 */
public class IntervalTest {

    @Test
    public void parseIntervals() {
        assertEquals(parse("15min"), new Interval(15, MINUTES));
        assertEquals(parse("1hr"), new Interval(1, HOURS));
        assertEquals(parse("1d"), new Interval(1, DAYS));

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
