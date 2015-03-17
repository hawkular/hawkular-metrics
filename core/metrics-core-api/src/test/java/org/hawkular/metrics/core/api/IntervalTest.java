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
package org.hawkular.metrics.core.api;

import static org.hawkular.metrics.core.api.Interval.Units.DAYS;
import static org.hawkular.metrics.core.api.Interval.Units.HOURS;
import static org.hawkular.metrics.core.api.Interval.Units.MINUTES;
import static org.hawkular.metrics.core.api.Interval.parse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

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
