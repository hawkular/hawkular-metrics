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
package org.hawkular.metrics.core.impl;

import static org.junit.Assert.assertEquals;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.junit.Test;

/**
 * Test the time slice generation
 *
 * @author Michael Burman
 */
public class DateTimeServiceTest {

    private DateTimeService service = new DateTimeService();

    @Test
    public void testDefaultDpart() throws Exception {
        // Default slice is 1 day
        DateTime now = DateTime.now();
        long startTime = now.getMillis();
        long currentDpart = service.getCurrentDpart(startTime, DateTimeService.DEFAULT_SLICE);
        long endTime = now.plusDays(3).getMillis(); // Advance by 3 days

        long[] dparts = service.getDparts(startTime, endTime, DateTimeService.DEFAULT_SLICE);
        assertEquals(4, dparts.length); // StartDay 3 days
        assertEquals("Matching dpart did not equal the first insert dpart", currentDpart, dparts[0]);
    }

    @Test
    public void testCustomDpart() throws Exception {
        DateTime now = DateTime.now();

        Duration slice = Duration.standardHours(3); // Bucket per each 3 hours

        long startTime = now.getMillis();
        long currentDpart = service.getCurrentDpart(startTime, slice);
        long endTime = now.plusHours(3).getMillis(); // Advance by 3 hours

        long[] dparts = service.getDparts(startTime, endTime, slice);
        assertEquals(2, dparts.length); // StartTime one 3 hour slot
        assertEquals("Matching dpart did not equal the first insert dpart", currentDpart, dparts[0]);
    }

    @Test
    public void testAddDataToThePastTimes() throws Exception {
        // Ensure this works elsewhere also
        DateTimeZone aDefault = DateTimeZone.getDefault();
        DateTimeZone zone = DateTimeZone.forID("Europe/Helsinki");
        DateTimeZone.setDefault(zone);

        long startTime = 1445418388588L;
        long endTime = 1445940268588L;

        Duration slice = Duration.standardDays(1L);

        long currentDpart = service.getCurrentDpart(startTime, slice);
        assertEquals(1445374800000L, currentDpart);

        long[] dparts = service.getDparts(startTime, endTime, slice);
        assertEquals(1445374800000L, dparts[0]);
        assertEquals(1445896800000L, dparts[dparts.length-1]);
        assertEquals(8, dparts.length);

        DateTimeZone.setDefault(aDefault);
    }
}
