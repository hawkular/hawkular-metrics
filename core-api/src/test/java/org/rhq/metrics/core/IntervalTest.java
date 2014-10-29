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
