package org.rhq.metrics.restServlet.influx.query.parse.definition;

import org.joda.time.Instant;

/**
 * @author Thomas Segismont
 */
public class DateOperand implements InstantOperand {
    private final Instant instant;

    public DateOperand(Instant instant) {
        this.instant = instant;
    }

    public Instant getInstant() {
        return instant;
    }
}
