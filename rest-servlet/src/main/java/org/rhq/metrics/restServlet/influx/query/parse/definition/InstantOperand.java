package org.rhq.metrics.restServlet.influx.query.parse.definition;

import org.joda.time.Instant;

/**
 * @author Thomas Segismont
 */
public interface InstantOperand extends Operand {
    Instant getInstant();
}
