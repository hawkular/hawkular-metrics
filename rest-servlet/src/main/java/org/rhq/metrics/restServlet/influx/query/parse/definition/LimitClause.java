package org.rhq.metrics.restServlet.influx.query.parse.definition;

/**
 * @author Thomas Segismont
 */
public class LimitClause {
    private final int limit;

    public LimitClause(int limit) {
        this.limit = limit;
    }

    public int getLimit() {
        return limit;
    }
}
