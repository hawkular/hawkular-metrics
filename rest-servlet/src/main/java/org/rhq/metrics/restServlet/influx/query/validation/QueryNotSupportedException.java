package org.rhq.metrics.restServlet.influx.query.validation;

/**
 * @author Thomas Segismont
 */
public class QueryNotSupportedException extends IllegalQueryException {
    private static final long serialVersionUID = 1L;

    public QueryNotSupportedException(String message) {
        super(message);
    }
}
