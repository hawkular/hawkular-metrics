package org.rhq.metrics.restServlet.influx.query.parse;

/**
 * @author Thomas Segismont
 */
public class QueryParseException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public QueryParseException() {
    }

    public QueryParseException(String message) {
        super(message);
    }

    public QueryParseException(String message, Throwable cause) {
        super(message, cause);
    }

    public QueryParseException(Throwable cause) {
        super(cause);
    }

    protected QueryParseException(String message, Throwable cause, boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
