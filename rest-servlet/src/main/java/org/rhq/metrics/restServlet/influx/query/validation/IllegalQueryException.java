package org.rhq.metrics.restServlet.influx.query.validation;

/**
 * @author Thomas Segismont
 */
public class IllegalQueryException extends Exception {
    private static final long serialVersionUID = 1L;

    public IllegalQueryException(String message) {
        super(message);
    }

}
