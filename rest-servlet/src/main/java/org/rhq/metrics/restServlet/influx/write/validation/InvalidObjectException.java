package org.rhq.metrics.restServlet.influx.write.validation;

/**
 * @author Thomas Segismont
 */
public class InvalidObjectException extends Exception {
    private static final long serialVersionUID = 1;

    public InvalidObjectException(String message) {
        super(message);
    }
}
