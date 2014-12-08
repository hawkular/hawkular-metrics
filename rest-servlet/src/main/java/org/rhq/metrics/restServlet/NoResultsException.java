package org.rhq.metrics.restServlet;

/**
 * @author John Sanda
 */
public class NoResultsException extends RuntimeException {

    public NoResultsException() {
        super();
    }

    public NoResultsException(String message) {
        super(message);
    }
}
