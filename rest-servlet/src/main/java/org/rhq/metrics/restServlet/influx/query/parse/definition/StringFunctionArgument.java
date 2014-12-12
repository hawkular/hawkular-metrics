package org.rhq.metrics.restServlet.influx.query.parse.definition;

/**
 * @author Thomas Segismont
 */
public class StringFunctionArgument implements FunctionArgument {
    private final String value;

    public StringFunctionArgument(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
