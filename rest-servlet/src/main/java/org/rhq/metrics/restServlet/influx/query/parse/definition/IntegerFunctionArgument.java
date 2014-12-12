package org.rhq.metrics.restServlet.influx.query.parse.definition;

/**
 * @author Thomas Segismont
 */
public class IntegerFunctionArgument implements NumberFunctionArgument {
    private final int value;

    public IntegerFunctionArgument(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    @Override
    public double getDoubleValue() {
        return value;
    }
}
