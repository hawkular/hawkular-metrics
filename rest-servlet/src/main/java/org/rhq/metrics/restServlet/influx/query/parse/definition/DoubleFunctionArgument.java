package org.rhq.metrics.restServlet.influx.query.parse.definition;

/**
 * @author Thomas Segismont
 */
public class DoubleFunctionArgument implements NumberFunctionArgument {
    private final double value;

    public DoubleFunctionArgument(double value) {
        this.value = value;
    }

    public double getValue() {
        return value;
    }

    @Override
    public double getDoubleValue() {
        return value;
    }
}
