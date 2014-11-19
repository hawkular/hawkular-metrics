package org.rhq.metrics.restServlet.influx.query.parse.definition;

/**
 * @author Thomas Segismont
 */
public class DoubleOperand implements Operand {
    private final double value;

    public DoubleOperand(double value) {
        this.value = value;
    }

    public double getValue() {
        return value;
    }
}
