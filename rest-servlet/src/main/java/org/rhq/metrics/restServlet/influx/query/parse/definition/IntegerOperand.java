package org.rhq.metrics.restServlet.influx.query.parse.definition;

/**
 * @author Thomas Segismont
 */
public class IntegerOperand implements Operand {
    private final int value;

    public IntegerOperand(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
