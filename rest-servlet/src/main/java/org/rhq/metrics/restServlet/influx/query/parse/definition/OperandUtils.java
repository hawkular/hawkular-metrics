package org.rhq.metrics.restServlet.influx.query.parse.definition;

/**
 * @author Thomas Segismont
 */
public class OperandUtils {

    public static boolean isTimeOperand(Operand operand) {
        return operand instanceof NameOperand && ((NameOperand) operand).getName().equals("time");
    }

    public static boolean isInstantOperand(Operand operand) {
        return operand instanceof InstantOperand;
    }

    private OperandUtils() {
        // Utility class
    }
}
