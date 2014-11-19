package org.rhq.metrics.restServlet.influx.query.parse.definition;

/**
 * @author Thomas Segismont
 */
public class MomentOperand implements Operand {
    private final String functionName;
    private final int timeshift;
    private final InfluxTimeUnit timeshiftUnit;

    public MomentOperand(String functionName, int timeshift, InfluxTimeUnit timeshiftUnit) {
        this.functionName = functionName;
        this.timeshift = timeshift;
        this.timeshiftUnit = timeshiftUnit;
    }

    public String getFunctionName() {
        return functionName;
    }

    public int getTimeshift() {
        return timeshift;
    }

    public InfluxTimeUnit getTimeshiftUnit() {
        return timeshiftUnit;
    }
}
