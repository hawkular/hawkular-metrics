package org.rhq.metrics.restServlet.influx.query.translate;

import static org.rhq.metrics.restServlet.influx.query.parse.definition.OperandUtils.isTimeOperand;

import javax.enterprise.context.ApplicationScoped;

import org.joda.time.Instant;
import org.joda.time.Interval;

import org.rhq.metrics.restServlet.influx.query.parse.definition.AndBooleanExpression;
import org.rhq.metrics.restServlet.influx.query.parse.definition.BooleanExpression;
import org.rhq.metrics.restServlet.influx.query.parse.definition.GtBooleanExpression;
import org.rhq.metrics.restServlet.influx.query.parse.definition.InstantOperand;
import org.rhq.metrics.restServlet.influx.query.parse.definition.LtBooleanExpression;

/**
 * @author Thomas Segismont
 */
@ApplicationScoped
public class ToIntervalTranslator {

    /**
     * Return the interval described by the where clause.
     *
     * @param whereClause the Influx query where clause
     * @return an {@link org.joda.time.Interval} or null if the where clause does define a proper interval (i.e.
     * <code>time &gt; '2012-08-15' and time &lt; '1998-08-15'</code>
     */
    public Interval toInterval(BooleanExpression whereClause) {
        if (whereClause instanceof GtBooleanExpression) {
            return getIntervalFromGtExpression((GtBooleanExpression) whereClause);
        } else if (whereClause instanceof LtBooleanExpression) {
            return getIntervalFromLtExpression((LtBooleanExpression) whereClause);
        }
        AndBooleanExpression and = (AndBooleanExpression) whereClause;
        Interval left;
        Interval right;
        if (and.getLeftExpression() instanceof GtBooleanExpression) {
            left = getIntervalFromGtExpression((GtBooleanExpression) and.getLeftExpression());
        } else {
            left = getIntervalFromLtExpression((LtBooleanExpression) and.getLeftExpression());
        }
        if (and.getRightExpression() instanceof GtBooleanExpression) {
            right = getIntervalFromGtExpression((GtBooleanExpression) and.getRightExpression());
        } else {
            right = getIntervalFromLtExpression((LtBooleanExpression) and.getRightExpression());
        }
        return left.overlap(right);
    }

    private Interval getIntervalFromLtExpression(LtBooleanExpression whereClause) {
        LtBooleanExpression lt = (LtBooleanExpression) whereClause;
        if (isTimeOperand(lt.getLeftOperand())) {
            // time < x
            InstantOperand instantOperand = (InstantOperand) lt.getRightOperand();
            return new Interval(new Instant(0), instantOperand.getInstant());
        } else {
            // x < time
            InstantOperand instantOperand = (InstantOperand) lt.getLeftOperand();
            return new Interval(instantOperand.getInstant(), Instant.now());
        }
    }

    private Interval getIntervalFromGtExpression(GtBooleanExpression gt) {
        if (isTimeOperand(gt.getLeftOperand())) {
            // time > x
            InstantOperand instantOperand = (InstantOperand) gt.getRightOperand();
            return new Interval(instantOperand.getInstant(), Instant.now());
        } else {
            // x > time
            InstantOperand instantOperand = (InstantOperand) gt.getLeftOperand();
            return new Interval(new Instant(0), instantOperand.getInstant());
        }
    }

}
