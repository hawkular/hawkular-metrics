package org.rhq.metrics.restServlet.influx.query.validation;

import org.rhq.metrics.restServlet.influx.query.parse.definition.AndBooleanExpression;
import org.rhq.metrics.restServlet.influx.query.parse.definition.BooleanExpression;
import org.rhq.metrics.restServlet.influx.query.parse.definition.DateOperand;
import org.rhq.metrics.restServlet.influx.query.parse.definition.GtBooleanExpression;
import org.rhq.metrics.restServlet.influx.query.parse.definition.LtBooleanExpression;
import org.rhq.metrics.restServlet.influx.query.parse.definition.MomentOperand;
import org.rhq.metrics.restServlet.influx.query.parse.definition.NameOperand;
import org.rhq.metrics.restServlet.influx.query.parse.definition.Operand;
import org.rhq.metrics.restServlet.influx.query.parse.definition.SelectQueryDefinitions;

/**
 * @author Thomas Segismont
 */
public class SimpleTimeRangesOnlyRule implements SelectQueryValidationRule {
    @Override
    public void checkQuery(SelectQueryDefinitions queryDefinitions) throws IllegalQueryException {
        BooleanExpression whereClause = queryDefinitions.getWhereClause();
        if (whereClause == null) {
            return;
        }
        if (whereClause instanceof LtBooleanExpression) {
            LtBooleanExpression lt = (LtBooleanExpression) whereClause;
            checkOneTimeOperand(lt.getLeftOperand(), lt.getRightOperand());
            checkOneDateOrMomentOperand(lt.getLeftOperand(), lt.getRightOperand());
        } else if (whereClause instanceof GtBooleanExpression) {
            GtBooleanExpression gt = (GtBooleanExpression) whereClause;
            checkOneTimeOperand(gt.getLeftOperand(), gt.getRightOperand());
            checkOneDateOrMomentOperand(gt.getLeftOperand(), gt.getRightOperand());
        } else if (whereClause instanceof AndBooleanExpression) {
            AndBooleanExpression and = (AndBooleanExpression) whereClause;
            BooleanExpression left = and.getLeftExpression();
            BooleanExpression right = and.getRightExpression();
            if (left instanceof LtBooleanExpression) {
                LtBooleanExpression lt = (LtBooleanExpression) left;
                checkOneTimeOperand(lt.getLeftOperand(), lt.getRightOperand());
                checkOneDateOrMomentOperand(lt.getLeftOperand(), lt.getRightOperand());
                if (right instanceof GtBooleanExpression) {
                    GtBooleanExpression gt = (GtBooleanExpression) right;
                    checkOneTimeOperand(gt.getLeftOperand(), gt.getRightOperand());
                    checkOneDateOrMomentOperand(gt.getLeftOperand(), gt.getRightOperand());
                    checkRestrictionIsARange(lt, gt);

                } else {
                    throw new QueryNotSupportedException("Not a simple time range restriction");
                }
            } else if (left instanceof GtBooleanExpression) {
                GtBooleanExpression gt = (GtBooleanExpression) left;
                checkOneTimeOperand(gt.getLeftOperand(), gt.getRightOperand());
                checkOneDateOrMomentOperand(gt.getLeftOperand(), gt.getRightOperand());
                if (right instanceof LtBooleanExpression) {
                    LtBooleanExpression lt = (LtBooleanExpression) right;
                    checkOneTimeOperand(lt.getLeftOperand(), lt.getRightOperand());
                    checkOneDateOrMomentOperand(lt.getLeftOperand(), lt.getRightOperand());
                    checkRestrictionIsARange(lt, gt);
                } else {
                    throw new QueryNotSupportedException("Not a simple time range restriction");
                }
            } else {
                throw new QueryNotSupportedException("Not a simple time range restriction");
            }
        } else {
            throw new QueryNotSupportedException("Not a simple time range restriction");
        }
    }

    private void checkRestrictionIsARange(LtBooleanExpression lt, GtBooleanExpression gt)
        throws QueryNotSupportedException {

        // Don't allow "time < y and z > time"
        if ((isTimeOperand(lt.getLeftOperand()) && isTimeOperand(gt.getRightOperand()))
            || (isTimeOperand(lt.getRightOperand()) && isTimeOperand(gt.getLeftOperand()))) {
            throw new QueryNotSupportedException("Not a simple time range restriction");
        }
    }

    private void checkOneTimeOperand(Operand leftOperand, Operand rightOperand) throws QueryNotSupportedException {
        // We want exactly one of the operands to be a time operand
        if (isTimeOperand(leftOperand) == isTimeOperand(rightOperand)) {
            throw new QueryNotSupportedException("Expected exactly one time operand");
        }
    }

    private boolean isTimeOperand(Operand operand) {
        return operand instanceof NameOperand && ((NameOperand) operand).getName().equals("time");
    }

    private void checkOneDateOrMomentOperand(Operand leftOperand, Operand rightOperand)
        throws QueryNotSupportedException {

        // We want exactly one of the operands to be a date or moment operand
        if (isDateOrMomentOperandOperand(leftOperand) == isDateOrMomentOperandOperand(rightOperand)) {
            throw new QueryNotSupportedException("Expected exactly one time operand");
        }
    }

    private boolean isDateOrMomentOperandOperand(Operand operand) {
        return operand instanceof MomentOperand || operand instanceof DateOperand;
    }
}
