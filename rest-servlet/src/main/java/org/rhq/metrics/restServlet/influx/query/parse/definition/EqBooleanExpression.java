package org.rhq.metrics.restServlet.influx.query.parse.definition;

import java.util.Collections;
import java.util.List;

/**
 * @author Thomas Segismont
 */
public class EqBooleanExpression implements BooleanExpression {
    private final Operand leftOperand;
    private final Operand rightOperand;

    public EqBooleanExpression(Operand leftOperand, Operand rightOperand) {
        this.leftOperand = leftOperand;
        this.rightOperand = rightOperand;
    }

    public Operand getLeftOperand() {
        return leftOperand;
    }

    public Operand getRightOperand() {
        return rightOperand;
    }

    @Override
    public boolean hasChildren() {
        return false;
    }

    @Override
    public List<BooleanExpression> getChildren() {
        return Collections.emptyList();
    }
}
