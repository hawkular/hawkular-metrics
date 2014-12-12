package org.rhq.metrics.restServlet.influx.query.parse.definition;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author Thomas Segismont
 */
public class AndBooleanExpression implements BooleanExpression {

    private final BooleanExpression leftExpression;
    private final BooleanExpression rightExpression;
    private final List<BooleanExpression> children;

    public AndBooleanExpression(BooleanExpression leftExpression, BooleanExpression rightExpression) {
        this.leftExpression = leftExpression;
        this.rightExpression = rightExpression;
        children = Collections.unmodifiableList(Arrays.asList(leftExpression, rightExpression));
    }

    public BooleanExpression getLeftExpression() {
        return leftExpression;
    }

    public BooleanExpression getRightExpression() {
        return rightExpression;
    }

    @Override
    public boolean hasChildren() {
        return true;
    }

    @Override
    public List<BooleanExpression> getChildren() {
        return children;
    }
}
