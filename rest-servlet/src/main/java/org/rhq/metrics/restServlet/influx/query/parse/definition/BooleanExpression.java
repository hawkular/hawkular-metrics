package org.rhq.metrics.restServlet.influx.query.parse.definition;

import java.util.List;

/**
 * @author Thomas Segismont
 */
public interface BooleanExpression {

    boolean hasChildren();

    List<BooleanExpression> getChildren();
}
