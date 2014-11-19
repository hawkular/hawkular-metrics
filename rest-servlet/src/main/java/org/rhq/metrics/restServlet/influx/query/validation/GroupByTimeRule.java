package org.rhq.metrics.restServlet.influx.query.validation;

import org.rhq.metrics.restServlet.influx.query.parse.definition.GroupByClause;
import org.rhq.metrics.restServlet.influx.query.parse.definition.SelectQueryDefinitions;

/**
 * @author Thomas Segismont
 */
public class GroupByTimeRule implements SelectQueryValidationRule {

    @Override
    public void checkQuery(SelectQueryDefinitions queryDefinitions) throws IllegalQueryException {
        GroupByClause groupByClause = queryDefinitions.getGroupByClause();
        if (groupByClause != null && !groupByClause.getBucketType().equalsIgnoreCase("time")) {
            throw new IllegalQueryException("Group by " + groupByClause.getBucketType());
        }
    }
}
