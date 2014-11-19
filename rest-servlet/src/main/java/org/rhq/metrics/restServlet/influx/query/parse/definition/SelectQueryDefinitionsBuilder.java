package org.rhq.metrics.restServlet.influx.query.parse.definition;

import java.util.List;

/**
 * @author Thomas Segismont
 */
public class SelectQueryDefinitionsBuilder {
    private boolean starColumn = true;
    private List<ColumnDefinition> columnDefinitions = null;
    private FromClause fromClause;
    private GroupByClause groupByClause = null;
    private BooleanExpression whereClause = null;
    private LimitClause limitClause = null;

    public SelectQueryDefinitionsBuilder setColumnDefinitions(List<ColumnDefinition> columnDefinitions) {
        this.columnDefinitions = columnDefinitions;
        this.starColumn = (columnDefinitions == null);
        return this;
    }

    public SelectQueryDefinitionsBuilder setFromClause(FromClause fromClause) {
        this.fromClause = fromClause;
        return this;
    }

    public SelectQueryDefinitionsBuilder setGroupByClause(GroupByClause groupByClause) {
        this.groupByClause = groupByClause;
        return this;
    }

    public SelectQueryDefinitionsBuilder setWhereClause(BooleanExpression whereClause) {
        this.whereClause = whereClause;
        return this;
    }

    public SelectQueryDefinitionsBuilder setLimitClause(LimitClause limitClause) {
        this.limitClause = limitClause;
        return this;
    }

    public SelectQueryDefinitions createSelectQueryDefinitions() {
        return new SelectQueryDefinitions(starColumn, columnDefinitions, fromClause, groupByClause, whereClause,
            limitClause);
    }
}
