package org.rhq.metrics.restServlet.influx.query.parse.definition;

import java.util.List;

/**
 * @author Thomas Segismont
 */
public class SelectQueryDefinitions {
    private final boolean starColumn;
    private final List<ColumnDefinition> columnDefinitions;
    private final FromClause fromClause;
    private final GroupByClause groupByClause;
    private final BooleanExpression whereClause;
    private final LimitClause limitClause;

    public SelectQueryDefinitions(boolean starColumn, List<ColumnDefinition> columnDefinitions, FromClause fromClause,
        GroupByClause groupByClause, BooleanExpression whereClause, LimitClause limitClause) {
        this.starColumn = starColumn;
        this.columnDefinitions = columnDefinitions;
        this.fromClause = fromClause;
        this.groupByClause = groupByClause;
        this.whereClause = whereClause;
        this.limitClause = limitClause;
    }

    public boolean isStarColumn() {
        return starColumn;
    }

    public List<ColumnDefinition> getColumnDefinitions() {
        return columnDefinitions;
    }

    public FromClause getFromClause() {
        return fromClause;
    }

    public GroupByClause getGroupByClause() {
        return groupByClause;
    }

    public BooleanExpression getWhereClause() {
        return whereClause;
    }

    public LimitClause getLimitClause() {
        return limitClause;
    }
}
