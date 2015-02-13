/*
 * Copyright 2014-2015 Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hawkular.metrics.api.jaxrs.influx.query.parse.definition;

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
    private final boolean orderDesc;

    public SelectQueryDefinitions(boolean starColumn, List<ColumnDefinition> columnDefinitions, FromClause fromClause,
        GroupByClause groupByClause, BooleanExpression whereClause, LimitClause limitClause, boolean orderDesc) {
        this.starColumn = starColumn;
        this.columnDefinitions = columnDefinitions;
        this.fromClause = fromClause;
        this.groupByClause = groupByClause;
        this.whereClause = whereClause;
        this.limitClause = limitClause;
        this.orderDesc = orderDesc;
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

    public boolean isOrderDesc() {
        return orderDesc;
    }
}
