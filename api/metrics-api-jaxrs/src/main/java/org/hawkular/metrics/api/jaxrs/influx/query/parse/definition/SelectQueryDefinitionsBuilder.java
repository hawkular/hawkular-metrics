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
public class SelectQueryDefinitionsBuilder {
    private boolean starColumn = true;
    private List<ColumnDefinition> columnDefinitions = null;
    private FromClause fromClause;
    private GroupByClause groupByClause = null;
    private BooleanExpression whereClause = null;
    private LimitClause limitClause = null;
    private boolean orderDesc = true;

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

    public SelectQueryDefinitionsBuilder setOrderDesc(boolean orderDesc) {
        this.orderDesc = orderDesc;
        return this;
    }

    public SelectQueryDefinitions createSelectQueryDefinitions() {
        return new SelectQueryDefinitions(starColumn, columnDefinitions, fromClause, groupByClause, whereClause,
            limitClause, orderDesc);
    }
}
