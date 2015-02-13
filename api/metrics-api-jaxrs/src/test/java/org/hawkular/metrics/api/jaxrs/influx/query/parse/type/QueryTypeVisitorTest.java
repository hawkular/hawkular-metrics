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
package org.hawkular.metrics.api.jaxrs.influx.query.parse.type;

import static org.hawkular.metrics.api.jaxrs.influx.query.parse.type.QueryType.LIST_SERIES;
import static org.hawkular.metrics.api.jaxrs.influx.query.parse.type.QueryType.SELECT;
import static org.junit.Assert.assertEquals;

import org.antlr.v4.runtime.tree.ParseTree;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.InfluxQueryParserFactory;
import org.junit.Test;

/**
 * @author Thomas Segismont
 */
public class QueryTypeVisitorTest {
    private final QueryTypeVisitor queryTypeVisitor = new QueryTypeVisitor();
    private final InfluxQueryParserFactory parserFactory = new InfluxQueryParserFactory();

    @Test
    public void shouldDetectListSeriesQuery() {
        ParseTree parseTree = parserFactory.newInstanceForQuery("list series").query();
        QueryType queryType = queryTypeVisitor.visit(parseTree);
        assertEquals(LIST_SERIES, queryType);
    }

    @Test
    public void shouldDetectSelectQuery() {
        ParseTree parseTree = parserFactory.newInstanceForQuery("select * from \"time-series\"").query();
        QueryType queryType = queryTypeVisitor.visit(parseTree);
        assertEquals(SELECT, queryType);
    }
}
