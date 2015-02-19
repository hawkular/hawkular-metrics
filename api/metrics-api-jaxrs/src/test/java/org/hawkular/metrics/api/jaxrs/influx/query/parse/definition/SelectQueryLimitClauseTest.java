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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.InfluxQueryParser;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.InfluxQueryParserFactory;
import org.junit.Test;

/**
 * @author Thomas Segismont
 */
public class SelectQueryLimitClauseTest {
    private final SelectQueryDefinitionsParser definitionsParser = new SelectQueryDefinitionsParser();
    private final ParseTreeWalker parseTreeWalker = ParseTreeWalker.DEFAULT;
    private final InfluxQueryParserFactory parserFactory = new InfluxQueryParserFactory();

    @Test
    public void shouldDetectQueryWithoutLimitClause() {
        InfluxQueryParser parser = parserFactory.newInstanceForQuery("select a from b");
        parseTreeWalker.walk(definitionsParser, parser.selectQuery());
        SelectQueryDefinitions definitions = definitionsParser.getSelectQueryDefinitions();
        LimitClause limitClause = definitions.getLimitClause();

        assertNull(limitClause);
    }

    @Test
    public void shouldDetectQueryWithLimitClause() {
        InfluxQueryParser parser = parserFactory.newInstanceForQuery("select a from b limit 377");
        parseTreeWalker.walk(definitionsParser, parser.selectQuery());
        SelectQueryDefinitions definitions = definitionsParser.getSelectQueryDefinitions();
        LimitClause limitClause = definitions.getLimitClause();

        assertNotNull(limitClause);
        assertEquals(377, limitClause.getLimit());
    }
}
