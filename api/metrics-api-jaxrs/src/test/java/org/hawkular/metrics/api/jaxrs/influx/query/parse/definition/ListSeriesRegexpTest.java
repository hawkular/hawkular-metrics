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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.InfluxQueryParser;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.InfluxQueryParserFactory;
import org.junit.Test;

/**
 * @author Thomas Segismont
 */
public class ListSeriesRegexpTest {
    private static final String REGEXP = "^sqd lk .\\sqkl'' *$";

    private final ListSeriesDefinitionsParser definitionsParser = new ListSeriesDefinitionsParser();
    private final ParseTreeWalker parseTreeWalker = ParseTreeWalker.DEFAULT;
    private final InfluxQueryParserFactory parserFactory = new InfluxQueryParserFactory();

    @Test
    public void shouldDetectQueryWithoutRegularExpression() {
        InfluxQueryParser parser = parserFactory.newInstanceForQuery("list series");
        parseTreeWalker.walk(definitionsParser, parser.listSeries());
        RegularExpression regularExpression = definitionsParser.getRegularExpression();

        assertNull(regularExpression);
    }

    @Test
    public void shouldDetectQueryWithRegularExpression() {
        InfluxQueryParser parser = parserFactory.newInstanceForQuery("list series /" + REGEXP + "/");
        parseTreeWalker.walk(definitionsParser, parser.listSeries());
        RegularExpression regularExpression = definitionsParser.getRegularExpression();

        assertNotNull(regularExpression);
        assertTrue(regularExpression.isCaseSensitive());
        assertEquals(REGEXP, regularExpression.getExpression());
    }

    @Test
    public void shouldDetectQueryWithCaseInsensitiveRegularExpression() {
        InfluxQueryParser parser = parserFactory.newInstanceForQuery("list series /" + REGEXP + "/I");
        parseTreeWalker.walk(definitionsParser, parser.listSeries());
        RegularExpression regularExpression = definitionsParser.getRegularExpression();

        assertNotNull(regularExpression);
        assertFalse(regularExpression.isCaseSensitive());
        assertEquals(REGEXP, regularExpression.getExpression());
    }
}
