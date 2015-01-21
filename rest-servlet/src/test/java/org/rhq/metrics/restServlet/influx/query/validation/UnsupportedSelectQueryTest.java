/*
 * Copyright 2014 Red Hat, Inc.
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

package org.rhq.metrics.restServlet.influx.query.validation;

import static org.junit.runners.Parameterized.Parameters;
import static org.rhq.metrics.restServlet.influx.query.parse.InfluxQueryParser.SelectQueryContext;

import java.net.URL;
import java.nio.charset.Charset;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.io.Resources;

import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.rhq.metrics.restServlet.influx.query.parse.InfluxQueryParserFactory;
import org.rhq.metrics.restServlet.influx.query.parse.definition.SelectQueryDefinitions;
import org.rhq.metrics.restServlet.influx.query.parse.definition.SelectQueryDefinitionsParser;

/**
 * @author Thomas Segismont
 */
@RunWith(Parameterized.class)
public class UnsupportedSelectQueryTest {

    @Parameters(name = "unsupportedQuery: {0}")
    public static Iterable<Object[]> testUnsupportedQueries() throws Exception {
        URL resource = Resources.getResource("influx/query/unsupported-select-queries.iql");
        return FluentIterable //
            .from(Resources.readLines(resource, Charset.forName("UTF-8"))) //
            // Filter out comment lines
            .filter(new Predicate<String>() {
                @Override
                public boolean apply(String input) {
                    return !input.startsWith("--") && !input.trim().isEmpty();
                }
            }) //
            .transform(new Function<String, Object[]>() {
                @Override
                public Object[] apply(String input) {
                    return new Object[] { input };
                }
            });
    }

    @Rule
    public final ExpectedException exception = ExpectedException.none();
    private final ValidationRulesProducer rulesProducer = new ValidationRulesProducer();
    private final QueryValidator queryValidator;
    private final InfluxQueryParserFactory parserFactory = new InfluxQueryParserFactory();

    private final String selectQuery;

    public UnsupportedSelectQueryTest(String selectQuery) {
        this.selectQuery = selectQuery;
        queryValidator = new QueryValidator();
        queryValidator.selectQueryValidationRules = rulesProducer.selectQueryValidationRules();
    }

    @Test
    public void unsupportedQueriesShouldFailValidation() throws Exception {
        SelectQueryContext queryContext = parserFactory.newInstanceForQuery(selectQuery).selectQuery();
        SelectQueryDefinitionsParser definitionsParser = new SelectQueryDefinitionsParser();
        ParseTreeWalker walker = ParseTreeWalker.DEFAULT;
        walker.walk(definitionsParser, queryContext);
        SelectQueryDefinitions definitions = definitionsParser.getSelectQueryDefinitions();

        exception.expect(IllegalQueryException.class);

        queryValidator.validateSelectQuery(definitions);
    }
}
