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
package org.hawkular.metrics.api.jaxrs.influx.query.parse;

import java.net.URL;
import java.nio.charset.Charset;
import java.util.Collections;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.io.Resources;

/**
 * @author Thomas Segismont
 */
@RunWith(Parameterized.class)
public class InvalidQuerySyntaxTest {

    @Parameters(name = "invalidQuery: {0}")
    public static Iterable<Object[]> testValidQueries() throws Exception {
        URL resource = Resources.getResource("influx/query/syntactically-incorrect-queries.iql");
        return Iterables.concat(
                // add an empty string as an invalid query
                Collections.singletonList(new Object[] { "" }),
                // plus the queries from the file
                FluentIterable //
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
                    })
                );
    }

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    private final String query;
    private final InfluxQueryParserFactory parserFactory;

    public InvalidQuerySyntaxTest(String query) {
        this.query = query;
        parserFactory = new InfluxQueryParserFactory();
    }

    @Test
    public void syntacticallyIncorrectQueryParsingShouldThrowException() throws Exception {
        InfluxQueryParser parser = parserFactory.newInstanceForQuery(query);

        exception.expect(QueryParseException.class);

        parser.query();
    }
}
