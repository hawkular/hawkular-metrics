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
package org.rhq.metrics.restServlet.influx.query.parse;

import static org.junit.runners.Parameterized.Parameters;

import java.net.URL;
import java.nio.charset.Charset;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.io.Resources;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * @author Thomas Segismont
 */
@RunWith(Parameterized.class)
public class ValidQuerySyntaxTest {

    @Parameters(name = "validQuery: {0}")
    public static Iterable<Object[]> testValidQueries() throws Exception {
        URL resource = Resources.getResource("influx/query/syntactically-correct-queries.iql");
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

    private final String query;
    private final InfluxQueryParserFactory parserFactory;

    public ValidQuerySyntaxTest(String query) {
        this.query = query;
        parserFactory = new InfluxQueryParserFactory();
    }

    @Test
    public void syntacticallyCorrectQueryShouldBeParsedWithoutError() throws Exception {
        // Should not fail
        parserFactory.newInstanceForQuery(query).query();
    }
}
