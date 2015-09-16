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

import static java.util.stream.Collectors.toList;

import static org.hawkular.metrics.api.jaxrs.influx.InfluxTimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.hawkular.metrics.api.jaxrs.influx.InfluxTimeUnit;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.InfluxQueryParser;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.InfluxQueryParserFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * @author Thomas Segismont
 */
@RunWith(Parameterized.class)
public class SelectQueryGroupByClauseTest {

    @Parameters(name = "{0}")
    public static Iterable<Object[]> testValidQueries() throws Exception {
        return Arrays.stream(InfluxTimeUnit.values())
                .filter(timeUnit -> timeUnit != MILLISECONDS)
                .map(timeUnit -> {
                    int bucketSize = timeUnit.ordinal() + 15;
                    return new Object[]{
                            "select * from a group by time ( " + bucketSize + timeUnit.getId() + " )",
                            bucketSize,
                            timeUnit};
                }).collect(toList());
    }

    private final SelectQueryDefinitionsParser definitionsParser = new SelectQueryDefinitionsParser();
    private final ParseTreeWalker parseTreeWalker = ParseTreeWalker.DEFAULT;
    private final InfluxQueryParserFactory parserFactory = new InfluxQueryParserFactory();

    private final String queryText;
    private final int bucketSize;
    private final InfluxTimeUnit bucketSizeUnit;

    public SelectQueryGroupByClauseTest(String queryText, int bucketSize, InfluxTimeUnit bucketSizeUnit) {
        this.queryText = queryText;
        this.bucketSize = bucketSize;
        this.bucketSizeUnit = bucketSizeUnit;
    }

    @Test
    public void shouldDetectTimespan() {
        InfluxQueryParser parser = parserFactory.newInstanceForQuery(queryText);
        parseTreeWalker.walk(definitionsParser, parser.selectQuery());
        SelectQueryDefinitions definitions = definitionsParser.getSelectQueryDefinitions();
        GroupByClause groupByClause = definitions.getGroupByClause();

        assertEquals("time", groupByClause.getBucketType());
        assertEquals(bucketSize, groupByClause.getBucketSize());
        assertEquals(bucketSizeUnit, groupByClause.getBucketSizeUnit());
    }
}
