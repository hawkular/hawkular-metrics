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

import static com.google.common.collect.FluentIterable.from;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.InfluxQueryParser;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.InfluxQueryParserFactory;
import org.junit.Test;

import com.google.common.base.Predicate;

/**
 * @author Thomas Segismont
 */
public class SelectQueryColumnDefinitionsTest {
    private final SelectQueryDefinitionsParser definitionsParser = new SelectQueryDefinitionsParser();
    private final ParseTreeWalker parseTreeWalker = ParseTreeWalker.DEFAULT;
    private final InfluxQueryParserFactory parserFactory = new InfluxQueryParserFactory();

    @Test
    public void shouldDetectStarColumn() {
        InfluxQueryParser parser = parserFactory.newInstanceForQuery("select * from c");
        parseTreeWalker.walk(definitionsParser, parser.selectQuery());
        SelectQueryDefinitions definitions = definitionsParser.getSelectQueryDefinitions();

        assertTrue(definitions.isStarColumn());
        assertNull(definitions.getColumnDefinitions());
    }

    @Test
    public void shouldDetectSingleColumn() {
        InfluxQueryParser parser = parserFactory.newInstanceForQuery("select a from c");
        parseTreeWalker.walk(definitionsParser, parser.selectQuery());
        SelectQueryDefinitions definitions = definitionsParser.getSelectQueryDefinitions();

        assertFalse(definitions.isStarColumn());
        assertNotNull(definitions.getColumnDefinitions());
        assertEquals(1, definitions.getColumnDefinitions().size());
    }

    @Test
    public void shouldDetectColumnList() {
        InfluxQueryParser parser = parserFactory
            .newInstanceForQuery("select a,\"b\",c.d,e as f,count(g),count(h) as i from j");
        parseTreeWalker.walk(definitionsParser, parser.selectQuery());
        SelectQueryDefinitions definitions = definitionsParser.getSelectQueryDefinitions();

        assertFalse(definitions.isStarColumn());
        assertNotNull(definitions.getColumnDefinitions());
        assertEquals(6, definitions.getColumnDefinitions().size());
    }

    @Test
    public void shouldDetectAliasedColumnDefinitions() {
        InfluxQueryParser parser = parserFactory.newInstanceForQuery("select val as mars, count(3) as eille from c");
        parseTreeWalker.walk(definitionsParser, parser.selectQuery());
        SelectQueryDefinitions definitions = definitionsParser.getSelectQueryDefinitions();
        List<ColumnDefinition> columnDefinitions = definitions.getColumnDefinitions();

        assertTrue(from(columnDefinitions).allMatch(columnDefinitionIsAliased()));
        assertEquals("mars", columnDefinitions.get(0).getAlias());
        assertEquals("mars", columnDefinitions.get(0).getDisplayName());
        assertEquals("eille", columnDefinitions.get(1).getAlias());
        assertEquals("eille", columnDefinitions.get(1).getDisplayName());
    }

    @Test
    public void shouldDetectRawColumnDefinitions() {
        InfluxQueryParser parser = parserFactory.newInstanceForQuery("select a,\" b \",c.d,e.\" f \" from g");
        parseTreeWalker.walk(definitionsParser, parser.selectQuery());
        SelectQueryDefinitions definitions = definitionsParser.getSelectQueryDefinitions();
        List<? extends ColumnDefinition> columnDefinitions = definitions.getColumnDefinitions();

        assertTrue(from(columnDefinitions).allMatch(columnDefinitionIsNotAliased()));
        assertTrue(from(columnDefinitions).allMatch(isRawColumnDefinition()));

        @SuppressWarnings("unchecked")
        List<RawColumnDefinition> rawColumnDefinitions = (List<RawColumnDefinition>) columnDefinitions;

        RawColumnDefinition rawColumnDefinition = rawColumnDefinitions.get(0);
        assertFalse(rawColumnDefinition.isPrefixed());
        assertEquals("a", rawColumnDefinition.getName());
        assertEquals("a", rawColumnDefinition.getDisplayName());

        rawColumnDefinition = rawColumnDefinitions.get(1);
        assertFalse(rawColumnDefinition.isPrefixed());
        assertEquals(" b ", rawColumnDefinition.getName());
        assertEquals(" b ", rawColumnDefinition.getDisplayName());

        rawColumnDefinition = rawColumnDefinitions.get(2);
        assertTrue(rawColumnDefinition.isPrefixed());
        assertEquals("c", rawColumnDefinition.getPrefix());
        assertEquals("d", rawColumnDefinition.getName());
        assertEquals("d", rawColumnDefinition.getDisplayName());

        rawColumnDefinition = rawColumnDefinitions.get(3);
        assertTrue(rawColumnDefinition.isPrefixed());
        assertEquals("e", rawColumnDefinition.getPrefix());
        assertEquals(" f ", rawColumnDefinition.getName());
        assertEquals(" f ", rawColumnDefinition.getDisplayName());
    }

    @Test
    public void shouldDetectAggregatedColumnDefinitionWithoutArguments() {
        InfluxQueryParser parser = parserFactory.newInstanceForQuery("select aa() from b");
        parseTreeWalker.walk(definitionsParser, parser.selectQuery());
        SelectQueryDefinitions definitions = definitionsParser.getSelectQueryDefinitions();
        ColumnDefinition columnDefinition = definitions.getColumnDefinitions().get(0);

        assertFalse(columnDefinition.isAliased());
        assertEquals(AggregatedColumnDefinition.class, columnDefinition.getClass());

        AggregatedColumnDefinition aggregatedColumnDefinition = (AggregatedColumnDefinition) columnDefinition;

        assertEquals("aa", aggregatedColumnDefinition.getAggregationFunction());
        assertEquals("aa", aggregatedColumnDefinition.getDisplayName());

        assertNull(aggregatedColumnDefinition.getAggregationFunctionArguments());
    }

    @Test
    public void shouldDetectAggregatedColumnDefinitionWithArguments() {
        InfluxQueryParser parser = parserFactory
            .newInstanceForQuery("select zz(-.52, a,\" b \",c.d,e.\" f \", 78, ' zimpi ') from g");
        parseTreeWalker.walk(definitionsParser, parser.selectQuery());
        SelectQueryDefinitions definitions = definitionsParser.getSelectQueryDefinitions();
        ColumnDefinition columnDefinition = definitions.getColumnDefinitions().get(0);

        assertFalse(columnDefinition.isAliased());
        assertEquals(AggregatedColumnDefinition.class, columnDefinition.getClass());

        AggregatedColumnDefinition aggregatedColumnDefinition = (AggregatedColumnDefinition) columnDefinition;

        assertEquals("zz", aggregatedColumnDefinition.getAggregationFunction());
        assertEquals("zz", aggregatedColumnDefinition.getDisplayName());

        List<FunctionArgument> aggregationFunctionArguments = aggregatedColumnDefinition
            .getAggregationFunctionArguments();

        assertNotNull(aggregationFunctionArguments);
        assertEquals(7, aggregationFunctionArguments.size());

        int i = 0;
        assertEquals(DoubleFunctionArgument.class, aggregationFunctionArguments.get(i).getClass());
        DoubleFunctionArgument doubleFunctionArgument = (DoubleFunctionArgument) aggregationFunctionArguments.get(i);
        assertEquals(-.52d, doubleFunctionArgument.getValue(), 0d);

        i++;
        assertEquals(NameFunctionArgument.class, aggregationFunctionArguments.get(i).getClass());
        NameFunctionArgument nameFunctionArgument = (NameFunctionArgument) aggregationFunctionArguments.get(i);
        assertFalse(nameFunctionArgument.isPrefixed());
        assertEquals("a", nameFunctionArgument.getName());

        i++;
        assertEquals(NameFunctionArgument.class, aggregationFunctionArguments.get(i).getClass());
        nameFunctionArgument = (NameFunctionArgument) aggregationFunctionArguments.get(i);
        assertFalse(nameFunctionArgument.isPrefixed());
        assertEquals(" b ", nameFunctionArgument.getName());

        i++;
        assertEquals(NameFunctionArgument.class, aggregationFunctionArguments.get(i).getClass());
        nameFunctionArgument = (NameFunctionArgument) aggregationFunctionArguments.get(i);
        assertTrue(nameFunctionArgument.isPrefixed());
        assertEquals("c", nameFunctionArgument.getPrefix());
        assertEquals("d", nameFunctionArgument.getName());

        i++;
        assertEquals(NameFunctionArgument.class, aggregationFunctionArguments.get(i).getClass());
        nameFunctionArgument = (NameFunctionArgument) aggregationFunctionArguments.get(i);
        assertTrue(nameFunctionArgument.isPrefixed());
        assertEquals("e", nameFunctionArgument.getPrefix());
        assertEquals(" f ", nameFunctionArgument.getName());

        i++;
        assertEquals(IntegerFunctionArgument.class, aggregationFunctionArguments.get(i).getClass());
        IntegerFunctionArgument integerFunctionArgument = (IntegerFunctionArgument) aggregationFunctionArguments.get(i);
        assertEquals(78, integerFunctionArgument.getValue());

        i++;
        assertEquals(StringFunctionArgument.class, aggregationFunctionArguments.get(i).getClass());
        StringFunctionArgument stringFunctionArgument = (StringFunctionArgument) aggregationFunctionArguments.get(i);
        assertEquals(" zimpi ", stringFunctionArgument.getValue());
    }

    private Predicate<ColumnDefinition> columnDefinitionIsAliased() {
        return new ColumnDefinitionIsAliased();
    }

    private Predicate<ColumnDefinition> columnDefinitionIsNotAliased() {
        return new ColumnDefinitionIsNotAliased();
    }

    private Predicate<ColumnDefinition> isRawColumnDefinition() {
        return new IsRawColumnDefinition();
    }

    private static class ColumnDefinitionIsAliased implements Predicate<ColumnDefinition> {
        @Override
        public boolean apply(ColumnDefinition columnDefinition) {
            return columnDefinition.isAliased();
        }
    }

    private static class ColumnDefinitionIsNotAliased implements Predicate<ColumnDefinition> {
        @Override
        public boolean apply(ColumnDefinition columnDefinition) {
            return !columnDefinition.isAliased();
        }
    }

    private class IsRawColumnDefinition implements Predicate<ColumnDefinition> {
        @Override
        public boolean apply(ColumnDefinition columnDefinition) {
            return columnDefinition.getClass().equals(RawColumnDefinition.class);
        }
    }
}
