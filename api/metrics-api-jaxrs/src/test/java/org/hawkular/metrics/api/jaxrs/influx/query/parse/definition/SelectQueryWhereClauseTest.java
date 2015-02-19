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
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.joda.time.MutableDateTime;
import org.junit.Test;

/**
 * @author Thomas Segismont;
 */
public class SelectQueryWhereClauseTest {
    private final SelectQueryDefinitionsParser definitionsParser = new SelectQueryDefinitionsParser();
    private final ParseTreeWalker parseTreeWalker = ParseTreeWalker.DEFAULT;
    private final InfluxQueryParserFactory parserFactory = new InfluxQueryParserFactory();

    @Test
    public void shouldDetectQueryWithoutWhereClause() {
        InfluxQueryParser parser = parserFactory.newInstanceForQuery("select a from b");
        parseTreeWalker.walk(definitionsParser, parser.selectQuery());
        SelectQueryDefinitions definitions = definitionsParser.getSelectQueryDefinitions();

        assertNull(definitions.getWhereClause());
    }

    @Test
    public void shouldParseBooleanExpressionTree() {
        InfluxQueryParser parser = parserFactory
            .newInstanceForQuery("select a from c "
                + "where a=2 and .35 =  '2005-11-09 15:12:01.623' and (b.\"z\"<>c or (d = _1 and 3 = '2005-11-09'))"
                + " or time > now() -  30w");
        parseTreeWalker.walk(definitionsParser, parser.selectQuery());
        SelectQueryDefinitions definitions = definitionsParser.getSelectQueryDefinitions();
        BooleanExpression whereClause = definitions.getWhereClause();

        assertNotNull(whereClause);

        // The right most 'or' should have precedence;
        assertEquals(OrBooleanExpression.class, whereClause.getClass());

        // Inspect "or time > now() -  30w";
        assertEquals(GtBooleanExpression.class, ((OrBooleanExpression) whereClause).getRightExpression().getClass());
        GtBooleanExpression gtExpr = (GtBooleanExpression) ((OrBooleanExpression) whereClause).getRightExpression();
        assertNameOperand(gtExpr.getLeftOperand(), "time");
        assertMomentOperand(gtExpr.getRightOperand(), "now", -30, InfluxTimeUnit.WEEKS);

        // Inspect "a=2 and .35 =  '2005-11-09 15:12:01.623' and (b."z"<>c or (d = _1 and 3 = '2005-11-09'))";
        assertEquals(AndBooleanExpression.class, ((OrBooleanExpression) whereClause).getLeftExpression().getClass());
        AndBooleanExpression whereLeftExpr = (AndBooleanExpression) ((OrBooleanExpression) whereClause)
            .getLeftExpression();

        // Inspect "a=2 and .35 =  '2005-11-09 15:12:01.623'";
        assertEquals(AndBooleanExpression.class, whereLeftExpr.getLeftExpression().getClass());
        AndBooleanExpression nestedLeftExpr = (AndBooleanExpression) whereLeftExpr.getLeftExpression();

        // Inspect "a=2";
        assertEquals(EqBooleanExpression.class, nestedLeftExpr.getLeftExpression().getClass());
        EqBooleanExpression eqExpr = (EqBooleanExpression) nestedLeftExpr.getLeftExpression();
        assertNameOperand(eqExpr.getLeftOperand(), "a");
        assertIntegerOperand(eqExpr.getRightOperand(), 2);

        // Inspect ".35 =  '2005-11-09 15:12:01.623'";
        assertEquals(EqBooleanExpression.class, nestedLeftExpr.getRightExpression().getClass());
        eqExpr = (EqBooleanExpression) nestedLeftExpr.getRightExpression();
        assertDoubleOperand(eqExpr.getLeftOperand(), .35);
        Instant instant = new MutableDateTime(2005, 11, 9, 15, 12, 1, 623, DateTimeZone.UTC).toInstant();
        assertDateOperand(eqExpr.getRightOperand(), instant);

        // Inspect "b."z"<>c or (d = _1 and 3 = '2005-11-09')";
        assertEquals(OrBooleanExpression.class, whereLeftExpr.getRightExpression().getClass());
        OrBooleanExpression nestedRightExpr = (OrBooleanExpression) whereLeftExpr.getRightExpression();

        // Inspect "b."z"<>c";
        assertEquals(NeqBooleanExpression.class, nestedRightExpr.getLeftExpression().getClass());
        NeqBooleanExpression neqExpr = (NeqBooleanExpression) nestedRightExpr.getLeftExpression();
        assertNameOperand(neqExpr.getLeftOperand(), "b", "z");
        assertNameOperand(neqExpr.getRightOperand(), "c");

        // Inspect "d = _1 and 3 = '2005-11-09'";
        assertEquals(AndBooleanExpression.class, nestedRightExpr.getRightExpression().getClass());
        AndBooleanExpression deeplyNestedRightExpr = (AndBooleanExpression) nestedRightExpr.getRightExpression();

        // Inspect "d = _1";
        assertEquals(EqBooleanExpression.class, deeplyNestedRightExpr.getLeftExpression().getClass());
        eqExpr = (EqBooleanExpression) deeplyNestedRightExpr.getLeftExpression();
        assertNameOperand(eqExpr.getLeftOperand(), "d");
        assertNameOperand(eqExpr.getRightOperand(), "_1");

        // Inspect "3 = '2005-11-09'";
        assertEquals(EqBooleanExpression.class, deeplyNestedRightExpr.getRightExpression().getClass());
        eqExpr = (EqBooleanExpression) deeplyNestedRightExpr.getRightExpression();
        assertIntegerOperand(eqExpr.getLeftOperand(), 3);
        instant = new MutableDateTime(2005, 11, 9, 0, 0, 0, 0, DateTimeZone.UTC).toInstant();
        assertDateOperand(eqExpr.getRightOperand(), instant);
    }

    private void assertDoubleOperand(Operand operand, double value) {
        assertEquals(DoubleOperand.class, operand.getClass());
        DoubleOperand doubleOperand = (DoubleOperand) operand;
        assertEquals(value, doubleOperand.getValue(), 0);
    }

    private void assertIntegerOperand(Operand operand, int value) {
        assertEquals(IntegerOperand.class, operand.getClass());
        IntegerOperand integerOperand = (IntegerOperand) operand;
        assertEquals(value, integerOperand.getValue());
    }

    private void assertNameOperand(Operand operand, String name) {
        assertNameOperand(operand, null, name);
    }

    private void assertNameOperand(Operand operand, String prefix, String name) {
        assertEquals(NameOperand.class, operand.getClass());
        NameOperand nameOperand = (NameOperand) operand;
        assertEquals(prefix != null, nameOperand.isPrefixed());
        assertEquals(prefix, nameOperand.getPrefix());
        assertEquals(name, nameOperand.getName());
    }

    private void assertMomentOperand(Operand operand, String functionName, int timeshift,
            InfluxTimeUnit timeshiftUnit) {
        assertEquals(MomentOperand.class, operand.getClass());
        MomentOperand momentOperand = (MomentOperand) operand;
        assertEquals(functionName, momentOperand.getFunctionName());
        assertEquals(timeshift, momentOperand.getTimeshift());
        assertEquals(timeshiftUnit, momentOperand.getTimeshiftUnit());
    }

    private void assertDateOperand(Operand operand, Instant instant) {
        assertEquals(DateOperand.class, operand.getClass());
        DateOperand dateOperand = (DateOperand) operand;
        assertEquals(instant, dateOperand.getInstant());
    }
}
