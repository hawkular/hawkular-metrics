package org.rhq.metrics.restServlet.influx.query.parse.definition;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.junit.Test;

import org.rhq.metrics.restServlet.influx.query.parse.InfluxQueryParser;
import org.rhq.metrics.restServlet.influx.query.parse.InfluxQueryParserFactory;

/**
 * @author Thomas Segismont
 */
public class SelectQueryOrderClauseTest {
    private final SelectQueryDefinitionsParser definitionsParser = new SelectQueryDefinitionsParser();
    private final ParseTreeWalker parseTreeWalker = ParseTreeWalker.DEFAULT;
    private final InfluxQueryParserFactory parserFactory = new InfluxQueryParserFactory();

    @Test
    public void shouldDetectQueryWithoutOrderClause() {
        InfluxQueryParser parser = parserFactory.newInstanceForQuery("select a from b");
        parseTreeWalker.walk(definitionsParser, parser.selectQuery());
        SelectQueryDefinitions definitions = definitionsParser.getSelectQueryDefinitions();

        assertTrue(definitions.isOrderDesc());
    }

    @Test
    public void shouldDetectQueryWithOrderDescClause() {
        InfluxQueryParser parser = parserFactory.newInstanceForQuery("select a from b order desc");
        parseTreeWalker.walk(definitionsParser, parser.selectQuery());
        SelectQueryDefinitions definitions = definitionsParser.getSelectQueryDefinitions();

        assertTrue(definitions.isOrderDesc());
    }

    @Test
    public void shouldDetectQueryWithOrderAscClause() {
        InfluxQueryParser parser = parserFactory.newInstanceForQuery("select a from b order asc");
        parseTreeWalker.walk(definitionsParser, parser.selectQuery());
        SelectQueryDefinitions definitions = definitionsParser.getSelectQueryDefinitions();

        assertFalse(definitions.isOrderDesc());
    }
}
