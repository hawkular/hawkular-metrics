package org.rhq.metrics.restServlet.influx.query.parse.definition;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.junit.Test;

import org.rhq.metrics.restServlet.influx.query.parse.InfluxQueryParser;
import org.rhq.metrics.restServlet.influx.query.parse.InfluxQueryParserFactory;

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
