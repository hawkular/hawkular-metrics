package org.rhq.metrics.restServlet.influx.query.parse.definition;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.junit.Test;

import org.rhq.metrics.restServlet.influx.query.parse.InfluxQueryParser;
import org.rhq.metrics.restServlet.influx.query.parse.InfluxQueryParserFactory;

/**
 * @author Thomas Segismont
 */
public class SelectQueryFromClauseTest {
    private final SelectQueryDefinitionsParser definitionsParser = new SelectQueryDefinitionsParser();
    private final ParseTreeWalker parseTreeWalker = ParseTreeWalker.DEFAULT;
    private final InfluxQueryParserFactory parserFactory = new InfluxQueryParserFactory();

    @Test
    public void shouldDetectAliasedName() {
        InfluxQueryParser parser = parserFactory.newInstanceForQuery("select * from c as _b");
        parseTreeWalker.walk(definitionsParser, parser.selectQuery());
        SelectQueryDefinitions definitions = definitionsParser.getSelectQueryDefinitions();
        FromClause fromClause = definitions.getFromClause();

        assertTrue(fromClause.isAliased());
        assertEquals("c", fromClause.getName());
        assertEquals("_b", fromClause.getAlias());
    }

    @Test
    public void shouldDetectSimpleName() {
        InfluxQueryParser parser = parserFactory.newInstanceForQuery("select * from _dkJl7k1");
        parseTreeWalker.walk(definitionsParser, parser.selectQuery());
        SelectQueryDefinitions definitions = definitionsParser.getSelectQueryDefinitions();
        FromClause fromClause = definitions.getFromClause();

        assertFalse(fromClause.isAliased());
        assertEquals("_dkJl7k1", fromClause.getName());
    }

    @Test
    public void shouldDetectDoubleQuotedName() {
        InfluxQueryParser parser = parserFactory.newInstanceForQuery("select * from \"1sqd !! \\\"  dkJl7k1\"");
        parseTreeWalker.walk(definitionsParser, parser.selectQuery());
        SelectQueryDefinitions definitions = definitionsParser.getSelectQueryDefinitions();
        FromClause fromClause = definitions.getFromClause();

        assertFalse(fromClause.isAliased());
        assertEquals("1sqd !! \\\"  dkJl7k1", fromClause.getName());
    }
}
