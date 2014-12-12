package org.rhq.metrics.restServlet.influx.query.parse.type;

import static org.junit.Assert.assertEquals;
import static org.rhq.metrics.restServlet.influx.query.parse.type.QueryType.LIST_SERIES;
import static org.rhq.metrics.restServlet.influx.query.parse.type.QueryType.SELECT;

import org.antlr.v4.runtime.tree.ParseTree;
import org.junit.Test;

import org.rhq.metrics.restServlet.influx.query.parse.InfluxQueryParserFactory;

/**
 * @author Thomas Segismont
 */
public class QueryTypeVisitorTest {
    private final QueryTypeVisitor queryTypeVisitor = new QueryTypeVisitor();
    private final InfluxQueryParserFactory parserFactory = new InfluxQueryParserFactory();

    @Test
    public void shouldDetectListSeriesQuery() {
        ParseTree parseTree = parserFactory.newInstanceForQuery("list series").query();
        QueryType queryType = queryTypeVisitor.visit(parseTree);
        assertEquals(LIST_SERIES, queryType);
    }

    @Test
    public void shouldDetectSelectQuery() {
        ParseTree parseTree = parserFactory.newInstanceForQuery("select * from \"time-series\"").query();
        QueryType queryType = queryTypeVisitor.visit(parseTree);
        assertEquals(SELECT, queryType);
    }
}
