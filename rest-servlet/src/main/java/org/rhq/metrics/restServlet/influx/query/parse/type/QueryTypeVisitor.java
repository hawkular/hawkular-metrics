package org.rhq.metrics.restServlet.influx.query.parse.type;

import static org.rhq.metrics.restServlet.influx.query.parse.InfluxQueryParser.ListSeriesContext;
import static org.rhq.metrics.restServlet.influx.query.parse.InfluxQueryParser.SelectQueryContext;
import static org.rhq.metrics.restServlet.influx.query.parse.type.QueryType.LIST_SERIES;
import static org.rhq.metrics.restServlet.influx.query.parse.type.QueryType.SELECT;

import org.antlr.v4.runtime.misc.NotNull;
import org.antlr.v4.runtime.tree.RuleNode;

import org.rhq.metrics.restServlet.influx.query.parse.InfluxQueryBaseVisitor;

/**
 * @author Thomas Segismont
 */
public class QueryTypeVisitor extends InfluxQueryBaseVisitor<QueryType> {

    private boolean stopVisiting;

    public QueryTypeVisitor() {
        stopVisiting = false;
    }

    @Override
    protected boolean shouldVisitNextChild(@NotNull RuleNode node, QueryType currentResult) {
        return !stopVisiting;
    }

    @Override
    public QueryType visitListSeries(@NotNull ListSeriesContext ctx) {
        stopVisiting = true;
        return LIST_SERIES;
    }

    @Override
    public QueryType visitSelectQuery(@NotNull SelectQueryContext ctx) {
        stopVisiting = true;
        return SELECT;
    }
}
