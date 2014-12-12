package org.rhq.metrics.restServlet.influx.query.parse;

import javax.enterprise.context.ApplicationScoped;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.TokenStream;

/**
 * @author Thomas Segismont
 */
@ApplicationScoped
public class InfluxQueryParserFactory {

    public InfluxQueryParser newInstanceForQuery(String queryText) {
        InfluxQueryLexer lexer = newInfluxQueryLexer(queryText);
        TokenStream tokenStream = new CommonTokenStream(lexer);
        InfluxQueryParser influxQueryParser = new InfluxQueryParser(tokenStream);
        influxQueryParser.removeErrorListeners();
        QueryErrorListener errorListener = new QueryErrorListener();
        influxQueryParser.addErrorListener(errorListener);
        influxQueryParser.setErrorHandler(new QueryErrorHandler(errorListener));
        return influxQueryParser;
    }

    private InfluxQueryLexer newInfluxQueryLexer(String queryText) {
        ANTLRInputStream input = new ANTLRInputStream(queryText);
        InfluxQueryLexer lexer = new InfluxQueryLexer(input);
        lexer.removeErrorListeners();
        return lexer;
    }
}
