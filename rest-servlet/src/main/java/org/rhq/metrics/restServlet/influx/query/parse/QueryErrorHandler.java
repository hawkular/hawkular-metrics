package org.rhq.metrics.restServlet.influx.query.parse;

import org.antlr.v4.runtime.DefaultErrorStrategy;
import org.antlr.v4.runtime.InputMismatchException;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Token;

/**
 * @author Thomas Segismont
 */
class QueryErrorHandler extends DefaultErrorStrategy {

    private final QueryErrorListener errorListener;

    QueryErrorHandler(QueryErrorListener errorListener) {
        this.errorListener = errorListener;
    }

    @Override
    public void recover(Parser recognizer, RecognitionException e) {
        if (errorListener.hasError()) {
            throw new QueryParseException(errorListener.getError());
        }
        throw new QueryParseException(e);
    }

    @Override
    public Token recoverInline(Parser recognizer) throws RecognitionException {
        throw new InputMismatchException(recognizer);
    }

    @Override
    public void sync(Parser recognizer) throws RecognitionException {
    }
}
