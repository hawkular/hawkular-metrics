package org.rhq.metrics.restServlet.influx.query.parse;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

/**
 * @author Thomas Segismont
 */
class QueryErrorListener extends BaseErrorListener {
    private String error;

    public boolean hasError() {
        return error != null;
    }

    public String getError() {
        return error;
    }

    @Override
    public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine,
        String msg, RecognitionException e) {
        error = "Line " + line + ", column " + charPositionInLine + ", " + msg;
    }
}
