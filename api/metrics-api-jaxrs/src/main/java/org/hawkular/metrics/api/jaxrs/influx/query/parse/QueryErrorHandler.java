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
package org.hawkular.metrics.api.jaxrs.influx.query.parse;

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
