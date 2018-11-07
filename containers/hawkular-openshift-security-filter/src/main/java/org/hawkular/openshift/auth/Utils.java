/*
 * Copyright 2014-2018 Red Hat, Inc. and/or its affiliates
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

package org.hawkular.openshift.auth;

import io.undertow.io.Sender;
import io.undertow.server.HttpServerExchange;


/**
 * @author Thomas Segismont
 */
public class Utils {

    /**
     * Changes the status code of the response and ends the exchange.
     *
     * @param exchange   the HTTP server request/response exchange
     * @param statusCode the HTTP status code
     *
     * @see HttpServerExchange#setStatusCode(int)
     * @see HttpServerExchange#endExchange()
     */
    public static void endExchange(HttpServerExchange exchange, int statusCode) {
        endExchange(exchange, statusCode, null, null);
    }

    /**
     * Changes the status code of the response, sets the HTTP reason phrase and ends the exchange.
     *
     * @param exchange     the HTTP server request/response exchange
     * @param statusCode   the HTTP status code
     * @param reasonPhrase the HTTP status message
     *
     * @see HttpServerExchange#setStatusCode(int)
     * @see HttpServerExchange#setReasonPhrase(String)
     * @see HttpServerExchange#endExchange()
     */
    public static void endExchange(HttpServerExchange exchange, int statusCode, String reasonPhrase) {
        endExchange(exchange, statusCode, reasonPhrase, null);
    }

    /**
     * Changes the status code of the response, sets the HTTP reason phrase and the response body, and ends the exchange.
     *
     * @param exchange     the HTTP server request/response exchange
     * @param statusCode   the HTTP status code
     * @param reasonPhrase the HTTP status message
     * @param body         the body of the response
     *
     * @see HttpServerExchange#setStatusCode(int)
     * @see HttpServerExchange#setReasonPhrase(String)
     * @see HttpServerExchange#endExchange()
     */
    public static void endExchange(HttpServerExchange exchange, int statusCode, String reasonPhrase, String body) {
        exchange.setStatusCode(statusCode);
        if (reasonPhrase != null) {
            exchange.setReasonPhrase(reasonPhrase);
        }
        if(body != null) {
            Sender sender = null;
            try {
                sender = exchange.getResponseSender();
                sender.send(body);
            } finally {
                if (sender != null) {
                    sender.close();
                }
            }
        }
        exchange.endExchange();
    }

    private Utils() {
        // Utility class
    }
}
