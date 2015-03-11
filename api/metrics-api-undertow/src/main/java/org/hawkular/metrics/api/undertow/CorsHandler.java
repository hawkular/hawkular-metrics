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
package org.hawkular.metrics.api.undertow;

import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderMap;
import io.undertow.util.HttpString;
import io.undertow.util.Methods;

class CorsHandler implements HttpHandler {
    public static final String DEFAULT_ALLOWED_METHODS = "GET, POST, PUT, DELETE, OPTIONS, HEAD";
    public static final String DEFAULT_ALLOWED_HEADERS = "origin,accept,content-type";

    private final HttpHandler next;

    public CorsHandler(HttpHandler next) {
        this.next = next;
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        final HeaderMap headers = exchange.getResponseHeaders();

        String origin = "*";
        if (headers.get("origin") != null && headers.get("origin").size() == 1
                && headers.get("origin").get(0) != null
                && !headers.get("origin").get(0).equals("null")) {
            origin = headers.get("origin").get(0).toString();
        }
        headers.put(new HttpString("Access-Control-Allow-Origin"), origin);

        headers.put(new HttpString("Access-Control-Allow-Credentials"), "true");
        headers.put(new HttpString("Access-Control-Allow-Methods"), DEFAULT_ALLOWED_METHODS);
        headers.put(new HttpString("Access-Control-Max-Age"), 72 * 60 * 60);
        headers.put(new HttpString("Access-Control-Allow-Headers"), DEFAULT_ALLOWED_HEADERS);

        if (Methods.OPTIONS.equals(exchange.getRequestMethod())) {
            exchange.setResponseCode(200);
        } else {
            next.handleRequest(exchange);
        }
    }
}