/*
 * Copyright 2014-2016 Red Hat, Inc. and/or its affiliates
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

package org.hawkular.metrics.security;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Base64;
import java.util.Deque;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderMap;
import io.undertow.util.Headers;
import io.undertow.util.HttpString;

/**
 * An Undertow handler to help with Influx authentication on Hawkular servers.
 * <p>
 * Code was initially created in a generic way by Juca and is here adapted to fit the very specifix Influx endpoint
 * needs.
 *
 * @author Juraci Paixão Kröhling
 * @author Thomas Segismont
 * @deprecated as of 0.17
 */
@Deprecated
public class InfluxAuthHttpHandler implements HttpHandler {
    private static final Pattern INFLUX_URI_PATTERN = Pattern.compile("^/hawkular/metrics/db/(.+)/series$");
    private static final HttpString TENANT_HEADER = HttpString.tryFromString("Hawkular-Tenant");
    private static final String USERNAME_PARAM_NAME = "u";
    private static final String PASSWORD_PARAM_NAME = "p";

    private HttpHandler next;

    public InfluxAuthHttpHandler(HttpHandler next) {
        this.next = next;
    }

    @Override
    public void handleRequest(HttpServerExchange httpServerExchange) throws Exception {
        if (httpServerExchange.isInIoThread()) {
            httpServerExchange.dispatch(this);
            return;
        }

        Matcher matcher = INFLUX_URI_PATTERN.matcher(httpServerExchange.getRequestURI());
        if (!matcher.matches()) {
            next.handleRequest(httpServerExchange);
            return;
        }

        HeaderMap requestHeaders = httpServerExchange.getRequestHeaders();

        String databaseName = matcher.group(1);
        requestHeaders.put(TENANT_HEADER, databaseName);

        Map<String, Deque<String>> requestParameters = httpServerExchange.getQueryParameters();
        Deque<String> usernameParamValues = requestParameters.get(USERNAME_PARAM_NAME);
        Deque<String> passwordParamValues = requestParameters.get(PASSWORD_PARAM_NAME);

        if (usernameParamValues != null && passwordParamValues != null) {
            String concat = usernameParamValues.getFirst() + ":" + passwordParamValues.getFirst();
            String encoded = Base64.getEncoder().encodeToString(concat.getBytes(UTF_8));
            String authHeader = "Basic " + encoded;
            requestHeaders.put(Headers.AUTHORIZATION, authHeader);
            httpServerExchange.getQueryParameters().remove(USERNAME_PARAM_NAME);
            httpServerExchange.getQueryParameters().remove(PASSWORD_PARAM_NAME);
        }

        next.handleRequest(httpServerExchange);
    }
}
