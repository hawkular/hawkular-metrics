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

package org.hawkular.openshift.auth;


import static io.undertow.util.Headers.ORIGIN;
import static io.undertow.util.Methods.OPTIONS;

import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;

/**
 * An Undertow handler which delegates authentication to one of the supported strategies in Openshift environments.
 *
 * @author Thomas Segismont
 */
public class OpenshiftAuthHandler implements HttpHandler {
    private static final String OPENSHIFT_OAUTH = "openshift-oauth";
    private static final String HTPASSWD = "htpasswd";
    private static final String DISABLED = "disabled";
    private static final String SECURITY_OPTION_SYSPROP = "hawkular-metrics.openshift.auth-methods";
    private static final String SECURITY_OPTION = System.getProperty(SECURITY_OPTION_SYSPROP, OPENSHIFT_OAUTH);

    private final HttpHandler containerHandler;
    private final Authenticator authenticator;

    public OpenshiftAuthHandler(HttpHandler containerHandler) {
        this.containerHandler = containerHandler;
        if (SECURITY_OPTION.contains(OPENSHIFT_OAUTH)) {
            authenticator = new TokenAuthenticator(containerHandler);
        } else if (SECURITY_OPTION.contains(HTPASSWD)) {
            authenticator = new BasicAuthenticator(containerHandler);
        } else if (SECURITY_OPTION.equalsIgnoreCase(DISABLED)) {
            authenticator = new DisabledAuthenticator(containerHandler);
        } else {
            throw new IllegalArgumentException("Unknown security option: " + SECURITY_OPTION);
        }
    }

    @Override
    public void handleRequest(HttpServerExchange serverExchange) throws Exception {
        // Skip authentication if we are dealing with a CORS preflight request
        // CORS preflight removes any user credentials and doesn't perform an actual invocation.
        // http://www.w3.org/TR/cors/#cross-origin-request-with-preflight-0
        if (OPTIONS.equals(serverExchange.getRequestMethod())
                && serverExchange.getRequestHeaders().contains(ORIGIN)) {
            containerHandler.handleRequest(serverExchange);
            return;
        }

        // There are a few endpoint that should not be secured. If we secure the status endpoint when we cannot
        // tell if the container is up and it will always be marked as pending
        String path = serverExchange.getRelativePath();
        if (path == null || path.equals("") || path.equals("/") || path.equals("/status") || path.equals
                ("/static")) {
            containerHandler.handleRequest(serverExchange);
            return;
        }

        authenticator.handleRequest(serverExchange);
    }

    public void stop() {
        authenticator.stop();
    }
}
