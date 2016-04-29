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


import static java.util.stream.Collectors.toSet;

import static org.hawkular.openshift.auth.BasicAuthenticator.BASIC_PREFIX;
import static org.hawkular.openshift.auth.SecurityOption.DISABLED;
import static org.hawkular.openshift.auth.SecurityOption.HTPASSWD;
import static org.hawkular.openshift.auth.SecurityOption.OPENSHIFT_OAUTH;
import static org.hawkular.openshift.auth.TokenAuthenticator.BEARER_PREFIX;
import static org.hawkular.openshift.auth.Utils.endExchange;

import static io.undertow.util.Headers.AUTHORIZATION;
import static io.undertow.util.Headers.ORIGIN;
import static io.undertow.util.Methods.OPTIONS;
import static io.undertow.util.StatusCodes.FORBIDDEN;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.Sets;

import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;

/**
 * An Undertow handler which delegates authentication to one of the supported strategies in Openshift environments.
 *
 * @author Thomas Segismont
 */
public class OpenshiftAuthHandler implements HttpHandler {
    private static final String SECURITY_OPTION_SYSPROP = "hawkular-metrics.openshift.auth-methods";
    private static final Set<SecurityOption> SECURITY_OPTIONS;

    static {
        Set<SecurityOption> active = new HashSet<>();
        String property = System.getProperty(SECURITY_OPTION_SYSPROP, OPENSHIFT_OAUTH.toString());
        Set<String> configured = Arrays.stream(property.split(",")).map(String::trim).collect(toSet());
        for (SecurityOption option : SecurityOption.values()) {
            if (configured.contains(option.toString())) {
                active.add(option);
            }
        }
        SECURITY_OPTIONS = Sets.immutableEnumSet(active);
    }

    private final HttpHandler containerHandler;
    private final TokenAuthenticator tokenAuthenticator;
    private final BasicAuthenticator basicAuthenticator;

    public OpenshiftAuthHandler(HttpHandler containerHandler) {
        this.containerHandler = containerHandler;
        tokenAuthenticator = new TokenAuthenticator(containerHandler);
        basicAuthenticator = new BasicAuthenticator(containerHandler);
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
        if (path == null || path.equals("") || path.equals("/") || path.equals("/status")
                || path.startsWith("/static")) {
            containerHandler.handleRequest(serverExchange);
            return;
        }

        // If it is set to be disabled, then just continue
        if (SECURITY_OPTIONS.contains(DISABLED)) {
            containerHandler.handleRequest(serverExchange);
            return;
        }

        // Get the authorization header and determine how it should be handled
        String authorizationHeader = serverExchange.getRequestHeaders().getFirst(AUTHORIZATION);
        if (authorizationHeader == null) {
            endExchange(serverExchange, FORBIDDEN);
        } else if (authorizationHeader.startsWith(BEARER_PREFIX) && SECURITY_OPTIONS.contains(OPENSHIFT_OAUTH)) {
            tokenAuthenticator.handleRequest(serverExchange);
        } else if (authorizationHeader.startsWith(BASIC_PREFIX) && SECURITY_OPTIONS.contains(HTPASSWD)) {
            basicAuthenticator.handleRequest(serverExchange);
        } else {
            endExchange(serverExchange, FORBIDDEN);
        }
    }

    public void stop() {
        basicAuthenticator.stop();
        tokenAuthenticator.stop();
    }
}
