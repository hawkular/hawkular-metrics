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
package org.hawkular.metrics.api.jaxrs.util;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Stefan Negrea
 */
public final class OriginValidation {

    /**
     * Helper method to check whether requests from the specified origin
     * must be allowed.
     *
     * @param requestOrigin origin as reported by the client, {@code null} if unknown.
     * @param allowedOrigins set of configured allowed origins
     * @return {@code true} if the origin is allowed, else {@code false}.
     */
    public static boolean isAllowedOrigin(final String requestOrigin, final Set<URI> allowedOrigins) {
        if (requestOrigin == null) {
            return false;
        }

        URI requestOriginURI;
        try {
            requestOriginURI = new URI(requestOrigin);
        } catch (URISyntaxException e) {
            return false;
        }

        String requestScheme = requestOriginURI.getScheme();
        String requestHost = requestOriginURI.getHost();
        int requestPort = requestOriginURI.getPort();

        if (requestScheme == null || requestHost == null) {
            return false;
        }

        for (URI allowedOrigin : allowedOrigins) {
            if ((requestHost.equalsIgnoreCase(allowedOrigin.getHost())
                    || requestHost.toLowerCase().endsWith("." + allowedOrigin.getHost().toLowerCase()))
                    && requestPort == allowedOrigin.getPort()
                    && requestScheme.equalsIgnoreCase(allowedOrigin.getScheme())) {
                return true;
            }
        }

        return false;
    }

    /**
     * Parse allowed origins setting into URIs
     *
     * @param allowedCorsOrigins string setting value
     * @return parsed URIs of allowed origins
     * @throws Exception parsing failure exception
     */
    public static Set<URI> parseAllowedCorsOrigins(String allowedCorsOrigins) throws URISyntaxException {
        String[] stringOrigins = allowedCorsOrigins.split(",");

        Set<URI> parsedOrigins = new HashSet<>();
        for (String origin : stringOrigins) {
            parsedOrigins.add(new URI(origin.trim()));
        }

        return parsedOrigins;
    }
}
