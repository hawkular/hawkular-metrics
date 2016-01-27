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
package org.hawkular.metrics.api.jaxrs.util;

import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.ALLOWED_CORS_ORIGINS;
import static org.hawkular.metrics.api.jaxrs.util.Headers.ALLOW_ALL_ORIGIN;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.hawkular.metrics.api.jaxrs.config.Configurable;
import org.hawkular.metrics.api.jaxrs.config.ConfigurationProperty;

import com.google.common.collect.ImmutableSet;

/**
 * @author Stefan Negrea
 */
@ApplicationScoped
@Eager
public class OriginValidation {

    @Inject
    @Configurable
    @ConfigurationProperty(ALLOWED_CORS_ORIGINS)
    String allowedCorsOriginsConfig;

    private boolean allowAnyOrigin;
    private Set<URI> allowedOrigins;

    @PostConstruct
    void init() {
        if (ALLOW_ALL_ORIGIN.equals(allowedCorsOriginsConfig.trim())) {
            allowAnyOrigin = true;
        } else {
            allowAnyOrigin = false;
            Set<URI> parsedOrigins = Arrays.stream(allowedCorsOriginsConfig.split(","))
                    .map(String::trim)
                    .map(URI::create)
                    .collect(Collectors.toSet());
            allowedOrigins = ImmutableSet.copyOf(parsedOrigins);
        }
    }

    /**
     * Helper method to check whether requests from the specified origin
     * must be allowed.
     *
     * @param requestOrigin origin as reported by the client, {@code null} if unknown.
     *
     * @return {@code true} if the origin is allowed, else {@code false}.
     */
    public boolean isAllowedOrigin(final String requestOrigin) {
        if (allowAnyOrigin) {
            return true;
        }

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
}
