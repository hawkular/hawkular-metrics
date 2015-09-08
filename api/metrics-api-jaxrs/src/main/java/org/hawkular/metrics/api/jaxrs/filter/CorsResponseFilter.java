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
package org.hawkular.metrics.api.jaxrs.filter;

import static org.hawkular.metrics.api.jaxrs.util.Headers.ACCESS_CONTROL_ALLOW_CREDENTIALS;
import static org.hawkular.metrics.api.jaxrs.util.Headers.ACCESS_CONTROL_ALLOW_HEADERS;
import static org.hawkular.metrics.api.jaxrs.util.Headers.ACCESS_CONTROL_ALLOW_METHODS;
import static org.hawkular.metrics.api.jaxrs.util.Headers.ACCESS_CONTROL_ALLOW_ORIGIN;
import static org.hawkular.metrics.api.jaxrs.util.Headers.ACCESS_CONTROL_MAX_AGE;
import static org.hawkular.metrics.api.jaxrs.util.Headers.ALLOW_ALL_ORIGIN;
import static org.hawkular.metrics.api.jaxrs.util.Headers.DEFAULT_CORS_ACCESS_CONTROL_ALLOW_HEADERS;
import static org.hawkular.metrics.api.jaxrs.util.Headers.DEFAULT_CORS_ACCESS_CONTROL_ALLOW_METHODS;
import static org.hawkular.metrics.api.jaxrs.util.Headers.ORIGIN;

import java.io.IOException;
import java.net.URI;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;

import org.hawkular.metrics.api.jaxrs.config.Configurable;
import org.hawkular.metrics.api.jaxrs.config.ConfigurationKey;
import org.hawkular.metrics.api.jaxrs.config.ConfigurationProperty;
import org.hawkular.metrics.api.jaxrs.util.OriginValidation;

/**
 * @author Stefan Negrea
 *
 */
@Provider
public class CorsResponseFilter implements ContainerResponseFilter {

    @Inject
    @Configurable
    @ConfigurationProperty(ConfigurationKey.ALLOWED_CORS_ORIGINS)
    private String allowedCorsOriginsConfig;

    private Set<URI> allowedOrigins;
    private boolean allowAnyOrigin;

    @Override
    public void filter(ContainerRequestContext requestContext,
            ContainerResponseContext responseContext) throws IOException {

        String requestOrigin = requestContext.getHeaderString(ORIGIN);
        if (requestOrigin == null) {
            return;
        }

        if (allowAnyOrigin || OriginValidation.isAllowedOrigin(requestOrigin, allowedOrigins)) {
            final MultivaluedMap<String, Object> responseHeaders = responseContext.getHeaders();
            responseHeaders.add(ACCESS_CONTROL_ALLOW_ORIGIN, requestOrigin);
            responseHeaders.add(ACCESS_CONTROL_ALLOW_CREDENTIALS, "true");
            responseHeaders.add(ACCESS_CONTROL_ALLOW_METHODS, DEFAULT_CORS_ACCESS_CONTROL_ALLOW_METHODS);
            responseHeaders.add(ACCESS_CONTROL_MAX_AGE, 72 * 60 * 60);
            responseHeaders.add(ACCESS_CONTROL_ALLOW_HEADERS, DEFAULT_CORS_ACCESS_CONTROL_ALLOW_HEADERS);
        } else {
            responseContext.setStatus(Response.Status.BAD_REQUEST.getStatusCode());
            return;
        }
    }

    @PostConstruct
    public void init() throws Exception {
        if (allowedCorsOriginsConfig == null || ALLOW_ALL_ORIGIN.equals(allowedCorsOriginsConfig.trim())) {
            allowAnyOrigin = true;
        } else {
            allowAnyOrigin = false;
            try {
                allowedOrigins = OriginValidation.parseAllowedCorsOrigins(allowedCorsOriginsConfig);
            } catch (Exception e) {
                throw e;
            }
        }
    }
}
