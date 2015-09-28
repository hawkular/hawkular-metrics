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

import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.ALLOWED_CORS_ACCESS_CONTROL_ALLOW_HEADERS;
import static org.hawkular.metrics.api.jaxrs.util.Headers.ACCESS_CONTROL_ALLOW_CREDENTIALS;
import static org.hawkular.metrics.api.jaxrs.util.Headers.ACCESS_CONTROL_ALLOW_HEADERS;
import static org.hawkular.metrics.api.jaxrs.util.Headers.ACCESS_CONTROL_ALLOW_METHODS;
import static org.hawkular.metrics.api.jaxrs.util.Headers.ACCESS_CONTROL_ALLOW_ORIGIN;
import static org.hawkular.metrics.api.jaxrs.util.Headers.ACCESS_CONTROL_MAX_AGE;
import static org.hawkular.metrics.api.jaxrs.util.Headers.ACCESS_CONTROL_REQUEST_METHOD;
import static org.hawkular.metrics.api.jaxrs.util.Headers.DEFAULT_CORS_ACCESS_CONTROL_ALLOW_HEADERS;
import static org.hawkular.metrics.api.jaxrs.util.Headers.DEFAULT_CORS_ACCESS_CONTROL_ALLOW_METHODS;
import static org.hawkular.metrics.api.jaxrs.util.Headers.ORIGIN;

import java.io.IOException;

import javax.inject.Inject;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.hawkular.metrics.api.jaxrs.config.Configurable;
import org.hawkular.metrics.api.jaxrs.config.ConfigurationProperty;
import org.hawkular.metrics.api.jaxrs.util.OriginValidation;

/**
 * @author Stefan Negrea
 */
public class CorsFilter implements Filter {
    private static final String PREFLIGHT_METHOD = "OPTIONS";

    @Inject
    OriginValidation validator;

    @Inject
    @Configurable
    @ConfigurationProperty(ALLOWED_CORS_ACCESS_CONTROL_ALLOW_HEADERS)
    String extraAccesControlAllowHeaders;

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        HttpServletResponse httpResponse = (HttpServletResponse) response;

        //NOT a CORS request
        String requestOrigin = httpRequest.getHeader(ORIGIN);

        if (requestOrigin == null) {
            chain.doFilter(request, response);
            return;
        }

        if (validator.isAllowedOrigin(requestOrigin)) {
            httpResponse.addHeader(ACCESS_CONTROL_ALLOW_ORIGIN, requestOrigin);
            httpResponse.addHeader(ACCESS_CONTROL_ALLOW_CREDENTIALS, "true");
            httpResponse.addHeader(ACCESS_CONTROL_ALLOW_METHODS, DEFAULT_CORS_ACCESS_CONTROL_ALLOW_METHODS);
            httpResponse.addHeader(ACCESS_CONTROL_MAX_AGE, (72 * 60 * 60) + "");

            if (extraAccesControlAllowHeaders != null) {
                httpResponse.addHeader(ACCESS_CONTROL_ALLOW_HEADERS,
                        DEFAULT_CORS_ACCESS_CONTROL_ALLOW_HEADERS + "," + extraAccesControlAllowHeaders.trim());
            } else {
                httpResponse.addHeader(ACCESS_CONTROL_ALLOW_HEADERS, DEFAULT_CORS_ACCESS_CONTROL_ALLOW_HEADERS);
            }

            if (!isPreflightRequest((HttpServletRequest) request)) {
                chain.doFilter(request, response);
            }
        } else {
            httpResponse.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            httpResponse.setContentLength(0);
        }
    }

    private boolean isPreflightRequest(final HttpServletRequest request) {
        return request.getHeader(ACCESS_CONTROL_REQUEST_METHOD) != null
                && request.getMethod() != null
                && request.getMethod().equalsIgnoreCase(PREFLIGHT_METHOD);

    }

    @Override
    public void init(FilterConfig config) throws ServletException {
    }

    @Override
    public void destroy() {
    }
}