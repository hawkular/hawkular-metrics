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
import static org.hawkular.metrics.api.jaxrs.util.Headers.ACCESS_CONTROL_REQUEST_METHOD;
import static org.hawkular.metrics.api.jaxrs.util.Headers.DEFAULT_CORS_ACCESS_CONTROL_ALLOW_HEADERS;
import static org.hawkular.metrics.api.jaxrs.util.Headers.DEFAULT_CORS_ACCESS_CONTROL_ALLOW_METHODS;
import static org.hawkular.metrics.api.jaxrs.util.Headers.ORIGIN;

import java.io.IOException;
import java.util.Enumeration;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.ext.Provider;

/**
 * @author Stefan Negrea
 *
 */
@Provider
public class CorsFilter implements Filter {

    public static final String PREFLIGHT_METHOD = "OPTIONS";

    @Override
    public void destroy() {
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {

        final HttpServletRequest httpRequest = (HttpServletRequest) request;
        final HttpServletResponse httpResponse = (HttpServletResponse) response;

        String origin = "*";
        Enumeration<String> originHeaders = httpRequest.getHeaders(ORIGIN);
        if (originHeaders != null && originHeaders.hasMoreElements()) {
            String originHeaderValue = originHeaders.nextElement();
            if (originHeaderValue != null && !originHeaderValue.equals("null")) {
                origin = originHeaderValue;
            }
        }
        httpResponse.addHeader(ACCESS_CONTROL_ALLOW_ORIGIN, origin);

        httpResponse.addHeader(ACCESS_CONTROL_ALLOW_CREDENTIALS, "true");
        httpResponse.addHeader(ACCESS_CONTROL_ALLOW_METHODS, DEFAULT_CORS_ACCESS_CONTROL_ALLOW_METHODS);
        httpResponse.addHeader(ACCESS_CONTROL_MAX_AGE, (72 * 60 * 60) + "");
        httpResponse.addHeader(ACCESS_CONTROL_ALLOW_HEADERS, DEFAULT_CORS_ACCESS_CONTROL_ALLOW_HEADERS);

        if (!isPreflightRequest((HttpServletRequest) request)) {
            chain.doFilter(request, response);
        }
    }

    @Override
    public void init(FilterConfig arg0) throws ServletException {
    }

    private boolean isPreflightRequest(final HttpServletRequest request) {
        if (request.getHeader(ACCESS_CONTROL_REQUEST_METHOD) != null &&
                request.getMethod() != null &&
                request.getMethod().equalsIgnoreCase(PREFLIGHT_METHOD)) {
            return true;
        }

        return false;
    }
}