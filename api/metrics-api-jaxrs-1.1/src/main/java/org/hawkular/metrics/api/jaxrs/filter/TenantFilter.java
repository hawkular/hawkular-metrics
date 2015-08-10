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

import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response.Status;

import org.hawkular.metrics.api.jaxrs.ApiError;
import org.hawkular.metrics.api.jaxrs.handler.BaseHandler;
import org.hawkular.metrics.api.jaxrs.handler.StatusHandler;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author Stefan Negrea
 */
public class TenantFilter implements Filter {
    public static final String TENANT_HEADER_NAME = "Hawkular-Tenant";

    private static final String MISSING_TENANT_MSG;

    static {
        MISSING_TENANT_MSG = "Tenant is not specified. Use '"
                             + TENANT_HEADER_NAME
                             + "' header.";
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {

        final HttpServletRequest httpRequest = (HttpServletRequest) request;
        String path = httpRequest.getRequestURI();

        if (path.startsWith("/tenants") || path.startsWith("/db") || path.startsWith(StatusHandler.PATH)
            || path.equals(BaseHandler.PATH)) {
            // Tenants, Influx and status handlers do not check the tenant header
            chain.doFilter(request, response);
            return;
        }

        String tenant = httpRequest.getHeader(TENANT_HEADER_NAME);
        if (tenant != null && !tenant.trim().isEmpty()) {
            // We're good already
            chain.doFilter(request, response);
            return;
        }

        // Fail on missing tenant info
        ObjectMapper mapper = new ObjectMapper();
        final HttpServletResponse httpResponse = (HttpServletResponse) response;
        httpResponse.setStatus(Status.BAD_REQUEST.getStatusCode());
        httpResponse.setContentType(APPLICATION_JSON_TYPE.toString());
        mapper.writeValue(response.getWriter(), new ApiError(MISSING_TENANT_MSG));
    }

    @Override
    public void destroy() {

    }

    @Override
    public void init(FilterConfig arg0) throws ServletException {
    }
}