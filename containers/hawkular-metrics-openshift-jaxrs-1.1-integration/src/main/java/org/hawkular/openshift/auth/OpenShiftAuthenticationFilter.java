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
package org.hawkular.openshift.auth;

import java.io.IOException;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author mwringe
 */
public class OpenShiftAuthenticationFilter implements Filter {

    private static final Logger logger = LoggerFactory.getLogger(OpenShiftAuthenticationFilter.class);

    public static final String AUTHORIZATION_HEADER = "authorization";

    private static final String OPENSHIFT_OAUTH = "openshift-oauth";
    private static final String HTPASSWD = "htpasswd";
    private static final String DISABLED = "disabled";
    private static final String SECURITY_OPTION = System.getProperty("hawkular-metrics.openshift.auth-methods",
                                                                  OPENSHIFT_OAUTH);


    private BasicAuthentication basicAuthentication;

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        try {
            if (SECURITY_OPTION.contains(HTPASSWD)) {
                basicAuthentication = new BasicAuthentication();
            }
        } catch (IOException e) {
            logger.error("Error trying to setup BasicAuthentication based on an htpasswd file.", e);
        }
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain)
            throws IOException, ServletException {
        HttpServletRequest request = (HttpServletRequest) servletRequest;
        HttpServletResponse response = (HttpServletResponse) servletResponse;

        // If it is set to be disabled, then just continue to the next filter.
        if (SECURITY_OPTION.equalsIgnoreCase(DISABLED)) {
            filterChain.doFilter(servletRequest, servletResponse);
            return;
        }

        // There are a few endpoint that should not be secured. If we secure the status endpoint when we cannot
        // tell if the container is up and it will always be marked as pending
        String servletPath = request.getServletPath();
        String path = request.getPathInfo();
        if (path == null || path.equals("") || path.equals("/") || path.equals("/status") || servletPath.equals
                ("/static")) {
            filterChain.doFilter(servletRequest, servletResponse);
            return;
        }

        // Get the authorization header and determine how it should be handled
        String authorizationHeader = request.getHeader(AUTHORIZATION_HEADER);
        if (authorizationHeader == null) {
            response.sendError(HttpServletResponse.SC_FORBIDDEN);
            return;
        } else if (authorizationHeader.startsWith("Bearer ") && SECURITY_OPTION.contains(OPENSHIFT_OAUTH)) {
            OpenShiftTokenAuthentication tokenAuthentication = new OpenShiftTokenAuthentication();
            tokenAuthentication.doFilter(request, response, filterChain);
            return;
        } else if (authorizationHeader.startsWith(BasicAuthentication.BASIC_PREFIX)
                && SECURITY_OPTION.contains(HTPASSWD)) {
            if (basicAuthentication != null) {
                basicAuthentication.doFilter(request, response, filterChain);
                return;
            }
        }

        response.sendError(HttpServletResponse.SC_FORBIDDEN);
    }

    @Override
    public void destroy() {

    }
}
