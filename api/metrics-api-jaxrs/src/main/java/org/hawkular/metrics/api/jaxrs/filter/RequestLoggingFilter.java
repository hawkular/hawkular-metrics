/*
 * Copyright 2014-2017 Red Hat, Inc. and/or its affiliates
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

import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.REQUEST_LOGGING_LEVEL;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.REQUEST_LOGGING_LEVEL_WRITES;
import static org.hawkular.metrics.api.jaxrs.filter.TenantFilter.TENANT_HEADER_NAME;

import java.io.IOException;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.ext.Provider;

import org.hawkular.metrics.api.jaxrs.config.Configurable;
import org.hawkular.metrics.api.jaxrs.config.ConfigurationProperty;
import org.hawkular.metrics.api.jaxrs.util.Logged;
import org.jboss.logging.Logger;

/**
 * @author jsanda
 */
@Logged
@Provider
public class RequestLoggingFilter implements ContainerRequestFilter {

    private static Logger logger = Logger.getLogger(RequestLoggingFilter.class);

    @Inject
    @Configurable
    @ConfigurationProperty(REQUEST_LOGGING_LEVEL)
    private String loggingLevelConfig;

    @Inject
    @Configurable
    @ConfigurationProperty(REQUEST_LOGGING_LEVEL_WRITES)
    private String writesLoggingLevelConfig;

    private Logger.Level logLevel;

    private Logger.Level writesLogLevel;

    @PostConstruct
    public void init() {
        logLevel = getLogLevel(loggingLevelConfig);
        writesLogLevel = getLogLevel(writesLoggingLevelConfig);
    }

    private Logger.Level getLogLevel(String config) {
        if (config == null) {
            return  null;
        }
        switch (config) {
            case "TRACE": return Logger.Level.TRACE;
            case "INFO": return Logger.Level.INFO;
            case "WARN": return Logger.Level.WARN;
            case "ERROR": return Logger.Level.ERROR;
            case "FATAL": return Logger.Level.FATAL;
            default: return Logger.Level.DEBUG;
        }
    }

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        try {
            if (isRequestLoggingEnabled()) {
                if (isInsertDataRequest(requestContext)) {
                    if (writesLogLevel != null) {
                        logRequest(writesLogLevel, requestContext);
                    }
                } else if (logLevel != null) {
                    logRequest(logLevel, requestContext);
                }
            }
        } catch (Exception e) {
            logger.info("Failed to log request", e);
        }
    }

    private boolean isRequestLoggingEnabled() {
        return !(logLevel == null && writesLogLevel == null);
    }

    private boolean isInsertDataRequest(ContainerRequestContext requestContext) {
        List<PathSegment> pathSegments = requestContext.getUriInfo().getPathSegments();
        return (requestContext.getMethod().equals("POST")) && !pathSegments.get(pathSegments.size() - 1).getPath()
                .equals("query");
    }

    private void logRequest(Logger.Level level, ContainerRequestContext requestContext) {
        String request =
                "\n" +
                "REST API request:\n" +
                "--------------------------------------\n" +
                "path: " + requestContext.getUriInfo().getPath() + "\n" +
                "segments: " + requestContext.getUriInfo().getPathSegments() + "\n" +
                "method: " + requestContext.getMethod() + "\n" +
                "query parameters: " + requestContext.getUriInfo().getQueryParameters() + "\n" +
                // Only log the tenant header to avoid logging any security sensitive information
                "Tenant: " + requestContext.getHeaders().get(TENANT_HEADER_NAME) + "\n" +
                "--------------------------------------\n";
        logger.log(level, request);
    }

}
