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

import java.util.List;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.Provider;

import org.hawkular.metrics.api.jaxrs.handler.BaseHandler;
import org.hawkular.metrics.api.jaxrs.handler.StatusHandler;
import org.hawkular.metrics.api.jaxrs.handler.VirtualClockHandler;
import org.hawkular.metrics.api.jaxrs.model.ApiError;
import org.jboss.resteasy.annotations.interception.ServerInterceptor;
import org.jboss.resteasy.core.ResourceMethod;
import org.jboss.resteasy.core.ServerResponse;
import org.jboss.resteasy.spi.Failure;
import org.jboss.resteasy.spi.HttpRequest;
import org.jboss.resteasy.spi.interception.PreProcessInterceptor;

/**
 * @author Stefan Negrea
 */
@Provider
@ServerInterceptor
public class TenantFilter implements PreProcessInterceptor {
    public static final String TENANT_HEADER_NAME = "Hawkular-Tenant";

    private static final String MISSING_TENANT_MSG;

    static {
        MISSING_TENANT_MSG = "Tenant is not specified. Use '"
                             + TENANT_HEADER_NAME
                             + "' header.";
    }

    @Override
    public ServerResponse preProcess(HttpRequest request, ResourceMethod method) throws Failure,
            WebApplicationException {
        String path = request.getUri().getPath();

        if (path.startsWith("/tenants") || path.startsWith("/db") || path.startsWith(StatusHandler.PATH)
                //On older version, the path value from localhost:8080/hawkular/metrics is an empty String instead of /
                || path.equals(BaseHandler.PATH) || path.equals("")
                || path.startsWith(VirtualClockHandler.PATH)) {
            // Tenants, Influx and status handlers do not check the tenant header
            return null;
        }

        List<String> requestHeader = request.getHttpHeaders().getRequestHeader(TENANT_HEADER_NAME);
        String tenant = requestHeader != null && !requestHeader.isEmpty() ? requestHeader.get(0) : null;
        if (tenant != null && !tenant.trim().isEmpty()) {
            // We're good already
            return null;
        }

        // Fail on missing tenant info
        Response response = Response.status(Status.BAD_REQUEST)
                .type(APPLICATION_JSON_TYPE)
                .entity(new ApiError(MISSING_TENANT_MSG))
                .build();
        return ServerResponse.copyIfNotServerResponse(response);
    }
}