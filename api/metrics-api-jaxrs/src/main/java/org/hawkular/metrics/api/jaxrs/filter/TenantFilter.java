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

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.ext.Provider;

import org.hawkular.metrics.api.jaxrs.ApiError;

/**
 * @author Stefan Negrea
 */
@Provider
public class TenantFilter implements ContainerRequestFilter {
    public static final String TENANT_HEADER_NAME = "Hawkular-Tenant";

    private static final String TENANT_QUERY_PARAM_NAME = "tenantId";
    private static final String MISSING_TENANT_MSG;

    static {
        MISSING_TENANT_MSG = "Tenant is not specified. Use '"
                             + TENANT_HEADER_NAME
                             + "' header or '"
                             + TENANT_QUERY_PARAM_NAME
                             + "' query parameter";
    }

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        UriInfo uriInfo = requestContext.getUriInfo();
        String path = uriInfo.getPath();

        if (path.startsWith("/tenants") || path.startsWith("/db")) {
            // Tenants and Influx handlers do not check the tenant header
            return;
        }

        String tenant = requestContext.getHeaders().getFirst(TENANT_HEADER_NAME);
        if (tenant != null && !tenant.trim().isEmpty()) {
            // We're good already
            return;
        }

        // Fail on missing tenant info
        Response response = Response.status(Status.BAD_REQUEST)
                                    .type(APPLICATION_JSON_TYPE)
                                    .entity(new ApiError(MISSING_TENANT_MSG))
                                    .build();
        requestContext.abortWith(response);
    }
}