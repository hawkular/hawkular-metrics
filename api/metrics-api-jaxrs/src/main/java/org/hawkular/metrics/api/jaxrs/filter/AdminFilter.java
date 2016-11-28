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

package org.hawkular.metrics.api.jaxrs.filter;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;

import java.io.IOException;

import javax.annotation.Priority;
import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.ext.Provider;

import org.hawkular.metrics.model.ApiError;
import org.hawkular.metrics.sysconfig.ConfigurationService;

import com.google.common.base.Charsets;
import com.google.common.hash.Hashing;

/**
 * @author Stefan Negrea
 */
@Provider
@PreMatching
@Priority(20)
public class AdminFilter implements ContainerRequestFilter {
    public static final String TENANT_HEADER_NAME = "Hawkular-Tenant";
    public static final String ADMIN_TOKEN_HEADER_NAME = "Hawkular-Admin-Token";

    private static final String TENANT_MISSING;
    private static final String ADMIN_TOKEN_INCORRECT;
    private static final String ADMIN_TOKEN_MISSING;

    static {
        TENANT_MISSING = "Tenant is not specified. Use '"
                + TENANT_HEADER_NAME
                + "' header.";

        ADMIN_TOKEN_MISSING = "Admin token is not specified. Use '"
                + ADMIN_TOKEN_HEADER_NAME +"' header";

        ADMIN_TOKEN_INCORRECT = "Admin token is wrong or not specified.";
    }

    @Inject
    private ConfigurationService configurationService;

    private String savedAdminToken;

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        UriInfo uriInfo = requestContext.getUriInfo();
        String path = uriInfo.getPath();

        if (path.startsWith("/tenants") || path.startsWith("/admin")) {
            String tenant = requestContext.getHeaders().getFirst(TENANT_HEADER_NAME);
            if (tenant == null || tenant.trim().isEmpty()) {
                // Fail on missing tenant info
                Response response = Response.status(Status.BAD_REQUEST)
                        .type(APPLICATION_JSON_TYPE)
                        .entity(new ApiError(TENANT_MISSING))
                        .build();
                requestContext.abortWith(response);
                return;
            }

            String adminToken = requestContext.getHeaders().getFirst(ADMIN_TOKEN_HEADER_NAME);
            if (adminToken != null && !adminToken.trim().isEmpty()) {
                if(!validAdminToken(adminToken)) {
                    // Admin token is not valid
                    Response response = Response.status(Status.FORBIDDEN)
                            .type(APPLICATION_JSON_TYPE)
                            .entity(new ApiError(ADMIN_TOKEN_INCORRECT))
                            .build();
                    requestContext.abortWith(response);
                    return;
                }
            } else {
                // Fail on missing admin token
                Response response = Response.status(Status.BAD_REQUEST)
                        .type(APPLICATION_JSON_TYPE)
                        .entity(new ApiError(ADMIN_TOKEN_MISSING))
                        .build();
                requestContext.abortWith(response);
                return;
            }
        }
    }

    private boolean validAdminToken(String adminToken) {
        if (savedAdminToken == null) {
            savedAdminToken = configurationService.load("org.hawkular.metrics", "admin.token").toBlocking()
                    .firstOrDefault("");
        }

        adminToken = Hashing.sha256().newHasher().putString(adminToken, Charsets.UTF_8).hash().toString();

        if (adminToken.equals(savedAdminToken)) {
            return true;
        }

        return false;
    }
}