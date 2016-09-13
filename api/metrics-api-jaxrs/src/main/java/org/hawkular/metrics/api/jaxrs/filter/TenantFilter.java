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

import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.ext.Provider;

import org.hawkular.metrics.api.jaxrs.handler.BaseHandler;
import org.hawkular.metrics.api.jaxrs.handler.StatusHandler;
import org.hawkular.metrics.model.ApiError;
import org.hawkular.metrics.sysconfig.ConfigurationService;

import com.google.common.base.Charsets;
import com.google.common.hash.Hashing;

/**
 * @author Stefan Negrea
 */
@Provider
@PreMatching
public class TenantFilter implements ContainerRequestFilter {
    public static final String TENANT_HEADER_NAME = "Hawkular-Tenant";
    public static final String ADMIN_TOKEN_HEADER_NAME = "Hawkular-Admin-Token";

    private static final String MISSING_TENANT_MSG;
    private static final String WRONG_ADMIN_TOKEN_MSG;

    static {
        MISSING_TENANT_MSG = "Tenant is not specified. Use '"
                             + TENANT_HEADER_NAME
                             + "' header.";

        WRONG_ADMIN_TOKEN_MSG = "Admin token is wrong or not specified.";
    }

    @Inject
    private ConfigurationService configurationService;

    private String savedAdminToken;

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        UriInfo uriInfo = requestContext.getUriInfo();
        String path = uriInfo.getPath();

        if (path.startsWith("/tenants")) {
            String adminToken = requestContext.getHeaders().getFirst(ADMIN_TOKEN_HEADER_NAME);
            if (adminToken != null && !adminToken.trim().isEmpty() && validAdminToken(adminToken)) {
                return;
            } else {
                // Fail on bad admin token
                Response response = Response.status(Status.BAD_REQUEST)
                        .type(APPLICATION_JSON_TYPE)
                        .entity(new ApiError(WRONG_ADMIN_TOKEN_MSG))
                        .build();
                requestContext.abortWith(response);
                return;
            }
        }

        if (path.startsWith(StatusHandler.PATH) || path.equals(BaseHandler.PATH)) {
            // Some handlers do not check the tenant header
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