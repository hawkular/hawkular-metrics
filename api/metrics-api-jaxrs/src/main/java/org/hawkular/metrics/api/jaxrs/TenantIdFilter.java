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
package org.hawkular.metrics.api.jaxrs;

import java.io.IOException;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;

/**
 * @author Stefan Negrea
 *
 */
@Provider
public class TenantIdFilter implements ContainerRequestFilter {

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        if (!requestContext.getUriInfo().getAbsolutePath().toString().contains("tenants")) {
            String tenantIdHeaderParam = requestContext.getHeaderString("tenantId");
            if (tenantIdHeaderParam == null || tenantIdHeaderParam.isEmpty()) {
                String tenandIdQueryParam = requestContext.getUriInfo().getQueryParameters().getFirst("tenantId");
                if (tenandIdQueryParam == null || tenandIdQueryParam.isEmpty()) {
                    requestContext.abortWith(Response.status(Response.Status.BAD_REQUEST)
                            .entity(new ApiError("Tenant id not specified")).build());
                }

                requestContext.getHeaders().add("tenantId", tenandIdQueryParam);
            }
        }
    }
}