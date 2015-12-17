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
package org.hawkular.metrics.api.jaxrs.handler;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import java.net.URI;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.hawkular.metrics.api.jaxrs.util.ApiUtils;
import org.hawkular.metrics.core.service.MetricsService;
import org.hawkular.metrics.model.ApiError;
import org.hawkular.metrics.model.TenantDefinition;
import org.hawkular.metrics.model.exception.TenantAlreadyExistsException;

/**
 * @author Thomas Segismont
 */
@Path("/tenants")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
public class TenantsHandler {

    @Inject
    private MetricsService metricsService;

    @POST
    public Response createTenant(
            TenantDefinition tenantDefinition,
            @Context UriInfo uriInfo
    ) {
        URI location = uriInfo.getBaseUriBuilder().path("/tenants").build();
        try {
            metricsService.createTenant(tenantDefinition.toTenant())
                    .toBlocking().lastOrDefault(null);
            return Response.created(location).build();
        } catch (TenantAlreadyExistsException e) {
            String message = "A tenant with id [" + e.getTenantId() + "] already exists";
            return Response.status(Response.Status.CONFLICT).entity(new ApiError(message)).build();
        } catch (Exception e) {
            return ApiUtils.serverError(e);
        }
    }

    @GET
    public Response findTenants() {
        try {
            return metricsService.getTenants().map(TenantDefinition::new).toList()
                    .map(ApiUtils::collectionToResponse).toBlocking().lastOrDefault(null);
        } catch (Exception e) {
            return ApiUtils.serverError(e);
        }
    }
}
