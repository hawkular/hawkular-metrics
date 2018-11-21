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
package org.hawkular.metrics.api.jaxrs.handler;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import static org.hawkular.metrics.api.jaxrs.util.ApiUtils.badRequest;
import static org.hawkular.metrics.api.jaxrs.util.ApiUtils.collectionToResponse;
import static org.hawkular.metrics.api.jaxrs.util.ApiUtils.serverError;

import java.net.URI;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.hawkular.metrics.api.jaxrs.handler.observer.TenantCreatedObserver;
import org.hawkular.metrics.api.jaxrs.util.Logged;
import org.hawkular.metrics.core.jobs.JobsService;
import org.hawkular.metrics.core.service.MetricsService;
import org.hawkular.metrics.model.ApiError;
import org.hawkular.metrics.model.TenantDefinition;
import org.jboss.logging.Logger;
import org.jboss.resteasy.annotations.GZIP;

import com.google.common.collect.ImmutableMap;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

/**
 * @author Thomas Segismont
 */
@Path("/tenants")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
@GZIP
@Api(tags = "Tenant")
@ApplicationScoped
@Logged
public class TenantsHandler {

    private Logger logger = Logger.getLogger(TenantsHandler.class);

    @Inject
    private MetricsService metricsService;

    @Inject
    private JobsService jobsService;

    @POST
    @ApiOperation(value = "Create a new tenant.", notes = "Clients are not required to create explicitly create a "
            + "tenant before starting to store metric data. It is recommended to do so however to ensure that there "
            + "are no tenant id naming collisions and to provide default data retention settings.")
    @ApiResponses(value = {
            @ApiResponse(code = 201, message = "Tenant has been succesfully created."),
            @ApiResponse(code = 400, message = "Missing or invalid retention properties. ",
                    response = ApiError.class),
            @ApiResponse(code = 409, message = "Given tenant id has already been created.",
                    response = ApiError.class),
            @ApiResponse(code = 500, message = "An unexpected error occured while trying to create a tenant.",
                    response = ApiError.class)
    })
    public void createTenant(
            @Suspended AsyncResponse asyncResponse,
            @ApiParam(required = true) TenantDefinition tenantDefinition,
            @ApiParam(value = "Overwrite previously created tenant configuration if it exists. "
                    + "Only data retention settings are overwriten; existing metrics and data points are unnafected. "
                    + "Defaults to false.",
                    required = false) @DefaultValue("false") @QueryParam("overwrite") Boolean overwrite,
            @Context UriInfo uriInfo
    ) {
        URI location = uriInfo.getBaseUriBuilder().path("/tenants").build();
        metricsService.createTenant(tenantDefinition.toTenant(), overwrite)
                .subscribe(new TenantCreatedObserver(asyncResponse, location));
    }

    @GET
    @ApiOperation(value = "Returns a list of tenants.", response = TenantDefinition.class, responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Returned a list of tenants successfully."),
            @ApiResponse(code = 204, message = "No tenants were found."),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching tenants.",
                    response = ApiError.class)
    })
    public void findTenants(@Suspended AsyncResponse asyncResponse) {
        metricsService.getTenants().map(TenantDefinition::new).toList().subscribe(
                tenants -> asyncResponse.resume(collectionToResponse(tenants)),
                error -> asyncResponse.resume(serverError(error))
        );
    }

    @DELETE
    @Path("/{id}")
    @ApiOperation(value = "Asynchronously deletes a tenant. All metrics and their data points will be deleted. " +
            "Internal indexes are also updated. A response is returned as soon as a job to delete the tenant gets " +
            "created and scheduled. The response returns the id of the tenant deletion job.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Tenant deletion job gets scheduled. The job id is returned."),
            @ApiResponse(code = 500, message = "Unexpected error occurred trying to scheduled the tenant deletion job.")
    })
    public void deleteTenant(@Suspended AsyncResponse asyncResponse, @PathParam("id") String id) {
        jobsService.submitDeleteTenantJob(id, "Delete" + id).subscribe(
                jobDetails -> asyncResponse.resume(Response.ok(ImmutableMap.of("jobId",
                        jobDetails.getJobId().toString())).build()),
                t -> {
                    logger.warn("Deleting tenant [" + id + "] failed", t);
                    asyncResponse.resume(badRequest(t));
                }
        );
    }
}
