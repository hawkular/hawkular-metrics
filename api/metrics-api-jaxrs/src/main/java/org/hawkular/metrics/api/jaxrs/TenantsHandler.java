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

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;
import static javax.ws.rs.core.Response.Status;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Response;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;

import org.hawkular.metrics.core.api.MetricType;
import org.hawkular.metrics.core.api.MetricsService;
import org.hawkular.metrics.core.api.Tenant;
import org.hawkular.metrics.core.api.TenantAlreadyExistsException;

/**
 * @author Thomas Segismont
 */
@Api(value = "/tenants", description = "Tenants related REST interface")
@Path("/tenants")
public class TenantsHandler {

    @Inject
    private MetricsService metricsService;

    @POST
    @ApiOperation(value = "Create a new tenant. ", notes = "Clients are not required to create explicitly create a "
            + "tenant before starting to store metric data. It is recommended to do so however to ensure that there "
            + "are no tenant id naming collisions and to provide default data retention settings. ")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Tenant has been succesfully created."),
            @ApiResponse(code = 400, message = "Retention properties are invalid. "),
            @ApiResponse(code = 409, message = "Given tenant id has already been created."),
            @ApiResponse(code = 500, message = "An unexpected error occured while trying to create a tenant.")})
    @Consumes(APPLICATION_JSON)
    public void createTenant(@Suspended AsyncResponse asyncResponse, TenantParams params) {
        Tenant tenant = new Tenant().setId(params.getId());
        for (String type : params.getRetentions().keySet()) {
            if (type.equals(MetricType.NUMERIC.getText())) {
                tenant.setRetention(MetricType.NUMERIC, params.getRetentions().get(type));
            } else if (type.equals(MetricType.AVAILABILITY.getText())) {
                tenant.setRetention(MetricType.AVAILABILITY, params.getRetentions().get(type));
            } else {
                Map<String, String> errors = ImmutableMap.of("errorMessage", "The retentions property is invalid. ["
                    + type + "] is not a recognized metric type");
                asyncResponse.resume(Response.status(Status.BAD_REQUEST).entity(errors).type(APPLICATION_JSON_TYPE)
                    .build());
                return;
            }
        }
        ListenableFuture<Void> insertFuture = metricsService.createTenant(tenant);
        Futures.addCallback(insertFuture, new FutureCallback<Void>() {
            @Override
            public void onSuccess(Void result) {
                asyncResponse.resume(Response.ok().type(APPLICATION_JSON_TYPE).build());
            }

            @Override
            public void onFailure(Throwable t) {
                if (t instanceof TenantAlreadyExistsException) {
                    TenantAlreadyExistsException exception = (TenantAlreadyExistsException) t;
                    Map<String, String> errors = ImmutableMap.of("errorMsg",
                        "A tenant with id [" + exception.getTenantId() + "] already exists");
                    asyncResponse.resume(Response.status(Status.CONFLICT).entity(errors).type(APPLICATION_JSON_TYPE)
                        .build());
                    return;
                }
                Map<String, String> errors = ImmutableMap.of("errorMsg", "Failed to create tenant due to an "
                    + "unexpected error: " + Throwables.getRootCause(t).getMessage());
                asyncResponse.resume(Response.status(Status.INTERNAL_SERVER_ERROR).entity(errors)
                    .type(APPLICATION_JSON_TYPE).build());
            }
        });
    }

    @GET
    @ApiOperation(value = "Returns a list of tenants.")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Returned a list of tenants successfully."),
            @ApiResponse(code = 204, message = "No tenants were found."),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching tenants.")})
    @Consumes(APPLICATION_JSON)
    public void findTenants(@Suspended AsyncResponse response) {
        ListenableFuture<List<Tenant>> tenantsFuture = metricsService.getTenants();
        Futures.addCallback(tenantsFuture, new FutureCallback<Collection<Tenant>>() {
            @Override
            public void onSuccess(Collection<Tenant> tenants) {
                if (tenants.isEmpty()) {
                    response.resume(Response.ok().status(Status.NO_CONTENT).build());
                    return;
                }
                List<TenantParams> output = new ArrayList<>(tenants.size());
                for (Tenant t : tenants) {
                    Map<String, Integer> retentions = new HashMap<>();
                    Integer numericRetention = t.getRetentionSettings().get(MetricType.NUMERIC);
                    Integer availabilityRetention = t.getRetentionSettings().get(MetricType.AVAILABILITY);
                    if (numericRetention != null) {
                        retentions.put(MetricType.NUMERIC.getText(), numericRetention);
                    }
                    if (availabilityRetention != null) {
                        retentions.put(MetricType.AVAILABILITY.getText(), availabilityRetention);
                    }
                    output.add(new TenantParams(t.getId(), retentions));
                }
                response.resume(Response.status(Status.OK).entity(output).type(APPLICATION_JSON_TYPE).build());
            }

            @Override
            public void onFailure(Throwable t) {
                Map<String, String> errors = ImmutableMap.of("errorMsg", "Failed to fetch tenants due to an "
                    + "unexpected error: " + Throwables.getRootCause(t).getMessage());
                response.resume(Response.status(Status.INTERNAL_SERVER_ERROR).entity(errors)
                    .type(APPLICATION_JSON_TYPE).build());
            }
        });
    }
}
