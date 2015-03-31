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

import static org.hawkular.metrics.api.jaxrs.util.ResponseUtils.alreadyExistsFallback;
import static org.hawkular.metrics.api.jaxrs.util.ResponseUtils.created;
import static org.hawkular.metrics.api.jaxrs.util.ResponseUtils.emptyPayload;

import java.net.URI;
import java.util.List;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.hawkular.metrics.api.jaxrs.service.TenantsServiceBase;
import org.hawkular.metrics.api.jaxrs.util.ResponseUtils;
import org.hawkular.metrics.core.api.MetricsService;
import org.hawkular.metrics.core.api.Tenant;
import org.hawkular.metrics.core.api.TenantAlreadyExistsException;

import com.google.common.util.concurrent.FutureFallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * @author Thomas Segismont
 */
@ApplicationScoped
public class TenantsHandler extends TenantsServiceBase {

    // TODO: add back retention settings

    @Inject
    private MetricsService metricsService;

    @Override
    protected ListenableFuture<Response> _createTenant(Tenant params, UriInfo uriInfo) {
        if (params == null) {
            return emptyPayload();
        }
        ListenableFuture<Void> insertFuture = metricsService.createTenant(params);
        URI location = uriInfo.getBaseUriBuilder().path("/tenants").build();
        ListenableFuture<Response> responseFuture = Futures.transform(insertFuture, created(location));
        FutureFallback<Response> fallback = alreadyExistsFallback(
                TenantAlreadyExistsException.class,
                TenantsHandler::alreadyExistsError
        );
        return Futures.withFallback(responseFuture, fallback);
    }

    private static ApiError alreadyExistsError(TenantAlreadyExistsException e) {
        return new ApiError("Tenant with id " + e.getTenantId() + " already exists");
    }

    @Override
    protected ListenableFuture<Response> _findTenants() {
        ListenableFuture<List<Tenant>> future = metricsService.getTenants();
        return Futures.transform(future, ResponseUtils.MAP_COLLECTION);
    }
}
