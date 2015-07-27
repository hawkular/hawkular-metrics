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

package org.hawkular.metrics.api.jaxrs.handler.observer;

import java.net.URI;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.hawkular.metrics.api.jaxrs.ApiError;
import org.hawkular.metrics.core.api.TenantAlreadyExistsException;

/**
 * An implementation of {@code EntityCreatedObserver} for tenant entities.
 *
 * @author Thomas Segismont
 */
public class TenantCreatedObserver extends EntityCreatedObserver<TenantAlreadyExistsException> {

    public TenantCreatedObserver(AsyncResponse asyncResponse, URI location) {
        super(
                asyncResponse,
                location,
                TenantAlreadyExistsException.class,
                TenantCreatedObserver::getTenantAlreadyExistsResponse
        );
    }

    private static Response getTenantAlreadyExistsResponse(TenantAlreadyExistsException e) {
        String message = "A tenant with id [" + e.getTenantId() + "] already exists";
        return Response.status(Status.CONFLICT).entity(new ApiError(message)).build();
    }
}
