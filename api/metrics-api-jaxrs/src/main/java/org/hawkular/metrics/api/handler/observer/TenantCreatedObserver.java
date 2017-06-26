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

package org.hawkular.metrics.api.handler.observer;

import java.net.URI;

import org.hawkular.metrics.model.exception.TenantAlreadyExistsException;

import io.vertx.ext.web.RoutingContext;

/**
 * An implementation of {@code EntityCreatedObserver} for tenant entities.
 *
 * @author Thomas Segismont
 */
public class TenantCreatedObserver extends EntityCreatedObserver<TenantAlreadyExistsException> {

    public TenantCreatedObserver(RoutingContext ctx, URI location) {
        super(
                ctx,
                location,
                TenantAlreadyExistsException.class,
                TenantCreatedObserver::getTenantAlreadyExistsResponse
        );
    }

    private static String getTenantAlreadyExistsResponse(TenantAlreadyExistsException e) {
        return "A tenant with id [" + e.getTenantId() + "] already exists";
    }
}
