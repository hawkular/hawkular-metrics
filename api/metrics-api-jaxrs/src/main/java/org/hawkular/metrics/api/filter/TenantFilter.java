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
package org.hawkular.metrics.api.filter;

import org.hawkular.metrics.api.jaxrs.handler.BaseHandler;
import org.hawkular.metrics.api.jaxrs.handler.StatusHandler;

import io.vertx.ext.web.RoutingContext;

/**
 * @author Stefan Negrea
 */
public class TenantFilter {
    public static final String TENANT_HEADER_NAME = "Hawkular-Tenant";

    private static final String MISSING_TENANT_MSG;

    static {
        MISSING_TENANT_MSG = "Tenant is not specified. Use '"
                             + TENANT_HEADER_NAME
                             + "' header.";
    }

    public static void filter(RoutingContext ctx) {
        String path = ctx.normalisedPath();
        if (path.startsWith("/tenants") || path.startsWith(StatusHandler.PATH) || path.equals(BaseHandler.PATH)) {
            // Some handlers do not check the tenant header
            ctx.next();
            return;
        }

        String tenant = getTenant(ctx);
        if (tenant != null && !tenant.trim().isEmpty()) {
            ctx.next();
            return;
        }

        ctx.fail(new Exception(MISSING_TENANT_MSG));
    }

    public static String getTenant(RoutingContext ctx) {
        return ctx.request().getHeader(TENANT_HEADER_NAME);
    }
}