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
package org.hawkular.metrics.security;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;

import java.io.IOException;

import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;

import org.hawkular.accounts.api.model.Persona;
import org.hawkular.metrics.model.ApiError;
import org.jboss.logging.Logger;

/**
 * When metrics is deployed in the full Hawkular server, the value of the tenant header is determined by the current
 * Persona which is injected from PersonaService. The person is only injected when the request supplies valid
 * credentials. This filter will not execute when the request has invalid credentials or no credentials at all. In
 * the former case the a 401 status code is included in the response. In the latter case, a 500 status code is included
 * in the response. See AuthenticationITest.groovy for examples.
 *
 * @author jsanda
 */
@Provider
public class PersonaFilter implements ContainerRequestFilter {

    private final Logger log = Logger.getLogger(PersonaFilter.class);

    public static final String TENANT_HEADER_NAME = "Hawkular-Tenant";

    public static final String MISSING_TENANT_MSG = "Tenant is not specified. Use '" + TENANT_HEADER_NAME
            + "' header.";

    @Inject
    Persona persona;

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        if (!checkPersona()) {
            Response response = Response.status(Response.Status.BAD_REQUEST)
                    .type(APPLICATION_JSON_TYPE)
                    .entity(new ApiError(MISSING_TENANT_MSG))
                    .build();
            requestContext.abortWith(response);
        } else {
            requestContext.getHeaders().putSingle(TENANT_HEADER_NAME, persona.getId());
        }
    }

    private boolean checkPersona() {
        if (persona == null) {
            log.warn("Persona is null. Possible issue with accounts integration ? ");
            return false;
        }
        if (isEmpty(persona.getId())) {
            log.warn("Persona is empty. Possible issue with accounts integration ? ");
            return false;
        }
        return true;
    }

    private boolean isEmpty(String s) {
        return s == null || s.trim().isEmpty();
    }

}
