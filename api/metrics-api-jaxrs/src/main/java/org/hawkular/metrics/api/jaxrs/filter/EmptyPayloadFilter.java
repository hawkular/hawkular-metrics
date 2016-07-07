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

import java.io.IOException;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.ext.Provider;

/**
 * Set the {@link org.hawkular.metrics.api.jaxrs.filter.EmptyPayloadFilter#EMPTY_PAYLOAD} context property to
 * {@link Boolean#TRUE} if the request is a POST.
 *
 * @author Thomas Segismont
 */
@Provider
public class EmptyPayloadFilter implements ContainerRequestFilter {
    public static final String EMPTY_PAYLOAD = EmptyPayloadFilter.class.getName();

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        if (HttpMethod.POST.equals(requestContext.getMethod()) ||
                HttpMethod.PUT.equals(requestContext.getMethod())) {
            requestContext.setProperty(EMPTY_PAYLOAD, Boolean.TRUE);
        }
    }
}
