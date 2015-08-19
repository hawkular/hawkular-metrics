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
package org.hawkular.metrics.api.jaxrs.filter;

import static org.hawkular.metrics.api.jaxrs.util.Headers.ACCESS_CONTROL_ALLOW_CREDENTIALS;
import static org.hawkular.metrics.api.jaxrs.util.Headers.ACCESS_CONTROL_ALLOW_HEADERS;
import static org.hawkular.metrics.api.jaxrs.util.Headers.ACCESS_CONTROL_ALLOW_METHODS;
import static org.hawkular.metrics.api.jaxrs.util.Headers.ACCESS_CONTROL_ALLOW_ORIGIN;
import static org.hawkular.metrics.api.jaxrs.util.Headers.ACCESS_CONTROL_MAX_AGE;
import static org.hawkular.metrics.api.jaxrs.util.Headers.DEFAULT_CORS_ACCESS_CONTROL_ALLOW_HEADERS;
import static org.hawkular.metrics.api.jaxrs.util.Headers.DEFAULT_CORS_ACCESS_CONTROL_ALLOW_METHODS;
import static org.hawkular.metrics.api.jaxrs.util.Headers.ORIGIN;

import java.io.IOException;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.Provider;

/**
 * @author Stefan Negrea
 *
 */
@Provider
@PreMatching
public class CorsResponseFilter implements ContainerResponseFilter {

    @Override
    public void filter(ContainerRequestContext requestContext,
            ContainerResponseContext responseContext) throws IOException {
        final MultivaluedMap<String, Object> headers = responseContext
                .getHeaders();

        String origin = "*";
        if (headers.get(ORIGIN) != null && headers.get(ORIGIN).size() != 0
                && headers.get(ORIGIN).get(0) != null
                && !headers.get(ORIGIN).get(0).equals("null")) {
            origin = headers.get(ORIGIN).get(0).toString();
        }
        headers.add(ACCESS_CONTROL_ALLOW_ORIGIN, origin);
        headers.add(ACCESS_CONTROL_ALLOW_CREDENTIALS, "true");
        headers.add(ACCESS_CONTROL_ALLOW_METHODS, DEFAULT_CORS_ACCESS_CONTROL_ALLOW_METHODS);
        headers.add(ACCESS_CONTROL_MAX_AGE, 72 * 60 * 60);
        headers.add(ACCESS_CONTROL_ALLOW_HEADERS, DEFAULT_CORS_ACCESS_CONTROL_ALLOW_HEADERS);
    }
}
