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

import java.io.IOException;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.Provider;

/**
 * @author Stefan Negrea
 *
 */
@Provider
public class CorsFilter implements ContainerResponseFilter {

    public static final String DEFAULT_ALLOWED_METHODS = "GET, POST, PUT, DELETE, OPTIONS, HEAD";
    public static final String DEFAULT_ALLOWED_HEADERS = "origin,accept,content-type,hawkular-tenant";

    @Override
    public void filter(ContainerRequestContext requestContext,
            ContainerResponseContext responseContext) throws IOException {
        final MultivaluedMap<String, Object> headers = responseContext
                .getHeaders();

        String origin = "*";
        if (headers.get("origin") != null && headers.get("origin").size() == 1
                && headers.get("origin").get(0) != null
                && !headers.get("origin").get(0).equals("null")) {
            origin = headers.get("origin").get(0).toString();
        }
        headers.add("Access-Control-Allow-Origin", origin);

        headers.add("Access-Control-Allow-Credentials", "true");
        headers.add("Access-Control-Allow-Methods", DEFAULT_ALLOWED_METHODS);
        headers.add("Access-Control-Max-Age", 72 * 60 * 60);
        headers.add("Access-Control-Allow-Headers", DEFAULT_ALLOWED_HEADERS);
    }
}
