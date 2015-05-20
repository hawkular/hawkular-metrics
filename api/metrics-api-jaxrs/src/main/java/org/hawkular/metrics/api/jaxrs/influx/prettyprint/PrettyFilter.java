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
package org.hawkular.metrics.api.jaxrs.influx.prettyprint;

import java.io.IOException;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.ext.Provider;

/**
 * Check if the request targets the Influx endpoint and the <em>pretty</em> query parameter is set to true. In this
 * case, the {@link #PRETTY_PRINT} context property is set to {@link Boolean#TRUE}.
 *
 * @author Thomas Segismont
 * @see PrettyInterceptor
 */
@Provider
public class PrettyFilter implements ContainerRequestFilter {
    static final String PRETTY_PRINT = PrettyFilter.class.getName();

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        UriInfo uriInfo = requestContext.getUriInfo();
        if (!uriInfo.getPath().startsWith("/db")) {
            return;
        }
        requestContext.setProperty(PRETTY_PRINT, Boolean.valueOf(uriInfo.getQueryParameters().getFirst("pretty")));
    }
}
