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
package org.hawkular.metrics.api.jaxrs.filter;

import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.CACHE_CONTROL_HEADER;

import java.io.IOException;

import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.Provider;

import org.hawkular.metrics.api.jaxrs.config.Configurable;
import org.hawkular.metrics.api.jaxrs.config.ConfigurationProperty;

/**
 * Class for registering Cache-Control header on responses (BZ 1492011)
 * @author Joel Takvorian
 */
@Provider
public class CacheControlFilter implements ContainerResponseFilter {

    @Inject
    @Configurable
    @ConfigurationProperty(CACHE_CONTROL_HEADER)
    String cacheControl;

    @Override
    public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext)
            throws IOException {

        if (cacheControl != null) {
            MultivaluedMap<String, Object> responseHeaders = responseContext.getHeaders();
            responseHeaders.add("Cache-Control", cacheControl);
            responseHeaders.add("Vary", "Origin,Accept-Encoding");
        }
    }
}
