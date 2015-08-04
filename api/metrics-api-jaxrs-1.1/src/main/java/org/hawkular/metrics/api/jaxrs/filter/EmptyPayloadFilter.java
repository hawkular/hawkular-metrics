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

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.ext.Provider;

import org.jboss.resteasy.core.ResourceMethod;
import org.jboss.resteasy.core.ServerResponse;
import org.jboss.resteasy.spi.Failure;
import org.jboss.resteasy.spi.HttpRequest;
import org.jboss.resteasy.spi.interception.PreProcessInterceptor;

/**
 * Set the {@link org.hawkular.metrics.api.jaxrs.filter.EmptyPayloadFilter#EMPTY_PAYLOAD} context property to
 * {@link Boolean#TRUE} if the request is a POST.
 *
 * @author Thomas Segismont
 * @author Stefan Negrea
 */
@Provider
public class EmptyPayloadFilter implements PreProcessInterceptor {
    public static final String EMPTY_PAYLOAD = EmptyPayloadFilter.class.getName();

    @Context
    HttpServletRequest servletRequest;

    @Override
    public ServerResponse preProcess(HttpRequest request,
            ResourceMethod resourceMethod) throws Failure,
            WebApplicationException {

        if (!HttpMethod.POST.equals(resourceMethod.getMethod())) {
            return null;
        }

        servletRequest.getServletContext().setAttribute(EMPTY_PAYLOAD, Boolean.TRUE);
        return null;
    }
}
