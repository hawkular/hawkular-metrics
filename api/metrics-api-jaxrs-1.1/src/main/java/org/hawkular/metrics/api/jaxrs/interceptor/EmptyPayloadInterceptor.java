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

package org.hawkular.metrics.api.jaxrs.interceptor;

import static java.lang.Boolean.TRUE;
import static org.hawkular.metrics.api.jaxrs.filter.EmptyPayloadFilter.EMPTY_PAYLOAD;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.ext.Provider;

import org.jboss.resteasy.annotations.interception.ServerInterceptor;
import org.jboss.resteasy.spi.interception.MessageBodyReaderContext;
import org.jboss.resteasy.spi.interception.MessageBodyReaderInterceptor;

/**
 * Make sure body is present if the {@link org.hawkular.metrics.api.jaxrs.filter.EmptyPayloadFilter#EMPTY_PAYLOAD}
 * context property is set to {@link Boolean#TRUE}.
 *
 * @author Thomas Segismont
 * @author Stefan Negrea
 * @see org.hawkular.metrics.api.jaxrs.filter.EmptyPayloadFilter
 */
@Provider
@ServerInterceptor
public class EmptyPayloadInterceptor implements MessageBodyReaderInterceptor {

    @Override
    public Object read(MessageBodyReaderContext context) throws IOException, WebApplicationException {
        Object object = context.proceed();
        if (context.getAttribute(EMPTY_PAYLOAD) != TRUE) {
            return object;
        }
        if (object instanceof Collection) {
            Collection collection = (Collection) object;
            if (collection.isEmpty()) {
                throw new EmptyPayloadException();
            }
        } else if (object instanceof Map) {
            Map map = (Map) object;
            if (map.isEmpty()) {
                throw new EmptyPayloadException();
            }
        } else if (object == null) {
            throw new EmptyPayloadException();
        }
        return object;
    }
}
