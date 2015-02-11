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
package org.hawkular.metrics.restServlet.jsonp;

import static java.nio.charset.StandardCharsets.UTF_8;
import static javax.ws.rs.RuntimeType.SERVER;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static org.hawkular.metrics.restServlet.CustomMediaTypes.APPLICATION_JAVASCRIPT;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

import javax.annotation.PostConstruct;
import javax.servlet.ServletContext;
import javax.ws.rs.ConstrainedTo;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;
import javax.ws.rs.ext.Providers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts {@link MediaType#APPLICATION_JSON_TYPE} compatible types to JSONP. Restricted to media type
 * {@link org.hawkular.metrics.restServlet.CustomMediaTypes#APPLICATION_JAVASCRIPT}.
 * <p>
 * JSONP callback parameter name may be defined with the
 * {@code org.hawkular.metrics.restServlet.jsonp.JsonPProvider} servlet context param. It defaults to
 * {@code jsonp}.
 * </p>
 * <p>
 * If the client does not provide a callback parameter, the provider will throw a {@link WebApplicationException} with
 * status code {@link javax.ws.rs.core.Response.Status#BAD_REQUEST}.
 * </p>
 *
 * @author Thomas Segismont
 */
@Provider
@Produces(APPLICATION_JAVASCRIPT)
@ConstrainedTo(SERVER)
public class JsonPProvider implements MessageBodyWriter<Object> {
    private static final Logger LOG = LoggerFactory.getLogger(JsonPProvider.class);

    private static final String CALLBACK_PARAM_CONFIG = "org.hawkular.metrics.restServlet.jsonp.JsonPProvider";
    private static final String CALLBACK_PARAM_DEFAULT = "jsonp";

    @Context
    private ServletContext servletContext;
    @Context
    private UriInfo uriInfo;
    @Context
    private Providers providers;
    private String callbackParam;

    @PostConstruct
    void init() {
        callbackParam = servletContext.getInitParameter(CALLBACK_PARAM_CONFIG);
        if (callbackParam == null) {
            callbackParam = CALLBACK_PARAM_DEFAULT;
        }
        LOG.debug("Using callbackParam '{}'", callbackParam);
    }

    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        MessageBodyWriter<?> jsonWriter = providers.getMessageBodyWriter(type, genericType, annotations,
            APPLICATION_JSON_TYPE);
        return jsonWriter.isWriteable(type, genericType, annotations, APPLICATION_JSON_TYPE);
    }

    @Override
    public long getSize(Object o, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return -1;
    }

    @Override
    public void writeTo(Object o, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType,
        MultivaluedMap<String, Object> httpHeaders, OutputStream entityStream) throws IOException,
        WebApplicationException {

        MessageBodyWriter jsonWriter = providers.getMessageBodyWriter(type, genericType, annotations,
            APPLICATION_JSON_TYPE);

        String callback = uriInfo.getQueryParameters().getFirst(callbackParam);

        if (callback == null || callback.trim().isEmpty()) {
            throw new WebApplicationException("Missing JSONP callback", BAD_REQUEST);
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        jsonWriter.writeTo(o, type, genericType, annotations, mediaType, httpHeaders, baos);

        entityStream.write((callback + "(").getBytes(UTF_8));
        baos.writeTo(entityStream);
        entityStream.write(");".getBytes(UTF_8));
    }
}
