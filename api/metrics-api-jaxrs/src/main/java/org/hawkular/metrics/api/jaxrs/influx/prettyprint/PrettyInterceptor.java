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

import static java.lang.Boolean.TRUE;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;

import static org.hawkular.metrics.api.jaxrs.influx.prettyprint.PrettyFilter.PRETTY_PRINT;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.ext.Provider;
import javax.ws.rs.ext.WriterInterceptor;
import javax.ws.rs.ext.WriterInterceptorContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * Check if {@link PrettyFilter#PRETTY_PRINT} context property is set. In this case, the JSON output is intercepted and
 * formatted.
 *
 * @author Thomas Segismont
 * @see PrettyFilter
 */
@Provider
public class PrettyInterceptor implements WriterInterceptor {
    private final ObjectMapper mapper;

    public PrettyInterceptor() {
        mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
    }

    @Override
    public void aroundWriteTo(WriterInterceptorContext context) throws IOException, WebApplicationException {
        if (context.getProperty(PRETTY_PRINT) != TRUE || !APPLICATION_JSON_TYPE.equals(context.getMediaType())) {
            context.proceed();
            return;
        }

        // Any content length set will be obsolete
        context.getHeaders().remove(HttpHeaders.CONTENT_LENGTH);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OutputStream old = context.getOutputStream();
        try {

            context.setOutputStream(baos);
            context.proceed();

            JsonNode jsonNode = mapper.readValue(baos.toByteArray(), JsonNode.class);
            mapper.writeValue(old, jsonNode);

        } finally {
            context.setOutputStream(old);
        }
    }
}
