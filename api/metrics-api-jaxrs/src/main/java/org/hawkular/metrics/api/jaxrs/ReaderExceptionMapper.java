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
package org.hawkular.metrics.api.jaxrs;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.jboss.resteasy.spi.ReaderException;

import com.google.common.base.Throwables;

/**
 * Exception mapper for any exception thrown by a body reader chain.
 * <p>
 * This mapper let us reply to the user with a pre-determined message format if, for example, a JSON entity cannot be
 * parsed.
 *
 * @author Thomas Segismont
 */
@Provider
public class ReaderExceptionMapper implements ExceptionMapper<ReaderException> {

    @Override
    public Response toResponse(ReaderException exception) {
        return Response.status(Response.Status.BAD_REQUEST)
                       .entity(new ApiError(Throwables.getRootCause(exception).getMessage()))
                       .type(MediaType.APPLICATION_JSON)
                       .build();
    }
}