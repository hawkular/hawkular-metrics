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
package org.hawkular.metrics.api.jaxrs.exception.mappers;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

/**
 * Exception mapper for any exception thrown by RESTEasy when HTTP Not Found (404) is encountered.
 * Also checks the case chain, if NumberFormatException is present building response with HTTP Bad Request (400).
 * <p>
 * This mapper let us reply to the user with a pre-determined message format if, for example, receive
 * a request for unavailable resource.
 *
 * @author Jeeva Kandasamy
 */
@Provider
public class NotFoundExceptionMapper implements ExceptionMapper<NotFoundException> {

    @Override
    public Response toResponse(NotFoundException exception) {
        if (exception.getCause() instanceof NumberFormatException) {
            return ExceptionMapperUtils.buildResponse(exception, Response.Status.BAD_REQUEST);
        } else {
            return ExceptionMapperUtils.buildResponse(exception, Response.Status.NOT_FOUND);
        }
    }

}
