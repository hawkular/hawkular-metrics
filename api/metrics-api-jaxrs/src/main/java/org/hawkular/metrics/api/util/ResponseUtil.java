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
package org.hawkular.metrics.api.util;

import static org.hawkular.metrics.api.util.JsonUtil.toJson;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import org.hawkular.metrics.model.ApiError;

import io.vertx.core.AsyncResult;
import io.vertx.ext.web.RoutingContext;

public class ResponseUtil {

    public static final String ACCEPT = "Accept";
    public static final String CONTENT_TYPE = "Content-Type";
    public static final String APPLICATION_JSON = "application/json";
    public static final String TENANT_HEADER_NAME = "Hawkular-Tenant";

    public static void ok(RoutingContext routing, Object o) {
        routing.response()
                .putHeader(ACCEPT, APPLICATION_JSON)
                .putHeader(CONTENT_TYPE, APPLICATION_JSON)
                .setStatusCode(OK.code())
                .end(toJson(o));
    }

    public static void ok(RoutingContext routing) {
        routing.response()
                .putHeader(ACCEPT, APPLICATION_JSON)
                .putHeader(CONTENT_TYPE, APPLICATION_JSON)
                .setStatusCode(OK.code())
                .end();
    }

    public static void internalServerError(RoutingContext routing, String errorMsg) {
        routing.response()
                .putHeader(ACCEPT, APPLICATION_JSON)
                .putHeader(CONTENT_TYPE, APPLICATION_JSON)
                .setStatusCode(INTERNAL_SERVER_ERROR.code())
                .end(toJson(new ApiError(errorMsg)));
    }

    public static void badRequest(RoutingContext routing, String errorMsg) {
        routing.response()
                .putHeader(ACCEPT, APPLICATION_JSON)
                .putHeader(CONTENT_TYPE, APPLICATION_JSON)
                .setStatusCode(BAD_REQUEST.code())
                .end(toJson(new ApiError(errorMsg)));
    }

    public static void notFound(RoutingContext routing, String errorMsg) {
        routing.response()
                .putHeader(ACCEPT, APPLICATION_JSON)
                .putHeader(CONTENT_TYPE, APPLICATION_JSON)
                .setStatusCode(NOT_FOUND.code())
                .end(toJson(new ApiError(errorMsg)));
    }

    @SuppressWarnings("unchecked")
    public static void result(RoutingContext routing, AsyncResult result) {
        if (result.succeeded()) {
            if (result.result() == null) {
                ok(routing);
                return;
            }
            ok(routing, result.result());
        } else {
            if (result.cause() instanceof BadRequestException) {
                badRequest(routing, result.cause().getMessage());
                return;
            }
            if (result.cause() instanceof NotFoundException) {
                notFound(routing, result.cause().getMessage());
                return;
            }
            internalServerError(routing, result.cause().getMessage());
        }
    }

    public static class BadRequestException extends RuntimeException {

        public BadRequestException(String message) {
            super(message);
        }

        public BadRequestException(Throwable cause) {
            super(cause);
        }

        public BadRequestException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static class InternalServerException extends RuntimeException {

        public InternalServerException(String message) {
            super(message);
        }

        public InternalServerException(Throwable cause) {
            super(cause);
        }

        public InternalServerException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static class NotFoundException extends RuntimeException {

        public NotFoundException(String message) {
            super(message);
        }

        public NotFoundException(Throwable cause) {
            super(cause);
        }

        public NotFoundException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
