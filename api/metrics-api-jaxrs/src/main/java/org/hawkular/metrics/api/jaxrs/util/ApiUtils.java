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
package org.hawkular.metrics.api.jaxrs.util;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import org.hawkular.metrics.api.jaxrs.ApiError;

/**
 * @author jsanda
 */
public class ApiUtils {

    public static Response collectionToResponse(Collection<?> collection) {
        return collection.isEmpty() ? noContent() : Response.ok(collection).build();
    }

    public static Response mapToResponse(Map<?, ?> collection) {
        return collection.isEmpty() ? noContent() : Response.ok(collection).build();
    }

    public static Response serverError(Throwable t, String message) {
        String errorMsg = message + ": " + Throwables.getRootCause(t).getMessage();
        return Response.serverError().entity(new ApiError(errorMsg)).build();
    }

    public static Response serverError(Throwable t) {
        return serverError(t, "Failed to perform operation due to an error");
    }

    public static Response valueToResponse(Optional<?> optional) {
        return optional.map(value -> Response.ok(value).build()).orElse(noContent());
    }

    @Deprecated
    public static final Function<List<Void>, Response> MAP_LIST_VOID = v -> Response.ok().build();

    @Deprecated
    public static final Function<Collection<?>, Response> MAP_COLLECTION = collection ->
            collection.isEmpty() ? noContent() : Response.ok(collection).build();

    public static Response noContent() {
        return Response.noContent().build();
    }

    public static Response emptyPayload() {
        return badRequest(new ApiError("Payload is empty"));
    }

    public static Response badRequest(ApiError error) {
        return Response.status(Response.Status.BAD_REQUEST).entity(error).build();
    }

    /**
     * @deprecated rx-migration
     */
    @Deprecated
    public static void executeAsync(AsyncResponse asyncResponse,
            java.util.function.Supplier<ListenableFuture<Response>> supplier) {
        ListenableFuture<Response> future = supplier.get();
        Futures.addCallback(future, new FutureCallback<Response>() {
            @Override
            public void onSuccess(Response response) {
                asyncResponse.resume(response);
            }

            @Override
            public void onFailure(Throwable t) {
                String msg = "Failed to perform operation due to an error: " + Throwables.getRootCause(t).getMessage();
                asyncResponse.resume(Response.serverError().entity(new ApiError(msg)).build());
            }
        });
    }

    private ApiUtils() {
        // Utility class
    }
}
