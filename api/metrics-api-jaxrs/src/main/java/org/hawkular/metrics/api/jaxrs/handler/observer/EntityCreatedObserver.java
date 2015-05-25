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

package org.hawkular.metrics.api.jaxrs.handler.observer;

import static org.hawkular.metrics.api.jaxrs.util.ApiUtils.serverError;

import java.net.URI;
import java.util.function.Function;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;

import rx.Observer;

/**
 * Base observer class used to build a JAX-RS response when creating an entity (metric, tenant, ...).
 * <p>
 * On success, a <em>201 CREATED</em> response is built with the <em>location</em> header to indicate the URI of the
 * new
 * resource.
 * <p>
 * On failure, a <em>409 CONFLICT</em> response is built if the exception indicates the resource already exists,
 * otherwise a <em>500 SERVER ERROR</em> reponse is built.
 *
 * @author Thomas Segismont
 */
public abstract class EntityCreatedObserver<E> implements Observer<Void> {
    private final AsyncResponse asyncResponse;
    private final URI location;
    private final Class<E> alreadyExistsException;
    private final Function<E, Response> alreadyExistsResponseBuilder;

    /**
     * @param asyncResponse                JAX-RS asynchronous response reference
     * @param location                     URI of the new resource if it is successfully created
     * @param alreadyExistsExceptionType   type of the exception indicating the resource already exists
     * @param alreadyExistsResponseBuilder a function to build a resource already exists response
     */
    public EntityCreatedObserver(
            AsyncResponse asyncResponse,
            URI location,
            Class<E> alreadyExistsExceptionType,
            Function<E, Response> alreadyExistsResponseBuilder
    ) {
        this.asyncResponse = asyncResponse;
        this.location = location;
        this.alreadyExistsException = alreadyExistsExceptionType;
        this.alreadyExistsResponseBuilder = alreadyExistsResponseBuilder;
    }

    @Override
    public void onNext(Void aVoid) {
    }

    @Override
    public void onError(Throwable t) {
        Response response;
        if (alreadyExistsException.isAssignableFrom(t.getClass())) {
            response = alreadyExistsResponseBuilder.apply(alreadyExistsException.cast(t));
        } else {
            response = serverError(t, "Failed to create tenant due to an unexpected error");
        }
        asyncResponse.resume(response);
    }

    @Override
    public void onCompleted() {
        asyncResponse.resume(Response.created(location).build());
    }
}
