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
package org.hawkular.metrics.api.jaxrs.callback;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.FutureCallback;

public class SimpleDataCallback<T> implements FutureCallback<T> {

    private AsyncResponse response;

    public SimpleDataCallback(AsyncResponse response) {
        this.response = response;
    }

    @Override
    public void onSuccess(Object responseData) {
        if (responseData == null) {
            response.resume(Response.status(Status.NO_CONTENT).type(APPLICATION_JSON_TYPE).build());
        } else {
            response.resume(Response.ok(responseData).type(APPLICATION_JSON_TYPE).build());
        }
    }

    @Override
    public void onFailure(Throwable t) {
        Error errors = new Error("Failed to retrieve tags due to " +
            "an unexpected error: " + Throwables.getRootCause(t).getMessage());
        response.resume(Response.status(Status.INTERNAL_SERVER_ERROR).entity(errors).type(APPLICATION_JSON_TYPE)
            .build());
    }
}