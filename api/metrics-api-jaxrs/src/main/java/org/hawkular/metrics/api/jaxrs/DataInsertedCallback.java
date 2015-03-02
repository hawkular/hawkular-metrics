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

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.FutureCallback;

/**
 * @author John Sanda
 */
public class DataInsertedCallback implements FutureCallback<Void> {

    private AsyncResponse response;

    private String errorMsg;

    public DataInsertedCallback(AsyncResponse response, String errorMsg) {
        this.response = response;
        this.errorMsg = errorMsg;
    }

    @Override
    public void onSuccess(Void result) {
        response.resume(Response.ok().type(MediaType.APPLICATION_JSON_TYPE).build());
    }

    @Override
    public void onFailure(Throwable t) {
        Error errors = new Error(errorMsg + ": " + Throwables.getRootCause(t).getMessage());
        response.resume(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(errors).type(
            MediaType.APPLICATION_JSON_TYPE).build());
    }
}
