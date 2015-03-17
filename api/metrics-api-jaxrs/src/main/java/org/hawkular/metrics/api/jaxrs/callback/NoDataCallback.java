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

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;

import org.hawkular.metrics.api.jaxrs.ApiError;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.FutureCallback;

/**
 * @author John Sanda
 */
public class NoDataCallback<T> implements FutureCallback<T> {

    protected AsyncResponse asyncResponse;

    public NoDataCallback(AsyncResponse asyncResponse) {
        this.asyncResponse = asyncResponse;
    }

    @Override
    public void onSuccess(Object result) {
        asyncResponse.resume(Response.ok().build());
    }

    @Override
    public void onFailure(Throwable t) {
        String msg = "Failed to perform operation due to an error: " + Throwables.getRootCause(t).getMessage();
        asyncResponse.resume(Response.serverError().entity(new ApiError(msg)).build());
    }
}
