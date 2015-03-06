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

import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricAlreadyExistsException;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.FutureCallback;

public class MetricCreatedCallback<?> implements FutureCallback<Void> {

    AsyncResponse response;
    Metric<?> metric;

    public MetricCreatedCallback(AsyncResponse response, Metric<?> metric) {
        this.response = response;
        this.metric = metric;
    }

    @Override
    public void onSuccess(Void result) {
        response.resume(Response.ok().type(APPLICATION_JSON_TYPE).build());
    }

    @Override
    public void onFailure(Throwable t) {
        if (t instanceof MetricAlreadyExistsException) {
            Error errors = new Error("A metric with name [" + metric.getId() + "] already exists");
            response.resume(Response.status(Status.BAD_REQUEST).entity(errors).type(APPLICATION_JSON_TYPE).build());
        } else {
            Error errors = new Error("Failed to create metric due to an " +
                "unexpected error: " + Throwables.getRootCause(t).getMessage());
            response.resume(Response.status(Status.INTERNAL_SERVER_ERROR).entity(errors)
                .type(APPLICATION_JSON_TYPE).build());
        }
    }
}