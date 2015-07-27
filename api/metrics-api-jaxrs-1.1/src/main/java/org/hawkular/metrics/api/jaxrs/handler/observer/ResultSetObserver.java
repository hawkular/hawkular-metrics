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

import javax.ws.rs.core.Response;

import org.hawkular.metrics.api.jaxrs.util.ApiUtils;

import rx.Observer;

/**
 * Observer that returns empty 200 if everything went alright and ApiError if there was an exception.
 *
 * @author miburman
 */
public class ResultSetObserver implements Observer<Void> {

    private Response response;

    public ResultSetObserver() {
    }

    @Override
    public void onCompleted() {
        response = Response.ok().build();
    }

    @Override
    public void onError(Throwable t) {
        response = ApiUtils.serverError(t);
    }

    @Override
    public void onNext(Void aVoid) {
        // We're only interested in onCompleted status
    }

    public Response getResponse() {
        return response;
    }
}
