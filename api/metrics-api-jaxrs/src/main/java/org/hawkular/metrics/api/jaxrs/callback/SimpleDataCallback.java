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

public class SimpleDataCallback<T> extends NoDataCallback<T> {

    public SimpleDataCallback(AsyncResponse asyncResponse) {
        super(asyncResponse);
    }

    @Override
    public void onSuccess(Object responseData) {
        if (responseData == null) {
            asyncResponse.resume(Response.noContent().build());
        } else {
            asyncResponse.resume(Response.ok(responseData).build());
        }
    }
}
