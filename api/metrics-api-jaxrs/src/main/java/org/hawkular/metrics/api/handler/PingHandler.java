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
package org.hawkular.metrics.api.handler;

import java.util.Date;
import java.util.Map;

import org.hawkular.handlers.RestEndpoint;
import org.hawkular.handlers.RestHandler;
import org.hawkular.metrics.api.jaxrs.util.StringValue;
import org.hawkular.metrics.api.util.JsonUtil;
import org.hawkular.metrics.api.util.Wrappers;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.swagger.annotations.ApiOperation;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import rx.Observable;
import rx.schedulers.Schedulers;

/**
 * @author Stefan Negrea
 */
@RestEndpoint(path = "/ping")
public class PingHandler implements RestHandler {

    @Override
    public void initRoutes(String baseUrl, Router router) {
        Wrappers.setupTenantRoute(router, HttpMethod.GET, baseUrl + "/ping", this::ping);
    }

    @ApiOperation(value = "Returns the current time and serves to check for the availability of the api.", response =
            Map.class)
    public void ping(RoutingContext ctx) {
        ctx.response().setChunked(true);

        Observable.just(JsonUtil.toJson(new StringValue(new Date().toString())))
                .doOnNext(ctx.response()::write)
                .doOnError(error -> {
                    ctx.response().setStatusCode(HttpResponseStatus.BAD_REQUEST.code()).end(error.getMessage());
                })
                .doOnCompleted(ctx.response()::end)
                .subscribeOn(Schedulers.io())
                .subscribe();
    }
}
