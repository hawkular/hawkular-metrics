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

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

import java.util.Date;
import java.util.Map;

import org.hawkular.handlers.RestEndpoint;
import org.hawkular.handlers.RestHandler;
import org.hawkular.metrics.api.filter.TenantFilter;
import org.hawkular.metrics.api.jaxrs.util.StringValue;
import org.hawkular.metrics.api.util.JsonUtil;

import io.swagger.annotations.ApiOperation;
import io.vertx.core.Handler;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.rx.java.ObservableHandler;
import io.vertx.rx.java.RxHelper;
import rx.Observable;

/**
 * @author Stefan Negrea
 */
@RestEndpoint(path = "/ping")
public class PingHandler implements RestHandler {

    @Override
    public void initRoutes(String baseUrl, Router router) {
        router.get(baseUrl + "/ping").handler(TenantFilter.filter());
        router.get(baseUrl + "/ping").handler(ping());
    }

    @ApiOperation(value = "Returns the current time and serves to check for the availability of the api.", response =
            Map.class)
    public Handler<RoutingContext> ping() {
        ObservableHandler<RoutingContext> oh = RxHelper.observableHandler(true);
        oh.subscribe(ctx -> {
            ctx.response().setChunked(true);

            Observable.just(JsonUtil.toJson(new StringValue(new Date().toString()))).subscribe(
                    ctx.response()::write,
                    error -> {
                        ctx.response().setStatusCode(BAD_REQUEST.code()).end(error.getMessage());
                    },
                    ctx.response()::end);
        });
        return oh.toHandler();
    }
}
