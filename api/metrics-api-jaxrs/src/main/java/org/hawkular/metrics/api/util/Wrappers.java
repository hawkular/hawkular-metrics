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
package org.hawkular.metrics.api.util;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

import org.hawkular.metrics.api.filter.TenantFilter;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.rx.java.ObservableHandler;
import io.vertx.rx.java.RxHelper;
import rx.Subscriber;
import rx.functions.Action1;
import rx.observers.Subscribers;

public class Wrappers {

    public static final String ACCEPT = "Accept";
    public static final String CONTENT_TYPE = "Content-Type";
    public static final String APPLICATION_JSON = "application/json";

    public static Handler<RoutingContext> wrap(Action1<RoutingContext> action) {
        ObservableHandler<RoutingContext> oh = RxHelper.observableHandler(true);
        oh.subscribe(ctx -> {
            try {
                action.call(ctx);
            } catch (Exception e) {
                ctx.fail(e);
            }
        });
        return oh.toHandler();
    }

    public static void setFailureHandlre(Route route) {
        route.failureHandler(ctx -> {
            System.out.println(ctx.failure());
            ctx.failure().printStackTrace();
            ctx.response().putHeader(ACCEPT, APPLICATION_JSON).putHeader(CONTENT_TYPE, APPLICATION_JSON)
                    .setStatusCode(BAD_REQUEST.code())
                    .end(JsonUtil.toJson(ctx.failure().getMessage()));
        });
    }

    public static void setupTenantRoute(Router router, HttpMethod method, String path,
            Action1<RoutingContext> action) {

        router.route(method, path).handler(Wrappers.wrap(TenantFilter::filter));
        router.route(method, path).handler(Wrappers.wrap(action));
        Wrappers.setFailureHandlre(router.route(method, path));
    }

    public static Subscriber<String> createSubscriber(RoutingContext ctx) {
        return Subscribers.<String> create(ctx.response()::write,
                error -> {
                    ctx.response().setStatusCode(HttpResponseStatus.BAD_REQUEST.code()).end(error.getMessage());
                },
                ctx.response()::end);
    }
}
