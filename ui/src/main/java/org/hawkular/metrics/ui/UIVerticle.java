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
package org.hawkular.metrics.ui;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.StaticHandler;

public class UIVerticle extends AbstractVerticle {

    private UIVerticle() {
    }

    public static void main(String[] args) throws InterruptedException {
        Vertx.vertx().deployVerticle(new UIVerticle());
    }

    @Override
    public void start() throws Exception {
        Router router = Router.router(vertx);
        router.route().handler(StaticHandler.create());
        // For the time being, serve it on 8081. That will have to change once vertx is truly in the place
        vertx.createHttpServer().requestHandler(router::accept).listen(8081);
    }
}
