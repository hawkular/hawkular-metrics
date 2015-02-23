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
package org.hawkular.metrics.api.undertow;


import static io.undertow.Handlers.path;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.server.RoutingHandler;
import io.undertow.util.Methods;

/**
 * Undertow app initialization
 *
 * @author Stefan Negrea
 */

public class MainServer {


    public static void main(final String[] args) {

        RoutingHandler commonHandler = Handlers.routing()
                .add(Methods.GET, "/ping", new PingHandler())
                .add(Methods.POST, "/ping", new PingHandler());

        MetricsHandlers metricsHandlers = new MetricsHandlers();
        metricsHandlers.setup(commonHandler);

        Undertow server = Undertow.builder().addHttpListener(8080, "0.0.0.0")
                .setHandler(new CorsHandler(path().addPrefixPath("/hawkular-metrics", commonHandler))).build();
        server.start();
    }


}

