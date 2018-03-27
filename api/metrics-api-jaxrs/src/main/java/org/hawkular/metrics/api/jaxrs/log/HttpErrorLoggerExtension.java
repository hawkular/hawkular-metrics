/*
 * Copyright 2014-2018 Red Hat, Inc. and/or its affiliates
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

package org.hawkular.metrics.api.jaxrs.log;

import javax.servlet.ServletContext;

import io.undertow.server.ExchangeCompletionListener;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.servlet.ServletExtension;
import io.undertow.servlet.api.DeploymentInfo;
import io.undertow.util.HttpString;
import io.undertow.util.StatusCodes;


public class HttpErrorLoggerExtension implements ServletExtension {

    private static final RestLogger log = RestLogging.getRestLogger(HttpErrorLoggerExtension.class);
    private HttpErrorHandler httpErrorHandler;

    @Override
    public void handleDeployment(DeploymentInfo deploymentInfo, ServletContext servletContext) {
        deploymentInfo.addInitialHandlerChainWrapper(containerHandler -> {
            httpErrorHandler = new HttpErrorHandler(containerHandler);
            return httpErrorHandler;
        });
    }

    class HttpErrorExchangeCompleteListener implements ExchangeCompletionListener {
        @Override
        public void exchangeEvent(HttpServerExchange exchange, ExchangeCompletionListener.NextListener nextListener) {
            int httpStatusCode = exchange.getStatusCode();
            if (httpStatusCode >= StatusCodes.BAD_REQUEST) {
                final String path;
                final String query = exchange.getQueryString();
                if (!query.isEmpty()) {
                    path = exchange.getRequestPath() + "?" + query;
                } else {
                    path = exchange.getRequestPath();
                }
                HttpString method = exchange.getRequestMethod();
                log.warnf("Endpoint %s %s fails with HTTP code: %d", method, path, httpStatusCode);
            }
            nextListener.proceed();
        }
    }

    class HttpErrorHandler implements HttpHandler {

        private HttpHandler next;

        public HttpErrorHandler(HttpHandler next) {
            this.next = next;
        }

        @Override
        public void handleRequest(HttpServerExchange exchange) throws Exception {
            exchange.addExchangeCompleteListener(new HttpErrorExchangeCompleteListener());
            next.handleRequest(exchange);
        }
    }
}
