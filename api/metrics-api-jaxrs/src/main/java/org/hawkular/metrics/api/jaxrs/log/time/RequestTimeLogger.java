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

package org.hawkular.metrics.api.jaxrs.log.time;

import org.hawkular.metrics.api.jaxrs.log.RestLogger;
import org.hawkular.metrics.api.jaxrs.log.RestLogging;

import io.undertow.server.ExchangeCompletionListener;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;

public class RequestTimeLogger implements HttpHandler {

    private static final RestLogger log = RestLogging.getRestLogger(RequestTimeLogger.class);

    private HttpHandler next;

    private long timeThreshold;

    public RequestTimeLogger(HttpHandler next, long timeThreshold) {
        this.next = next;
        this.timeThreshold = timeThreshold;
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {

        TimeMeasurer timeMeasurer = new TimeMeasurer(this.timeThreshold);
        timeMeasurer.setStartTime(System.currentTimeMillis());
        exchange.addExchangeCompleteListener(timeMeasurer);
        next.handleRequest(exchange);
    }

    private class TimeMeasurer implements ExchangeCompletionListener {

        private long start;

        private long timeThreshold;

        TimeMeasurer(long timeThreshold){
            this.timeThreshold = timeThreshold;
        }

        @Override
        public void exchangeEvent(HttpServerExchange exchange, NextListener nextListener) {
            try {
                long end = System.currentTimeMillis();
                long duration = end - start;
                if (duration > this.timeThreshold) {
                    String method = exchange.getRequestMethod().toString();
                    String uri = exchange.getRequestURI();
                    String query = exchange.getQueryString();
                    log.warn("Request " + method + " " + uri + (query.isEmpty() ? "":("?" + query)) + " took: "+
                            duration + " ms, exceeds " +  this.timeThreshold +" ms threshold");
                }
            } finally {
                if (nextListener != null) {
                    nextListener.proceed();
                }
            }
        }

        void setStartTime(long start) {
            this.start = start;
        }
    }


}