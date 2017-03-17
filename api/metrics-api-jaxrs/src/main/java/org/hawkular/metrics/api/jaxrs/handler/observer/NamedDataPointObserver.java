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
package org.hawkular.metrics.api.jaxrs.handler.observer;

import java.io.IOException;

import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.hawkular.jaxrs.filter.cors.Headers;
import org.hawkular.metrics.model.ApiError;
import org.hawkular.metrics.model.AvailabilityType;
import org.hawkular.metrics.model.MetricType;
import org.hawkular.metrics.model.NamedDataPoint;
import org.jboss.logging.Logger;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;

import rx.Subscriber;

/**
 * @author jsanda
 */
public class NamedDataPointObserver<T> extends Subscriber<NamedDataPoint<T>> {
    private static final Logger log = Logger.getLogger(NamedDataPointObserver.class);

    @FunctionalInterface
    private interface WriteValue<T> {
        void call(NamedDataPoint<T> dataPoint) throws IOException;
    }

    private final HttpServletRequest request;
    private final HttpServletResponse response;
    private final ObjectMapper mapper;
    private final JsonGenerator generator;
    private final WriteValue<T> writeValue;

    private volatile String currentMetric;


    public NamedDataPointObserver(AsyncContext asyncContext, ObjectMapper mapper, MetricType<T> type,
            String extraAccesControlAllowHeaders) {
        request = (HttpServletRequest) asyncContext.getRequest();
        response = (HttpServletResponse) asyncContext.getResponse();
        this.mapper = mapper;
        try {
            this.generator = mapper.getFactory().createGenerator(response.getOutputStream(), JsonEncoding.UTF8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (type == MetricType.GAUGE || type == MetricType.GAUGE_RATE || type == MetricType.COUNTER_RATE) {
            writeValue = dataPoint -> generator.writeNumberField("value", (Double) dataPoint.getValue());
        } else if (type == MetricType.COUNTER) {
            writeValue = dataPoint -> generator.writeNumberField("value", (Long) dataPoint.getValue());
        } else if (type == MetricType.AVAILABILITY) {
            writeValue = dataPoint -> {
                AvailabilityType availability = (AvailabilityType) dataPoint.getValue();
                generator.writeStringField("value", availability.getText());
            };
        } else if (type == MetricType.STRING) {
            writeValue = dataPoint -> generator.writeStringField("value", (String) dataPoint.getValue());
        } else {
            throw new IllegalArgumentException(type + " is not supported metric type. This class should be " +
                    "updated to add support for it!");
        }
    }

    private void setCorsHeaders(String extraAccesControlAllowHeaders) {
        String requestOrigin = request.getHeader(Headers.ORIGIN);
        if (requestOrigin == null) {
            return;
        }
        response.setHeader(Headers.ACCESS_CONTROL_ALLOW_ORIGIN, requestOrigin);
        response.setHeader(Headers.ACCESS_CONTROL_ALLOW_CREDENTIALS, "true");
        response.setHeader(Headers.ACCESS_CONTROL_ALLOW_METHODS, Headers.DEFAULT_CORS_ACCESS_CONTROL_ALLOW_METHODS);
        response.setIntHeader(Headers.ACCESS_CONTROL_MAX_AGE, 72 * 60 * 60);

        if (extraAccesControlAllowHeaders != null) {
            response.setHeader(Headers.ACCESS_CONTROL_ALLOW_HEADERS,
                    Headers.DEFAULT_CORS_ACCESS_CONTROL_ALLOW_HEADERS + ","
                            + extraAccesControlAllowHeaders.trim());
        } else {
            response.setHeader(Headers.ACCESS_CONTROL_ALLOW_HEADERS, Headers.DEFAULT_CORS_ACCESS_CONTROL_ALLOW_HEADERS);
        }
    }

    @Override
    public void onNext(NamedDataPoint<T> dataPoint) {
        try {
            if (currentMetric == null) {
                response.setStatus(HttpServletResponse.SC_OK);
                response.setHeader("Content-Type", "application/json");

                generator.writeStartArray();
                generator.writeStartObject();
                generator.writeStringField("id", dataPoint.getName());
                generator.writeArrayFieldStart("data");

                generator.writeStartObject();
                generator.writeNumberField("timestamp", dataPoint.getTimestamp());
                writeValue.call(dataPoint);
                generator.writeEndObject();
            } else if (currentMetric.equals(dataPoint.getName())) {
                generator.writeStartObject();
                generator.writeNumberField("timestamp", dataPoint.getTimestamp());
                writeValue.call(dataPoint);
                generator.writeEndObject();
            } else {
                generator.writeEndArray();
                generator.writeEndObject();
                generator.writeStartObject();
                generator.writeStringField("id", dataPoint.getName());
                generator.writeArrayFieldStart("data");

                generator.writeStartObject();
                generator.writeNumberField("timestamp", dataPoint.getTimestamp());
                writeValue.call(dataPoint);
                generator.writeEndObject();
            }
            currentMetric = dataPoint.getName();
        } catch (IOException e) {
            throw new RuntimeException("Streaming data to client failed", e);
        }
    }

    @Override
    public void onError(Throwable e) {
        log.trace("Fetching data failed", e);
        try {
            if (currentMetric == null) {
                response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                response.setHeader("Content-Type", "application/json");
                ApiError apiError = new ApiError(Throwables.getRootCause(e).getMessage());
                mapper.writeValue(response.getOutputStream(), apiError);
            } else {
                generator.close();
            }
        } catch (IOException ignored) {
        } finally {
            request.getAsyncContext().complete();
        }
    }

    @Override
    public void onCompleted() {
        try {
            if (currentMetric == null) {
                response.setStatus(HttpServletResponse.SC_NO_CONTENT);
            } else {
                generator.writeEndArray();
                generator.writeEndObject();
                generator.writeEndArray();
            }
            generator.close();
        } catch (IOException e) {
            log.trace("Error while finishing streaming data", e);
        } finally {
            request.getAsyncContext().complete();
        }
    }
}
