/*
 * Copyright 2017 Red Hat, Inc. and/or its affiliates
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.hawkular.metrics.api.jaxrs.util.ApiUtils;
import org.hawkular.metrics.model.Metric;
import org.jboss.logging.Logger;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;

import rx.Subscriber;

/**
 * @author jsanda
 */
// TODO Create a base class for MetricObserver and NamedDataPointObserver
public class MetricObserver<T> extends Subscriber<Metric<T>> {

    private static Logger logger = Logger.getLogger(MetricObserver.class);

    @FunctionalInterface
    private interface WriteValue<T> {
        void call(Metric<T> dataPoint) throws IOException;
    }

    private final AsyncResponse response;
    private final JsonGenerator generator;
    private AtomicInteger count;
    private final ByteArrayOutputStream jsonOutputStream;

    private volatile Metric<T> current;

    public MetricObserver(AsyncResponse response, ObjectMapper mapper) {
        this.response = response;
        count = new AtomicInteger();
        jsonOutputStream = new ByteArrayOutputStream();
        try {
            generator = mapper.getFactory().createGenerator(jsonOutputStream, JsonEncoding.UTF8);
            generator.writeStartArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onNext(Metric<T> metric) {
        try {
            count.incrementAndGet();
            generator.writeStartObject();
            generator.writeStringField("id", metric.getMetricId().getName());
            generator.writeStringField("tenantId", metric.getMetricId().getTenantId());
            generator.writeStringField("type", metric.getMetricId().getType().toString());
            if (!metric.getTags().isEmpty()) {
                generator.writeObjectFieldStart("tags");
                for (Map.Entry<String, String> tag : metric.getTags().entrySet()) {
                    generator.writeStringField(tag.getKey(), tag.getValue());
                }
                generator.writeEndObject();
            }
            if (metric.getDataRetention() != null) {
                generator.writeNumberField("dataRetention", metric.getDataRetention());
            }
            if (metric.getMinTimestamp() != null) {
                generator.writeNumberField("minTimestamp", metric.getMinTimestamp());
            }
            if (metric.getMaxTimestamp() != null) {
                generator.writeNumberField("maxTimestamp", metric.getMaxTimestamp());
            }
            generator.writeEndObject();
        } catch (IOException e) {
            throw new RuntimeException("Streaming data to client failed", e);
        }
    }

    @Override
    public void onError(Throwable e) {
        logger.warn("Fetching data failed", e);
        try {
            if (count.get() == 0) {
                response.resume(ApiUtils.serverError(e));
            } else {
                generator.close();
            }
        } catch (IOException ie) {
            logger.warn("There was an error closing the JSON generator", ie);
        }
    }

    @Override
    public void onCompleted() {
        Response result = null;
        try {
            if (count.get() == 0) {
                generator.close();
                result = Response.ok().status(HttpServletResponse.SC_NO_CONTENT).build();
            } else {
                generator.writeEndArray();
                generator.close();
                result = Response.ok(new String(jsonOutputStream.toByteArray()), MediaType.APPLICATION_JSON_TYPE)
                        .build();
            }
        } catch (IOException e) {
            logger.warn("There was an error while finishing streaming data", e);
            result = ApiUtils.serverError(e);
        } finally {
            if (result == null) {
                result = Response.serverError().build();
            }
            response.resume(result);
        }
    }
}
