/*
 * Copyright 2014-2016 Red Hat, Inc. and/or its affiliates
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
import java.util.concurrent.CountDownLatch;

import org.hawkular.metrics.model.AvailabilityType;
import org.hawkular.metrics.model.MetricType;
import org.hawkular.metrics.model.NamedDataPoint;
import org.jboss.logging.Logger;

import com.fasterxml.jackson.core.JsonGenerator;

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

    private final JsonGenerator generator;
    private final CountDownLatch latch;
    private final WriteValue<T> writeValue;

    private volatile String currentMetric;


    public NamedDataPointObserver(JsonGenerator generator, CountDownLatch latch, MetricType<T> type) {
        this.generator = generator;
        this.latch = latch;
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

    @Override
    public void onNext(NamedDataPoint<T> dataPoint) {
        log.trace("Next data point is " + dataPoint);
        try {
            if (currentMetric == null) {
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
        try {
            log.warn("Fetching data failed", e);
            generator.close();
        } catch (IOException e1) {
            log.error("Failed to close " + generator.getClass().getName());
        } finally {
            latch.countDown();
        }
    }

    @Override
    public void onCompleted() {
        try {
            if (currentMetric == null) {
                generator.writeStartArray();
                generator.writeEndArray();
            } else {
                generator.writeEndArray();
                generator.writeEndObject();
                generator.writeEndArray();
            }
            generator.close();
            latch.countDown();
        } catch (IOException e) {
            log.warn("Error while finishing streaming data", e);
        } finally {
            latch.countDown();
        }
    }
}
